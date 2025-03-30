import asyncio
import hashlib
import re
import time
import websockets
import json
import threading
import os
import ssl
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Secure certificate paths
SSL_CERT = os.path.join(BASE_DIR, ".certs", "cert.pem")
SSL_KEY = os.path.join(BASE_DIR, ".certs", "key.pem")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Directory where files are stored
FILE_STORAGE_DIR = "./uploads"
# .partial extension used during upload transaction
PARTIAL_SUFFIX = ".partial"
# Timeout for `.partial` files in seconds (10 minutes)
PARTIAL_FILE_TIMEOUT = 10 * 60

os.makedirs(FILE_STORAGE_DIR, exist_ok=True)

# File ignore settings
IGNORED_PREFIXES = ("~$", ".")
IGNORED_SUFFIXES = (".swp", ".tmp", ".lock", ".part", ".partial", ".crdownload", ".download", ".bak", ".old", ".temp",
                    ".sha256")

# Set for persistent notification connections only
persistent_clients = set()


# _______________________ Stale Partial Files Cleanup ____________________________
def clean_stale_partial_files():
    time.sleep(PARTIAL_FILE_TIMEOUT + 1)
    current_time = time.time()
    try:
        for filename in os.listdir(FILE_STORAGE_DIR):
            if filename.endswith(PARTIAL_SUFFIX):
                file_path = os.path.normpath(os.path.join(FILE_STORAGE_DIR, filename))
                last_modified_time = os.path.getmtime(file_path)
                if current_time - last_modified_time > PARTIAL_FILE_TIMEOUT:
                    logging.info(f"Deleting stale {PARTIAL_SUFFIX} file: {file_path}")
                    os.remove(file_path)
    except (FileNotFoundError, PermissionError) as e:
        logging.error(f"Error during stale file cleanup: {e}")


def start_stale_file_cleanup():
    if any(filename.endswith(PARTIAL_SUFFIX) for filename in os.listdir(FILE_STORAGE_DIR)):
        logging.info("Starting stale file cleanup thread.")
        cleanup_thread = threading.Thread(target=clean_stale_partial_files, daemon=True)
        cleanup_thread.start()
    else:
        logging.info("No .partial files found. Cleanup thread not started.")


# ----------------------- Helper Methods -----------------------

def compute_hash(file_path):
    hasher = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(4096):
                hasher.update(chunk)
    except Exception as e:
        logging.error(f"Error computing hash for {file_path}: {e}")
        return None
    return hasher.hexdigest()


def get_hash_file_path(file_path):
    return file_path + ".sha256"


def get_cached_hash(file_path):
    hash_file = get_hash_file_path(file_path)
    if os.path.exists(hash_file):
        try:
            with open(hash_file, "r") as hf:
                cached = hf.read().strip()
            return cached
        except Exception as e:
            logging.error(f"Error reading cached hash for {file_path}: {e}")
    new_hash = compute_hash(file_path)
    if new_hash:
        try:
            with open(hash_file, "w") as hf:
                hf.write(new_hash)
            logging.info(f"Cached new hash for {file_path}.")
        except Exception as e:
            logging.error(f"Error writing cached hash for {file_path}: {e}")
    return new_hash


def invalidate_cached_hash(file_path):
    hash_file = get_hash_file_path(file_path)
    if os.path.exists(hash_file):
        try:
            os.remove(hash_file)
            logging.info(f"Invalidated cached hash for {file_path}.")
        except Exception as e:
            logging.error(f"Error removing cached hash for {file_path}: {e}")


def sanitize_filename(filename: str) -> str:
    filename = os.path.basename(filename)
    return re.sub(r'[^a-zA-Z0-9_.-]', '_', filename)


# ----------------------- WebSocket Handlers -----------------------

async def websocket_handler(websocket: websockets.ServerConnection) -> None:
    """
    Handles connections. If the first message is a subscription, the connection is added to persistent_clients.
    Otherwise, the connection is treated as a transient file-transfer connection.
    """
    persistent = False
    try:
        async for message in websocket:
            data = json.loads(message)
            # If this is a subscription message, mark as persistent and add to the set.
            if not persistent and data.get("subscribe") is True:
                persistent = True
                persistent_clients.add(websocket)
                await send_sync_metadata(websocket)
                logging.info(f"Added {websocket.remote_address} as persistent notification connection, syncing files.")
                continue  # Do not process further messages on subscription.

            # Process file transfer commands:
            command = data.get("command")
            filename = data.get("filename", "")
            if command == "UPLOAD":
                await receive_file(websocket, filename)
            elif command == "DOWNLOAD":
                await send_file(websocket, filename)
            elif command == "LIST":
                await list_files(websocket)
            elif command == "DELETE":
                await delete_file(websocket, filename)
            elif command == "SYNC":
                await send_sync_metadata(websocket)
            elif data.get("event") is not None:
                await handle_client_notification(data, websocket)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if persistent:
            persistent_clients.discard(websocket)
            logging.info(f"Removed {websocket.remote_address} persistent notification connection.")


async def handle_client_notification(data, websocket):
    event_type = data.get("event")
    filename = data.get("filename")

    if filename and should_ignore(filename):
        logging.info(f"Ignoring file notification for '{filename}' because it matches ignored patterns.")
        return

    client_timestamp = data.get("timestamp")
    client_file_size = int(data.get("size", 0))
    client_hash = data.get("hash")
    file_path = os.path.join(FILE_STORAGE_DIR, filename)

    logging.info(
        f"Received client notification: File '{filename}' {event_type} at {client_timestamp} with size {client_file_size} bytes")

    if event_type in ["created", "modified"]:
        if os.path.exists(file_path) and os.path.isdir(file_path):
            logging.info(f"'{filename}' is a directory. Skipping upload request.")
            return

        await asyncio.sleep(1)

        server_timestamp = int(os.path.getmtime(file_path)) if os.path.exists(file_path) else 0
        server_file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        logging.info(f"Client timestamp: {client_timestamp} ({time.ctime(client_timestamp)}), size: {client_file_size}")
        logging.info(f"Server timestamp: {server_timestamp} ({time.ctime(server_timestamp)}), size: {server_file_size}")

        if client_timestamp > server_timestamp or client_file_size != server_file_size:
            if client_hash:
                server_hash = get_cached_hash(file_path) if os.path.exists(file_path) else None
                if server_hash and server_hash == client_hash:
                    logging.info(f"File '{filename}' hash matches; no upload required.")
                    return
                else:
                    logging.info(f"Hash mismatch (client: {client_hash}, server: {server_hash}).")
            request = json.dumps({"command": "REQUEST_UPLOAD", "filename": filename})
            await websocket.send(request)
            logging.info(f"Requested upload for file '{filename}' from client.")
        else:
            logging.info(f"No upload needed for '{filename}'.")
    elif event_type == "deleted":
        if os.path.exists(file_path):
            if os.path.isdir(file_path):
                logging.info(f"Directory '{filename}' deleted. Removing contents.")
                for root, _, files in os.walk(file_path, topdown=False):
                    for f in files:
                        os.remove(os.path.join(root, f))
                        invalidate_cached_hash(os.path.join(root, f))
                for root, dirs, _ in os.walk(file_path, topdown=False):
                    for d in dirs:
                        try:
                            os.rmdir(os.path.join(root, d))
                        except OSError:
                            logging.warning(f"Could not delete non-empty directory: {d}")
                try:
                    os.rmdir(file_path)
                except OSError:
                    logging.warning(f"Could not delete directory: {file_path}")
            else:
                os.remove(file_path)
                invalidate_cached_hash(file_path)
                logging.info(f"File '{filename}' deleted on server.")
            await notify_clients("deleted", filename)
        else:
            logging.info(f"File '{filename}' already absent.")


async def notify_clients(event_type, filename):
    if not persistent_clients:
        logging.warning(f"No persistent clients to notify about '{filename}' {event_type}.")
        return

    if filename and should_ignore(filename):
        logging.info(f"Ignoring file notification for '{filename}' because it matches ignored patterns.")
        return

    file_path = os.path.normpath(os.path.join(FILE_STORAGE_DIR, filename))
    relative_filename = os.path.relpath(file_path, FILE_STORAGE_DIR)
    timestamp = int(os.path.getmtime(file_path)) if os.path.exists(file_path) else int(time.time())
    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
    hash = get_cached_hash(file_path)

    message = json.dumps({
        "event": event_type,
        "filename": relative_filename,
        "timestamp": timestamp,
        "size": file_size,
        "hash": hash
    })

    logging.info(
        f"[NOTIFYING {len(persistent_clients)} CLIENTS] File '{relative_filename}' {event_type} at {timestamp} with size {file_size} bytes and hash {hash}")
    for client in list(persistent_clients):
        try:
            await client.send(message)
        except websockets.exceptions.ConnectionClosed as e:
            logging.info(f"Client connection closed, removing client: {e}")
            persistent_clients.discard(client)
        except Exception as e:
            logging.error(f"Error sending notification: {e}", exc_info=True)


# ----------------------- File Synchronisation -----------------------

async def send_sync_metadata(websocket):
    file_metadata_list = []
    for root, _, filenames in os.walk(FILE_STORAGE_DIR):
        for filename in filenames:
            relative_path = os.path.relpath(os.path.join(root, filename), FILE_STORAGE_DIR)
            if should_ignore(relative_path):
                continue
            abs_path = os.path.join(FILE_STORAGE_DIR, relative_path)
            metadata = {
                "filename": relative_path,
                "timestamp": int(os.path.getmtime(abs_path)),
                "size": os.path.getsize(abs_path),
                "hash": get_cached_hash(abs_path)
            }
            file_metadata_list.append(metadata)

    await websocket.send(json.dumps({"command": "SYNC_DATA", "files": file_metadata_list}))
    logging.info(f"Sent sync metadata to client: {len(file_metadata_list)} files.")

# ----------------------- File Reception and Transfer -----------------------

async def receive_file(websocket, filename):
    file_path = os.path.normpath(os.path.join(FILE_STORAGE_DIR, filename))
    file_directory = os.path.dirname(file_path)
    if not os.path.exists(file_directory):
        os.makedirs(file_directory, exist_ok=True)

    temp_filename = filename + PARTIAL_SUFFIX
    temp_file_path = os.path.join(FILE_STORAGE_DIR, temp_filename)
    logging.info(f"Receiving file: {filename} -> {file_path}")
    try:
        with open(temp_file_path, "wb") as f:
            while True:
                chunk = await websocket.recv()
                if chunk == "EOF":
                    break
                f.write(chunk)
        os.rename(temp_file_path, file_path)
        logging.info(f"Converted '{temp_filename}' to '{filename}'")
        new_hash = compute_hash(file_path)
        if new_hash:
            with open(get_hash_file_path(file_path), "w") as hf:
                hf.write(new_hash)
            logging.info(f"Cached SHA256 hash for '{filename}'.")
        logging.info(f"File {filename} uploaded successfully!")
        await asyncio.sleep(5)
        await notify_clients("created", filename)
        await websocket.send(json.dumps({"status": "OK", "message": f"File {filename} uploaded"}))
    except Exception as e:
        logging.error(f"Error receiving file {filename}: {e}")
        await websocket.send(json.dumps({"status": "ERROR", "message": "Upload failed"}))
    finally:
        start_stale_file_cleanup()


async def send_file(websocket, filename):
    file_path = os.path.normpath(os.path.join(FILE_STORAGE_DIR, filename))
    if not os.path.exists(file_path):
        await websocket.send(json.dumps({"status": "ERROR", "message": "File not found"}))
        return
    logging.info(f"Sending file: {filename}")
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(4096):
                await websocket.send(chunk)
        await websocket.send("EOF")
        logging.info(f"File {filename} sent successfully!")
    except Exception as e:
        logging.error(f"Error sending file {filename}: {e}")
        await websocket.send(json.dumps({"status": "ERROR", "message": "Transfer failed"}))


async def delete_file(websocket, filename):
    file_path = os.path.normpath(os.path.join(FILE_STORAGE_DIR, filename))
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            invalidate_cached_hash(file_path)
            logging.info(f"File '{filename}' deleted.")
            await notify_clients("deleted", filename)
            await websocket.send(json.dumps({"status": "OK", "message": f"Deleted {filename}"}))
            dir_path = os.path.dirname(file_path)
            while dir_path != FILE_STORAGE_DIR and not os.listdir(dir_path):
                os.rmdir(dir_path)
                logging.info(f"Removed empty directory: {dir_path}")
                dir_path = os.path.dirname(dir_path)
        except Exception as e:
            logging.error(f"Error deleting file {filename}: {e}")
            await websocket.send(json.dumps({"status": "ERROR", "message": "Deletion failed"}))
    else:
        logging.warning(f"File '{filename}' not found for deletion.")
        await websocket.send(json.dumps({"status": "ERROR", "message": "File not found"}))


async def list_files(websocket):
    files = []
    for root, _, filenames in os.walk(FILE_STORAGE_DIR):
        for filename in filenames:
            file_path = os.path.normpath(os.path.join(root, filename))
            relative_path = os.path.relpath(file_path, FILE_STORAGE_DIR)
            if not should_ignore(relative_path):
                files.append(relative_path)
    files.sort()
    await websocket.send(json.dumps({"files": files}))
    logging.info("Sent file list to client.")


def should_ignore(path):
    file_name = os.path.basename(path)
    # Check for ignored prefixes and suffixes.
    if file_name.startswith(IGNORED_PREFIXES) or file_name.endswith(IGNORED_SUFFIXES):
        return True
    # If the file name has no extension, assume it's a directory.
    if not os.path.splitext(file_name)[1]:
        return True
    return False


# ----------------------- Watchdog Setup -----------------------

class AsyncSyncEventHandler(FileSystemEventHandler):
    def __init__(self, loop: asyncio.AbstractEventLoop, debounce_interval: float = 0.3):
        super().__init__()
        self.loop = loop
        self.debounce_interval = debounce_interval
        self.debounce_tasks = {}
        self.latest_events = {}


    def enqueue_event(self, event_type: str, file_path: str) -> None:
        if should_ignore(file_path):
            return
        relative_path = os.path.relpath(file_path, FILE_STORAGE_DIR)
        logging.info(f"[WATCHDOG] {event_type.capitalize()}: {relative_path}")
        self.latest_events[relative_path] = event_type
        if relative_path in self.debounce_tasks:
            self.debounce_tasks[relative_path].cancel()
        self.debounce_tasks[relative_path] = self.loop.create_task(self._debounced_notify(relative_path))

    async def _debounced_notify(self, relative_path: str) -> None:
        try:
            await asyncio.sleep(self.debounce_interval)
            event_type = self.latest_events.pop(relative_path, None)
            self.debounce_tasks.pop(relative_path, None)
            if event_type:
                await notify_clients(event_type, relative_path)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"Error in debounced notify for '{relative_path}': {e}", exc_info=True)

    def on_created(self, event: FileSystemEvent) -> None:
        self.enqueue_event("created", event.src_path)

    def on_modified(self, event: FileSystemEvent) -> None:
        self.enqueue_event("modified", event.src_path)

    def on_deleted(self, event: FileSystemEvent) -> None:
        self.enqueue_event("deleted", event.src_path)

    def on_moved(self, event: FileSystemEvent) -> None:
        self.enqueue_event("deleted", event.src_path)
        self.enqueue_event("created", event.dest_path)


async def start_websocket_server():
    async with websockets.serve(
            websocket_handler,
            "0.0.0.0",
            443,
            ssl=ssl_context,
            max_size=None,  # No limit on message size
            ping_interval=30,
            ping_timeout=30
    ):
        logging.info("WebSocket server started on wss://0.0.0.0:443")
        await asyncio.Future()


def start_watchdog_observer(loop):
    event_handler = AsyncSyncEventHandler(loop, debounce_interval=0.3)
    observer = Observer()
    observer.schedule(event_handler, FILE_STORAGE_DIR, recursive=True)
    observer.start()
    logging.info(f"[WATCHDOG] Observer started, watching: {FILE_STORAGE_DIR}")
    return observer


ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ssl_context.load_cert_chain(SSL_CERT, SSL_KEY)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    observer = start_watchdog_observer(loop)
    start_stale_file_cleanup()


    async def main():
        await start_websocket_server()


    try:
        loop.run_until_complete(main())
    finally:
        observer.stop()
        observer.join()