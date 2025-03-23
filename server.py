import asyncio
import hashlib
import re
import time
import websockets
import json
import os
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Directory where files are stored
FILE_STORAGE_DIR = "./uploads"
os.makedirs(FILE_STORAGE_DIR, exist_ok=True)

# Common temporary file prefixes & extensions to ignore
IGNORED_PREFIXES = ("~$", ".")
IGNORED_SUFFIXES = (".swp", ".tmp", ".lock", ".part")

# Directory to store file specific locks
file_locks = {}

# Set to keep track of connected clients
connected_clients = set()


async def websocket_handler(websocket: websockets.ServerConnection) -> None:
    """Handles WebSocket connections and processes commands and event messages."""
    connected_clients.add(websocket)
    try:
        async for message in websocket:
            data = json.loads(message)

            if data.get("event"):  # Handle client file changes
                await handle_client_notification(data, websocket)
                continue

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

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_clients.remove(websocket)


async def handle_client_notification(data, websocket):
    """
    Processes a notification message sent from a client.
    Checks both timestamps and file sizes to determine if an upload is needed.
    """

    event_type = data.get("event")
    filename = data.get("filename")
    client_timestamp = data.get("timestamp")
    client_file_size = int(data.get("size", 0))  # Include file size in client notifications
    file_path = os.path.join(FILE_STORAGE_DIR, filename)

    # Ignore temporary files
    if filename.startswith("~$"):
        logging.info(f"Ignoring temporary file: {filename}")
        return

    logging.info(
        f"Received client notification: File '{filename}' {event_type} at {client_timestamp} with size {client_file_size} bytes")

    if event_type in ["created", "modified"]:
        await asyncio.sleep(1)  # Give time for saving before checking

        server_timestamp = int(os.path.getmtime(file_path)) if os.path.exists(file_path) else 0
        server_file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        logging.info(f"Client timestamp: {client_timestamp} ({time.ctime(client_timestamp)}), size: {client_file_size}")
        logging.info(f"Server timestamp: {server_timestamp} ({time.ctime(server_timestamp)}), size: {server_file_size}")

        # Ignore empty files from clients
        if client_file_size == 0:
            logging.info(f"Ignoring file '{filename}' because its size is 0 bytes.")
            return

        # Upload file if the client's version is newer OR the file size is different
        if client_timestamp > server_timestamp or client_file_size != server_file_size:
            request = json.dumps({"command": "REQUEST_UPLOAD", "filename": filename})
            await websocket.send(request)
            logging.info(f"Requested upload for file '{filename}' from client.")
        else:
            logging.info(f"No upload needed for '{filename}': server version is up-to-date.")

    elif event_type == "deleted":
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"File '{filename}' deleted on server as per client notification.")
            await notify_clients("deleted", filename)
        else:
            logging.info(f"File '{filename}' already absent on server.")


async def notify_clients(event_type, filename):
    """Sends a notification to all connected clients, including the file's last modified timestamp."""
    if not connected_clients:
        logging.warning(f"[NOTIFY] No connected clients to notify about '{filename}' {event_type}.")
        return  # Exit early if there are no clients

    file_path = os.path.join(FILE_STORAGE_DIR, filename)
    timestamp = int(os.path.getmtime(file_path)) if os.path.exists(file_path) else int(time.time())
    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
    message = json.dumps({
        "event": event_type,
        "filename": filename,
        "timestamp": timestamp,
        "size": file_size
    })

    logging.info(
        f"[NOTIFYING {len(connected_clients)} CLIENTS] File '{filename}' {event_type} at {timestamp} (size: {file_size} bytes)")
    await asyncio.gather(*(client.send(message) for client in connected_clients))


def should_ignore(filename):
    """Checks if the file should be ignored based on its name."""
    return filename.startswith(IGNORED_PREFIXES) or filename.endswith(IGNORED_SUFFIXES)


class AsyncSyncEventHandler(FileSystemEventHandler):
    """
    Monitors file system changes and debounces events before notifying clients.
    If multiple events for the same file occur within a short interval, only the last event is processed.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop, debounce_interval: float = 0.3):
        super().__init__()
        self.loop = loop
        self.debounce_interval = debounce_interval
        self.debounce_interval = debounce_interval
        self.debounce_tasks: dict[str, asyncio.Task] = {}
        self.latest_events: dict[str, str] = {}

    def enqueue_event(self, event_type: str, filename: str) -> None:
        if should_ignore(filename):
            return
        logging.info(f"[WATCHDOG] File {event_type}: {filename}")
        # Update the latest event for this file.
        self.latest_events[filename] = event_type

        # Cancel any previously scheduled task for this file.
        if filename in self.debounce_tasks:
            self.debounce_tasks[filename].cancel()

        # Schedule a new task that waits for the debounce interval before notifying.
        self.debounce_tasks[filename] = self.loop.create_task(self._debounced_notify(filename))

    async def _debounced_notify(self, filename: str) -> None:
        try:
            await asyncio.sleep(self.debounce_interval)
            event_type = self.latest_events.get(filename)
            # Clean up the stored event and task.
            self.latest_events.pop(filename, None)
            self.debounce_tasks.pop(filename, None)
            await notify_clients(event_type, filename)
        except asyncio.CancelledError:
            # Task was cancelled because a new event arrived for the same file.
            pass
        except Exception as e:
            logging.error(f"Error in debounced notify for '{filename}': {e}", exc_info=True)

    def on_created(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self.enqueue_event("created", os.path.basename(event.src_path))

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self.enqueue_event("modified", os.path.basename(event.src_path))

    def on_deleted(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self.enqueue_event("deleted", os.path.basename(event.src_path))

    def on_moved(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            # Treat a move as a deletion of the source file and creation of the destination file.
            self.enqueue_event("deleted", os.path.basename(event.src_path))
            self.enqueue_event("created", os.path.basename(event.dest_path))


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


def sanitize_filename(filename: str) -> str:
    filename = os.path.basename(filename)
    return re.sub(r'[^a-zA-Z0-9_.-]', '_', filename)


async def receive_file(websocket, filename):
    """Receives a file from the client and saves it on the server."""
    file_lock = await get_file_lock(filename)

    async with file_lock:  # Lock the file to prevent race conditions
        file_path = os.path.join(FILE_STORAGE_DIR, filename)
        logging.info(f"Receiving file: {filename}")

        try:
            with open(file_path, "wb") as f:
                while True:
                    chunk = await websocket.recv()
                    if chunk == "EOF":
                        break
                    f.write(chunk)

            logging.info(f"File {filename} uploaded successfully!")
            await notify_clients("created", filename)
            await websocket.send(json.dumps({"status": "OK", "message": f"File {filename} uploaded"}))
        except Exception as e:
            logging.error(f"Error receiving file {filename}: {e}")
            await websocket.send(json.dumps({"status": "ERROR", "message": "File upload failed"}))


async def send_file(websocket, filename):
    """Sends a requested file to the client."""
    file_lock = await get_file_lock(filename)

    async with file_lock:  # Ensure no upload is in progress before reading
        file_path = os.path.join(FILE_STORAGE_DIR, filename)
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
            await websocket.send(json.dumps({"status": "ERROR", "message": "File transfer failed"}))


async def delete_file(websocket, filename):
    """Deletes a file on the server and notifies clients."""
    file_lock = await get_file_lock(filename)

    async with file_lock:  # Ensure no upload is in progress before reading
        file_path = os.path.join(FILE_STORAGE_DIR, filename)

        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"File '{filename}' deleted as per request.")

            # Notify all clients about the deletion
            await notify_clients("deleted", filename)

            await websocket.send(json.dumps({"status": "OK", "message": f"File '{filename}' deleted"}))
        else:
            logging.warning(f"File '{filename}' not found for deletion.")
            await websocket.send(json.dumps({"status": "ERROR", "message": "File not found"}))


async def list_files(websocket):
    """Sends a list of available files to the client."""
    files = [f for f in os.listdir(FILE_STORAGE_DIR) if os.path.isfile(os.path.join(FILE_STORAGE_DIR, f))]
    await websocket.send(json.dumps({"files": files}))
    logging.info("Sent file list to client.")

async def get_file_lock(filename):
    """Ensure each file has its own asyncio.Lock instance."""
    if filename not in file_locks:
        file_locks[filename] = asyncio.Lock()
    return file_locks[filename]

async def start_websocket_server():
    """Starts the WebSocket server."""
    async with websockets.serve(websocket_handler, "0.0.0.0", 5678):
        logging.info("WebSocket server started on ws://0.0.0.0:5678")
        await asyncio.Future()  # Keeps the server running


def start_watchdog_observer(loop):
    """Starts a file system observer to track file changes."""
    event_handler = AsyncSyncEventHandler(loop, debounce_interval=0.3)
    observer = Observer()
    observer.schedule(event_handler, FILE_STORAGE_DIR, recursive=True)
    observer.start()
    logging.info(f"[WATCHDOG] File sync observer started, watching: {FILE_STORAGE_DIR}")
    return observer


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Start the watchdog observer using our debounced event handler.
    observer = start_watchdog_observer(loop)

    async def main():
        await start_websocket_server()

    try:
        loop.run_until_complete(main())
    finally:
        observer.stop()
        observer.join()
