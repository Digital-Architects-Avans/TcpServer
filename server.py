import socket
import threading
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Directory where files are stored (best practice: separate from the root).
FILE_STORAGE_DIR = './uploads'

# Ensure the storage directory exists.
if not os.path.exists(FILE_STORAGE_DIR):
    os.makedirs(FILE_STORAGE_DIR)


def handle_client(conn: socket.socket, address: tuple):
    conn.settimeout(60)  # Set a timeout of 60 seconds for the connection
    logging.info(f"Connected by {address}")
    try:
        # Wrap the connection with a file-like object for line-by-line reading.
        file_obj = conn.makefile('rb')

        # Read the first line to determine the command.
        first_line = file_obj.readline().decode('utf-8').strip()
        if not first_line:
            logging.error(f"Empty command from {address}")
            return

        # Check if the command is LIST.
        if first_line.upper() == "LIST":
            files = [f for f in os.listdir(FILE_STORAGE_DIR)
                     if os.path.isfile(os.path.join(FILE_STORAGE_DIR, f)) and not f.endswith('.part')]
            response = "\n".join(files) + "\n"
            conn.sendall(response.encode('utf-8'))
            logging.info(f"Sent file list to {address}")
            return

        # Check if the command is DOWNLOAD.
        if first_line.upper().startswith("DOWNLOAD"):
            parts = first_line.split(maxsplit=1)
            if len(parts) < 2:
                error_msg = "ERROR Missing file name\n"
                conn.sendall(error_msg.encode('utf-8'))
                logging.error(f"Missing file name for download from {address}")
                return

            file_name = parts[1]
            file_path = os.path.join(FILE_STORAGE_DIR, file_name)
            if not os.path.exists(file_path):
                error_msg = "ERROR File not found\n"
                conn.sendall(error_msg.encode('utf-8'))
                logging.error(f"File '{file_name}' not found for download from {address}")
                return

            total_size = os.path.getsize(file_path)
            header = f"FILESIZE {total_size}\n"
            conn.sendall(header.encode('utf-8'))
            logging.info(f"Sending file '{file_name}' ({total_size} bytes) to {address}")

            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    conn.sendall(chunk)
            logging.info(f"File '{file_name}' sent successfully to {address}")
            return

        # Check if the command is an UPLOAD command.
        if first_line.upper().startswith("UPLOAD"):
            # Extract the filename from the command.
            parts = first_line.split(maxsplit=1)
            if len(parts) < 2:
                logging.error(f"UPLOAD command missing file name from {address}")
                return
            file_name = parts[1]
        else:
            # If no explicit command is given, we may decide to reject the connection.
            logging.error(f"Unknown command received: {first_line} from {address}")
            return

        # Now, handle the upload:
        # Read the file size from the next line.
        file_size_line = file_obj.readline().decode('utf-8').strip()
        if not file_size_line:
            logging.error(f"Missing file size from {address}")
            return
        try:
            total_size = int(file_size_line)
        except ValueError:
            logging.error(f"Invalid file size from {address}: {file_size_line}")
            return

        part_file = os.path.join(FILE_STORAGE_DIR, file_name + ".part")
        resume_offset = 0
        if os.path.exists(part_file):
            resume_offset = os.path.getsize(part_file)
            logging.info(f"Partial file detected for '{file_name}', resume offset: {resume_offset} bytes")
        else:
            logging.info(f"No partial file found for '{file_name}'. Starting new transfer.")

        # Inform the client whether to resume or start a new transfer.
        response = f"RESUME {resume_offset}\n" if resume_offset > 0 else "START\n"
        conn.sendall(response.encode('utf-8'))

        remaining = total_size - resume_offset
        if remaining < 0:
            logging.error(f"Existing partial file size is larger than expected for '{file_name}' from {address}")
            return

        logging.info(f"Expecting {remaining} bytes from {address} for file '{file_name}'")
        with open(part_file, 'ab') as f:
            received_bytes = 0
            while received_bytes < remaining:
                to_read = min(4096, remaining - received_bytes)
                try:
                    chunk = file_obj.read(to_read)
                except socket.timeout:
                    logging.error(f"Socket timeout while receiving data from {address}")
                    return
                if not chunk:
                    break
                f.write(chunk)
                received_bytes += len(chunk)

        if resume_offset + received_bytes == total_size:
            final_file = os.path.join(FILE_STORAGE_DIR, file_name)
            os.rename(part_file, final_file)
            logging.info(f"File '{file_name}' received successfully from {address}")
        else:
            logging.warning(
                f"Incomplete transfer from {address}: received {resume_offset + received_bytes} of {total_size} bytes")

    except socket.timeout:
        logging.error(f"Connection timed out with {address}")
    except Exception as e:
        logging.error(f"Error handling client {address}: {e}")
    finally:
        conn.close()
        logging.info(f"Connection with {address} closed.")


def start_server(host: str = '0.0.0.0', port: int = 12345):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((host, port))
        server_sock.listen()
        logging.info(f"Server listening on {host}:{port}")

        while True:
            try:
                conn, address = server_sock.accept()
                thread = threading.Thread(target=handle_client, args=(conn, address), daemon=True)
                thread.start()
            except Exception as e:
                logging.error(f"Error accepting a new connection: {e}")


if __name__ == '__main__':
    start_server()