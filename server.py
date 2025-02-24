import socket
import threading
import logging
import os
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Directory where files are stored (best practice: separate from the root).
FILE_STORAGE_DIR = './uploads'

# Ensure the storage directory exists.
if not os.path.exists(FILE_STORAGE_DIR):
    os.makedirs(FILE_STORAGE_DIR)

def sanitize_filename(filename: str) -> str:
    # Sanitize the filename by allowing only letters, digits, underscore, dash, and dot.
    filename = os.path.basename(filename)
    return re.sub(r'[^a-zA-Z0-9_.-]', '_', filename)


def handle_client(conn: socket.socket, address: tuple):
    conn.settimeout(60)
    logging.info(f"Connected by {address}")
    try:
        file_obj = conn.makefile('rb')

        # Read request line
        request_line = file_obj.readline().decode('utf-8').strip()
        if not request_line:
            logging.error(f"Empty request from {address}")
            return

        # Read headers until an empty line
        headers = {}
        while True:
            line = file_obj.readline().decode('utf-8').strip()
            logging.info(f"Read header line: '{line}'")
            if line == "":
                logging.info("End of headers reached.")
                break
            if ":" in line:
                key, value = line.split(":", 1)
                headers[key.strip()] = value.strip()

        # Process the request based on the method in request_line
        if request_line.upper() == "UPLOAD":
            # Check required headers
            if "File-Name" not in headers or "Content-Length" not in headers:
                logging.error(f"Missing required headers from {address}")
                return
            file_name = sanitize_filename(headers["File-Name"])
            try:
                total_size = int(headers["Content-Length"])
            except ValueError:
                logging.error(f"Invalid Content-Length from {address}: {headers['Content-Length']}")
                return

            # Determine resume offset if a partial file exists
            part_file = os.path.join(FILE_STORAGE_DIR, file_name + ".part")
            if os.path.exists(part_file):
                resume_offset = os.path.getsize(part_file)
                status = f"Status: RESUME {resume_offset}\r\n"
                logging.info(f"Partial file detected for '{file_name}', resume offset: {resume_offset} bytes")
            else:
                resume_offset = 0
                status = "Status: 200 OK\r\n"
                logging.info(f"Starting new transfer for '{file_name}' from {address}")

            # Send status response to client
            conn.sendall(status.encode('utf-8'))

            # Receive file body starting from the resume offset
            expected_bytes = total_size - resume_offset
            with open(part_file, 'ab') as f:
                received_bytes = 0
                while received_bytes < expected_bytes:
                    chunk = file_obj.read(min(4096, expected_bytes - received_bytes))
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

        elif request_line.upper() == "DOWNLOAD":
            # Download functionality
            if "File-Name" not in headers:
                error_msg = "Status: ERROR Missing File-Name\r\n"
                conn.sendall(error_msg.encode('utf-8'))
                logging.error(f"Missing File-Name header for download from {address}")
                return
            file_name = sanitize_filename(headers["File-Name"])
            file_path = os.path.join(FILE_STORAGE_DIR, file_name)
            if not os.path.exists(file_path):
                error_msg = "Status: ERROR File not found\r\n"
                conn.sendall(error_msg.encode('utf-8'))
                logging.error(f"File '{file_name}' not found for download from {address}")
                return

            total_size = os.path.getsize(file_path)
            response_header = f"Content-Length: {total_size}\r\n\r\n"
            conn.sendall(response_header.encode('utf-8'))
            logging.info(f"Sending file '{file_name}' ({total_size} bytes) to {address}")

            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    conn.sendall(chunk)
            logging.info(f"File '{file_name}' sent successfully to {address}")

        elif request_line.upper() == "LIST":
            # List all files (excluding .part files)
            files = [f for f in os.listdir(FILE_STORAGE_DIR)
                     if os.path.isfile(os.path.join(FILE_STORAGE_DIR, f)) and not f.endswith('.part')]
            response = "\n".join(files) + "\r\n"
            conn.sendall(response.encode('utf-8'))
            logging.info(f"Sent file list to {address}")

        elif request_line.upper() == "PING":
            message = (f"Pong! Connected to server.\n"
                       f"Your IP and port: {address}\n"
                       f"Server IP and port: {conn.getsockname()} \n")
            logging.info(f"Have been pinged, will pong to {address}")
            # Send pong message response to client
            conn.sendall(message.encode('utf-8'))
        else:
            logging.error(f"Unknown request method {request_line} from {address}")

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