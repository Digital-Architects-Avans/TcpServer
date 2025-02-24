import socket
import os
import logging

# Default server settings
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 12345

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)


def print_help():
    help_text = f"""
Available commands:
/help                             - Show this help message.
/ping                             - Send a ping request and shows current IP and ports.
/upload <file_path>               - Upload a file to the server.
/download <file_name>             - Download a file from the server.
/list                             - List all files on the server.
/set <server_host> <server_port>  - Set the server host and port (default is {SERVER_HOST}:{SERVER_PORT}).
/quit                             - Exit the application.
"""
    print(help_text)

# UPLOAD
def send_file(server_host: str, server_port: int, file_path: str):
    # Remove any surrounding quotes from the file path.
    file_path = file_path.strip().strip('\'"')

    if not os.path.isfile(file_path):
        logging.error(f"File '{file_path}' does not exist.")
        return

    file_name = os.path.basename(file_path)
    total_size = os.path.getsize(file_path)

    # File size as a string representation
    total_size_str = str(total_size)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(60)
            sock.connect((server_host, server_port))

            # Build the request header (HTTP-inspired)
            #   - First line: UPLOAD
            #   - Headers: File-Name and Content-Length
            #   - Blank line indicating end of header.
            request_lines = [
                "UPLOAD",
                f"File-Name: {file_name}",
                f"Content-Length: {total_size_str}",
                ""  # Blank line to separate headers from body
            ]
            # We append an extra \r\n after joining the header lines so that a proper blank line is sent immediately.
            # since join() does not add a trailing delimiter after the last element.
            request = "\r\n".join(request_lines) + "\r\n"
            sock.sendall(request.encode('utf-8'))

            # Wait for the server's status response
            response = sock.recv(1024).decode('utf-8').strip()
            logging.info(f"Server response: {response}")

            # Parse response to check for resume offset
            if response.startswith("Status: RESUME"):
                try:
                    resume_offset = int(response.split()[2])
                    logging.info(f"Resuming transfer from byte {resume_offset}")
                except (IndexError, ValueError):
                    logging.error("Invalid resume response format.")
                    return
            elif response.startswith("Status: 200"):
                resume_offset = 0
                logging.info("Starting new transfer")
            else:
                logging.error(f"Unexpected server response: {response}")
                return

            # Open the file and send remaining data starting at resume_offset
            with open(file_path, 'rb') as file:
                file.seek(resume_offset)
                sent_bytes = resume_offset
                while sent_bytes < total_size:
                    chunk = file.read(min(4096, total_size - sent_bytes))
                    if not chunk:
                        break
                    try:
                        sock.sendall(chunk)
                    except socket.timeout:
                        logging.error("Socket timeout during file send.")
                        return
                    sent_bytes += len(chunk)
            logging.info(f"File '{file_name}' sent successfully. Total bytes sent: {sent_bytes}/{total_size}")

    except socket.timeout:
        logging.error("Connection timed out.")
    except Exception as e:
        logging.error(f"Error sending file: {e}")

# DOWNLOAD
def download_file(server_host: str, server_port: int, file_name: str):
    # (Download implementation can be adjusted similarly to use a header format)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(60)
            sock.connect((server_host, server_port))

            # Build a simple download request: DOWNLOAD header and blank line.
            request_lines = [
                "DOWNLOAD",
                f"File-Name: {file_name}",
                ""
            ]
            # We append an extra \r\n after joining the header lines so that a proper blank line is sent immediately.
            # since join() does not add a trailing delimiter after the last element.
            request = "\r\n".join(request_lines) + "\r\n"
            sock.sendall(request.encode('utf-8'))

            # Read the response header from the server
            header = sock.recv(1024).decode('utf-8').strip()
            if header.startswith("Status: ERROR"):
                logging.error(f"Server error: {header}")
                return
            if not header.startswith("Content-Length:"):
                logging.error(f"Unexpected header from server: {header}")
                return

            try:
                total_size = int(header.split(":", 1)[1].strip())
            except (IndexError, ValueError):
                logging.error(f"Invalid Content-Length header: {header}")
                return

            logging.info(f"Downloading '{file_name}' ({total_size} bytes) from the server.")
            received_bytes = 0
            with open(file_name, 'wb') as f:
                while received_bytes < total_size:
                    to_read = min(4096, total_size - received_bytes)
                    chunk = sock.recv(to_read)
                    if not chunk:
                        break
                    f.write(chunk)
                    received_bytes += len(chunk)
            if received_bytes == total_size:
                logging.info(f"File '{file_name}' downloaded successfully.")
            else:
                logging.warning(f"Incomplete download: received {received_bytes} of {total_size} bytes.")

    except socket.timeout:
        logging.error("Connection timed out.")
    except Exception as e:
        logging.error(f"Error downloading file: {e}")


# LIST
def list_files(server_host: str, server_port: int):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(60)
            sock.connect((server_host, server_port))

            # LIST request
            request = "LIST\r\n\r\n"
            sock.sendall(request.encode('utf-8'))

            response = sock.recv(4096).decode('utf-8')
            if response:
                print("Files on server:")
                for line in response.strip().split("\n"):
                    print(line)
            else:
                print("No files found on server.")
    except socket.timeout:
        logging.error("Connection timed out.")
    except Exception as e:
        logging.error(f"Error listing files: {e}")


def ping(server_host: str, server_port: int):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            sock.connect((server_host, server_port))
            # Send a simple PING request
            request = "PING\r\n\r\n"
            sock.sendall(request.encode('utf-8'))
            response = sock.recv(4096).decode('utf-8').strip()
            print(f"Server response: {response}")
    except socket.timeout:
        logging.error("Ping request timed out.")
    except Exception as e:
        logging.error(f"Error during ping: {e}")


def main():
    global SERVER_HOST, SERVER_PORT

    print("Welcome to the File Transfer Client!")
    print_help()

    while True:
        try:
            user_input = input("Enter command: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not user_input:
            continue

        if user_input.startswith("/help"):
            print_help()
        # Handle '/ping' command
        elif user_input.startswith("/ping"):
            try:
                ping(SERVER_HOST, SERVER_PORT)
            except Exception as e:
                logging.error(f"Ping command failed: {e}")
            continue


        elif user_input.startswith("/upload"):
            parts = user_input.split(maxsplit=1)
            if len(parts) < 2:
                print("Usage: /upload <file_path>")
                continue
            file_path = parts[1].strip()
            send_file(SERVER_HOST, SERVER_PORT, file_path)
        elif user_input.startswith("/download"):
            parts = user_input.split(maxsplit=1)
            if len(parts) < 2:
                print("Usage: /download <file_name>")
                continue
            file_name = parts[1].strip()
            download_file(SERVER_HOST, SERVER_PORT, file_name)
        elif user_input.startswith("/list"):
            list_files(SERVER_HOST, SERVER_PORT)
        elif user_input.startswith("/set"):
            parts = user_input.split()
            if len(parts) != 3:
                print("Usage: /set <server_host> <server_port>")
                continue
            SERVER_HOST = parts[1]
            try:
                SERVER_PORT = int(parts[2])
            except ValueError:
                print("Server port must be an integer.")
                continue
            print(f"Server settings updated to {SERVER_HOST}:{SERVER_PORT}")
        elif user_input.startswith("/quit"):
            print("Exiting.")
            break
        else:
            print("Unknown command. Type /help for a list of commands.")


if __name__ == '__main__':
    main()