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

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(60)  # Set a timeout for the connection
            sock.connect((server_host, server_port))

            # Send the UPLOAD command with the file name as the first line.
            upload_command = f"UPLOAD {file_name}\n"
            sock.sendall(upload_command.encode('utf-8'))

            # Send the file size as the second line.
            sock.sendall((str(total_size) + "\n").encode('utf-8'))

            # Wait for the server's response about resuming or starting fresh.
            response = sock.recv(1024).decode('utf-8').strip()
            logging.info(f"Server response: {response}")

            resume_offset = 0
            if response.startswith("RESUME"):
                try:
                    resume_offset = int(response.split()[1])
                    logging.info(f"Resuming transfer from byte {resume_offset}")
                except (IndexError, ValueError):
                    logging.error("Invalid resume response from server.")
                    return
            elif response == "START":
                logging.info("Starting new transfer")
            else:
                logging.error(f"Unexpected response from server: {response}")
                return

            # Open the file and seek to the resume offset if needed.
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
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(60)
            sock.connect((server_host, server_port))

            # Send the DOWNLOAD command to the server.
            command = f"DOWNLOAD {file_name}\n"
            sock.sendall(command.encode('utf-8'))

            # Read the header response.
            header = sock.recv(1024).decode('utf-8').strip()
            if header.startswith("ERROR"):
                logging.error(f"Server error: {header}")
                return
            if not header.startswith("FILESIZE"):
                logging.error(f"Unexpected header from server: {header}")
                return

            try:
                total_size = int(header.split()[1])
            except (IndexError, ValueError):
                logging.error(f"Invalid FILESIZE header: {header}")
                return

            logging.info(f"Downloading '{file_name}' ({total_size} bytes) from the server.")
            received_bytes = 0
            # Save the downloaded file in the current working directory.
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

            # Send the "LIST" command to the server.
            sock.sendall("LIST\n".encode('utf-8'))

            # Receive the response.
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