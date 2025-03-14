FROM python:3.11-slim
LABEL authors="alexvdv116"

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project into the container, including .certs and other directories
COPY . .

# Expose port 433 for secure WebSocket connections
EXPOSE 433

# Run the server from the project root
CMD ["python", "server.py"]