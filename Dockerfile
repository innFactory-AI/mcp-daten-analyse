FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY server.py .
COPY http_server.py .
COPY launcher.py .

# Create directories for data persistence
RUN mkdir -p /app/data /app/output

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8000
ENV MCP_TRANSPORT=http
ENV HTTP_HOST=0.0.0.0  
ENV HTTP_PORT=8001

# Expose both ports
EXPOSE 8000 8001

# Health check - check both servers
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health && curl -f http://localhost:8001/health || exit 1

# Run the dual server launcher
CMD ["python", "launcher.py"]