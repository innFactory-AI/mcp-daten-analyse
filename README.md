# CSV Analysis MCP Server

A Model Context Protocol (MCP) server that provides tools for analyzing CSV files with wide-format headers and converting them to normalized SQLite databases.

## Overview

This MCP server exposes four tools for processing CSV files that use European number formatting and have a two-row header structure:

1. **analyze_csv** - Analyze CSV structure and create TransformSpec
2. **transform_csv** - Convert wide format to normalized long format  
3. **load_sqlite** - Import normalized data into SQLite database
4. **query_sqlite** - Execute safe SELECT queries on the database

## CSV Format Expected

- Delimiter: semicolon (`;`)
- First header row: Column names like "1 kum", "2 kum", ..., "12 kum"
- Second header row: Years (e.g., 2025, 2024, 2020)
- Data rows: Factory name in first column, YTD values in European format (1.126.286)

## Installation

### Option 1: Docker (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd mcp-data-analysis

# Build and run with Docker Compose
docker-compose up -d

# The server will be available at http://localhost:8000
```

### Option 2: Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server locally
python server.py

# Server runs on http://localhost:8000 by default
```

### Option 3: Docker Build

```bash
# Build the Docker image
docker build -t csv-analysis-server .

# Run the container
docker run -p 8000:8000 -v $(pwd)/data:/app/data csv-analysis-server
```

## Tools Available

### analyze_csv
Analyzes CSV structure with wide headers and returns TransformSpec JSON.

**Parameters:**
- `csv_input` (required): Either path to input CSV file or CSV content as string
- `is_content` (optional): If true, csv_input is treated as CSV content; if false, as file path (default: false)

**Returns:** JSON string containing the TransformSpec

### transform_csv
Transforms wide format CSV to normalized long format using TransformSpec.

**Parameters:**  
- `csv_input` (required): Either path to CSV file or CSV content as string
- `spec_input` (required): Either path to TransformSpec JSON file or JSON content as string
- `is_csv_content` (optional): If true, csv_input is treated as CSV content; if false, as file path (default: false)
- `is_spec_content` (optional): If true, spec_input is treated as JSON content; if false, as file path (default: false)
- `output_path` (optional): Output path for normalized file (default: "normalized.csv")
- `output_json` (optional): Output as JSON instead of CSV (default: false)

### load_sqlite
Loads normalized CSV/JSON data into SQLite database.

**Parameters:**
- `data_path` (required): Path to normalized CSV or JSON file
- `db_path` (optional): SQLite database path (default: "data.db")
- `is_json` (optional): Input file is JSON format (default: false)

### query_sqlite
Executes SELECT queries on SQLite database (read-only for security).

**Parameters:**
- `db_path` (required): Path to SQLite database file
- `query` (optional): SELECT query to execute
- `show_schema` (optional): Show database schema instead (default: false)

## Usage

### MCP Client Integration

The server runs over HTTP and can be accessed by MCP clients:

```python
# Using the MCP client to call tools:

# 1. Analyze CSV structure from file
spec_json = await client.call_tool("analyze_csv", {
    "csv_input": "input.csv"
})

# Or analyze CSV content directly
csv_content = """Factory;1 kum;2 kum;3 kum
;2025;2025;2025
WerkA;1.250.000;2.500.000;3.750.000"""

spec_json = await client.call_tool("analyze_csv", {
    "csv_input": csv_content,
    "is_content": True
})

# 2. Transform to normalized format using spec JSON directly
await client.call_tool("transform_csv", {
    "csv_input": "input.csv",
    "spec_input": spec_json,  # Pass the JSON directly
    "is_spec_content": True,
    "output_path": "normalized.csv"
})

# 3. Load into database
await client.call_tool("load_sqlite", {
    "data_path": "normalized.csv",
    "db_path": "analysis.db"
})

# 4. Query the data
await client.call_tool("query_sqlite", {
    "db_path": "analysis.db",
    "query": "SELECT factory, year, SUM(ytd_value) FROM factory_data GROUP BY factory, year"
})
```

### Docker Environment Variables

Configure the server using environment variables:

- `HOST` - Server host (default: 0.0.0.0)
- `PORT` - Server port (default: 8000) 
- `PYTHONUNBUFFERED` - Disable Python output buffering

### Volume Mounts

The Docker setup includes persistent volume mounts:

- `./data:/app/data` - For databases and temporary files
- `./output:/app/output` - For generated outputs
- `./input:/app/input:ro` - For input CSV files (read-only)

### Production Deployment

For production use with nginx proxy:

```bash
# Start with nginx reverse proxy
docker-compose --profile production up -d

# Server available at http://localhost (port 80)
```

## Database Schema

The server creates two tables:

### factory_data
- `factory` (TEXT): Factory name
- `year` (INTEGER): Year  
- `month` (INTEGER): Month (1-12)
- `ytd_value` (REAL): Year-to-date value

### monthly_values  
- `factory`, `year`, `month`, `ytd_value`: Same as factory_data
- `month_value` (REAL): Calculated monthly value (difference from previous month)

## Security Features

- Only SELECT queries allowed in query_sqlite tool
- Input validation for file paths
- European number format parsing (handles 1.126.286 → 1126286)
- Graceful error handling with descriptive messages

## Dependencies

- Python 3.11+
- fastmcp (FastMCP framework for MCP servers)
- Standard library: csv, json, sqlite3, re, os, pathlib

## API Endpoints

When running, the server exposes these HTTP endpoints:

- `POST /tools/call` - Call MCP tools
- `GET /tools/list` - List available tools  
- `GET /health` - Health check endpoint

## Troubleshooting

### Common Issues

1. **Port already in use**: Change the port with `PORT=8001 docker-compose up`
2. **File not found**: Ensure CSV files are in the mounted volume directories
3. **Permission denied**: Check file permissions in mounted volumes

### Logs

View container logs:
```bash
docker-compose logs -f csv-analysis-server
```

### Development Mode

For development with auto-reload:
```bash
# Install development dependencies
pip install -r requirements.txt

# Run in development mode (stdio transport)
TRANSPORT=stdio python server.py

# Or run in HTTP mode for testing
python server.py
```

## What is FastMCP?

FastMCP is a simplified framework for building Model Context Protocol servers. Key benefits over the standard MCP library:

- **Simpler setup**: Just use decorators to define tools
- **Automatic schema generation**: Type hints become JSON schemas automatically  
- **Built-in HTTP server**: No need for FastAPI/uvicorn setup
- **Transport flexibility**: Easily switch between HTTP and stdio transports

The server uses `@mcp.tool` decorators to expose functions as MCP tools, making the code much cleaner and more maintainable.