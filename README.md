# CSV Analysis MCP Server

A Model Context Protocol (MCP) server that provides tools for analyzing CSV files with wide-format headers and converting them to normalized SQLite databases.

## Deploy

```bash
helm upgrade --install mcp-daten-analyse ./k8s/charts --namespace mcp --values ./k8s/charts/values.yaml --kube-context aks-innfactoryai-prod
```

## Overview

This MCP server exposes six tools for processing CSV files with dataset management:

1. **analyze_csv** - Analyze CSV structure and create named dataset
2. **transform_csv** - Convert wide format to normalized long format  
3. **load_sqlite** - Import normalized data into SQLite database
4. **query_sqlite** - Execute safe SELECT queries on the database
5. **list_datasets** - List all available datasets and their status
6. **delete_dataset** - Remove a dataset and all associated files

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
Analyzes CSV structure with wide headers and creates a named dataset.

**Parameters:**
- `csv_input` (required): CSV content as string (or file path if is_content=false)
- `dataset_name` (required): Name for the dataset (converted to snake_case)
- `is_content` (optional): If true, csv_input is CSV content; if false, file path (default: true)

**Creates:**
- `data/{dataset_name}_raw.csv` - Copy of input CSV
- `data/{dataset_name}_spec.json` - TransformSpec for processing

### transform_csv
Transforms a dataset from wide format to normalized long format.

**Parameters:**  
- `dataset_name` (required): Name of the dataset to transform

**Creates:**
- `data/{dataset_name}_normalized.csv` - Normalized data ready for database loading

### load_sqlite
Loads a normalized dataset into SQLite database.

**Parameters:**
- `dataset_name` (required): Name of the dataset to load

**Creates:**
- `data/{dataset_name}.db` - SQLite database with factory_data and monthly_values tables

### query_sqlite
Executes SELECT queries on a dataset's database (read-only for security).

**Parameters:**
- `dataset_name` (required): Name of the dataset to query
- `query` (optional): SELECT query to execute
- `show_schema` (optional): Show database schema instead (default: false)

### list_datasets
Lists all available datasets and their processing status.

**Returns:** Formatted list showing which files exist for each dataset and their current status.

### delete_dataset
Removes a dataset and all its associated files.

**Parameters:**
- `dataset_name` (required): Name of the dataset to delete

**Removes:** All files associated with the dataset (raw CSV, spec, normalized CSV, database)

## Usage

### MCP Client Integration

The server runs over HTTP and can be accessed by MCP clients:

```python
# Using the MCP client for dataset management:

# 1. Start by listing existing datasets
datasets = await client.call_tool("list_datasets")

# 2. Create a new dataset from CSV content (default behavior)
csv_content = """Factory;1 kum;2 kum;3 kum
;2025;2025;2025
WerkA;1.250.000;2.500.000;3.750.000"""

await client.call_tool("analyze_csv", {
    "csv_input": csv_content,
    "dataset_name": "Factory Production 2025"  # Converted to "factory_production_2025"
})

# Or create dataset from CSV file (when file is already on container)
await client.call_tool("analyze_csv", {
    "csv_input": "/path/to/input.csv",
    "dataset_name": "test_data",
    "is_content": False
})

# 3. Transform the dataset to normalized format
await client.call_tool("transform_csv", {
    "dataset_name": "factory_production_2025"
})

# 4. Load into database
await client.call_tool("load_sqlite", {
    "dataset_name": "factory_production_2025"
})

# 5. Query the data multiple times (saves tokens!)
await client.call_tool("query_sqlite", {
    "dataset_name": "factory_production_2025",
    "query": "SELECT factory, year, SUM(ytd_value) FROM factory_data GROUP BY factory, year"
})

await client.call_tool("query_sqlite", {
    "dataset_name": "factory_production_2025", 
    "query": "SELECT * FROM monthly_values WHERE factory = 'WerkA' ORDER BY year, month"
})

# 6. Clean up when done
await client.call_tool("delete_dataset", {
    "dataset_name": "factory_production_2025"
})
```

## Dataset File Structure

Each dataset creates the following files in the `data/` directory:

```
data/
├── {dataset_name}_raw.csv       # Original CSV data
├── {dataset_name}_spec.json     # Transform specification  
├── {dataset_name}_normalized.csv # Normalized long-format data
└── {dataset_name}.db           # SQLite database
```

## Typical Workflow

1. **Start Session**: `list_datasets()` - See what's available
2. **Import Data**: `analyze_csv(csv_content, "dataset_name")` - Paste CSV content, create dataset  
3. **Process Data**: `transform_csv("dataset_name")` - Normalize structure
4. **Load Database**: `load_sqlite("dataset_name")` - Create queryable database
5. **Query Multiple Times**: `query_sqlite("dataset_name", "SELECT...")` - Analyze data efficiently
6. **Clean Up**: `delete_dataset("dataset_name")` - Remove when done

## Key Benefits

- **Easy Import**: Just paste CSV content directly (no file uploads needed)
- **Token Efficient**: After import, all operations are file-based on the container
- **Session Persistent**: Create once, query many times without data transfer
- **Organized**: Clean file structure with snake_case naming
- **Full Lifecycle**: List → Create → Process → Query → Delete

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