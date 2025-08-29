#!/usr/bin/env python3
"""
FastMCP Server for CSV Data Analysis Query Tools

Exposes three MCP tools:
1. query_dataset - Execute SELECT queries on SQLite database
2. list_datasets - List all available datasets and their status
3. delete_dataset - Delete all files associated with a dataset
"""

import json
import logging
import sqlite3
import re
import os
from typing import Optional

from fastmcp import FastMCP

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("csv-analysis-server")

# Initialize FastMCP server
mcp = FastMCP("CSV Analysis Query Server 📊")

def is_safe_query(query: str) -> bool:
    """Check if query is a safe SELECT statement"""
    cleaned = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    cleaned = ' '.join(cleaned.split())
    
    if not re.match(r'^\s*SELECT\s', cleaned, re.IGNORECASE):
        return False
    
    dangerous = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'PRAGMA']
    for keyword in dangerous:
        if re.search(rf'\b{keyword}\b', cleaned, re.IGNORECASE):
            return False
    
    return True


@mcp.tool
def query_dataset(dataset_name: str, query: str = "", show_schema: bool = False) -> str:
    """
    Execute SELECT queries on dataset's SQLite database (read-only).
    
    Args:
        dataset_name: Name of the dataset to query
        query: SELECT query to execute
        show_schema: Show database schema instead of running query
    
    Returns:
        Query results or schema information
    """
    try:
        # Convert dataset name to snake_case
        dataset_name = re.sub(r'[^a-zA-Z0-9_]', '_', dataset_name.lower())
        dataset_name = re.sub(r'_+', '_', dataset_name).strip('_')
        
        db_path = f'data/{dataset_name}.db'
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database for dataset '{dataset_name}' not found. Run load_sqlite first.")
            
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        if show_schema or not query:
            # Show schema
            result_text = f"Database Schema for dataset '{dataset_name}':\n" + "="*50 + "\n"
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            for table_name, in tables:
                result_text += f"\nTable: {table_name}\n"
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                for col in columns:
                    col_name, col_type, not_null, default, pk = col[1], col[2], col[3], col[4], col[5]
                    constraints = []
                    if pk:
                        constraints.append("PRIMARY KEY")
                    if not_null:
                        constraints.append("NOT NULL") 
                    if default:
                        constraints.append(f"DEFAULT {default}")
                    
                    constraint_str = " " + ", ".join(constraints) if constraints else ""
                    result_text += f"  {col_name}: {col_type}{constraint_str}\n"
        else:
            # Execute query
            if not is_safe_query(query):
                raise ValueError("Only SELECT statements are allowed")
            
            cursor.execute(query)
            results = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            
            if results:
                result_text = '\t'.join(column_names) + '\n'
                result_text += '-' * len('\t'.join(column_names)) + '\n'
                
                for row in results:
                    formatted_row = []
                    for value in row:
                        if value is None:
                            formatted_row.append('NULL')
                        else:
                            formatted_row.append(str(value))
                    result_text += '\t'.join(formatted_row) + '\n'
                
                result_text += f"\n{len(results)} row(s) returned"
            else:
                result_text = "No results found"
        
        conn.close()
        return result_text
        
    except Exception as e:
        raise Exception(f"Error querying dataset: {str(e)}")

@mcp.tool
def list_datasets() -> str:
    """
    List all available datasets and their status.
    
    Returns:
        Formatted list of datasets with their files and database status
    """
    try:
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Find all spec files to identify datasets
        datasets = {}
        for filename in os.listdir('data'):
            if filename.endswith('_spec.json'):
                dataset_name = filename.replace('_spec.json', '')
                datasets[dataset_name] = {
                    'spec': f'data/{filename}',
                    'raw_csv': f'data/{dataset_name}_raw.csv',
                    'normalized_csv': f'data/{dataset_name}_normalized.csv',
                    'database': f'data/{dataset_name}.db'
                }
        
        if not datasets:
            return "No datasets found. Use analyze_csv to create a dataset."
        
        result = "Available Datasets:\n" + "="*50 + "\n"
        
        for name, files in datasets.items():
            result += f"\nDataset: {name}\n"
            
            # Check file existence
            spec_exists = os.path.exists(files['spec'])
            raw_exists = os.path.exists(files['raw_csv'])
            normalized_exists = os.path.exists(files['normalized_csv'])
            db_exists = os.path.exists(files['database'])
            
            result += f"  ✓ Spec file: {files['spec']}\n" if spec_exists else f"  ✗ Spec file: {files['spec']}\n"
            result += f"  ✓ Raw CSV: {files['raw_csv']}\n" if raw_exists else f"  ✗ Raw CSV: {files['raw_csv']}\n"
            result += f"  ✓ Normalized CSV: {files['normalized_csv']}\n" if normalized_exists else f"  ✗ Normalized CSV: {files['normalized_csv']}\n"
            result += f"  ✓ Database: {files['database']}\n" if db_exists else f"  ✗ Database: {files['database']}\n"
            
            # Show status
            if spec_exists and raw_exists and normalized_exists and db_exists:
                status = "Ready for querying"
            elif spec_exists and raw_exists and normalized_exists:
                status = "Ready for database load"
            elif spec_exists and raw_exists:
                status = "Ready for transformation"
            elif spec_exists:
                status = "Analyzed (missing raw CSV)"
            else:
                status = "Incomplete"
            
            result += f"  Status: {status}\n"
        
        return result
        
    except Exception as e:
        raise Exception(f"Error listing datasets: {str(e)}")

@mcp.tool
def delete_dataset(dataset_name: str) -> str:
    """
    Delete all files associated with a dataset.
    
    Args:
        dataset_name: Name of the dataset to delete
    
    Returns:
        Status message with deleted files
    """
    try:
        # Convert dataset name to snake_case
        dataset_name = re.sub(r'[^a-zA-Z0-9_]', '_', dataset_name.lower())
        dataset_name = re.sub(r'_+', '_', dataset_name).strip('_')
        
        # Define all possible files for the dataset
        files_to_delete = [
            f'data/{dataset_name}_spec.json',
            f'data/{dataset_name}_raw.csv',
            f'data/{dataset_name}_normalized.csv',
            f'data/{dataset_name}.db'
        ]
        
        deleted_files = []
        missing_files = []
        
        for file_path in files_to_delete:
            if os.path.exists(file_path):
                os.remove(file_path)
                deleted_files.append(file_path)
            else:
                missing_files.append(file_path)
        
        if not deleted_files and not missing_files:
            return f"Dataset '{dataset_name}' not found."
        
        result = f"Dataset '{dataset_name}' deletion complete\n"
        
        if deleted_files:
            result += f"Deleted files ({len(deleted_files)}):\n"
            for file_path in deleted_files:
                result += f"  - {file_path}\n"
        
        if missing_files:
            result += f"Files not found ({len(missing_files)}):\n"
            for file_path in missing_files:
                result += f"  - {file_path}\n"
        
        return result
        
    except Exception as e:
        raise Exception(f"Error deleting dataset: {str(e)}")

if __name__ == "__main__":
    # Get configuration from environment variables
    transport = os.getenv("TRANSPORT", "http")  # Default to HTTP for Docker
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    
    logger.info(f"Starting CSV Analysis FastMCP Server on {transport}://{host}:{port}")
    
    if transport.lower() == "http":
        mcp.run(transport="http", host=host, port=port)
    else:
        # Fallback to stdio
        mcp.run()