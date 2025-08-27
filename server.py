#!/usr/bin/env python3
"""
FastMCP Server for CSV Data Analysis Tools

Exposes four tools:
1. analyze_csv - Detect CSV structure and output TransformSpec
2. transform_csv - Convert wide format to normalized format  
3. load_sqlite - Import normalized data into SQLite
4. query_sqlite - Execute SELECT queries on SQLite database
"""

import json
import logging
import csv
import sqlite3
import re
import os
import io
from typing import Optional, Union
from pathlib import Path

from fastmcp import FastMCP

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("csv-analysis-server")

# Initialize FastMCP server
mcp = FastMCP("CSV Analysis Server 📊")

def parse_european_number(value_str: str) -> Optional[float]:
    """Convert European number format (1.126.286) to float"""
    if not value_str or value_str.strip() == '':
        return None
    
    cleaned = value_str.strip().replace('.', '')
    try:
        return float(cleaned)
    except ValueError:
        return None

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
def analyze_csv(csv_input: str, dataset_name: str, is_content: bool = True) -> str:
    """
    Analyze CSV structure with wide headers and save TransformSpec to file.
    
    Args:
        csv_input: Either CSV content as string or path to CSV file
        dataset_name: Name for the dataset (will be converted to snake_case)
        is_content: If True, csv_input is treated as CSV content; if False, as file path (default: True)
    
    Returns:
        Status message with analysis results and file paths
    """
    try:
        # Convert dataset name to snake_case
        dataset_name = re.sub(r'[^a-zA-Z0-9_]', '_', dataset_name.lower())
        dataset_name = re.sub(r'_+', '_', dataset_name).strip('_')
        
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Handle input based on type
        if is_content:
            # Parse CSV content directly and save to file for consistency
            csv_file_path = f'data/{dataset_name}_raw.csv'
            with open(csv_file_path, 'w', encoding='utf-8') as f:
                f.write(csv_input)
            csv_data = io.StringIO(csv_input)
            reader = csv.reader(csv_data, delimiter=';')
        else:
            # Read from file path
            if not os.path.exists(csv_input):
                raise FileNotFoundError(f"CSV file not found: {csv_input}")
            # Copy to data directory with dataset naming
            csv_file_path = f'data/{dataset_name}_raw.csv'
            import shutil
            shutil.copy2(csv_input, csv_file_path)
            with open(csv_input, 'r', encoding='utf-8') as f:
                csv_content = f.read()
            csv_data = io.StringIO(csv_content)
            reader = csv.reader(csv_data, delimiter=';')
        
        headers_row1 = next(reader)  # '1 kum', '2 kum', etc.
        headers_row2 = next(reader)  # years: 2025, 2024, etc.
        
        factory_column = headers_row1[0]
        
        columns = []
        for i in range(1, len(headers_row1)):
            col_name = headers_row1[i]
            year_str = headers_row2[i].strip()
            
            month_match = re.match(r'(\d+)', col_name)
            month = int(month_match.group(1)) if month_match else i
            
            try:
                year = int(year_str)
            except ValueError:
                year = None
            
            columns.append({
                "column_index": i,
                "column_name": col_name, 
                "month": month,
                "year": year
            })
        
        spec = {
            "dataset_name": dataset_name,
            "csv_file_path": csv_file_path,
            "factory_column": factory_column,
            "factory_column_index": 0,
            "data_columns": columns,
            "delimiter": ";"
        }
        
        # Save spec to file
        spec_path = f'data/{dataset_name}_spec.json'
        with open(spec_path, 'w', encoding='utf-8') as f:
            json.dump(spec, f, indent=2)
        
        return f"CSV analysis complete for dataset '{dataset_name}'\n" + \
               f"- Raw CSV saved: {csv_file_path}\n" + \
               f"- TransformSpec saved: {spec_path}\n" + \
               f"- Found {len(columns)} data columns for factory column '{factory_column}'"
        
    except Exception as e:
        raise Exception(f"Error analyzing CSV: {str(e)}")

@mcp.tool
def transform_csv(dataset_name: str) -> str:
    """
    Transform wide format CSV to normalized long format using existing dataset files.
    
    Args:
        dataset_name: Name of the dataset to transform (snake_case)
    
    Returns:
        Status message with transformation results
    """
    try:
        # Convert dataset name to snake_case
        dataset_name = re.sub(r'[^a-zA-Z0-9_]', '_', dataset_name.lower())
        dataset_name = re.sub(r'_+', '_', dataset_name).strip('_')
        
        # Check if dataset files exist
        spec_path = f'data/{dataset_name}_spec.json'
        if not os.path.exists(spec_path):
            raise FileNotFoundError(f"Dataset '{dataset_name}' not found. Run analyze_csv first.")
        
        # Load TransformSpec
        with open(spec_path, 'r', encoding='utf-8') as f:
            spec = json.load(f)
        
        csv_path = spec['csv_file_path']
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        # Process CSV
        normalized_data = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=spec['delimiter'])
            
            # Skip header rows
            next(reader)
            next(reader)
            
            # Process data rows
            for row in reader:
                if not row or not row[0].strip():
                    continue
                    
                factory = row[spec['factory_column_index']]
                
                for col_spec in spec['data_columns']:
                    col_idx = col_spec['column_index']
                    if col_idx < len(row):
                        raw_value = row[col_idx]
                        ytd_value = parse_european_number(raw_value)
                        
                        normalized_data.append({
                            'factory': factory,
                            'year': col_spec['year'],
                            'month': col_spec['month'], 
                            'ytd_value': ytd_value
                        })
        
        # Write normalized CSV
        normalized_path = f'data/{dataset_name}_normalized.csv'
        with open(normalized_path, 'w', encoding='utf-8', newline='') as f:
            fieldnames = ['factory', 'year', 'month', 'ytd_value']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(normalized_data)
        
        return f"Dataset '{dataset_name}' transformation complete\n" + \
               f"- Normalized data saved: {normalized_path}\n" + \
               f"- {len(normalized_data)} records processed"
        
    except Exception as e:
        raise Exception(f"Error transforming dataset: {str(e)}")

@mcp.tool
def load_sqlite(dataset_name: str) -> str:
    """
    Load normalized dataset into SQLite database.
    
    Args:
        dataset_name: Name of the dataset to load into database
    
    Returns:
        Status message with load results
    """
    try:
        # Convert dataset name to snake_case
        dataset_name = re.sub(r'[^a-zA-Z0-9_]', '_', dataset_name.lower())
        dataset_name = re.sub(r'_+', '_', dataset_name).strip('_')
        
        # Check if normalized CSV exists
        normalized_path = f'data/{dataset_name}_normalized.csv'
        if not os.path.exists(normalized_path):
            raise FileNotFoundError(f"Normalized dataset '{dataset_name}' not found. Run transform_csv first.")
        
        db_path = f'data/{dataset_name}.db'
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS factory_data (
                factory TEXT NOT NULL,
                year INTEGER,
                month INTEGER,
                ytd_value REAL,
                PRIMARY KEY (factory, year, month)
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_factory ON factory_data(factory)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_year_month ON factory_data(year, month)')
        
        # Load data from normalized CSV
        with open(normalized_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                ytd_value = float(row['ytd_value']) if row['ytd_value'] else None
                cursor.execute('''
                    INSERT OR REPLACE INTO factory_data (factory, year, month, ytd_value)
                    VALUES (?, ?, ?, ?)
                ''', (row['factory'], int(row['year']), int(row['month']), ytd_value))
        
        # Create derived table
        cursor.execute('DROP TABLE IF EXISTS monthly_values')
        cursor.execute('''
            CREATE TABLE monthly_values AS
            SELECT 
                f1.factory,
                f1.year,
                f1.month,
                f1.ytd_value,
                CASE 
                    WHEN f1.month = 1 THEN f1.ytd_value
                    ELSE f1.ytd_value - COALESCE(f2.ytd_value, 0)
                END as month_value
            FROM factory_data f1
            LEFT JOIN factory_data f2 ON 
                f1.factory = f2.factory AND 
                f1.year = f2.year AND 
                f1.month = f2.month + 1
            ORDER BY f1.factory, f1.year, f1.month
        ''')
        
        conn.commit()
        
        # Get summary
        cursor.execute('SELECT COUNT(*) FROM factory_data')
        count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT factory) FROM factory_data')  
        factories = cursor.fetchone()[0]
        
        conn.close()
        
        return f"Dataset '{dataset_name}' loaded successfully into database\n" + \
               f"- Database: {db_path}\n" + \
               f"- Records: {count}, Factories: {factories}\n" + \
               f"- Tables created: factory_data, monthly_values"
        
    except Exception as e:
        raise Exception(f"Error loading dataset: {str(e)}")

@mcp.tool
def query_sqlite(dataset_name: str, query: str = "", show_schema: bool = False) -> str:
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