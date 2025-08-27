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
def analyze_csv(csv_input: str, is_content: bool = False) -> str:
    """
    Analyze CSV structure with wide headers and return TransformSpec JSON.
    
    Args:
        csv_input: Either path to CSV file or CSV content as string
        is_content: If True, csv_input is treated as CSV content; if False, as file path
    
    Returns:
        JSON string containing the TransformSpec
    """
    try:
        # Handle input based on type
        if is_content:
            # Parse CSV content directly
            import io
            csv_data = io.StringIO(csv_input)
            reader = csv.reader(csv_data, delimiter=';')
        else:
            # Read from file path
            if not os.path.exists(csv_input):
                raise FileNotFoundError(f"CSV file not found: {csv_input}")
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
            "factory_column": factory_column,
            "factory_column_index": 0,
            "data_columns": columns,
            "delimiter": ";"
        }
        
        return json.dumps(spec, indent=2)
        
    except Exception as e:
        raise Exception(f"Error analyzing CSV: {str(e)}")

@mcp.tool
def transform_csv(csv_input: str, spec_input: str, is_csv_content: bool = False, is_spec_content: bool = False, output_path: str = "normalized.csv", output_json: bool = False) -> str:
    """
    Transform wide format CSV to normalized long format using TransformSpec.
    
    Args:
        csv_input: Either path to CSV file or CSV content as string
        spec_input: Either path to TransformSpec JSON file or JSON content as string
        is_csv_content: If True, csv_input is treated as CSV content; if False, as file path
        is_spec_content: If True, spec_input is treated as JSON content; if False, as file path
        output_path: Path for output normalized file
        output_json: Output as JSON instead of CSV
    
    Returns:
        Status message with transformation results
    """
    try:
        # Load TransformSpec
        if is_spec_content:
            spec = json.loads(spec_input)
        else:
            if not os.path.exists(spec_input):
                raise FileNotFoundError(f"TransformSpec file not found: {spec_input}")
            with open(spec_input, 'r', encoding='utf-8') as f:
                spec = json.load(f)
        
        # Handle CSV input
        if is_csv_content:
            csv_data = io.StringIO(csv_input)
            reader = csv.reader(csv_data, delimiter=spec['delimiter'])
        else:
            if not os.path.exists(csv_input):
                raise FileNotFoundError(f"CSV file not found: {csv_input}")
            with open(csv_input, 'r', encoding='utf-8') as f:
                csv_content = f.read()
            csv_data = io.StringIO(csv_content)
            reader = csv.reader(csv_data, delimiter=spec['delimiter'])
        
        normalized_data = []
        
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
        
        # Write output
        if output_json:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(normalized_data, f, indent=2)
        else:
            with open(output_path, 'w', encoding='utf-8', newline='') as f:
                fieldnames = ['factory', 'year', 'month', 'ytd_value']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(normalized_data)
        
        return f"CSV transformation complete. {len(normalized_data)} records written to {output_path}"
        
    except Exception as e:
        raise Exception(f"Error transforming CSV: {str(e)}")

@mcp.tool
def load_sqlite(data_path: str, db_path: str = "data.db", is_json: bool = False) -> str:
    """
    Load normalized CSV/JSON data into SQLite database.
    
    Args:
        data_path: Path to normalized CSV or JSON file
        db_path: SQLite database path
        is_json: Input file is JSON format
    
    Returns:
        Status message with load results
    """
    try:
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Data file not found: {data_path}")
            
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
        
        # Load data
        if is_json:
            with open(data_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for record in data:
                cursor.execute('''
                    INSERT OR REPLACE INTO factory_data (factory, year, month, ytd_value)
                    VALUES (?, ?, ?, ?)
                ''', (record['factory'], record['year'], record['month'], record['ytd_value']))
        else:
            with open(data_path, 'r', encoding='utf-8') as f:
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
        
        return f"Data loaded successfully into {db_path}\nRecords: {count}, Factories: {factories}\nTables created: factory_data, monthly_values"
        
    except Exception as e:
        raise Exception(f"Error loading data: {str(e)}")

@mcp.tool
def query_sqlite(db_path: str, query: str = "", show_schema: bool = False) -> str:
    """
    Execute SELECT queries on SQLite database (read-only).
    
    Args:
        db_path: Path to SQLite database file
        query: SELECT query to execute
        show_schema: Show database schema instead of running query
    
    Returns:
        Query results or schema information
    """
    try:
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
            
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        if show_schema or not query:
            # Show schema
            result_text = "Database Schema:\n================\n"
            
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
        raise Exception(f"Error querying database: {str(e)}")

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