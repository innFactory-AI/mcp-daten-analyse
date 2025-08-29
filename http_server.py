#!/usr/bin/env python3
"""
HTTP Server for CSV Data Analysis File Operations

Exposes three HTTP endpoints for file-based operations:
1. POST /analyze-csv - Detect CSV structure and output TransformSpec
2. POST /transform-csv - Convert wide format to normalized format  
3. POST /load-sqlite - Import normalized data into SQLite
"""

import json
import logging
import csv
import sqlite3
import re
import os
import io
from typing import Optional
from pathlib import Path
from flask import Flask, request, jsonify

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("csv-http-server")

app = Flask(__name__)

def parse_european_number(value_str: str) -> Optional[float]:
    """Convert European number format (1.126.286) to float"""
    if not value_str or value_str.strip() == '':
        return None
    
    cleaned = value_str.strip().replace('.', '')
    try:
        return float(cleaned)
    except ValueError:
        return None

@app.route('/analyze-csv', methods=['POST'])
def analyze_csv():
    """
    Analyze CSV structure with wide headers and save TransformSpec to file.
    
    Request Body:
        {
            "csv_input": "CSV content as string or file path",
            "dataset_name": "Name for the dataset",
            "is_content": true/false (optional, defaults to true)
        }
    
    Returns:
        JSON response with analysis results and file paths
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request must contain JSON data"}), 400
        
        csv_input = data.get('csv_input')
        dataset_name = data.get('dataset_name')
        is_content = data.get('is_content', True)
        
        if not csv_input or not dataset_name:
            return jsonify({"error": "csv_input and dataset_name are required"}), 400
        
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
                return jsonify({"error": f"CSV file not found: {csv_input}"}), 404
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
        
        return jsonify({
            "status": "success",
            "message": f"CSV analysis complete for dataset '{dataset_name}'",
            "dataset_name": dataset_name,
            "csv_file_path": csv_file_path,
            "spec_path": spec_path,
            "columns_found": len(columns),
            "factory_column": factory_column
        })
        
    except Exception as e:
        logger.error(f"Error analyzing CSV: {str(e)}")
        return jsonify({"error": f"Error analyzing CSV: {str(e)}"}), 500

@app.route('/transform-csv', methods=['POST'])
def transform_csv():
    """
    Transform wide format CSV to normalized long format using existing dataset files.
    
    Request Body:
        {
            "dataset_name": "Name of the dataset to transform"
        }
    
    Returns:
        JSON response with transformation results
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request must contain JSON data"}), 400
        
        dataset_name = data.get('dataset_name')
        if not dataset_name:
            return jsonify({"error": "dataset_name is required"}), 400
        
        # Convert dataset name to snake_case
        dataset_name = re.sub(r'[^a-zA-Z0-9_]', '_', dataset_name.lower())
        dataset_name = re.sub(r'_+', '_', dataset_name).strip('_')
        
        # Check if dataset files exist
        spec_path = f'data/{dataset_name}_spec.json'
        if not os.path.exists(spec_path):
            return jsonify({"error": f"Dataset '{dataset_name}' not found. Run analyze-csv first."}), 404
        
        # Load TransformSpec
        with open(spec_path, 'r', encoding='utf-8') as f:
            spec = json.load(f)
        
        csv_path = spec['csv_file_path']
        if not os.path.exists(csv_path):
            return jsonify({"error": f"CSV file not found: {csv_path}"}), 404
        
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
                        monthly_value = parse_european_number(raw_value)
                        
                        normalized_data.append({
                            'factory': factory,
                            'year': col_spec['year'],
                            'month': col_spec['month'], 
                            'monthly_value': monthly_value
                        })
        
        # Write normalized CSV
        normalized_path = f'data/{dataset_name}_normalized.csv'
        with open(normalized_path, 'w', encoding='utf-8', newline='') as f:
            fieldnames = ['factory', 'year', 'month', 'monthly_value']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(normalized_data)
        
        return jsonify({
            "status": "success",
            "message": f"Dataset '{dataset_name}' transformation complete",
            "dataset_name": dataset_name,
            "normalized_path": normalized_path,
            "records_processed": len(normalized_data)
        })
        
    except Exception as e:
        logger.error(f"Error transforming dataset: {str(e)}")
        return jsonify({"error": f"Error transforming dataset: {str(e)}"}), 500

@app.route('/load-sqlite', methods=['POST'])
def load_sqlite():
    """
    Load normalized dataset into SQLite database.
    
    Request Body:
        {
            "dataset_name": "Name of the dataset to load into database"
        }
    
    Returns:
        JSON response with load results
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request must contain JSON data"}), 400
        
        dataset_name = data.get('dataset_name')
        if not dataset_name:
            return jsonify({"error": "dataset_name is required"}), 400
        
        # Convert dataset name to snake_case
        dataset_name = re.sub(r'[^a-zA-Z0-9_]', '_', dataset_name.lower())
        dataset_name = re.sub(r'_+', '_', dataset_name).strip('_')
        
        # Check if normalized CSV exists
        normalized_path = f'data/{dataset_name}_normalized.csv'
        if not os.path.exists(normalized_path):
            return jsonify({"error": f"Normalized dataset '{dataset_name}' not found. Run transform-csv first."}), 404
        
        db_path = f'data/{dataset_name}.db'
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS factory_data (
                factory TEXT NOT NULL,
                year INTEGER,
                month INTEGER,
                monthly_value REAL,
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
                monthly_value = float(row['monthly_value']) if row['monthly_value'] else None
                cursor.execute('''
                    INSERT OR REPLACE INTO factory_data (factory, year, month, monthly_value)
                    VALUES (?, ?, ?, ?)
                ''', (row['factory'], int(row['year']), int(row['month']), monthly_value))

        # # Create derived table
        # cursor.execute('DROP TABLE IF EXISTS monthly_values')
        # cursor.execute('''
        #     CREATE TABLE monthly_values AS
        #     SELECT 
        #         f1.factory,
        #         f1.year,
        #         f1.month,
        #         f1.ytd_value,
        #         CASE 
        #             WHEN f1.month = 1 THEN f1.ytd_value
        #             ELSE f1.ytd_value - COALESCE(f2.ytd_value, 0)
        #         END as month_value
        #     FROM factory_data f1
        #     LEFT JOIN factory_data f2 ON 
        #         f1.factory = f2.factory AND 
        #         f1.year = f2.year AND 
        #         f1.month = f2.month + 1
        #     ORDER BY f1.factory, f1.year, f1.month
        # ''')
        
        conn.commit()
        
        # Get summary
        cursor.execute('SELECT COUNT(*) FROM factory_data')
        count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT factory) FROM factory_data')  
        factories = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            "status": "success",
            "message": f"Dataset '{dataset_name}' loaded successfully into database",
            "dataset_name": dataset_name,
            "database_path": db_path,
            "records_loaded": count,
            "factories_count": factories,
            "tables_created": ["factory_data"]
        })
        
    except Exception as e:
        logger.error(f"Error loading dataset: {str(e)}")
        return jsonify({"error": f"Error loading dataset: {str(e)}"}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "CSV Analysis HTTP Server"})

if __name__ == "__main__":
    # Get configuration from environment variables
    host = os.getenv("HTTP_HOST", "0.0.0.0")
    port = int(os.getenv("HTTP_PORT", "8001"))
    
    logger.info(f"Starting CSV Analysis HTTP Server on http://{host}:{port}")
    
    app.run(host=host, port=port, debug=False)