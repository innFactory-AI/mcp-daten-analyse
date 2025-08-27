#!/usr/bin/env python3
"""
analyze_csv.py - Detect CSV structure and output TransformSpec JSON

Usage: python analyze_csv.py input.csv output_spec.json
"""
import csv
import json
import sys
import re

def analyze_csv(input_path, output_path):
    """Analyze CSV structure and create TransformSpec"""
    with open(input_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        
        # Read first two rows (headers)
        headers_row1 = next(reader)  # '1 kum', '2 kum', etc.
        headers_row2 = next(reader)  # years: 2025, 2024, etc.
        
        # Factory column is first column
        factory_column = headers_row1[0]
        
        # Extract month/year mapping for data columns (skip factory column)
        columns = []
        for i in range(1, len(headers_row1)):
            col_name = headers_row1[i]
            year_str = headers_row2[i].strip()
            
            # Extract month from column name (e.g., "1 kum" → 1)
            month_match = re.match(r'(\d+)', col_name)
            month = int(month_match.group(1)) if month_match else i
            
            # Parse year
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
    
    # Create TransformSpec
    spec = {
        "factory_column": factory_column,
        "factory_column_index": 0,
        "data_columns": columns,
        "delimiter": ";"
    }
    
    # Write TransformSpec JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(spec, f, indent=2)
    
    print(f"TransformSpec written to {output_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python analyze_csv.py input.csv output_spec.json")
        sys.exit(1)
    
    analyze_csv(sys.argv[1], sys.argv[2])