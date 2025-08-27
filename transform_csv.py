#!/usr/bin/env python3
"""
transform_csv.py - Transform wide format CSV to normalized long format

Usage: python transform_csv.py input.csv transform_spec.json output.csv [--json]
"""
import csv
import json
import sys
import argparse

def parse_european_number(value_str):
    """Convert European number format (1.126.286) to float"""
    if not value_str or value_str.strip() == '':
        return None
    
    # Remove thousand separators (periods) and convert to float
    cleaned = value_str.strip().replace('.', '')
    try:
        return float(cleaned)
    except ValueError:
        return None

def transform_csv(input_path, spec_path, output_path, output_json=False):
    """Transform wide CSV to normalized format using TransformSpec"""
    
    # Load TransformSpec
    with open(spec_path, 'r', encoding='utf-8') as f:
        spec = json.load(f)
    
    # Read CSV and transform
    normalized_data = []
    
    with open(input_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=spec['delimiter'])
        
        # Skip header rows
        next(reader)  # Skip first header row
        next(reader)  # Skip second header row
        
        # Process data rows
        for row in reader:
            if not row or not row[0].strip():  # Skip empty rows
                continue
                
            factory = row[spec['factory_column_index']]
            
            # Process each data column
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
        # Write CSV
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            fieldnames = ['factory', 'year', 'month', 'ytd_value']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(normalized_data)
    
    print(f"Normalized data written to {output_path} ({len(normalized_data)} records)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_csv', help='Input CSV file')
    parser.add_argument('transform_spec', help='TransformSpec JSON file')
    parser.add_argument('output', help='Output file')
    parser.add_argument('--json', action='store_true', help='Output as JSON instead of CSV')
    
    args = parser.parse_args()
    transform_csv(args.input_csv, args.transform_spec, args.output, args.json)