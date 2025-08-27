#!/usr/bin/env python3
"""
load_sqlite.py - Load normalized CSV/JSON into SQLite database

Usage: python load_sqlite.py input_file database.db [--json]
"""
import csv
import json
import sqlite3
import sys
import argparse

def load_sqlite(input_path, db_path, is_json=False):
    """Load normalized data into SQLite database"""
    
    # Connect to database
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
    
    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_factory ON factory_data(factory)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_year_month ON factory_data(year, month)')
    
    # Load data
    if is_json:
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Insert data
        for record in data:
            cursor.execute('''
                INSERT OR REPLACE INTO factory_data (factory, year, month, ytd_value)
                VALUES (?, ?, ?, ?)
            ''', (record['factory'], record['year'], record['month'], record['ytd_value']))
    
    else:
        # Read CSV
        with open(input_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                ytd_value = float(row['ytd_value']) if row['ytd_value'] else None
                cursor.execute('''
                    INSERT OR REPLACE INTO factory_data (factory, year, month, ytd_value)
                    VALUES (?, ?, ?, ?)
                ''', (row['factory'], int(row['year']), int(row['month']), ytd_value))
    
    # Create derived column for monthly values (difference between consecutive months)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monthly_values AS
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
    
    # Commit and close
    conn.commit()
    
    # Print summary
    cursor.execute('SELECT COUNT(*) FROM factory_data')
    count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT factory) FROM factory_data')
    factories = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"Loaded {count} records for {factories} factories into {db_path}")
    print("Created tables: factory_data (main), monthly_values (derived)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='Input CSV or JSON file')
    parser.add_argument('database', help='SQLite database file')
    parser.add_argument('--json', action='store_true', help='Input is JSON format')
    
    args = parser.parse_args()
    load_sqlite(args.input_file, args.database, args.json)