#!/usr/bin/env python3
"""
query_sqlite.py - Execute SELECT queries on SQLite database

Usage: python query_sqlite.py database.db "SELECT query"
"""
import sqlite3
import sys
import re

def is_safe_query(query):
    """Check if query is a safe SELECT statement"""
    # Remove comments and normalize whitespace
    cleaned = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    cleaned = ' '.join(cleaned.split())
    
    # Must start with SELECT (case insensitive)
    if not re.match(r'^\s*SELECT\s', cleaned, re.IGNORECASE):
        return False
    
    # Check for dangerous statements (case insensitive)
    dangerous = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'PRAGMA']
    for keyword in dangerous:
        if re.search(rf'\b{keyword}\b', cleaned, re.IGNORECASE):
            return False
    
    return True

def query_sqlite(db_path, query):
    """Execute SELECT query and print results"""
    
    # Safety check
    if not is_safe_query(query):
        print("Error: Only SELECT statements are allowed", file=sys.stderr)
        sys.exit(1)
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Execute query
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Get column names
        column_names = [description[0] for description in cursor.description]
        
        # Print results
        if results:
            # Print header
            print('\t'.join(column_names))
            print('-' * (len('\t'.join(column_names))))
            
            # Print data rows
            for row in results:
                formatted_row = []
                for value in row:
                    if value is None:
                        formatted_row.append('NULL')
                    else:
                        formatted_row.append(str(value))
                print('\t'.join(formatted_row))
            
            print(f"\n{len(results)} row(s) returned")
        else:
            print("No results found")
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

def show_schema(db_path):
    """Show database schema"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("Database Schema:")
    print("================")
    
    # Get tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    for table_name, in tables:
        print(f"\nTable: {table_name}")
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
            print(f"  {col_name}: {col_type}{constraint_str}")
    
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python query_sqlite.py database.db [query]")
        print("       python query_sqlite.py database.db --schema")
        sys.exit(1)
    
    db_path = sys.argv[1]
    
    if len(sys.argv) == 2:
        print("Interactive mode - showing schema. Provide a query as second argument.")
        show_schema(db_path)
    elif sys.argv[2] == '--schema':
        show_schema(db_path)
    else:
        query = sys.argv[2]
        query_sqlite(db_path, query)