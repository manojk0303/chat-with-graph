#!/usr/bin/env python3
"""
Ingest script for loading JSONL files from a directory structure into DuckDB.

Each subdirectory in the data directory is treated as a table name.
All *.jsonl files in each subdirectory are loaded into that table.

Usage:
    python ingest.py --data-dir /path/to/data --db-path /path/to/o2c.db
"""

import argparse
import duckdb
from pathlib import Path
from glob import glob


def ingest(data_dir: str, db_path: str):
    """
    Ingest all JSONL files from data_dir into DuckDB.
    
    Args:
        data_dir: Root directory containing subdirectories (one per table)
        db_path: Path to DuckDB database file
    """
    data_path = Path(data_dir)
    
    if not data_path.is_dir():
        print(f"Error: {data_dir} is not a directory")
        return
    
    conn = duckdb.connect(db_path)
    
    # Get all subdirectories in data_dir
    subdirs = sorted([d for d in data_path.iterdir() if d.is_dir()])
    
    if not subdirs:
        print(f"Error: No subdirectories found in {data_dir}")
        return
    
    print(f"Loading data from {data_dir} into {db_path}")
    print("-" * 70)
    
    for subdir in subdirs:
        table_name = subdir.name
        
        # Glob all *.jsonl files in this subdirectory
        jsonl_files = glob(str(subdir / "*.jsonl"))
        
        if not jsonl_files:
            print(f"⚠️  {table_name:40s} | No JSONL files found")
            continue
        
        # Create a glob pattern for DuckDB's read_ndjson_auto
        glob_pattern = str(subdir / "*.jsonl")
        
        try:
            # Create or replace the table
            sql = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_ndjson_auto('{glob_pattern}', ignore_errors=true)
            """
            conn.execute(sql)
            
            # Get row count
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = result[0] if result else 0
            
            print(f"✓ {table_name:40s} | {row_count:>10,} rows")
        
        except Exception as e:
            print(f"✗ {table_name:40s} | Error: {e}")
    
    print("-" * 70)
    print("Ingestion complete!")
    
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Ingest JSONL files from a directory structure into DuckDB"
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Root directory containing subdirectories (one per table)"
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database file"
    )
    
    args = parser.parse_args()
    
    ingest(args.data_dir, args.db_path)


if __name__ == "__main__":
    main()
