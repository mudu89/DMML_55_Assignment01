"""
Task 3 - Raw Data Storage
Handles writing DataFrames to organized data lake structure
"""

import os
import pandas as pd
from datetime import date
import sys

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
from utils.logger import get_logger

logger = get_logger("data_storage", log_file=os.path.join(project_root, "logs", "data_storage.log"))

def store_dataframe_to_raw(data_dict, table_name=None):
    """
    Store a DataFrame to the raw data storage with proper partitioning
    
    Args:
        data_dict: Dictionary containing 'data' (DataFrame) and metadata
        table_name: Optional override for table name (uses data_dict['table'] if not provided)
    
    Returns:
        Dictionary with storage results and metadata
    """
    try:
        # Extract DataFrame and metadata
        df = data_dict['data']
        table = table_name or data_dict.get('table', 'unknown')
        ingestion_date = data_dict.get('ingestion_date', date.today().isoformat())
        
        # Create partitioned directory structure (relative to project root)
        out_dir = os.path.join(project_root, "data", "raw", table, f"dt={ingestion_date}")
        os.makedirs(out_dir, exist_ok=True)
        
        # Define output file path
        out_file = os.path.join(out_dir, f"{table}.csv")
        
        # Write DataFrame to CSV
        df.to_csv(out_file, index=False)
        
        # Log success
        logger.info(f"Data stored successfully: {out_file} ({len(df)} records)")
        
        return {
            "status": "success",
            "table": table,
            "file_path": out_file,
            "records_stored": len(df),
            "storage_date": ingestion_date,
            "directory": out_dir
        }
        
    except Exception as e:
        logger.error(f"Failed to store {table} data: {e}")
        raise

def store_multiple_tables(ingested_data):
    """
    Store multiple tables from ingestion results
    
    Args:
        ingested_data: Dictionary containing multiple table data
        
    Returns:
        Dictionary with all storage results
    """
    storage_results = {}
    
    try:
        
        for source in ingested_data:
            logger.info(f"Storing {source['table']} data...")
            result = store_dataframe_to_raw(source, source['table'])
            storage_results[source['table']] = result
        
        logger.info(f"All tables stored successfully: {list(storage_results.keys())}")
        return {
            "status": "success",
            "tables_stored": list(storage_results.keys()),
            "storage_results": storage_results,
            "total_records": sum(r["records_stored"] for r in storage_results.values())
        }
        
    except Exception as e:
        logger.error(f"Failed to store multiple tables: {e}")
        raise

if __name__ == "__main__":
    # Test the storage functions
    import pandas as pd
    
    # Create test data
    test_data = {
        "data": pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]}),
        "table": "test_table",
        "records": 3,
        "ingestion_date": date.today().isoformat()
    }
    
    # Test storage
    result = store_dataframe_to_raw(test_data)
    print(f"Test storage result: {result}")
