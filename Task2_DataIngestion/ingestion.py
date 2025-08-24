import os
import sqlite3
import pandas as pd
from datetime import date
import sys
import os

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
print(sys.path)
from utils.logger import get_logger
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "sources", "telecom.db"))
log_file_path = os.path.join(project_root, "logs","ingestion.log")

def ingest_billing_data():
    """Read billing data from DB and return DataFrame for pipeline use"""
    
    logger = get_logger("billing", log_file=log_file_path)

    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM billing", conn)
        conn.close()
        
        logger.info(f"Billing data loaded from DB: {len(df)} records")
        return {
            "data": df,
            "table": "billing",
            "records": len(df),
            "ingestion_date": date.today().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to ingest billing data: {e}")
        raise

def ingest_subscriptions_data():
    """Read subscriptions data from DB and return DataFrame for pipeline use"""
    logger = get_logger("subscriptions", log_file=log_file_path)
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM subscriptions", conn)
        conn.close()
        
        logger.info(f"Subscriptions data loaded from DB: {len(df)} records")
        return {
            "data": df,
            "table": "subscriptions",
            "records": len(df),
            "ingestion_date": date.today().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to ingest subscriptions data: {e}")
        raise

def ingest_crm_data():
    """Read CRM data from CSV and return DataFrame for pipeline use"""
    logger = get_logger("crm", log_file=log_file_path)
    try:
        # Path to CRM CSV file
        crm_path = os.path.join(os.path.dirname(__file__), "sources", "crm.csv")
        
        if not os.path.exists(crm_path):
            raise FileNotFoundError(f"CRM data file not found: {crm_path}")
        
        # Read CRM data from CSV
        df = pd.read_csv(crm_path)
        
        # Convert datetime column
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        logger.info(f"CRM data loaded from CSV: {len(df)} records")
        return {
            "data": df,
            "table": "crm",
            "records": len(df),
            "ingestion_date": date.today().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to ingest CRM data: {e}")
        raise

def ingest_all_data():
    """
    Ingest all data sources and return combined results for pipeline use
    """

    logger = get_logger("ingestion", log_file=log_file_path)

    try:
        logger.info("Starting data ingestion for all data sources...")

        
        # Ingest billing data (from SQLite DB)
        billing_data = ingest_billing_data()
        
        # Ingest subscriptions data (from SQLite DB)
        subscriptions_data = ingest_subscriptions_data()
        
        # Ingest CRM data (from CSV)
        crm_data = ingest_crm_data()
        
        # Combine results
        all_data = [billing_data, subscriptions_data, crm_data]
        total_records = sum(data['records'] for data in all_data)
        
        result = {
            'data': all_data,
            'status': 'success',
            'total_records': total_records
        }
        
        logger.info(f"All data ingested successfully. Sources: {len(all_data)}, Total records: {total_records}")
        logger.info(f"Data sources: billing ({billing_data['records']}), subscriptions ({subscriptions_data['records']}), crm ({crm_data['records']})")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to ingest all data: {e}")
        raise

if __name__ == "__main__":
    ingest_all_data()
    