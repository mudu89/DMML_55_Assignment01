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

# def ingest_billing():
#     logger=get_logger("billing", log_file="logs/ingestion/ingestion.log")
#     try:
#         conn = sqlite3.connect(db_path)
#         df = pd.read_sql_query("SELECT * FROM billing", conn)
#         conn.close()

#         today = date.today().isoformat()
#         out_dir = f"data/raw/billing/dt={today}"
#         os.makedirs(out_dir, exist_ok=True)

#         out_file = os.path.join(out_dir, "billing.csv")
#         df.to_csv(out_file, index=False)

#         logger.info(f"Billing data ingested successfully: {out_file}")
#         print(f"[SUCCESS] Billing data ingested → {out_file}")

#     except Exception as e:
#         logger.error(f"Failed to ingest billing: {e}")
#         print(f"[ERROR] {e}")

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

# def ingest_subscriptions():
#     logger = get_logger("subscriptions", log_file="logs/ingestion/ingestion.log")
#     try:
#         conn = sqlite3.connect(db_path)
#         df = pd.read_sql_query("SELECT * FROM subscriptions", conn)
#         conn.close()

#         today = date.today().isoformat()
#         out_dir = f"data/raw/subscriptions/dt={today}"
#         os.makedirs(out_dir, exist_ok=True)

#         out_file = os.path.join(out_dir, "subscriptions.csv")
#         df.to_csv(out_file, index=False)

#         logger.info(f"Subscriptions data ingested successfully: {out_file}")
#         print(f"[SUCCESS] Subscriptions data ingested → {out_file}")

#     except Exception as e:
#         logger.error(f"Failed to ingest subscriptions: {e}")
#         print(f"[ERROR] {e}")

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

def ingest_all_data():
    """
    Ingest all data sources and return combined results for pipeline use
    """

    logger = get_logger("ingestion", log_file=log_file_path)

    try:
        logger.info("Starting data ingestion for DB source data...")

        
        # Ingest billing data
        billing_data = ingest_billing_data()
        
        # Ingest subscriptions data  
        subscriptions_data = ingest_subscriptions_data()
        
        # Combine results
        result = {
            'data': [billing_data, subscriptions_data],
            'status': 'success',
            'total_records': billing_data['records'] + subscriptions_data['records']
        }
        
        logger.info(f"All data ingested successfully. Total records: {result['total_records']}")
        return result
        
    except Exception as e:
        logger.error(f"Failed to ingest all data: {e}")
        raise

if __name__ == "__main__":
    ingest_all_data()
    