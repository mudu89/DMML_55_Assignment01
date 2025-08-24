"""
Test the new data flow: Ingestion -> Storage
"""

import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Task2_DataIngestion.ingestion import ingest_all_data
from Task3_RawDataStorage.data_storage import store_multiple_tables

def test_data_flow():
    """Test the complete data flow from ingestion to storage"""
    
    print("🔄 Testing Data Flow: Ingestion -> Storage")
    print("="*50)
    
    try:
        # Step 1: Ingest data (returns DataFrames)
        print("📥 Step 1: Ingesting data from database...")
        ingested_data = ingest_all_data()
        
        print(f"✅ Ingestion complete:")
        print(f"   - Billing records: {ingested_data['billing']['records']}")
        print(f"   - Subscriptions records: {ingested_data['subscriptions']['records']}")
        print(f"   - Total records: {ingested_data['total_records']}")
        
        # Step 2: Store data (writes DataFrames to files)
        print("\n💾 Step 2: Storing data to raw storage...")
        storage_result = store_multiple_tables(ingested_data)
        
        print(f"✅ Storage complete:")
        print(f"   - Tables stored: {storage_result['tables_stored']}")
        print(f"   - Total records stored: {storage_result['total_records']}")
        
        # Show file paths
        print(f"\n📁 Files created:")
        for table, result in storage_result['storage_results'].items():
            print(f"   - {table}: {result['file_path']}")
        
        print(f"\n🎉 Data flow test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    test_data_flow()
