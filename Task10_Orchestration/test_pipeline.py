"""
Quick test script to run just the ingestion task
"""

import sys
import os

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from ml_pipeline import task_data_ingestion

if __name__ == "__main__":
    print("🧪 Testing ingestion task...")
    try:
        result = task_data_ingestion()
        print(f"✅ Test passed: {result}")
    except Exception as e:
        print(f"❌ Test failed: {e}")
