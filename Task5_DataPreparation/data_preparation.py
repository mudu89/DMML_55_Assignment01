"""
Task 5: Data Preparation
Clean and merge raw data into a single, clean master dataset
Focus: Data cleaning, basic joining, simple EDA - NO feature engineering
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date
import glob
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import base64
from io import BytesIO

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from utils.logger import get_logger

# Set plotting style
plt.style.use('default')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10

# Initialize logger
logger = get_logger("data_preparation", log_file=os.path.join(project_root, "logs", "data_preparation.log"))

class DataPreparation:
    """Data cleaning and basic preparation class"""
    
    def __init__(self):
        self.cleaning_summary = {}
        self.data_quality_issues = []
        self.eda_insights = {}
    
    def load_latest_validated_data(self):
        """Load the most recent validated data from the data lake"""
        try:
            data = {}
            data_root = os.path.join(project_root, "data", "raw")
            
            # Find the latest date partition
            latest_date = None
            for table_dir in os.listdir(data_root):
                table_path = os.path.join(data_root, table_dir)
                if os.path.isdir(table_path):
                    for date_dir in os.listdir(table_path):
                        if date_dir.startswith("dt="):
                            date_str = date_dir.replace("dt=", "")
                            if latest_date is None or date_str > latest_date:
                                latest_date = date_str
            
            if latest_date is None:
                raise FileNotFoundError("No dated partitions found in data lake")
            
            logger.info(f"Loading data from latest partition: {latest_date}")
            
            # Load data from latest partition
            for table_dir in os.listdir(data_root):
                table_path = os.path.join(data_root, table_dir, f"dt={latest_date}")
                if os.path.isdir(table_path):
                    csv_files = glob.glob(os.path.join(table_path, "*.csv"))
                    if csv_files:
                        df = pd.read_csv(csv_files[0])
                        data[table_dir] = df
                        logger.info(f"Loaded {table_dir}: {df.shape}")
            
            return data, latest_date
            
        except Exception as e:
            logger.error(f"Error loading validated data: {str(e)}")
            raise
    
    def clean_billing_data(self, billing_df):
        """Clean and prepare billing data"""
        logger.info("Cleaning billing data...")
        
        cleaned_df = billing_df.copy()
        original_count = len(cleaned_df)
        
        # Convert date columns
        if 'billing_date' in cleaned_df.columns:
            cleaned_df['billing_date'] = pd.to_datetime(cleaned_df['billing_date'], errors='coerce')
        
        # Handle missing values
        if 'amount' in cleaned_df.columns:
            # Remove records with negative or zero billing amounts
            before_count = len(cleaned_df)
            cleaned_df = cleaned_df[cleaned_df['amount'] > 0]
            removed_count = before_count - len(cleaned_df)
            if removed_count > 0:
                self.data_quality_issues.append(f"Removed {removed_count} billing records with invalid amounts")
        
        # Handle customer_id standardization
        if 'customer_id' in cleaned_df.columns:
            # Remove any null customer IDs
            before_count = len(cleaned_df)
            cleaned_df = cleaned_df.dropna(subset=['customer_id'])
            removed_count = before_count - len(cleaned_df)
            if removed_count > 0:
                self.data_quality_issues.append(f"Removed {removed_count} billing records with missing customer_id")
        
        self.cleaning_summary['billing'] = {
            'original_records': original_count,
            'cleaned_records': len(cleaned_df),
            'records_removed': original_count - len(cleaned_df)
        }
        
        logger.info(f"Billing data cleaned: {original_count} to {len(cleaned_df)} records")
        return cleaned_df
    
    def clean_subscriptions_data(self, subscriptions_df):
        """Clean and prepare subscriptions data"""
        logger.info("Cleaning subscriptions data...")
        
        cleaned_df = subscriptions_df.copy()
        original_count = len(cleaned_df)
        
        # Convert date columns
        date_columns = ['subscription_start', 'subscription_end', 'start_date', 'end_date']
        for col in date_columns:
            if col in cleaned_df.columns:
                cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')
        
        # Standardize status values
        if 'status' in cleaned_df.columns:
            cleaned_df['status'] = cleaned_df['status'].str.lower().str.strip()
            # Map common variations
            status_mapping = {
                'act': 'active',
                'activated': 'active',
                'inactive': 'inactive',
                'cancelled': 'cancelled',
                'suspended': 'suspended'
            }
            cleaned_df['status'] = cleaned_df['status'].replace(status_mapping)
        
        # Remove records with missing customer_id
        if 'customer_id' in cleaned_df.columns:
            before_count = len(cleaned_df)
            cleaned_df = cleaned_df.dropna(subset=['customer_id'])
            removed_count = before_count - len(cleaned_df)
            if removed_count > 0:
                self.data_quality_issues.append(f"Removed {removed_count} subscription records with missing customer_id")
        
        self.cleaning_summary['subscriptions'] = {
            'original_records': original_count,
            'cleaned_records': len(cleaned_df),
            'records_removed': original_count - len(cleaned_df)
        }
        
        logger.info(f"Subscriptions data cleaned: {original_count} to {len(cleaned_df)} records")
        return cleaned_df
    
    def clean_crm_data(self, crm_df):
        """Clean and prepare CRM data"""
        logger.info("Cleaning CRM data...")
        
        cleaned_df = crm_df.copy()
        original_count = len(cleaned_df)
        
        # Convert datetime columns
        if 'created_at' in cleaned_df.columns:
            cleaned_df['created_at'] = pd.to_datetime(cleaned_df['created_at'], errors='coerce')
        
        # Standardize request types and status
        if 'request_type' in cleaned_df.columns:
            cleaned_df['request_type'] = cleaned_df['request_type'].str.lower().str.strip()
        
        if 'status' in cleaned_df.columns:
            cleaned_df['status'] = cleaned_df['status'].str.lower().str.strip()
        
        # Standardize disconnect reasons (only for disconnect requests)
        if 'disconnect_reason' in cleaned_df.columns:
            cleaned_df['disconnect_reason'] = cleaned_df['disconnect_reason'].str.lower().str.strip()
        
        # Standardize request reasons (for non-disconnect requests)
        if 'request_reason' in cleaned_df.columns:
            cleaned_df['request_reason'] = cleaned_df['request_reason'].str.lower().str.strip()
        
        # Remove records with missing customer_id
        if 'customer_id' in cleaned_df.columns:
            before_count = len(cleaned_df)
            cleaned_df = cleaned_df.dropna(subset=['customer_id'])
            removed_count = before_count - len(cleaned_df)
            if removed_count > 0:
                self.data_quality_issues.append(f"Removed {removed_count} CRM records with missing customer_id")
        
        self.cleaning_summary['crm'] = {
            'original_records': original_count,
            'cleaned_records': len(cleaned_df),
            'records_removed': original_count - len(cleaned_df)
        }
        
        logger.info(f"CRM data cleaned: {original_count} to {len(cleaned_df)} records")
        return cleaned_df
    
    def create_churn_labels(self, crm_df):
        """Create simple churn labels from CRM disconnect tickets"""
        logger.info("Creating churn labels from CRM data...")
        
        # Identify churn events (disconnect requests that are closed)
        churn_events = crm_df[
            (crm_df['request_type'] == 'disconnect') & 
            (crm_df['status'] == 'closed')
        ].copy()
        
        # Create simple customer-level churn indicator
        churned_customers = churn_events['customer_id'].unique()
        
        logger.info(f"Identified {len(churned_customers)} customers with churn events")
        
        return churned_customers
    
    def join_customer_data(self, billing_df, subscriptions_df, crm_df, churned_customers):
        """Simple join of all data sources into master table"""
        logger.info("Joining customer data from all sources...")
        
        # Start with billing data as base (most customers should have billing)
        master_df = billing_df.copy()
        logger.info(f"Starting with billing data: {len(master_df)} records")
        
        # Add subscription data
        if not subscriptions_df.empty:
            master_df = master_df.merge(
                subscriptions_df, 
                on='customer_id', 
                how='left',
                suffixes=('_billing', '_subscription')
            )
            logger.info(f"After joining subscriptions: {len(master_df)} records")
        
        # Add CRM data (aggregate to customer level for simplicity)
        if not crm_df.empty:
            # Simple CRM aggregation
            crm_summary = crm_df.groupby('customer_id').agg({
                'ticket_id': 'count',
                'request_type': lambda x: ', '.join(x.unique()),
                'created_at': 'max'
            }).rename(columns={
                'ticket_id': 'total_tickets',
                'request_type': 'request_types',
                'created_at': 'last_ticket_date'
            })
            
            master_df = master_df.merge(
                crm_summary,
                on='customer_id',
                how='left'
            )
            logger.info(f"After joining CRM data: {len(master_df)} records")
        
        # Add simple churn label
        master_df['is_churned'] = master_df['customer_id'].isin(churned_customers).astype(int)
        
        # Fill basic missing values
        master_df['total_tickets'] = master_df['total_tickets'].fillna(0)
        master_df['request_types'] = master_df['request_types'].fillna('none')
        
        logger.info(f"Final master dataset: {len(master_df)} records with {len(master_df.columns)} columns")
        
        return master_df
    
    def perform_basic_eda(self, master_df):
        """Perform basic exploratory data analysis - NO feature engineering"""
        logger.info("Performing basic EDA...")
        
        eda_insights = {}
        
        # Basic dataset statistics
        eda_insights['total_customers'] = len(master_df)
        eda_insights['total_columns'] = len(master_df.columns)
        
        # Churn statistics
        churn_rate = master_df['is_churned'].mean()
        eda_insights['churn_rate'] = round(churn_rate * 100, 2)
        eda_insights['churned_customers'] = master_df['is_churned'].sum()
        eda_insights['retention_rate'] = round((1 - churn_rate) * 100, 2)
        
        # Data completeness
        missing_data = master_df.isnull().sum()
        eda_insights['columns_with_missing'] = len(missing_data[missing_data > 0])
        eda_insights['overall_completeness'] = round((1 - missing_data.sum() / (len(master_df) * len(master_df.columns))) * 100, 2)
        
        # Basic business metrics (if available)
        if 'amount' in master_df.columns:
            eda_insights['avg_billing_amount'] = round(master_df['amount'].mean(), 2)
        
        if 'monthly_fee' in master_df.columns:
            eda_insights['avg_monthly_fee'] = round(master_df['monthly_fee'].mean(), 2)
        
        self.eda_insights = eda_insights
        logger.info(f"Basic EDA completed. Churn rate: {churn_rate:.2%}")
        
        return eda_insights
    
    def save_cleaned_dataset(self, master_df, partition_date):
        """Save the cleaned dataset to the clean data location"""
        try:
            # Create clean data directory structure
            clean_dir = os.path.join(project_root, "data", "clean", "churn_dataset", f"dt={partition_date}")
            os.makedirs(clean_dir, exist_ok=True)
            
            # Save the master dataset
            output_file = os.path.join(clean_dir, "cleaned_churn_dataset.csv")
            master_df.to_csv(output_file, index=True)  # Include customer_id as index
            
            logger.info(f"Cleaned dataset saved: {output_file}")
            logger.info(f"Dataset shape: {master_df.shape}")
            
            return {
                "status": "success",
                "output_file": output_file,
                "records": len(master_df),
                "features": len(master_df.columns),
                "partition_date": partition_date
            }
            
        except Exception as e:
            logger.error(f"Error saving cleaned dataset: {str(e)}")
            raise
    
    def generate_preparation_summary(self):
        """Generate comprehensive preparation summary"""
        return {
            "cleaning_summary": self.cleaning_summary,
            "data_quality_issues": self.data_quality_issues,
            "eda_insights": self.eda_insights,
            "preparation_timestamp": datetime.now().isoformat()
        }

def prepare_clean_dataset():
    """Main function to prepare clean, joined dataset (NO feature engineering)"""
    logger.info("Starting data preparation - cleaning and joining only...")
    
    prep = DataPreparation()
    
    try:
        # Step 1: Load validated data
        data, latest_date = prep.load_latest_validated_data()
        logger.info(f"Loaded {len(data)} tables from partition: {latest_date}")
        
        # Step 2: Clean individual datasets
        billing_clean = prep.clean_billing_data(data['billing'])
        subscriptions_clean = prep.clean_subscriptions_data(data['subscriptions'])
        crm_clean = prep.clean_crm_data(data['crm'])
        
        # Step 3: Create simple churn labels
        churned_customers = prep.create_churn_labels(crm_clean)
        
        # Step 4: Join all data into master table (NO feature engineering)
        master_dataset = prep.join_customer_data(
            billing_clean, subscriptions_clean, crm_clean, churned_customers
        )
        
        # Step 5: Basic EDA only
        eda_results = prep.perform_basic_eda(master_dataset)
        
        # Step 6: Save clean dataset
        save_results = prep.save_cleaned_dataset(master_dataset, latest_date)
        
        # Step 7: Generate summary
        preparation_summary = prep.generate_preparation_summary()
        
        logger.info("Data preparation (cleaning & joining) completed successfully!")
        
        return {
            "status": "success",
            "master_dataset_shape": master_dataset.shape,
            "save_results": save_results,
            "eda_insights": eda_results,
            "preparation_summary": preparation_summary
        }
        
    except Exception as e:
        logger.error(f"Data preparation failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Test the data preparation
    result = prepare_clean_dataset()
    print(f"Data preparation completed: {result['master_dataset_shape']}")
    print(f"Churn rate: {result['eda_insights']['churn_rate']}%")
