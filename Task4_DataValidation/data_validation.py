"""
Task 4: Data Validation
Comprehensive data quality checks and validation reports
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import glob

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from utils.logger import get_logger

# Initialize logger
logger = get_logger("data_validation", log_file=os.path.join(project_root, "logs", "data_validation.log"))

class DataValidator:
    """Data validation and quality assessment class"""
    
    def __init__(self, date):
        self.validation_results = {}
        self.issues_found = []
        self.data_quality_score = 0
        self.file_date = date
    
    def load_data(self):
        """Load the most recent data from the data lake"""
        try:
            data = {}
            data_root = os.path.join(project_root, "data", "raw")
        
            
            # Load data from latest partition
            for table_dir in os.listdir(data_root):
                table_path = os.path.join(data_root, table_dir, f"dt={self.file_date}")
                if os.path.isdir(table_path):
                    csv_files = glob.glob(os.path.join(table_path, "*.csv"))
                    if csv_files:
                        data[table_dir] = pd.read_csv(csv_files[0])
                        logger.info(f"Loaded {table_dir}: {data[table_dir].shape}")
                else:
                    logger.warning(f"File for given date {self.file_date} not found for {table_dir}")

            return data
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def validate_data_completeness(self, df, table_name):
        """Check for missing values and completeness"""
        results = {
            "table": table_name,
            "total_records": len(df),
            "total_columns": len(df.columns),
            "missing_values": {},
            "completeness_score": 0
        }
        
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            missing_pct = (missing_count / len(df)) * 100
            results["missing_values"][col] = {
                "count": int(missing_count),
                "percentage": round(missing_pct, 2)
            }
            
            if missing_pct > 50:
                self.issues_found.append(f"HIGH: {table_name}.{col} has {missing_pct:.1f}% missing values")
            elif missing_pct > 20:
                self.issues_found.append(f"MEDIUM: {table_name}.{col} has {missing_pct:.1f}% missing values")
        
        total_missing = sum(df.isnull().sum())
        total_cells = len(df) * len(df.columns)
        completeness = ((total_cells - total_missing) / total_cells) * 100
        results["completeness_score"] = round(completeness, 2)
        
        return results
    
    def validate_data_types(self, df, table_name):
        """Validate data types and detect inconsistencies"""
        results = {
            "table": table_name,
            "data_types": {},
            "type_issues": []
        }
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            results["data_types"][col] = dtype
            
            # Check for mixed types in object columns
            if dtype == 'object':
                try:
                    # Try to convert to numeric
                    pd.to_numeric(df[col], errors='raise')
                    results["type_issues"].append(f"Column '{col}' is object but appears numeric")
                except:
                    pass
            
            # Check for suspicious values
            if col.lower() in ['id', 'customer_id', 'user_id'] and dtype != 'int64':
                self.issues_found.append(f"WARNING: {table_name}.{col} (ID field) is not integer type")
        
        return results
    
    def validate_data_ranges(self, df, table_name):
        """Check for outliers and suspicious data ranges"""
        results = {
            "table": table_name,
            "numeric_summaries": {},
            "outliers": {}
        }
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            stats = {
                "min": float(df[col].min()),
                "max": float(df[col].max()),
                "mean": float(df[col].mean()),
                "std": float(df[col].std()),
                "median": float(df[col].median())
            }
            results["numeric_summaries"][col] = stats
            
            # Detect outliers using IQR method
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            outlier_count = len(outliers)
            outlier_pct = (outlier_count / len(df)) * 100
            
            results["outliers"][col] = {
                "count": outlier_count,
                "percentage": round(outlier_pct, 2),
                "lower_bound": float(lower_bound),
                "upper_bound": float(upper_bound)
            }
            
            if outlier_pct > 10:
                self.issues_found.append(f"HIGH: {table_name}.{col} has {outlier_pct:.1f}% outliers")
        
        return results
    
    def validate_business_rules(self, data):
        """Apply business-specific validation rules"""
        business_validation = {
            "rules_checked": [],
            "violations": []
        }
        
        # Example business rules for telecom data
        if 'billing' in data:
            billing_df = data['billing']
            
            # Rule 1: Billing amounts should be positive
            if 'amount' in billing_df.columns:
                negative_amounts = billing_df[billing_df['amount'] < 0]
                if len(negative_amounts) > 0:
                    business_validation["violations"].append({
                        "rule": "Billing amounts must be positive",
                        "violations": len(negative_amounts),
                        "table": "billing"
                    })
                business_validation["rules_checked"].append("Positive billing amounts")
            
            # Rule 2: Check for reasonable billing date ranges
            if 'billing_date' in billing_df.columns:
                try:
                    billing_df['billing_date'] = pd.to_datetime(billing_df['billing_date'])
                    future_dates = billing_df[billing_df['billing_date'] > datetime.now()]
                    if len(future_dates) > 0:
                        business_validation["violations"].append({
                            "rule": "Billing dates cannot be in the future",
                            "violations": len(future_dates),
                            "table": "billing"
                        })
                    business_validation["rules_checked"].append("Billing date validity")
                except:
                    pass
        
        if 'subscriptions' in data:
            subs_df = data['subscriptions']
            
            # Rule 3: Check subscription status values
            if 'status' in subs_df.columns:
                valid_statuses = ['active', 'inactive', 'suspended', 'cancelled']
                invalid_status = subs_df[~subs_df['status'].str.lower().isin(valid_statuses)]
                if len(invalid_status) > 0:
                    business_validation["violations"].append({
                        "rule": "Subscription status must be valid",
                        "violations": len(invalid_status),
                        "table": "subscriptions"
                    })
                business_validation["rules_checked"].append("Subscription status validity")
        
        if 'crm' in data:
            crm_df = data['crm']
            
            # Rule 4: CRM ticket request types should be valid
            if 'request_type' in crm_df.columns:
                valid_request_types = ['disconnect', 'complaint', 'upgrade', 'inquiry', 'billing_issue']
                invalid_requests = crm_df[~crm_df['request_type'].str.lower().isin(valid_request_types)]
                if len(invalid_requests) > 0:
                    business_validation["violations"].append({
                        "rule": "CRM request types must be valid",
                        "violations": len(invalid_requests),
                        "table": "crm"
                    })
                business_validation["rules_checked"].append("CRM request type validity")
            
            # Rule 5: CRM ticket status should be valid
            if 'status' in crm_df.columns:
                valid_ticket_statuses = ['open', 'closed', 'pending', 'resolved']
                invalid_statuses = crm_df[~crm_df['status'].str.lower().isin(valid_ticket_statuses)]
                if len(invalid_statuses) > 0:
                    business_validation["violations"].append({
                        "rule": "CRM ticket status must be valid",
                        "violations": len(invalid_statuses),
                        "table": "crm"
                    })
                business_validation["rules_checked"].append("CRM ticket status validity")
            
            # Rule 6: Check for reasonable ticket creation dates
            if 'created_at' in crm_df.columns:
                try:
                    crm_df['created_at'] = pd.to_datetime(crm_df['created_at'])
                    future_tickets = crm_df[crm_df['created_at'] > datetime.now()]
                    if len(future_tickets) > 0:
                        business_validation["violations"].append({
                            "rule": "CRM ticket dates cannot be in the future",
                            "violations": len(future_tickets),
                            "table": "crm"
                        })
                    business_validation["rules_checked"].append("CRM ticket date validity")
                except:
                    pass
            
            # Rule 7: Disconnect tickets should have valid disconnect reasons, non-disconnect should have request reasons
            if 'request_type' in crm_df.columns:
                # Check disconnect_reason for disconnect requests
                if 'disconnect_reason' in crm_df.columns:
                    disconnect_tickets = crm_df[crm_df['request_type'].str.lower() == 'disconnect']
                    missing_disconnect_reasons = disconnect_tickets[disconnect_tickets['disconnect_reason'].isnull()]
                    if len(missing_disconnect_reasons) > 0:
                        business_validation["violations"].append({
                            "rule": "Disconnect tickets must have disconnect reason",
                            "violations": len(missing_disconnect_reasons),
                            "table": "crm"
                        })
                    
                    # Check that non-disconnect requests don't have disconnect_reason
                    non_disconnect_with_disconnect_reason = crm_df[
                        (crm_df['request_type'].str.lower() != 'disconnect') & 
                        (crm_df['disconnect_reason'].notna())
                    ]
                    if len(non_disconnect_with_disconnect_reason) > 0:
                        business_validation["violations"].append({
                            "rule": "Non-disconnect tickets should not have disconnect reason",
                            "violations": len(non_disconnect_with_disconnect_reason),
                            "table": "crm"
                        })
                    
                    business_validation["rules_checked"].append("Disconnect reason completeness")
                
                # Check request_reason for non-disconnect requests
                if 'request_reason' in crm_df.columns:
                    non_disconnect_tickets = crm_df[crm_df['request_type'].str.lower() != 'disconnect']
                    missing_request_reasons = non_disconnect_tickets[non_disconnect_tickets['request_reason'].isnull()]
                    if len(missing_request_reasons) > 0:
                        business_validation["violations"].append({
                            "rule": "Non-disconnect tickets must have request reason",
                            "violations": len(missing_request_reasons),
                            "table": "crm"
                        })
                    business_validation["rules_checked"].append("Request reason completeness")
        
        return business_validation
    
    def calculate_data_quality_score(self):
        """Calculate overall data quality score"""
        total_score = 100
        
        # Deduct points for issues
        for issue in self.issues_found:
            if issue.startswith("HIGH"):
                total_score -= 15
            elif issue.startswith("MEDIUM"):
                total_score -= 10
            elif issue.startswith("WARNING"):
                total_score -= 5
        
        # Ensure score doesn't go below 0
        self.data_quality_score = max(0, total_score)
        return self.data_quality_score
    
    def generate_validation_report(self):
        """Generate comprehensive validation report"""
        return {
            "validation_timestamp": datetime.now().isoformat(),
            "data_quality_score": self.data_quality_score,
            "total_issues": len(self.issues_found),
            "issues_summary": self.issues_found[:10],  # Top 10 issues
            "validation_results": self.validation_results,
            "quality_assessment": self._get_quality_assessment()
        }
    
    def _get_quality_assessment(self):
        """Get quality assessment based on score"""
        score = self.data_quality_score
        if score >= 90:
            return "EXCELLENT - Data is ready for analysis"
        elif score >= 80:
            return "GOOD - Minor issues that should be addressed"
        elif score >= 70:
            return "FAIR - Several issues need attention"
        elif score >= 60:
            return "POOR - Significant data quality problems"
        else:
            return "CRITICAL - Major data quality issues require immediate attention"

def validate_all_data(validation_date):
    """Main validation function"""
    logger.info("Starting comprehensive data validation...")
    validator = DataValidator(date=validation_date)
    
    try:
        # Load data
        data = validator.load_data()
        logger.info(f"Loaded {len(data)} tables for validation")
        
        # Run validation checks for each table
        for table_name, df in data.items():
            logger.info(f"Validating {table_name}...")
            
            # Completeness validation
            completeness_results = validator.validate_data_completeness(df, table_name)
            validator.validation_results[f"{table_name}_completeness"] = completeness_results
            
            # Data type validation
            type_results = validator.validate_data_types(df, table_name)
            validator.validation_results[f"{table_name}_types"] = type_results
            
            # Range validation
            range_results = validator.validate_data_ranges(df, table_name)
            validator.validation_results[f"{table_name}_ranges"] = range_results
        
        # # Business rules validation
        # business_results = validator.validate_business_rules(data)
        # validator.validation_results["business_rules"] = business_results
        
        # Calculate quality score
        quality_score = validator.calculate_data_quality_score()
        
        # Generate final report
        validation_report = validator.generate_validation_report()
        
        logger.info(f"Validation completed. Quality Score: {quality_score}/100")
        logger.info(f"Issues found: {len(validator.issues_found)}")
        
        return {
            "status": "success",
            "quality_score": quality_score,
            "total_issues": len(validator.issues_found),
            "validation_report": validation_report,
            "data_summary": {table: df.shape for table, df in data.items()}
        }
        
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Test the validation
    result = validate_all_data(datetime.today().isoformat())
    print(f"Validation completed with quality score: {result['quality_score']}")
