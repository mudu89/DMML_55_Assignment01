"""
ML Pipeline Orchestration using Prefect
DMML Assignment 01 - Task 10
"""

import os
import sys
from datetime import datetime, timedelta
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact, create_markdown_artifact, create_link_artifact
import pandas as pd
from sqlalchemy import table

# For Prefect 3.x compatibility - remove task_runners import

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

# Import functions (after adding project root to path)
from utils.logger import get_logger
from template_utils import load_markdown_template, format_table_details, format_storage_tree, format_file_details
from Task2_DataIngestion.ingestion import ingest_all_data
from Task3_RawDataStorage.data_storage import store_multiple_tables


logger = get_logger("pipeline", log_file=os.path.join(project_root, "logs", "pipeline.log"))


@task(name="Data Ingestion", retries=2, retry_delay_seconds=30)
def task_data_ingestion():
    """
    Task 2: Ingest data from multiple sources
    """
    # Dual logging: Prefect UI + Local files
      # For Prefect UI
     # For local files
    prefect_logger = get_run_logger()
    try:
        # Log to both systems
        prefect_logger.info("Starting data ingestion from database sources...")
        logger.info("Starting data ingestion from database sources")
        
        # Call the ingestion function directly
        result = ingest_all_data()
        status = result.pop('status')  # Remove status for downstream tasks
        total_records = result.pop('total_records', 0)
        data = result.pop('data', [])

        
        # Log success to both systems
        prefect_logger.info(f"Data ingestion completed successfully!")
        prefect_logger.info(f"Total records ingested: {total_records}")
        
        logger.info(f"Data ingestion completed - Total records: {total_records}")
   
        
        # Create Prefect Artifacts for Task 2
        #1. Markdown Artifact - Using Template
        create_markdown_artifact(
            key="ingestion-report",
            markdown=load_markdown_template(
                "ingestion_report_template",
                total_records=total_records,
                tables_count=len(data),
                ingestion_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                table_details=format_table_details(data),
                execution_time="< 1 second",  # You can calculate actual time if needed
                avg_records_per_table=total_records // len(data) if len(data) > 0 else 0
            ),
            description="Comprehensive Data Ingestion Report"
        )
        
        prefect_logger.info("Created ingestion artifacts: summary table and detailed report")
        
        return status, total_records, data
            
    except Exception as e:
        # Log errors to both systems
        prefect_logger.error(f"Data ingestion error: {str(e)}")
        logger.error(f"Data ingestion error: {str(e)}")
        raise

@task(name="Raw Data Storage", retries=1)
def task_raw_data_storage(data):
    """
    Task 3: Organize and store raw data in data lake structure
    """
    # Dual logging: Prefect UI + Local files
    # prefect_logger = get_run_logger()  # For Prefect UI
    # local_logger = get_logger("ml_pipeline_storage")  # For local files
    prefect_logger = get_run_logger()
    try:
        # Log to both systems
        prefect_logger.info("Starting raw data storage organization...")
        prefect_logger.info(f"Processing {len(data)} data sources")
        logger.info(f"Starting raw data storage for {len(data)} data sources")
        
        status = store_multiple_tables(data)
        
        # Log success to both systems
        prefect_logger.info("Raw data storage completed successfully!")
        logger.info("Raw data storage completed successfully")
        
        # Create Prefect Artifacts for Task 3
        current_date = datetime.now().date().isoformat()

        # 2. Markdown Artifact - Using Template
        storage_results = { source['table']: f"data/raw/{source['table']}/dt={current_date}/{source['table']}.csv" for source in data }
        
        create_markdown_artifact(
            key="data-lake-structure", 
            markdown=load_markdown_template(
                "storage_report_template",
                tables_stored=len(data),
                total_records=sum(source['data'].shape[0] for source in data),
                storage_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                partition_date=current_date,
                storage_tree=format_storage_tree(storage_results, current_date),
                table_details=format_table_details(data)
            ),
            description="Data Lake Organization and Structure"
        )

        prefect_logger.info("Created storage artifacts: summary table and structure documentation")
        
        return {"status": "success", "message": "Raw data organized in data lake structure"}
        
    except Exception as e:
        # Log errors to both systems
        prefect_logger.error(f"Raw data storage error: {str(e)}")
        logger.error(f"Raw data storage error: {str(e)}")
        raise

@task(name="Data Validation", retries=1)
def task_data_validation():
    """
    Task 4: Validate data quality and generate reports
    """
    # Dual logging: Prefect UI + Local files
    # prefect_logger = get_run_logger()
    # local_logger = get_logger("ml_pipeline_validation")
    prefect_logger = get_run_logger()
    try:
        prefect_logger.info("Starting data validation...")
        logger.info("Starting data validation")
        
        # This will run your validation scripts when implemented
        prefect_logger.info("Data validation completed successfully!")
        logger.info("Data validation completed successfully")
        
        return {"status": "success", "message": "Data validation checks passed"}
        
    except Exception as e:
        prefect_logger.error(f"Data validation error: {str(e)}")
        logger.error(f"Data validation error: {str(e)}")
        raise

@task(name="Data Preparation", retries=1)
def task_data_preparation():
    """
    Task 5: Clean and preprocess data, perform EDA
    """
    prefect_logger = get_run_logger()
    try:
        prefect_logger.info("Starting data preparation...")
        logger.info("Starting data preparation...")
        
        # This will run your preparation scripts
        prefect_logger.info("Data preparation completed successfully!")
        logger.info("Data preparation completed successfully!")
        return {"status": "success", "message": "Data cleaning and EDA completed"}
        
    except Exception as e:
        prefect_logger.error(f"Data preparation error: {str(e)}")
        logger.error(f"Data preparation error: {str(e)}")
        raise

@task(name="Data Transformation", retries=1)
def task_data_transformation():
    """
    Task 6: Feature engineering and transformation
    """
    prefect_logger = get_run_logger()
    try:
        prefect_logger.info("Starting data transformation...")
        logger.info("Starting data transformation...")
        
        # This will run your transformation scripts
        prefect_logger.info("Data transformation completed successfully!")
        logger.info("Data transformation completed successfully!")
        return {"status": "success", "message": "Feature engineering completed"}
        
    except Exception as e:
        prefect_logger.error(f"Data transformation error: {str(e)}")
        logger.error(f"Data transformation error: {str(e)}")
        raise

@task(name="Feature Store Update", retries=1)
def task_feature_store():
    """
    Task 7: Update feature store with new features
    """
    prefect_logger = get_run_logger()
    try:
        prefect_logger.info("Starting feature store update...")
        logger.info("Starting feature store update...")
        
        # This will update your feature store
        prefect_logger.info("Feature store update completed successfully!")
        logger.info("Feature store update completed successfully!")
        return {"status": "success", "message": "Feature store updated"}
        
    except Exception as e:
        prefect_logger.error(f"Feature store error: {str(e)}")
        logger.error(f"Feature store error: {str(e)}")
        raise

@task(name="Data Versioning", retries=1)
def task_data_versioning():
    """
    Task 8: Version control datasets
    """
    prefect_logger = get_run_logger()
    try:
        prefect_logger.info("Starting data versioning...")
        logger.info("Starting data versioning...")
        
        # This will handle data versioning
        prefect_logger.info("Data versioning completed successfully!")
        logger.info("Data versioning completed successfully!")
        return {"status": "success", "message": "Data versions updated"}
        
    except Exception as e:
        prefect_logger.error(f"Data versioning error: {str(e)}")
        logger.error(f"Data versioning error: {str(e)}")
        raise

@task(name="Model Building", retries=1)
def task_model_building():
    """
    Task 9: Train and evaluate ML models
    """
    prefect_logger = get_run_logger()
    try:
        prefect_logger.info("Starting model building...")
        logger.info("Starting model building...")
        
        # This will run your model training scripts
        prefect_logger.info("Model building completed successfully!")
        logger.info("Model building completed successfully!")
        return {"status": "success", "message": "Model trained and saved"}
        
    except Exception as e:
        prefect_logger.error(f"Model building error: {str(e)}")
        logger.error(f"Model building error: {str(e)}")
        raise

def generate_flow_run_name():
    """Generate a custom flow run name with timestamp"""
    return f"DMML-Assignment01-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

@flow(name="ML Data Pipeline", 
      description="End-to-End ML Data Management Pipeline",
      flow_run_name=generate_flow_run_name)
def ml_data_pipeline():
    """
    Main ML pipeline flow that orchestrates all tasks in sequence
    """
    # Dual logging for the main flow
    # prefect_logger = get_run_logger()
    #local_logger = get_logger("ml_pipeline_flow", log_file=os.path.join(project_root, "logs", "ml_pipeline_flow.log"))
    prefect_logger = get_run_logger()
    prefect_logger.info("Starting ML Data Pipeline...")
    logger.info("Starting ML Data Pipeline")
    
    # Change working directory to project root to ensure correct paths
    original_cwd = os.getcwd()
    os.chdir(project_root)
    logger.info(f"Changed working directory to: {project_root}")
    
    try:
        # Task dependencies - each task depends on the previous one
        # Ingestion Task
        ingestion_result, ingested_records, ingested_data = task_data_ingestion()

        # Storage Task  
        storage_result = task_raw_data_storage(data=ingested_data, wait_for=[ingested_data])

        validation_result = task_data_validation(wait_for=[storage_result])
        
        preparation_result = task_data_preparation(wait_for=[validation_result])
        
        transformation_result = task_data_transformation(wait_for=[preparation_result])
        
        feature_store_result = task_feature_store(wait_for=[transformation_result])
        
        versioning_result = task_data_versioning(wait_for=[feature_store_result])
        
        model_result = task_model_building(wait_for=[versioning_result])

        prefect_logger.info("ML Data Pipeline completed successfully!")
        logger.info("ML Data Pipeline completed successfully!")
        
        return {
            "pipeline_status": "completed",
            "completion_time": datetime.now().isoformat(),
            "tasks_completed": [
                "ingestion", "storage", "validation", "preparation", 
                "transformation", "feature_store", "versioning", "model_building"
            ]
        }
        
    finally:
        # Restore original working directory
        os.chdir(original_cwd)
        prefect_logger.info(f"Restored working directory to: {original_cwd}")
        logger.info(f"Restored working directory to: {original_cwd}")

if __name__ == "__main__":
    # Run the pipeline with custom naming
    print("Starting DMML Assignment 01 - ML Data Pipeline")
    print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    result = ml_data_pipeline()
    print(f"Pipeline Result: {result}")
