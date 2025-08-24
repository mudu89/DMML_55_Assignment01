"""
Prefect Server Startup Script
Run this to start the Prefect UI and execute the pipeline
"""

import subprocess
import sys, os
import time
import webbrowser
import requests
from threading import Thread

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
# Import functions (after adding project root to path)
from utils.logger import get_logger

logger = get_logger("pipeline", log_file=os.path.join(project_root, "logs", "pipeline.log"))

def check_prefect_server_running(url="http://localhost:4200", timeout=3):
    """Check if Prefect server is already running"""
    try:
        response = requests.get(f"{url}/api/health", timeout=timeout)
        return response.status_code == 200
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError):
        return False

def start_prefect_server():
    """Start Prefect server in background if not already running"""
    if check_prefect_server_running():
        logger.info("Prefect server is already running at http://localhost:4200")
        return

    logger.info("Starting Prefect server...")
    subprocess.run([sys.executable, "-m", "prefect", "server", "start"], shell=True)

def run_pipeline():
    """Wait for server to be ready then run the pipeline"""
    # Check if server is ready
    max_retries = 30  # 30 seconds timeout
    for i in range(max_retries):
        if check_prefect_server_running():
            logger.info("Prefect server is ready!")
            break
        logger.info(f"Waiting for server to be ready... ({i+1}/{max_retries})")
        time.sleep(1)
    else:
        logger.error("Prefect server failed to start within timeout period")
        return

    logger.info("Running ML Pipeline...")
    try:
        subprocess.run([sys.executable, "ml_pipeline.py"], cwd=os.path.abspath(os.path.join(os.path.dirname(__file__))))
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
    finally:
        logger.info("ML Pipeline run completed.")
        
        # Ask user if they want to keep the server running
        print("\nPipeline execution finished.")
        print("The Prefect server is still running for you to review results.")
        print("You can view the dashboard at: http://localhost:4200")
        print("Run 'prefect server stop' manually when you're done reviewing.")

if __name__ == "__main__":
    print("="*50)
    print("DMML Assignment 01 - Pipeline Orchestration")
    print("="*50)
    
    # Check if server is already running
    if check_prefect_server_running():
        print("Prefect server is already running at: http://localhost:4200")
    else:
        print("Starting Prefect server at: http://localhost:4200")
    
    print("\n" + "="*50)
    
    # Start server in background thread (will check if already running)
    server_thread = Thread(target=start_prefect_server, daemon=True)
    server_thread.start()
    
    # Wait a bit and open browser
    time.sleep(5)
    print("Please manually open: http://localhost:4200")
    
    # Run the pipeline
    run_pipeline()
