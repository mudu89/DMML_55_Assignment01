# Task 10 - Pipeline Orchestration with Prefect

## 🎯 Overview
This task implements the complete ML pipeline orchestration using **Prefect** for the DMML Assignment 01.

## 📁 Files Structure
```
Task10_Orchestration/
├── ml_pipeline.py          # Main pipeline with all 8 tasks
├── start_pipeline.py       # Script to start Prefect server and run pipeline
├── test_pipeline.py        # Test individual tasks
└── README.md              # This file
```

## 🚀 Quick Start

### 1. Start the Prefect Server & Pipeline
```cmd
cd Task10_Orchestration
"C:/Users/msf12/OneDrive - Sky/Documents/BITS/Semester 2/DMML/DMML_55_Assignment01/.venv/Scripts/python.exe" start_pipeline.py
```

### 2. Monitor Pipeline
- **Prefect UI**: http://localhost:4200
- Watch tasks execute in real-time
- View logs and task dependencies

### 3. Run Pipeline Manually
```cmd
"C:/Users/msf12/OneDrive - Sky/Documents/BITS/Semester 2/DMML/DMML_55_Assignment01/.venv/Scripts/python.exe" ml_pipeline.py
```

## 📊 Pipeline Flow

```
Data Ingestion (Task 2)
    ↓
Raw Data Storage (Task 3)
    ↓
Data Validation (Task 4)
    ↓
Data Preparation (Task 5)
    ↓
Data Transformation (Task 6)
    ↓
Feature Store Update (Task 7)
    ↓
Data Versioning (Task 8)
    ↓
Model Building (Task 9)
```

## ✅ Current Status

- ✅ **Task 2 (Ingestion)**: Implemented and working
- 🚧 **Tasks 3-9**: Framework ready, scripts to be implemented
- ✅ **Task 10 (Orchestration)**: Complete Prefect pipeline setup

## 🔧 Key Features

- **Error Handling**: Automatic retries with delays
- **Logging**: Comprehensive logging for each task
- **Dependencies**: Proper task sequencing with wait_for
- **Monitoring**: Real-time UI dashboard
- **Scalable**: Easy to add new tasks or modify existing ones

## 📹 Demo Instructions

For your video demonstration:
1. Start the pipeline with `start_pipeline.py`
2. Show the Prefect UI at http://localhost:4200
3. Demonstrate task execution and monitoring
4. Show task logs and results

## 🛠 Next Steps

As you implement each task (3-9), simply update the corresponding function in `ml_pipeline.py` to call your actual scripts instead of the placeholder implementations.

Example for Task 3:
```python
@task(name="Raw Data Storage", retries=1)
def task_raw_data_storage():
    script_path = os.path.join(project_root, "Task3_RawDataStorage", "storage.py")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    # Handle result...
```
