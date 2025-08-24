
# End-to-End Data Management Pipeline for Machine Learning  
*Assignment I – Data Management for Machine Learning (BITS Pilani)*  

---

## Project Overview
This project implements a complete **data management pipeline** for a **Telecom / Pay-TV churn prediction** use case.  
It covers all stages from **problem formulation → ingestion → validation → preparation → transformation → feature store → versioning → modeling → orchestration**.  

---

## Repository Structure

├── README.md # Project index (this file)
├── Task1_Problem_Formulation/
├── Task2_DataIngestion/ # scripts + logs
├── Task3_RawDataStorage/
├── Task4_DataValidation/
├── Task5_DataPreparation/
├── Task6_DataTransformation/
├── Task7_FeatureStore/
├── Task8_DataVersioning/
├── Task9_ModelBuilding/
├── Task10_Orchestration/
└── data/ # raw, validated, transformed datasets
---

## Deliverables by Task

### Task 1 – Problem Formulation
- 📄 [Task1_Problem_Formulation/Task1_Problem_Formulation.md](Task1_Problem_Formulation.md)  
- Defines the **business problem, objectives, and data sources** for churn prediction in the Telecom/Pay-TV domain.  

### Task 2 – Data Ingestion
- Scripts to fetch and log data ingestion from multiple sources (CSV, REST API).  
- Deliverables: ingestion scripts, logs, raw data snapshots.  

### Task 3 – Raw Data Storage
- Data lake folder structure (partitioned by source/type/date).
- [Task3_RawDataStorage/Task3_RawDataStorage.md](Folder Structure)  
- Python scripts to upload/store ingested raw data.  

### Task 4 – Data Validation
- Automated validation checks for missing values, anomalies, and schema mismatches.  
- Deliverables: validation scripts + sample data quality reports.  

### Task 5 – Data Preparation
- Preprocessing pipeline: imputation, encoding, normalization, and EDA.  
- Deliverables: clean datasets + visualizations.  

### Task 6 – Data Transformation
- Feature engineering & transformation logic.  
- Deliverables: transformed dataset, SQL schema, sample queries.  

### Task 7 – Feature Store
- Centralized registry of features with metadata and versioning.  
- Deliverables: feature store configuration & retrieval examples.  

### Task 8 – Data Versioning
- Data versioning strategy using DVC/Git.  
- Deliverables: repo showing dataset versions + documentation.  

### Task 9 – Model Building
- Churn prediction models trained with prepared features.  
- Deliverables: training scripts, evaluation reports, versioned model files.  

### Task 10 – Orchestration
- Automated DAG pipeline (Prefect/Airflow).  
- Deliverables: pipeline DAG, screenshots/logs of runs, monitoring setup.  

---

## How to Run
1. Clone this repo.  
2. Install dependencies (`requirements.txt`).  
3. Run task scripts individually (or orchestrate via Prefect/Airflow).  
4. Explore deliverables in each task folder.  

---

## 📽️ Video Walkthrough
A short demo video (5–10 mins) will showcase the pipeline workflow and orchestration runs. *(to be added in final submission)*  

---
