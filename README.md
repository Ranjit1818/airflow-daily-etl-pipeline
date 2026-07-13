# Daily API Data Ingestion and Processing ETL Pipeline

## 📖 Overview
This project is a Data Engineering ETL pipeline built using **Apache Airflow** to extract daily user activity data from mock APIs, process it, and perform data quality checks. It was designed to gain practical experience with core Data Engineering concepts such as incremental loading, DAG scheduling, TaskFlow API, branching, and task monitoring.

## 🎯 Problem Statement
In a typical e-commerce or digital environment, user activity data (login events, product views, and checkouts) is generated continuously. To support downstream analytics, we need a reliable daily pipeline to:
1. Extract data incrementally for the current day from multiple API endpoints.
2. Ensure data quality (e.g., verifying checkout amounts are valid).
3. Merge the validated datasets for analytics.
4. Alert the engineering team if data anomalies are detected during the ingestion process.

## 💡 Solution
I implemented a daily batch ETL pipeline using Apache Airflow that automates this workflow. The pipeline runs on a daily schedule and performs the following tasks:
- **Extraction:** Concurrently fetches daily data from three separate endpoints (`/loginUsers`, `/productUsers`, `/checkoutUsers`) using Python tasks.
- **Incremental Loading:** Uses Airflow's logical execution date (`ds`) to fetch and partition only the data relevant to that specific run day.
- **Branching & Data Quality Check:** Calculates the total checkout amount from the daily payload. A `@task.branch` decorator evaluates the sum:
  - If the sum is `> 0`, the pipeline safely proceeds to merge the datasets.
  - If the sum is `<= 0`, it diverges to an `alarming_situation` task to alert the team.
- **Transformation:** Merges the daily JSON files into a consolidated, processed dataset.
- **Notification:** A Bash Operator is triggered to simulate team notifications upon successful data availability.

## 🛠️ Tech Stack
- **Orchestration:** Apache Airflow
- **Language:** Python
- **Airflow Features Used:** 
  - `@task` (Airflow TaskFlow API)
  - `@task.branch` (Branching Logic)
  - `BashOperator` (System commands and Alerts)
  - `EmptyOperator` (Workflow structuring)
- **Concepts Applied:** 
  - Airflow Variables (for API Base URLs and Auth Headers)
  - Incremental data extraction
  - Conditional task execution (Branching)
  - Trigger Rules (e.g., `trigger_rule="none_failed"`)

## 🚀 Learning Outcomes
Building this pipeline helped me solidify several core Data Engineering and Airflow concepts:
1. **Incremental Data Loads:** Understood how to use Airflow's execution dates (`ds`) to partition and load data incrementally without fetching historical data repeatedly.
2. **TaskFlow API vs Classic Operators:** Gained hands-on experience using modern Airflow decorators to cleanly pass data dynamically between tasks, abstracting away manual XCom management.
3. **Control Flow and Branching:** Learned how to build resilient pipelines by adding data quality checks in-flight and designing branching paths based on dynamic conditions.
4. **Security & Configuration:** Utilized Airflow Variables to manage external API URLs and authentication headers instead of hardcoding them in the scripts.
5. **Monitoring & Trigger Rules:** Learned how to manage complex task dependencies and handle end-states using `trigger_rule="none_failed"` to ensure the DAG finishes correctly regardless of which branch was taken.

## 📂 Project Structure
- `dags/etl_pipeline.py`: Main Airflow DAG definition containing the tasks and execution flow.
- `include/api_client.py`: Python module handling robust API requests.
- `include/business_logic.py`: Core logic for merging JSON files and calculating checkout metrics.
- `include/utils.py`: Helper functions for reading/writing JSON files securely.
- `output_files/`: Output directories partitioned by data domain (`login/`, `product/`, `checkout/`, `merged/`).
