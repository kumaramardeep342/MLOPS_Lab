"""
Apache Airflow DAG - DA5402W Assignment 1 Part C
Automates and orchestrates the end-to-end sensor data engineering workflow.

Fully mapped with explicit annotations for Requirements 1-9.
Roll Number : DA25M502
Name : Amardeep Kumar
"""

from datetime import datetime, timedelta
import random
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# ==============================================================================
# REQUIREMENT 5: CONFIGURE RETRY POLICIES (Retries = 3, Delay = 1 Minute)
# ==============================================================================
default_args = {
    'owner': 'da25m502 Amardeep Kumar',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 3,                                  # 3 retries per task
    'retry_delay': timedelta(minutes=1),           # 1-minute delay between retries
    'email_on_failure': False,
    'email_on_retry': False,
}

# Dummy Python functions acting as task drivers for execution simulation
def generate_sensor_data():
    print("[EXECUTION] Simulating mock pipeline ingestion parameters from Kafka...")
    return "raw_ingested_v1"

# ==============================================================================
# REQUIREMENT 6: BRANCHPYTHONOPERATOR FOR CONDITIONAL EXECUTION
# ==============================================================================
def evaluate_data_quality(**kwargs):
    """
    Acts as the 'Data Validation' stage. Inspects ingestion stream quality
    and dynamically dictates DAG routing branches.
    """
    print("[EXECUTION] Checking data constraints, formatting, and structural issues...")
    quality_score = random.uniform(0.7, 1.0) # Simulate a dynamic code execution metric
    
    if quality_score > 0.75:
        print(f"[BRANCH] Quality check PASSED ({round(quality_score, 2)}). Proceeding to refining stage.")
        return "data_refinement_stage.Data_Preprocessing"
    else:
        print(f"[BRANCH] Quality check FAILED ({round(quality_score, 2)}). Diverting to bypass task.")
        return "anomaly_bypass_log"

def preprocess_sensor_records():
    print("[EXECUTION] Stripping duplicate sensor ids and formatting timelines...")

def engineer_features():
    print("[EXECUTION] Deriving hour_of_day, day_of_week, and weekend markers...")

def run_analytics():
    print("[EXECUTION] Computing windowed average and maximum temperature distribution states...")

def generate_report():
    print("[EXECUTION] Compiling summary evaluation JSON blocks into final report markdown maps...")


# ==============================================================================
# REQUIREMENT 8: SCHEDULE THE DAG TO EXECUTE EVERY 5 MINUTES
# ==============================================================================
with DAG(
    dag_id='sensor_processing_orchestration',
    default_args=default_args,
    description='Automated orchestration pipeline for real-time sensor processing and cleaning verification',
    schedule='*/5 * * * *',              # Executes every 5 minutes
    catchup=False,
    tags=['MLOps', 'Assignment1_PartC']
) as dag:

    # Requirement 1: Task 1 - DataGeneration
    task_generation = PythonOperator(
        task_id='DataGeneration',
        python_callable=generate_sensor_data,
        priority_weight=10                         # Standard baseline entry execution priority
    )

    # Requirement 1 & 6: Task 2 - Data_Validation (Conditional Router)
    task_validation = BranchPythonOperator(
        task_id='Data_Validation',
        python_callable=evaluate_data_quality,
        priority_weight=15
    )

    # ==============================================================================
    # REQUIREMENT 4: ORGANIZE RELATED TASKS USING A TASKGROUP
    # ==============================================================================
    with TaskGroup(group_id='data_refinement_stage') as data_refinement_stage:
        
        # Requirement 1: Task 3 - Data Preprocessing
        task_preprocessing = PythonOperator(
            task_id='Data_Preprocessing',
            python_callable=preprocess_sensor_records
        )

        # Requirement 1: Task 4 - Feature Engineering
        task_feature_eng = PythonOperator(
            task_id='Feature_Engineering',
            python_callable=engineer_features
        )

        # Requirement 2: Strict linear dependency inside the refined group
        task_preprocessing >> task_feature_eng

    # Anomaly tracking branch to handle data quality failures gracefully
    task_anomaly_bypass = EmptyOperator(
        task_id='anomaly_bypass_log'
    )

    # Requirement 1: Task 5 - Analytics
    task_analytics = PythonOperator(
        task_id='Analytics',
        python_callable=run_analytics,
        priority_weight=30
    )

    # ==============================================================================
    # REQUIREMENT 7: ASSIGN SUITABLE PRIORITY WEIGHTS TO SELECTED TASKS
    # ==============================================================================
    # Downstream summary generation is highly critical; assigned the highest priority weight 
    # to prevent execution starvation if cluster resources become heavily saturated.
    task_report_gen = PythonOperator(
        task_id='Report_Generation',
        python_callable=generate_report,
        trigger_rule='one_success',                # Executes if either the main pipeline or bypass finishes
        priority_weight=50                         # Highest priority execution node
    )

    # ==============================================================================
    # REQUIREMENT 2 & 3: DEPENDENCIES & PARALLEL EXECUTION OPPORTUNITIES
    # ==============================================================================
    # 1. Main Path: Data Generation flows immediately into Validation checking
    task_generation >> task_validation

    # 2. Branch Mapping: Validation dynamically selects the next execution target
    task_validation >> [data_refinement_stage, task_anomaly_bypass]

    # 3. Parallel Execution: Once the refinement TaskGroup finishes, Analytics 
    # runs in parallel with any other isolated evaluation paths
    data_refinement_stage >> task_analytics >> task_report_gen
    
    # 4. Alternative Path: Anomalous track connects back directly into Report Generation
    task_anomaly_bypass >> task_report_gen
