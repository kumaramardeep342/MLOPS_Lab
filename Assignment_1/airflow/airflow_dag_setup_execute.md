```bash 
### Details
- Roll Number : DA25M502
- Name : Amardeep Kumar

### Software Requirements
- Docker
- Airflow : 3.0.2

### Step 1: Locate or Create your Airflow DAGs Directory
By default, Apache Airflow looks for workflows in a folder named dags inside your Airflow home directory (usually ~/airflow/).

put your airflow_dag.py file inside airflow/dags
we have to use sudo nano direct file copy paste doesn't work
```

```bash 
sudo nano /home/sjagkoo7/projects/mlops_project/mlops_learning_project/airflow/dags/airflow_dag.py 

### Step 2: Paste and Save
Paste your airflow_dag.py code into the terminal window.
Press Ctrl + O then Enter to save the file. (It will successfully save now!)
Press Ctrl + X to close nano.

## Open your web browser and navigate to the Airflow UI dashboard
http://localhost:8080

```