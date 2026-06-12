# Apache Airflow 25 Practice Questions

## Section 1: Conceptual & Functional Questions

### Question 1 (MCQ)

Which Spark component is responsible for scheduling tasks, managing the user application, and converting operations into Directed Acyclic Graph (DAG) computations?

- A. Spark Executor
- B. Cluster Manager
- C. Spark Driver
- D. Spark SQL

**Correct Answer:** C

**Rationale:** The Spark Driver is the central coordinator of a Spark application. It executes the main function, creates the SparkSession, builds the logical and physical execution plans, and schedules tasks across workers.

---

### Question 2 (MCQ)

In Apache Airflow, if a task fails to execute successfully on its first attempt, which parameter configuration directly dictates the time delay applied before a worker attempts to retry that specific task instance?

- A. retries
- B. execution_timeout
- C. retry_delay
- D. retry_exponential_backoff

**Correct Answer:** C

**Rationale:** While retries sets the maximum number of attempts, retry_delay defines the explicit timedelta duration that Airflow must wait between those failed attempts.

---

### Question 3 (MCQ)

When using the standard Apache Airflow task flow API (@task), how is data or state implicitly passed between consecutive tasks downstream without manually invoking the metastore?

- A. Shared cluster file storage volumes
- B. External Metadata Database locks
- C. Airflow XComs (Cross-Communications)
- D. Spark RDD memory serialization

**Correct Answer:** C

**Rationale:** Airflow uses XComs under the hood to automatically serialize and transfer small return values between tasks when using the functional @task decorator API.

---

### Question 4 (MCQ)

Which specific Airflow component is continually running in the background to poll the DAG folder, verify schedule intervals, and send ready tasks to the queueing system or executor?

- A. Web Server
- B. Executor
- C. Worker Node
- D. Scheduler

**Correct Answer:** D

**Rationale:** The Scheduler processes DAG files, monitors task states in the metadata database, and triggers ready tasks when their upstream dependencies and scheduling conditions are fully satisfied.

---

### Question 5 (MCQ)

When configuring a Spark application to write a DataFrame out to a file system using `df.write.json(path, orient='records', lines=True)`, what design property is enforced on the output file structure?

- A. A single large valid JSON array containing nested sub-objects.
- B. A sequence of newline-delimited, independent JSON objects (JSON Lines format).
- C. A compressed binary format unreadable by standard text parsers.
- D. A directory containing schema structural definitions exclusively.

**Correct Answer:** B

**Rationale:** Specifying lines=True writes data as newline-delimited JSON objects, which prevents loading the whole dataset into memory arrays when processing downstream line-by-line.

---

### Question 6 (MCQ)

In Apache Airflow's core architecture, which of the following is true regarding the role of the Executor?

- A. It directly runs the code inside every Python task sequentially on its own thread.
- B. It acts as an abstract mechanism that determines how and where tasks are allocated to workers to be executed.
- C. It hosts the UI dashboard web application for viewing workflow executions.
- D. It serves as the relational database storing persistent state logs.

**Correct Answer:** B

**Rationale:** The Executor is responsible for taking tasks from the Scheduler and assigning resources to execute them (e.g., locally via Sequential/Local, or distributed via Celery/Kubernetes).

---

### Question 7 (MCQ)

If you want a specific task in an Airflow DAG to execute regardless of whether its upstream tasks succeeded, failed, or were skipped, which trigger_rule must be explicitly defined?

- A. TriggerRule.ALL_SUCCESS
- B. TriggerRule.ONE_FAILED
- C. TriggerRule.ALL_DONE
- D. TriggerRule.NONE_FAILED

**Correct Answer:** C

**Rationale:** all_done ensures that the execution engine launches the task as soon as all upstream direct ancestors have completed their execution cycle, regardless of their final states.

---

## Section 2: Scenario-Based Architecture Questions

### Question 8 (MCQ – Scenario)

You are a Senior Data Engineer designing an automated pipeline that must ingest large volumes of daily taxi transactions from a cloud bucket. The pipeline needs to wait dynamically until today's source .parquet file appears before launching an intensive PySpark processing job on an external cluster. Which combination of Airflow operators provides the most resource-efficient design?

- A. A long-running BashOperator that sleeps in a loop, followed by a local python loop task.
- B. A FileSensor (or custom cloud sensor) to block/poke for the file, followed by a task executing the Spark application via cluster managers.
- C. A single @task that reads the dataset iteratively using Pandas until it stops failing.
- D. Running a Web Server cron task that forces DAG triggers every 5 minutes.

**Correct Answer:** B

**Rationale:** Using a Sensor allows Airflow to gracefully wait for an external condition to be met before handing off the execution step to heavy computation frameworks like Spark.

---

### Question 9 (MCQ – Scenario)

You are building an ML pipeline where an upstream data quality step counts the remaining records after a filtering stage. If the remaining records drop below an acceptable safety threshold, you must halt the main training process and immediately trigger a specific error-flagging logging task. If data quality is fine, training must proceed. How should this be orchestrated using Airflow?

- A. Use an unconditional linear chain task_a >> task_b >> task_c.
- B. Utilize a branch operator (like @task.branch or BranchPythonOperator) that dynamically evaluates the record count and returns the target task ID to execute.
- C. Rely on a single massive Python script that catches all exceptions globally.
- D. Configure catchup=True on the DAG definition to run historical periods automatically.

**Correct Answer:** B

**Rationale:** Branching allows conditional pathing inside a DAG execution graph based on runtime metrics generated by upstream jobs.

---

### Question 10 (MCQ – Scenario)

A data engineering team needs to move an existing batch workflow from standard cron to Apache Airflow. The pipeline needs to process historical data partitions ranging back over the last two years sequentially to build a backfilled data warehouse layer. Which DAG configuration parameter must be enabled to ensure past unrun periods are executed upon activation?

- A. schedule=None
- B. catchup=True
- C. retries=5
- D. trigger_rule='all_success'

**Correct Answer:** B

**Rationale:** When catchup=True (which is default behavior in many Airflow core versions), the scheduler automatically creates and runs DAG runs for every historical scheduling interval between the start_date and the current time.

---

### Question 11 (MCQ – Scenario)

Your production workflow contains a critical task that modifies an analytical table in a data lakehouse. If this task throws a runtime database connectivity exception, the DevOps team needs an automated notification written immediately to an external system alert log file without failing the entire infrastructure. How can you implement this cleanly inside the DAG?

- A. Edit the metadata database manually to change states.
- B. Register a custom Python function to the task's on_failure_callback parameter to handle file writing alerts.
- C. Force the Scheduler to restart automatically using a Bash loop.
- D. Wrap the entire workflow inside a single EmptyOperator.

**Correct Answer:** B

**Rationale:** The on_failure_callback hook accepts a callable function that receives the execution context dictionary when a task instance transitions into a FAILED state, allowing custom notifications or cleanups.

---

### Question 12 (MCQ – Scenario)

As an ML Engineer, you are deploying a pipeline that trains 10 independent regional models simultaneously, followed by a final model aggregation task. You notice that the 10 training tasks are running one after the other instead of concurrently, slowing down your pipeline. Which component or configuration change should you look into to enable parallel task execution?

- A. Increase the retries parameter inside the DAG definition.
- B. Switch the Airflow core executor configuration from SequentialExecutor to LocalExecutor or CeleryExecutor.
- C. Change the DAG schedule from @daily to None.
- D. Set the catchup parameter explicitly to False.

**Correct Answer:** B

**Rationale:** SequentialExecutor uses a SQLite backend which locks database writes and can only run one task instance at a time. Upgrading to a parallel executor (like Local or Celery) allows concurrent task execution.

---

## Section 3: Code Snippet Analysis Questions

### Question 13 (MCQ – Code)

Examine the following Airflow code fragment:

```python
with DAG(dag_id="demo_flow", start_date=datetime(2025,1,1), schedule=None) as dag:
    
    @task()
    def step_1():
        return [1, 2, 3]

    @task()
    def step_2(data):
        print(f"Data received: {data}")

    step_2(step_1())
```

What does this code do when executed?

- A. It crashes because step_1() returns an explicit list object that cannot be processed in parameters.
- B. It establishes a functional task dependency where step_1 runs first, and its return value is sent to step_2 via XCom implicitly.
- C. It executes step_2 first and sends empty mock strings to step_1.
- D. It compiles but does not form any valid relationship line inside the DAG UI.

**Correct Answer:** B

**Rationale:** The TaskFlow API allows data dependencies to be declared via function invocations. Passing the output of step_1() as an argument to step_2() informs Airflow that step_2 depends on step_1 and passes the value via XCom.

---

### Question 14 (MCQ – Code)

Given the downstream dependency specification below:

```
task_a >> [task_b, task_c] >> task_d
```

Which of the following describes the correct execution order of these tasks?

- A. task_a runs first. Then task_b and task_c can run concurrently. task_d runs only after both task_b and task_c complete.
- B. task_a runs, then task_b runs, then task_c runs, then task_d runs in a straight line.
- C. All four tasks run at the exact same moment in parallel.
- D. task_d runs first, triggering paths backward to task_a.

**Correct Answer:** A

**Rationale:** The bitshift operators >> dictate direction. Placing a list of tasks [task_b, task_c] downstream of task_a forks execution, while shifting that list into task_d converges them back together.

---

### Question 15 (MCQ – Code)

Consider this PySpark data transformation code block:

```python
df = spark.read.parquet("/data/trips.parquet")
filtered_df = df.filter(df["passenger_count"] > 2)
grouped_df = filtered_df.groupBy("vendor_id").count()
grouped_df.write.mode("overwrite").json("/output/summary")
```

At which line does Spark actually execute the computational heavy lifting across the worker nodes cluster?

- A. df = spark.read.parquet(...)
- B. filtered_df = df.filter(...)
- C. grouped_df = filtered_df.groupBy(...)
- D. grouped_df.write.mode(...).json(...)

**Correct Answer:** D

**Rationale:** Spark operations are divided into transformations (lazy) and actions (eager). filter and groupBy are transformations that build the logical plan. write is an action that forces the execution of the computed DAG.

---

### Question 16 (MCQ – Code)

Analyze the structure of this dictionary processing block within a pipeline script:

```python
import json
record = '{"name": "Alka", "age": 25, "city": "Bengaluru"}'
data = json.loads(record)
filtered = {k: data.get(k) for k in ["name", "state"]}
print(filtered)
```

What will be printed to the logs when this runs?

- A. {"name": "Alka", "age": 25}
- B. {'name': 'Alka', 'state': None}
- C. KeyError exception because "state" does not exist in the string.
- D. ['name', 'state']

**Correct Answer:** B

**Rationale:** json.loads converts the JSON string to a Python dictionary. The dictionary comprehension requests keys "name" and "state". Since dictionary .get() returns None for missing keys instead of raising a KeyError, "state" evaluates safely to None.

---

### Question 17 (MCQ – Code)

Look at this configuration block in an Airflow DAG:

```python
with DAG(
    dag_id="failure_demo",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task()
    def task_x():
        raise ValueError("Simulated Error")

    @task(trigger_rule='all_success')
    def task_y():
        print("Hello World")

    task_x() >> task_y()
```

What will the final execution state of task_y be when this DAG is triggered?

- A. SUCCESS
- B. FAILED
- C. UPSTREAM_FAILED
- D. SKIPPED

**Correct Answer:** C

**Rationale:** The default trigger rule is all_success. If an immediate upstream task fails (task_x), its downstream child tasks that require all successes cannot run and are automatically marked as UPSTREAM_FAILED.

---

### Question 18 (MCQ – Code)

Consider the following code script utilizing pandas:

```python
import pandas as pd
df = pd.DataFrame({"A": [10, 20], "B": [30, 40]})
df.loc[len(df)] = [50, 60]
print(len(df))
```

What value is output by the print statement?

- A. 2
- B. 3
- C. 4
- D. 0

**Correct Answer:** B

**Rationale:** The initial DataFrame has 2 rows (indices 0 and 1). len(df) evaluates to 2. Running df.loc[2] = [50, 60] appends a third row at index 2, making the new length 3.
---

## Section 4: Numerical & Computational Scheduling Calculations

### Question 19 (MCQ – Numerical)

An Airflow DAG is configured with the cron string expression `schedule="30 2 * * *"`. How many times will this pipeline execute in a standard non-leap calendar week?

- A. 30 times
- B. 2 times
- C. 7 times
- D. 14 times

**Correct Answer:** C

**Rationale:** The cron shorthand breaks down to minute=30, hour=2, day_of_month=*, month=*, day_of_week=*. This translates to "run at 02:30 AM every single day". Since a week contains 7 days, it runs exactly 7 times.

---

### Question 20 (MCQ – Numerical)

A cluster contains a dataset split into 24 distributed file blocks. A PySpark job applies a filter operation, reducing the active data size, and the engineer triggers `.coalesce(6)` followed by an action. Assuming the target cluster has 6 available executor slots working concurrently, how many distinct data partitions will be written out to storage concurrently?

- A. 24 partitions
- B. 4 partitions
- C. 6 partitions
- D. 1 partition

**Correct Answer:** C

**Rationale:** The coalesce(n) operation reduces the number of partitions in a DataFrame down to n without forcing a full data shuffle across the network. Thus, exactly 6 partition files will be written.

---

### Question 21 (MCQ – Numerical)

A pipeline needs to perform a safe backfill job. The DAG configuration parameters are set as: `start_date = datetime(2026, 6, 1)`, `end_date = datetime(2026, 6, 5)`, `schedule = "@daily"`, and `catchup = True`. How many automated historical DAG runs will the scheduler generate and enqueue?

- A. 1
- B. 4
- C. 5
- D. 0

**Correct Answer:** B

**Rationale:** Daily intervals created for backfilling are based on the periods completed. The intervals are June 1-2, June 2-3, June 3-4, and June 4-5. At the start of June 5, 4 full intervals have completed.

---

### Question 22 (MCQ – Numerical)

A PySpark processing application reads a raw tracking dataset containing exactly 1,200,000 records. A filtering step removes 45% of the entries due to missing sensor data. Next, a deduplication step eliminates 10,000 duplicate records. What is the precise final row count written to the target location?

- A. 660,000
- B. 540,000
- C. 650,000
- D. 1,190,000

**Correct Answer:** C

**Rationale:** 45% of 1,200,000 is 540,000 rows filtered out, leaving $1,200,000 - 540,000 = 660,000$ rows. Subtracting the 10,000 duplicates leaves exactly 650,000 rows.

---

### Question 23 (MCQ – Numerical)

An infrastructure team runs a cluster with 4 worker nodes, where each node has 8 CPU cores. A PySpark job is launched with configurations specifying `--num-executors 4 --executor-cores 4`. How many tasks can this specific configuration execute concurrently at peak capacity?

- A. 32 tasks
- B. 8 tasks
- C. 16 tasks
- D. 4 tasks

**Correct Answer:** C

**Rationale:** Total parallel execution slots are calculated by multiplying the number of allocated executors by the number of cores per executor: $4 \text{ executors} \times 4 \text{ cores/executor} = 16 \text{ concurrent tasks}$.

---

## Section 5: Multiple Select Questions (MSQs)

### Question 24 (MSQ)

Which of the following components are core parts of the Apache Airflow system architecture? (Select ALL correct choices)

- A. Web Server (UI dashboard)
- B. Metadata Database (Metastore)
- C. Spark Executor Engine
- D. Scheduler

**Correct Answers:** A, B, D

**Rationale:** Airflow relies on the Web Server, Scheduler, Worker, and a Metadata Database. The Spark Executor is an external compute component and is not part of Airflow's core framework architecture.

---

### Question 25 (MSQ)

An engineer is troubleshooting a pipeline that is supposed to write a CSV file converted to JSON. If the task fails because the input file is missing, which parameters inside the default_args dictionary can help make the DAG fault-tolerant? (Select ALL correct choices)

- A. retries
- B. retry_delay
- C. catchup
- D. owner

**Correct Answers:** A, B

**Rationale:** retries allows the task to automatically try executing again if it encounters an exception, and retry_delay configures how long it waits between attempts. catchup and owner do not control task retries.




