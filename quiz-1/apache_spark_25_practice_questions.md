# Apache Spark 25 Practice Questions

## Section 1: Deep Conceptual & Functional Architecture

### Question 1 (MCQ)

During the compilation of a structured query (`df.groupBy().count()`), which internal component of the Spark SQL Catalyst Optimizer is responsible for converting an Unresolved Logical Plan into a Resolved Logical Plan using data variations from the Metastore?

- A. Cost-Based Optimizer (CBO)
- B. Analyzer
- C. Physical Planner
- D. Code Generator

**Answer:** B

**Explanation:** The Catalyst Optimizer first passes the Unresolved Logical Plan to the Analyzer, which validates column names and data types against the catalog (Metastore) to output a Resolved Logical Plan. The CBO operates later during the physical planning phase.

---

### Question 2 (MCQ)

If an Apache Spark application is deployed on a cluster using YARN Cluster Mode, where exactly does the Spark Driver program execute?

- A. Inside the Client machine that submitted the command flag.
- B. On the Master Node hosting the YARN ResourceManager process.
- C. Within an ApplicationMaster container running on an arbitrary worker node.
- D. Spread dynamically across all allocated Executor JVM threads.

**Answer:** C

**Explanation:** In YARN Cluster mode, the driver runs inside the YARN ApplicationMaster container on a worker node. In YARN Client mode, the driver executes directly on the client machine outside the cluster.

---

### Question 3 (MSQ)

Which of the following internal events will forcingly cause Spark's execution engine to break a processing pipeline down and instantiate a completely new Stage boundary? (Select ALL correct choices)

- A. Invoking a `.join()` transformation on two DataFrames with different partitioning layouts.
- B. Utilizing `.select()` to rename and cast multiple data structures.
- C. Triggering a `.repartition(10)` function call to enforce network load balancing.
- D. Executing a `.filter()` query to strip null values out of an incoming stream.

**Answer:** A, C

**Explanation:** Stage boundaries are created whenever a Wide Dependency (shuffle) occurs. Joins and repartitioning force data shuffles across the cluster nodes network. select and filter represent Narrow Dependencies that can execute within the same stage pipeline.

---

### Question 4 (MCQ)

When utilizing Apache Airflow to coordinate a production workflow pipeline, what is the core architectural disadvantage of choosing the SequentialExecutor over the LocalExecutor?

- A. It completely disables the use of task retries and exponential backoff flags.
- B. It relies on an embedded SQLite backend database, restricting task execution to a single concurrent process.
- C. It prevents the Web Server dashboard from rendering task dependency graphs visually.
- D. It requires an external Kubernetes cluster environment to parse basic DAG definitions.

**Answer:** B

**Explanation:** The SequentialExecutor runs one task at a time sequentially because SQLite does not support multiple concurrent database writes, making it unsuitable for production scaling.

---

### Question 5 (MSQ)

Identify all operations that are classified as Actions in Apache Spark, which explicitly force the evaluation of the built Directed Acyclic Graph (DAG): (Select ALL correct choices)

- A. `take(n)`
- B. `withColumn()`
- C. `foreach()`
- D. `sortWithinPartitions()`

**Answer:** A, C

**Explanation:** take() and foreach() are actions that materialize data for consumption or application. withColumn() and sortWithinPartitions() are transformations that append evaluation steps to the logical plan lazily.

---

### Question 6 (MCQ)

When compiling code inside Spark SQL, which physical plan selection metric does the Cost-Based Optimizer (CBO) leverage to determine whether to rewrite a join operation as a Broadcast Hash Join instead of a Shuffle Hash Join?

- A. The count of columns present in the input target files.
- B. The estimated size-in-bytes statistics of the data collection stored in metadata.
- C. The physical geographic network proximity of the cluster workers.
- D. The number of explicit partitions defined by the spark.sql.shuffle.partitions flag.

**Answer:** B

**Explanation:** The CBO relies on size statistics (e.g., table size in bytes) collected in the metastore to evaluate execution costs and choose the most efficient plan, such as broadcasting tables that fall under the size threshold limit.

---

### Question 7 (MCQ)

In Apache Airflow's core architecture, what is the specific operational purpose of the Scheduler daemon compared to the roles of Executors and Workers?

- A. It hosts the persistent state data logs and renders the web interface.
- B. It continuously parses DAG files, updates task states in the metadata database, and pushes runnable task instances into the queue.
- C. It executes the individual Python tasks inside independent containerized worker environments.
- D. It spins up specialized micro-batch streams to ingest real-time data hooks.

**Answer:** B

**Explanation:** The Scheduler is the engine that checks the state database, monitors DAG definitions, calculates scheduling intervals, and orchestrates task queues. Executors specify how to allocate tasks, and Workers execute them.
---

## Section 2: Real-World Scenario Engineering Problems

### Question 8 (MCQ – Scenario)

You are a Lead Data Engineer building an analytics pipeline. You have a massive factual DataFrame containing 500 million rows that you need to join with a small metadata lookup DataFrame containing only 50 records (approx. 15 KB).

To prevent network shuffles and maximize performance, which optimization path should you enforce?

- A. Apply `.coalesce(1)` onto the factual DataFrame before performing the join.
- B. Trigger a broadcast join by wrapping the small lookup DataFrame in `broadcast()`.
- C. Enforce a `groupByKey()` strategy across both tables using an identical key index.
- D. Convert both datasets to standard Python lists using `.collect()` and join via an Airflow loop.

**Answer:** B

**Explanation:** Broadcast Hash Joins replicate the small lookup table to the memory space of every executor across the cluster. This allows tasks to perform localized joins on the large DataFrame partitions without inducing a wide network shuffle.

---

### Question 9 (MCQ – Scenario)

An ML Pipeline running inside a Spark cluster frequently suffers from Out-Of-Memory (OOM) errors on specific executors during a `.groupBy()` operation. Data analysis reveals that 85% of all tracking logs share the exact same tenant_id key value.

What distinct distributed data engineering issue are you encountering here?

- A. Driver memory leakage exhaustion.
- B. Data Skew.
- C. Narrow dependency execution collapse.
- D. Executor Core under-utilization.

**Answer:** B

**Explanation:** Data Skew happens when a disproportionate volume of data is associated with a single key. During wide transformations like groupBy, all data for that key is sent to a single executor, causing memory overload and OOM errors while other executors sit idle.

---

### Question 10 (MSQ – Scenario)

You are architecting an enterprise data platform. The requirements state:

- Low-latency, streaming data must be consumed from an Apache Kafka cluster continually.
- Data must be cleaned, transformed, and validated using distributed SQL semantics.
- The overall processing steps must execute automatically every day at 03:00 AM UTC, with full error logging and automated failure email alerts sent to Slack.

Which decoupled architectural tool stack perfectly fulfills these combined needs? (Select ALL correct choices)

- A. Apache Airflow as the scheduling dependency orchestrator.
- B. Apache Spark Structured Streaming as the real-time processing engine.
- C. GraphX as the structural distributed micro-batch storage layer.
- D. Pandas running inside a single Docker Container to process Kafka offsets.

**Answer:** A, B

**Explanation:** Apache Airflow manages orchestration, alerting, and crontab schedules. Spark Structured Streaming is built to scale streaming workloads with SQL compatibility. GraphX is for graph computations, and Pandas cannot scale across a distributed architecture natively.

---

### Question 11 (MCQ – Scenario)

A financial data engineering team is moving an on-premise transactional workflow to Apache Airflow. The analytics pipeline needs to rerun past historical daily partitions for the last full calendar year to generate a clean backfilled metrics warehouse layer.

Which DAG-level configuration parameter must be set to True to allow the scheduler to automatically create and catch up on unexecuted historical pipeline periods?

- A. provide_context
- B. catchup
- C. render_template_as_native_obj
- D. is_paused_upon_creation

**Answer:** B

**Explanation:** When catchup=True, the scheduler automatically schedules and runs any historical intervals between the DAG's defined start_date and the current timestamp that have not yet executed.

---

### Question 12 (MCQ – Scenario)

You are developing an ETL pipeline in Airflow where a file validation task downloads a dataset from an external vendor. The vendor's network is unstable, resulting in frequent connection dropouts that resolve within a few minutes.

To prevent these transient network drops from failing your entire workflow, which set of default_args should you apply to your task definition?

- A. Set trigger_rule='all_done' and configure catchup=False.
- B. Provide a custom on_failure_callback function that restarts the Airflow web server.
- C. Enforce retries to a value greater than 0 and define a reasonable retry_delay.
- D. Set execution_timeout to 5 seconds to skip the task quickly.

**Answer:** C

**Explanation:** Setting retries allows Airflow to re-attempt execution if a task fails. Combining it with a retry_delay ensures that the transient network issue has time to clear before the system tries again.

---

### Question 13 (MCQ – Scenario)

A machine learning engineer needs to design a data validation pipeline that runs an extensive PySpark job. The requirement dictates that the pipeline must wait at the beginning of the workflow until a tracking log file is written into an HDFS landing path by a third-party application.

Which native Airflow operator model handles this file-waiting condition efficiently without wasting processing resources?

- A. BashOperator running a continuous Unix sleep loop command.
- B. FileSensor configured to monitor the explicit target HDFS file path.
- C. PythonOperator performing a memory-bound file validation.
- D. EmptyOperator utilizing an all_success trigger rule logic block.

**Answer:** B

**Explanation:** Sensors are specialized operators that poll at specified intervals (using a poke or reschedule mechanism) to wait for an external state or file to exist before letting downstream execution proceed.
---

## Section 3: Code Snippet Execution Analysis

### Question 14 (MCQ – Code Output)

Consider the following script executed via pyspark:

```python
df_sales = spark.read.parquet("/data/sales_records")
df_filtered = df_sales.filter(df_sales["net_amount"] > 5000)
df_final = df_filtered.withColumn("taxed_amount", df_filtered["net_amount"] * 1.18)

print("Step 1 Complete")
record_count = df_final.count()
print(f"Total Rows: {record_count}")
```

Which of the statements below correctly describes the cluster behavior during runtime?

- A. Spark triggers three consecutive cluster-wide jobs instantly during the first three setup lines.
- B. No distributed cluster computations happen until the `.count()` action is invoked on line 5.
- C. The print statement "Step 1 Complete" prints after the total rows count calculation finishes.
- D. The JVM crashes immediately at line 3 due to variable pointer re-use.

**Answer:** B

**Explanation:** Spark transformations are entirely lazy. Reading metadata, filtering, and adding columns merely construct the logical execution plan. A distributed job is only submitted to the executors when the action `.count()` is reached.

---

### Question 15 (MCQ – Code Output)

Analyze this PySpark snippet:

```python
raw_data = [("User_A", 10), ("User_B", 20), ("User_A", 30)]
rdd_base = sc.parallelize(raw_data)
rdd_mapped = rdd_base.map(lambda x: (x[0], x[1] * 2))
rdd_reduced = rdd_mapped.reduceByKey(lambda a, b: a + b)
output = rdd_reduced.collect()
print(output)
```

What is the exact printed output array format?

- A. [('User_A', 40), ('User_B', 40)]
- B. [('User_A', 80), ('User_B', 40)]
- C. [('User_A', 20), ('User_B', 20), ('User_A', 60)]
- D. None

**Answer:** B

**Explanation:** 
1. `.map()` multiplies values by 2: [("User_A", 20), ("User_B", 40), ("User_A", 60)]
2. `reduceByKey()` sums values for matching keys: For "User_A", $20 + 60 = 80$. For "User_B", it remains $40$.
3. Result: [('User_A', 80), ('User_B', 40)]

---

### Question 16 (MCQ – Code Execution)

An engineer runs the following workflow snippet inside an environment:

```python
df = spark.read.json("unstructured_logs.json")
df_sampled = df.sample(withReplacement=False, fraction=0.01, seed=42)
df_sampled.persist()
print(df_sampled.toDebugString().decode())
```

What happens under the hood when `df_sampled.persist()` is executed?

- A. The sample dataset is immediately processed and written out to permanent HDFS storage disk volumes.
- B. The DataFrame is flagged for storage caching, but it will only be cached in memory when an action is subsequently called on it.
- C. Spark wipes out the current SparkSession memory context to free cache slots.
- D. The data is duplicated across every available worker node using disk serialization exclusively.

**Answer:** B

**Explanation:** `.persist()` or `.cache()` are lazy operations in Spark. They flag a dataset to be stored in memory/disk, but the caching mechanism isn't triggered until an action passes through that node of the DAG.

---

### Question 17 (MCQ – Code Output)

Observe the following code script utilizing pandas:

```python
import pandas as pd
df = pd.DataFrame({"X": [100, 200], "Y": [300, 400]})
df.loc[len(df)] = [500, 600]
print(df.index.tolist())
```

What precise structural value is printed to the terminal console output?

- A. [0, 1]
- B. [1, 2]
- C. [0, 1, 2]
- D. [3]

**Answer:** C

**Explanation:** The initial DataFrame contains 2 rows, with indices 0 and 1. Evaluating len(df) returns 2. Running df.loc[2] = [500, 600] appends a new row at index 2, making the final index array [0, 1, 2].

---

### Question 18 (MCQ – Code Dependency)

Review the downstream functional dependency definition structure from an Airflow DAG file:

```
task_1 >> [task_2, task_3] >> task_4
```

Which of the statements below correctly describes the execution sequence of this task chain?

- A. All four tasks are triggered simultaneously by the scheduler to maximize parallelism.
- B. task_1 must execute successfully. Then task_2 and task_3 can run concurrently. task_4 will execute only after both task_2 and task_3 finish.
- C. task_1 executes, followed sequentially by task_2, then task_3, and finally task_4 in a single line.
- D. task_4 executes first, then passes parameters backward to evaluate task_1.

**Answer:** B

**Explanation:** The bitshift operator >> specifies upstream-to-downstream relationships. Branching out to a list [task_2, task_3] splits execution paths, and shifting that list into a single task task_4 joins them back together.

---

## Section 4: Numerical & Computational Calculations

### Question 19 (MCQ – Numerical Partitioning)

Your data platform processes a flat uncompressed file containing exactly 120 GB of raw tabular tracking information. By default, Apache Spark sets its maximum split partition size constraint to 128 MB when reading raw files from a distributed storage bucket.

How many parallel partitions will Spark generate automatically when ingesting this file into a DataFrame?

- A. 120 partitions
- B. 960 partitions
- C. 1,200 partitions
- D. 96 partitions

**Answer:** B

**Explanation:** Convert total data into Megabytes: $120 \text{ GB} \times 1024 \text{ MB/GB} = 122,880 \text{ MB}$. Divide this by the default partition split target: $122,880 \text{ MB} / 128 \text{ MB} = 960 \text{ partitions}$.

---

### Question 20 (MCQ – Concurrency Math)

You provision a production Spark cluster with the following exact cluster specification metrics:
- Worker Nodes = 8
- CPU Cores per Worker Node = 16
- RAM per Worker Node = 64 GB

You submit your job using these precise configurations: `--num-executors 14 --executor-cores 4 --executor-memory 16G`.

What is the maximum number of tasks this cluster can execute concurrently at peak load based on your resource allocations?

- A. 128 tasks
- B. 64 tasks
- C. 56 tasks
- D. 14 tasks

**Answer:** C

**Explanation:** Max task concurrency is bounded by the number of active executors multiplied by the processing slots allocated per executor: $14 \text{ executors} \times 4 \text{ cores/executor} = 56 \text{ concurrent tasks}$. The hardware ceilings are not reached, so allocation variables dictate the limits.

---

### Question 21 (MCQ – Pipeline Task Math)

An advanced analytical application consists of a pipeline structure with the following metrics:
- The application runs exactly 3 independent Jobs.
- Each Job maps out to 4 distinct consecutive Stages.
- Each Stage contains a partitioning distribution splitting data across 16 Tasks.

How many individual Task Instances must the Cluster Manager schedule and track from initialization to completion?

- A. 48 tasks
- B. 64 tasks
- C. 192 tasks
- D. 12 tasks

**Answer:** C

**Explanation:** The calculation requires multiplying the entire execution hierarchy: $\text{Jobs} \times \text{Stages/Job} \times \text{Tasks/Stage}$. Thus: $3 \times 4 \times 16 = 192 \text{ distinct task instances}$.

---

### Question 22 (MCQ – Cron & Backfill Calculation)

An Airflow pipeline's crontab schedule is configured with the following expression: `schedule="45 10 * * 1-5"`. The pipeline's parameters are configured with start_date = datetime(2026, 6, 1) (which is a Monday), end_date = datetime(2026, 6, 8) (the following Monday), and catchup = True.

How many active DAG Runs will the scheduler compute and enqueue for execution over this historical backfill window?

- A. 7 runs
- B. 5 runs
- C. 24 runs
- D. 1 run

**Answer:** B

**Explanation:** The cron syntax `45 10 * * 1-5` translates to: "Run at 10:45 AM every day from Monday through Friday". Over the week from Monday, June 1 to Monday, June 8, there are exactly 5 weekdays (June 1, 2, 3, 4, and 5). The weekend days (Saturday and Sunday) are skipped by the cron filter, resulting in 5 runs.

---

### Question 23 (MCQ – Row Ingestion Calculus)

A PySpark batch cleaning task processes an active dataset containing exactly 2,500,000 raw tracking records. The application first executes an identity parsing operation that drops 30% of the rows due to formatting defects. Next, a deduplication pass identifies and removes exactly 50,000 completely identical duplicate rows.

What is the final record row count that is written out to storage?

- A. 1,750,000 rows
- B. 1,700,000 rows
- C. 2,450,000 rows
- D. 1,250,000 rows

**Answer:** B

**Explanation:** Dropping 30% removes $2,500,000 \times 0.30 = 750,000$ defective rows, leaving $1,750,000$ rows. Subtracting the 50,000 duplicate rows leaves exactly $1,700,000$ final rows.

---

## Section 5: Complex Multi-Select Evaluation Puzzles (MSQs)

### Question 24 (MSQ)

An engineer is debugging a Spark architecture and notices that data is continually being spilled to disk during an active processing step. Which of the following parameters or programmatic adjustments can directly optimize memory space or control shuffling to mitigate this issue? (Select ALL correct choices)

- A. Adjusting spark.memory.fraction to expand the execution pool storage limits.
- B. Enforcing a `.map()` transformation loop inside an Airflow custom Operator script.
- C. Increasing the partition frequency count via `.repartition()` to decrease individual partition size.
- D. Allocating more container RAM memory capacity by configuring `--executor-memory`.

**Answer:** A, C, D

**Explanation:** Disk spilling happens when a data partition exceeds the allocated memory execution slot size of a JVM thread. Increasing overall memory per executor (D), adjusting the memory allocation ratios (A), or splitting the data into smaller partitions (C) all keep processing operations within memory limits. Airflow map loops do not modify Spark executor mechanics.

---

### Question 25 (MSQ)

Which of the following descriptions accurately characterize the architectural behavior of Wide Transformations in Apache Spark? (Select ALL correct choices)

- A. They require no data movement or redistribution across network nodes in the cluster.
- B. They necessitate a shuffle phase to gather matching data keys onto the same target executor.
- C. They serve as the foundational boundaries that define separate execution Stages inside the DAG.
- D. Examples include basic operations like `.select()`, `.rename()`, and `.drop()`.

**Answer:** B, C

**Explanation:** Wide transformations require moving data across different cluster nodes (shuffling), which breaks the execution pipeline into multiple stages. Operations like select and drop are narrow transformations because they don't require data from other partitions.

---

### Question 26 (MSQ)

Identify all statements that accurately describe how Apache Spark handles internal Lazy Evaluation mechanics: (Select ALL correct choices)

- A. It allows the Catalyst Optimizer to look at the entire lineage chain and optimize execution plans before running jobs.
- B. It eliminates the need for an external cluster manager like YARN or Kubernetes entirely.
- C. It saves execution time and memory resources by preventing unnecessary intermediate data materialization steps.
- D. It forces Spark to execute write tasks backwards, starting from data destinations up to the sources.

**Answer:** A, C

**Explanation:** Lazy evaluation allows Spark to build an execution plan (lineage) without immediately executing it. This gives the Catalyst Optimizer a holistic view of the pipeline to optimize operations (e.g., combining filter steps) and prevents wasting memory on intermediate results. It has no bearing on cluster manager prerequisites.

---

### Question 27 (MSQ)

When configuring storage formats for a modern analytics data lakehouse pipeline, what clear technical advantages does Parquet have over raw CSV files for large-scale queries? (Select ALL correct choices)

- A. It uses columnar storage, allowing Spark to read only the specific columns requested by a query (projection pushdown).
- B. It is a human-readable text file format that can be edited inside any standard terminal shell.
- C. It embeds descriptive data schemas and statistics natively within the file footer metadata.
- D. It supports native file compression (like Snappy), which reduces storage footprints and network transfer costs.

**Answer:** A, C, D

**Explanation:** Parquet is a binary, columnar storage format that supports file compression (D) and query pruning via projection and predicate pushdown (A). It also embeds schema information natively in its metadata footer (C). It is not a human-readable plaintext format like CSV.

