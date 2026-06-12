# Apache Spark, Apache Airflow, Apache Kafka, Apache Beam & Spark ML: 25 Practice Questions

## Section 1: Apache Airflow Architecture & Multi-Engine Orchestration

### Q1 (MCQ)

In a production enterprise deployment using Apache Airflow, what is the precise operational risk of executing long-running, resource-intensive data extractions inside a standard PythonOperator task instead of delegating the work to a specialized hook or remote operator (e.g., SparkSubmitOperator)?

A. The Airflow Web Server UI will crash due to localized browser thread blockages.

B. The Scheduler will immediately corrupt the underlying Metadata Database state tables.

C. The task will consume local resources on the Airflow worker node, risking Out-Of-Memory (OOM) errors that can crash the worker and interfere with other parallel tasks running on that same node.

D. Airflow will permanently downgrade the DAG schedule to manual execution mode.

**Answer: C**

**Explanation:** Airflow is fundamentally an orchestrator, not a heavy distributed compute engine. Running raw, compute-heavy Python code inside an Airflow worker process can starve the worker node of memory and CPU, crashing the worker process and interrupting concurrent tasks. Complex tasks should instead be offloaded to external compute environments like Spark via specialized operators.

### Q2 (MCQ – Scenario)

You are building an Airflow pipeline that handles automated retail order files. Your workflow needs to sleep and block execution until a daily transaction ledger file lands in a designated shared filesystem path.

Which native Airflow primitive component is explicitly designed to handle this event-driven waiting pattern efficiently?

A. BashOperator

B. @task.branch

C. FileSensor

D. EmptyOperator

**Answer: C**

**Explanation:** Airflow Sensors are specialized operators designed to wait for a specific external condition to turn true (such as checking file existence, a database record check, or a web hook). The FileSensor specifically blocks downstream execution until the target file is readable.

### Q3 (MSQ)

An engineer is structuring a critical workflow DAG and wants to configure complex execution conditions. Which of the following parameters or components represent key functional attributes that define an Airflow DAG? (Select ALL correct choices)

- [x] A. `schedule`: Determines exactly when or under what conditional intervals the workflow triggers.
- [x] B. `tasks`: Discrete, individual units of work (defined via operators or the TaskFlow API) that execute on worker nodes.
- [x] C. `trigger_rule`: Explicit settings (such as `all_success` or `all_done`) that dictate the conditional conditions under which a downstream task runs relative to its parent tasks.
- [x] D. `on_failure_callback`: A callable function reference that triggers alerts or actions if a specific task or complete DAG execution run fails.

**Answer: A, B, C, D**

**Explanation:** All four items are primary components of an Airflow pipeline DAG configuration. They specify scheduling, encapsulate actual units of work (tasks), define advanced upstream-downstream state conditions (trigger rules), and provide hooks for alerting/monitoring (callbacks).

### Q4 (MCQ – Scenario)

Look at the following execution topology configured across four independent Airflow tasks:

- Task A completes successfully.
- Task B encounters a data validation error and terminates with an uncaught Exception.
- Task C is wired directly downstream from Task B (`B >> C`) and has its trigger rule set to the default value (`TriggerRule.ALL_SUCCESS`).

What execution state will the Airflow scheduler assign to Task C, and will it run?

A. Task C will run normally because Task A was successful.

B. Task C will be marked as FAILED and will continually attempt retries for 24 hours.

C. Task C will be marked as UPSTREAM_FAILED and will be skipped entirely without executing.

D. Task C will be executed in a safe isolated fallback queue block.

**Answer: C**

**Explanation:** The default trigger rule is `all_success`. If any direct parent task fails, the downstream task cannot fulfill this constraint. As a result, the scheduler skips its execution entirely and assigns it an UPSTREAM_FAILED status.

## Section 2: Apache Spark Architecture & Data Processing Mechanics

### Q5 (MCQ)

During the physical compilation and execution of a distributed data transformation application, which core component of the Apache Spark architecture is exclusively responsible for turning user code into a Directed Acyclic Graph (DAG) of logical stages, scheduling execution tasks, and tracking worker node metrics?

A. Cluster Manager

B. Spark Executor

C. Spark Driver

D. Catalyst Optimizer

**Answer: C**

**Explanation:** The Spark Driver program runs the main application code, initializes the SparkSession, creates the logical/physical execution plan via DAGs, and schedules individual tasks across distributed worker executors.

### Q6 (MCQ)

An analytical routine performs an aggregate operation over a distributed dataset using the Spark DataFrame API:

```python
df.groupBy("store_id").count()
```

What fundamental architectural side-effect takes place across the cluster when this operation transitions from logical planning into physical task execution?

A. A narrow transformation is executed, where each partition is processed completely independently without network traffic.

B. A wide transformation is executed, causing a physical shuffle of data records across network segments to ensure rows sharing the same `store_id` wind up on the same executor node.

C. The operation pulls all raw tables directly into the single driver node's JVM heap space memory.

D. The metadata database instantly drops its indexes to speed up table writing.

**Answer: B**

**Explanation:** Aggregations, joins, and grouping operations require consolidating data across multiple partitions. This represents a wide transformation, which forces a physical Shuffle operation to redistribute matching keys across the network to common worker nodes.

### Q7 (MSQ)

Identify all operations or methods within the PySpark DataFrame API that are explicitly classified as Actions that will force Spark to materialize data and trigger a physical cluster job: (Select ALL correct choices)

- [ ] A. `df.filter()`
- [x] B. `df.count()`
- [x] C. `df.collect()`
- [x] D. `df.show()`

**Answer: B, C, D**

**Explanation:** Spark uses lazy evaluation. Transformations (`filter`, `select`, `withColumn`) only build up the logical graph without moving data. Actions like `count()`, `collect()`, and `show()` force Spark to execute the plan and return or write results.

### Q8 (MCQ)

When configuring a distributed cluster, how should the total number of data partitions in a Spark DataFrame generally relate to the aggregate number of execution cores available across all combined Spark Executors?

A. The number of partitions must be exactly half the total number of executor cores to avoid locking threads.

B. The number of partitions should be greater than the total number of executor cores to maximize parallel resource allocation and avoid idle cores.

C. There should be exactly 1 partition per cluster node, regardless of internal processor counts.

D. Spark requires all partitions to be collapsed into a single file before transformations can proceed.

**Answer: B**

**Explanation:** For optimal resource usage, the partition count should exceed the total number of executor cores (typically 2x to 4x more). This keeps all cores busy with work tasks, preventing a few slow-running partitions from leaving other cores idle.

## Section 3: Apache Kafka Distributed Messaging & Stream Topologies

### Q9 (MCQ)

In an Apache Kafka cluster, how does the system maintain data durability and message ordering guarantees when a topic partition has a replication factor of 3 and a high volume of producers publishing streams?

A. All 3 replica brokers accept concurrent client writes and continuously vote using a raft consensus matrix for each incoming record.

B. A single broker is elected as the Partition Leader to handle all client read and write traffic, while the remaining two nodes act as Follower Replicas that pull log segments sequentially to stay synchronized.

C. Messages are split into random bits and distributed round-robin across separate geographical data centers.

D. Kafka holds all messages inside volatile worker RAM caches, flushing to disk only during cluster reboots.

**Answer: B**

**Explanation:** Kafka manages replication through a single-leader model per partition. The Partition Leader coordinates all incoming client writes and reads, while Follower Replicas replicate the leader's append-only log in the background to maintain fault tolerance.

### Q10 (MCQ – Scenario)

An e-commerce monitoring system processes telemetry events from a Kafka topic that has exactly 3 partitions. A consumer application belongs to a consumer group with a `group_id` set to "telemetry-processors".

If you launch exactly 5 independent consumer instances all sharing this same "telemetry-processors" group ID, how will Kafka distribute the partition consumption assignments across these instances?

A. All 5 instances will read from all 3 partitions simultaneously, duplicating processing.

B. 3 consumer instances will each be assigned exactly 1 partition, while the remaining 2 consumer instances will sit completely idle as hot-standby units.

C. Kafka will dynamically shard each partition into nested sub-segments to accommodate all 5 consumers.

D. The cluster controller will reject the connection and throw an active thread assignment exception.

**Answer: B**

**Explanation:** Inside a single Consumer Group, Kafka balances consumption by mapping each partition to exactly one consumer instance. Since a partition cannot be split across multiple consumers within the same group, if the number of consumers exceeds the number of partitions ($5 > 3$), the extra consumers remain idle.

### Q11 (MSQ)

Which of the following infrastructure conditions or client operations can actively trigger an automatic Consumer Group Rebalance within an active Kafka cluster? (Select ALL correct choices)

- [x] A. A brand new consumer instance initialized with an identical `group_id` boots up and registers with the group coordinator.
- [x] B. An active consumer instance experiences a severe JVM Garbage Collection pause and fails to send heartbeats within the defined `session.timeout.ms` window.
- [x] C. A cluster engineer executes a command utility to scale out the topic partition size from 3 partitions to 6 partitions.
- [ ] D. A producer updates its client settings to switch its data serialization format from JSON strings to Avro binary records.

**Answer: A, B, C**

**Explanation:** A rebalance occurs whenever the cluster coordinator needs to redistribute partition ownership. This can be triggered by a consumer joining (A), a consumer failing or disconnecting (B), or a change to the underlying topic partitions being monitored (C). Changing producer serialization formats has no effect on consumer group state balancing.

### Q12 (MCQ)

When configuring a Kafka topic cleanup policy to perform Log Compaction (`cleanup.policy=compact`), what guarantee does Kafka provide regarding historical message retention inside the log segments?

A. All data records are unconditionally purged from the disk after exactly 7 calendar days.

B. Kafka will preserve at least the last known value payload for each unique message key within a partition log, removing older duplicate values during background segment cleanups.

C. Every message lacking an explicit Apache Avro validation schema is discarded.

D. Log fragments are compressed into standard zipped flat text spreadsheets.

**Answer: B**

**Explanation:** Log compaction ensures that Kafka always retains the latest state update for a given message key within a topic partition log. It discards older historical values for that key during background compaction sweeps, which is ideal for state restoration use cases.

## Section 4: Apache Beam Unified Programming Model

### Q13 (MCQ)

What is the fundamental architectural value proposition of utilizing the Apache Beam unified model for building data enterprise pipelines compared to writing code directly in engine-specific APIs like PySpark or Flink?

A. Apache Beam speeds up computation times by translating all Python operations directly into low-level native assembly instructions.

B. It decouples the pipeline's logical business graph from the physical execution engine, allowing the exact same code to run on different environments (like Apache Spark, Flink, or Google Cloud Dataflow) by simply changing the Runner configuration.

C. Beam completely eliminates the need to specify schemas or handle nested data types.

D. It restricts processing to single-node localized memory spaces, eliminating network shuffles.

**Answer: B**

**Explanation:** Apache Beam separates the construction of a data processing pipeline from its execution. By defining logic using Beam's abstractions (PCollection, PTransform), you can switch the underlying execution engine (the Runner) without modifying your business code.

### Q14 (MSQ)

Which of the statements below correctly capture the internal operational characteristics and design constraints of an Apache Beam PCollection object instance? (Select ALL correct choices)

- [x] A. It is completely immutable; applying a transformation to a PCollection always yields a brand new, independent PCollection instance.
- [ ] B. It allows developers to make direct, in-place index pointer modifications to individual row values at runtime.
- [x] C. It can represent either a bounded, static file layout (Batch processing) or an unbounded, continuous streaming telemetry source (Streaming processing).
- [x] D. Elements contained within a PCollection can be processed concurrently across separate distributed worker nodes.

**Answer: A, C, D**

**Explanation:** A PCollection represents a distributed data stream or dataset within Beam. It is immutable (A), can encapsulate both bounded batch and unbounded streaming workloads uniformly (C), and maps operations in parallel across workers (D). It does not support in-place mutations.

### Q15 (MCQ)

When implementing a custom mapping or parsing transformation stage in an Apache Beam pipeline by sub-classing the `beam.DoFn` primitive, where must your main element-by-element transformation logic be placed?

A. Inside the class initializer `__init__(self)` routine.

B. Inside a method named `process(self, element)`.

C. Inside an overridden execution method named `fit(self, data)`.

D. Inside a static registration lambda function labeled `transform()`.

**Answer: B**

**Explanation:** To perform custom distributed data processing using `beam.ParDo`, you must pass an object inheriting from `beam.DoFn`. This class must implement a `process` method, which acts on each individual element of the incoming collection and yields or returns the results.

### Q16 (MCQ)

What is the primary operational difference between the `beam.GroupByKey` and `beam.CombinePerKey` primitives when aggregating massive volumes of data across a distributed cluster runner?

A. GroupByKey can only handle numbers, while CombinePerKey is restricted to parsing text.

B. GroupByKey transfers all values matching a key across the network without reduction, whereas CombinePerKey optimizes network throughput by executing localized partial pre-aggregations on worker nodes before shuffling records.

C. CombinePerKey forces an eager download of all cluster bytes back to a local driver process instantly.

D. Both operations generate identical logical execution plans and network shuffle sizes across all runners.

**Answer: B**

**Explanation:** CombinePerKey leverages a design pattern similar to MapReduce combiners. It aggregates records locally within individual worker partitions before sending them across the network for the final shuffle. This dramatically reduces the total data volume transmitted compared to GroupByKey, which shuffles all raw elements over the network.

## Section 5: Spark ML Architecture, Feature Engineering, & Tuning

### Q17 (MCQ)

In the Spark ML structural framework, what is the exact functional distinction between a Transformer and an Estimator object asset?

A. A Transformer produces predictions as raw lists, while an Estimator requires converting data to dictionaries first.

B. A Transformer maps data types using string expressions, while an Estimator converts categorical text to float values.

C. A Transformer appends columns to an input DataFrame using an algebraic method via `.transform()`, while an Estimator fits parameters against data via `.fit()` to produce a corresponding Transformer model.

D. An Estimator can only handle classification targets, whereas a Transformer is restricted to regression analytics.

**Answer: C**

**Explanation:** Transformers implement `.transform()` to map one DataFrame to another (such as mapping feature columns or generating predictions). Estimators implement `.fit()`, which observes a training DataFrame to discover weights/parameters and outputs a trained Transformer (a Model).

### Q18 (MCQ)

When building an analytical workflow using `pyspark.ml.Pipeline`, how are the input DataFrame schemas and execution dependencies verified before processing large-scale records across the cluster?

A. Spark evaluates execution dependencies via static compile-time type checking at the time the script is written.

B. Pipelines utilize dynamic, schema-based validation checking at runtime as data passes through each configured stage boundary.

C. The cluster duplicates all data structures into flat text log formats to trace names.

D. Every model stage automatically forces a full cluster reboot to clean out memory caches.

**Answer: B**

**Explanation:** Spark ML pipelines evaluate schemas dynamically at runtime as data flows across structural boundaries, validating that columns required by downstream transformers are present in the incoming DataFrame.

### Q19 (MCQ)

Why is the `VectorAssembler` transformer considered an indispensable preprocessing stage when designing feature pipelines for training machine learning algorithms (like LinearRegression or LogisticRegression) in Spark ML?

A. It standardizes features to have a mean of 0 and a standard deviation of 1.

B. It parses unstructured JSON text into flat string columns.

C. It consolidates multiple independent scalar and vector feature columns into a single consolidated vector column, matching the mandatory structural schema input required by downstream Spark ML models.

D. It drops all row inputs that contain missing values or empty strings.

**Answer: C**

**Explanation:** Spark ML estimators for regression and classification assume that all input feature columns have been bundled into a single vector column (conventionally named "features"). VectorAssembler is the component that handles this aggregation.

### Q20 (MCQ)

When saving an enterprise asset via `PipelineModel.save("/path/to/model")`, what core benefit does Spark ML's native persistence mechanism provide to an operational machine learning team?

A. It translates the Python execution code into an inline C++ desktop application.

B. It enables full cross-language interoperability, allowing an ML pipeline trained in Python to be loaded and served inside Scala or Java JVM systems.

C. It automatically converts the models into plain text files.

D. It blocks access to the pipeline asset unless it matches specific geographical cluster zones.

**Answer: B**

**Explanation:** Spark ML persistence saves model metadata and weights using open schemas and structured parameters. This decoupling allows cross-language model sharing—pipelines fit in PySpark can be reloaded natively in Scala or Java microservices without translating the model.

## Section 6: Numerical Calculations & Resource Capacity Calculus

### Q21 (MCQ – Resource Task Math)

You are running a hyperparameter optimization routine over an operational dataset using Spark ML's CrossValidator:

- The parameter grid (ParamGridBuilder) defines exactly 6 distinct hyperparameter combinations.
- The cross-validation loop is configured to use 4 folds (`numFolds=4`).
- You explicitly set parallel execution capacity via `.setParallelism(2)`.

How many individual model training iterations must your Spark cluster compute in total to complete the cross-validation routing and export the final optimized model?

A. 24 models

B. 25 models

C. 26 models

D. 12 models

**Answer: B**

**Explanation:** 
1. Cross-validation model count: $\text{Combinations} \times \text{Folds} = 6 \times 4 = 24 \text{ models}$
2. Final model export: After identifying the best hyperparameter footprint, the CrossValidator fits a single final model over the entire original training dataset using those optimized values ($+1$ model).
3. Total iterations required: $24 + 1 = 25 \text{ models}$ (The parallelism parameter controls task concurrency, not the total number of evaluations required).

### Q22 (MCQ – Data Throughput Calculus)

A real-time analytics stream consumes messages from a Kafka topic partition log. An engine reports the following live state metrics for that target partition:

- Log End Offset (LEO): $5,300,000$ (The position of the next message appended to the log)
- High Watermark (HW): $5,280,000$ (The last message fully replicated across all ISR members)
- Current Committed Offset: $5,210,000$ (The latest message checkpoint saved by your consumer group)

What is the precise Consumer Lag metric for this partition group, and what is the maximum message offset position available for safe consumption by standard consumer clients?

A. Lag = $90,000$; Max Safe Offset = $5,300,000$

B. Lag = $70,000$; Max Safe Offset = $5,280,000$

C. Lag = $20,000$; Max Safe Offset = $5,210,000$

D. Lag = $70,000$; Max Safe Offset = $5,300,000$

**Answer: B**

**Explanation:** 
1. Consumer Lag is the distance between the consumer group's last committed offset and the highest safely replicated offset (the High Watermark): $\text{HW} - \text{Committed Offset} = 5,280,000 - 5,210,000 = 70,000 \text{ messages}$
2. Standard consumers cannot read un-replicated messages past the High Watermark, making $5,280,000$ the maximum safe offset available for consumption.

### Q23 (MCQ – Graph Complexity Math)

A complex pipeline graph is designed inside an Apache Beam application. The DAG features:

- An initial ingestion source split into 4 independent parallel branches.
- Each individual branch applies a sequence of 3 sequential PTransform operations.
- All 4 parallel paths then converge back together into a single, combined collection using a `beam.Flatten()` operation node.

How many total PTransform operation execution nodes compose this logical execution graph?

A. 12 nodes

B. 13 nodes

C. 16 nodes

D. 8 nodes

**Answer: B**

**Explanation:** 
1. Parallel branches execution tracking: $4 \text{ branches} \times 3 \text{ sequential transforms/branch} = 12 \text{ transform nodes}$
2. Convergence node layer: The final `beam.Flatten()` transformation adds exactly $1$ node.
3. Total structural transform nodes: $12 + 1 = 13 \text{ nodes}$

## Section 7: Code Diagnostics & Implementation Logic Puzzles

### Q24 (MCQ – Code Implementation Analysis)

Analyze the following PySpark Structured Streaming script designed to ingest data from an enterprise Kafka cluster:

```python
stream_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "taxi-events")
    .option("startingOffsets", "latest")
    .load())

query = (stream_df.writeStream
    .format("console")
    .outputMode("append")
    .start())
```

If your streaming consumer group goes completely offline, remains down for 3 consecutive hours while producers continue writing to the topic, and then restarts using this exact code block, how will Spark compute data offsets?

A. It will automatically backfill and reprocess all messages produced during the 3 hours it was offline.

B. It will skip past the 3 hours of unread records and consume only new messages arriving after the current execution start time.

C. It will crash instantly due to an offset boundary exception.

D. It will roll back and process all records from the beginning of the topic log (offset 0).

**Answer: B**

**Explanation:** Setting "startingOffsets" to "latest" instructs Spark to look only at the very end of the partition log when initializing a stream. Consequently, if the application boots up without an existing checkpoint, it will skip all historical records written while it was offline and only capture data arriving after the stream started. To avoid data loss across restarts, production applications use checkpoint directories (`.option("checkpointLocation", "...)`).

### Q25 (MCQ – Code Diagnostics)

Review the following PySpark machine learning configuration block:

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler

assembler = VectorAssembler(inputCols=["trip_distance", "fare_amount"], outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)

# Engineering pipeline assembly
pipeline = Pipeline(stages=[assembler, scaler, scaler])
```

What will happen when an engineer attempts to invoke `pipeline.fit(training_df)` on this pipeline instance?

A. The cluster executes perfectly because duplicate stages act as computational caches.

B. A runtime crash or exception will occur because a pipeline cannot reuse the exact same stage object instance due to uid uniqueness rules.

C. The target DataFrame columns are deleted to prevent circular loops.

D. Spark bypasses the second scaler stage and replaces it with an identity model step.

**Answer: B**

**Explanation:** Every independent stage added to a Spark ML pipeline must possess a completely unique string identifier property (uid). Passing the exact same scaler object instance twice results in duplicate UIDs within the pipeline's stage graph, which triggers an exception during structural validation.