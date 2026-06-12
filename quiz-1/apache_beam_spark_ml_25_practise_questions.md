# Apache Beam & Spark ML: 25 Practice Questions

## Section 1: Apache Beam Functional Architecture & Mechanics

### Q1 (MCQ)

In the Apache Beam unified programming model, what is the core architectural consequence of changing the execution Runner (e.g., from DirectRunner to SparkRunner)?

A. The pipeline's business logic code and data transformations must be rewritten entirely into native PySpark syntax.

B. The underlying data collections change from immutable PCollections to mutable Pandas DataFrames at runtime.

C. The pipeline graph logic remains identical while the physical execution engine is swapped, providing zero-change portability across environments.

D. Stateful windowing functions are completely stripped because only DirectRunner handles out-of-order event streams.

**Answer: C**

**Explanation:** Apache Beam separates the pipeline's logical graph from its execution engine. By switching the runner configuration parameter, the exact same business logic runs unmodified on local processes (DirectRunner) or distributed environments like Apache Spark or Flink.

### Q2 (MCQ)

When implementing a custom data processing mutation step in Apache Beam by extending the `DoFn` primitive class, inside which lifecycle method must your core record-by-record processing logic reside?

A. `initialize_process()`

B. `process(self, element)`

C. `fit(self, pcollection)`

D. `transform_element(self)`

**Answer: B**

**Explanation:** A custom Beam transformation uses `beam.ParDo()`, which takes a class inheriting from `beam.DoFn`. This class must implement the `process` method, which accepts an element and yields or returns zero or more processed elements.

### Q3 (MSQ)

In real-time stream analytical systems utilizing Apache Beam, what are the distinct roles of Watermarks and Allowed Lateness configurations? (Select ALL correct choices)

- [x] A. A Watermark is a monotonically increasing timestamp that tracks the pipeline's processing progress relative to the event time of incoming records.
- [ ] B. Allowed Lateness specifies the maximum number of network sockets that can remain open concurrently before crashing the cluster manager.
- [x] C. Allowed Lateness defines an explicit temporal grace window during which the pipeline will accept and process out-of-order data that arrives after the watermark has passed.
- [ ] D. Watermarks force the stream to execute exclusively under micro-batch constraints.

**Answer: A, C**

**Explanation:** Watermarks represent the system's understanding of event-time progression (where it expects no further records with earlier timestamps). Allowed Lateness sets a window after the watermark where late data is integrated rather than discarded.

### Q4 (MCQ)

What is the fundamental difference in execution behavior between the `beam.GroupByKey` and `beam.CombinePerKey` primitives when handling massive data streams?

A. GroupByKey can only be applied to plain string payloads, whereas CombinePerKey requires structural dictionaries.

B. GroupByKey gathers all values for a matching key across the network onto a single worker node without reducing data size, whereas CombinePerKey executes localized partial pre-aggregations (combiners) on workers before moving records across network shuffles.

C. CombinePerKey forces an eager collection back to the local driver thread memory pool instantly.

D. There is no behavioral difference; they produce identical physical execution steps under all runners.

**Answer: B**

**Explanation:** CombinePerKey applies optimization patterns similar to MapReduce combiners. It aggregates values locally within worker partitions before transferring them over the network, dramatically minimizing the data volume moved during a network shuffle compared to GroupByKey.

### Q5 (MSQ)

Identify all correct characteristics of an Apache Beam PCollection: (Select ALL correct choices)

- [x] A. It is completely immutable; applying a transformation always generates a new PCollection.
- [ ] B. It allows arbitrary random row updates using localized in-place point indices.
- [x] C. It can represent either a fixed-size dataset from static files (Batch) or an unbounded continuous real-time input stream (Streaming).
- [x] D. Elements inside a PCollection can be processed in parallel across multiple distributed worker threads.

**Answer: A, C, D**

**Explanation:** A PCollection is an immutable, distributed multi-element data collection. It cannot be mutated in place (A), supports both batch and streaming boundaries natively (C), and maps computations concurrently across workers (D).

## Section 2: Spark ML Pipeline Architecture & Design

### Q6 (MCQ)

In the Spark ML architecture framework, what is the precise functional definition that distinguishes a Transformer from an Estimator?

A. A Transformer outputs raw NumPy arrays, while an Estimator outputs specialized PySpark DataFrames.

B. A Transformer alters column data using compile-time type schemas, while an Estimator bypasses validation entirely.

C. A Transformer appends columns to an input DataFrame using an algebraic method via `.transform()`, while an Estimator fits parameters against data via `.fit()` to output a corresponding Transformer model.

D. An Estimator can only handle classification targets, whereas a Transformer is restricted to regression analytics.

**Answer: C**

**Explanation:** Transformers implement `.transform()` to map one DataFrame to another (such as mapping feature columns or generating predictions). Estimators implement `.fit()`, which observes a training DataFrame to discover weights/parameters and outputs a trained Transformer (a Model).

### Q7 (MCQ)

When evaluating the side-effects of execution inside a Spark ML pipeline, what state changes happen to your input DataFrames when executing operations like `Transformer.transform()` or `Estimator.fit()`?

A. The input DataFrames are mutated in place across the cluster to save executor memory space.

B. These operations are completely stateless and return an entirely new DataFrame or Model object, leaving original instances untouched.

C. The underlying JVM cluster context is crashed if data structures are reused.

D. Partitions are automatically coalesced down to 1 row to guarantee synchronous data writes.

**Answer: B**

**Explanation:** Spark ML operations are designed to be immutable and stateless. They take an input dataset and produce a brand new distinct instance, preserving the integrity of the original variables and lineage path.

### Q8 (MSQ)

Which of the statements below correctly capture the internal schema and metadata validation policies executed by a `pyspark.ml.Pipeline` container? (Select ALL correct choices)

- [x] A. Pipelines use schema-based runtime validation rather than compile-time safety testing before executing heavy cluster logic.
- [ ] B. Every stage added to a pipeline must possess the exact same object instance memory identifier pointer.
- [x] C. Each distinct stage inside a Spark ML Pipeline must have a completely unique string identifier (uid) parameter to prevent naming collisions.
- [ ] D. Pipelines cannot handle multi-input non-linear execution paths.

**Answer: A, C**

**Explanation:** Spark ML pipelines evaluate schemas dynamically at runtime as data flows across structural boundaries (A). Additionally, every component added to a pipeline requires a unique string asset identifier (uid) to preserve distinct configuration parameters during saving/loading cycles (C).

### Q9 (MCQ)

Why is the `VectorAssembler` a mandatory preparatory transformer stage when structuring features for training standard classification and regression models in Spark ML?

A. It converts continuous floating point data type columns into high-density strings for serialization.

B. It drops all row vectors that contain negative mathematical parameters automatically.

C. It consolidates multiple independent scalar and vector feature columns into a single consolidated vector column, matching the mandatory structural schema input required by downstream Spark ML models.

D. It enforces a standard normal distribution scaling metric over data values.

**Answer: C**

**Explanation:** Machine learning models in Spark ML (such as LinearRegression or LogisticRegression) assume that all explanatory variables have been bundled into a single vector column (conventionally named "features"). VectorAssembler is the transformer that handles this aggregation.

### Q10 (MCQ)

When persisting an ML asset via `PipelineModel.save("/path/to/model")`, what distinct advantage does Spark ML's native persistence mechanism provide across an enterprise analytics team?

A. It automatically translates the python runtime into an inline C++ desktop application.

B. It enables full cross-language interoperability, allowing an ML pipeline trained in Python to be loaded and served inside Scala or Java JVM systems.

C. It compresses raw parquet tracking tables down to standard raw CSV files.

D. It restricts access keys to specific geographic cluster regions.

**Answer: B**

**Explanation:** Spark ML persistence saves metadata and weights using open schemas and structured parameters. This decoupling allows cross-language model sharing—pipelines fit in PySpark can be reloaded natively in Scala or Java microservices without translating the model.

## Section 3: Scenario-Based Engineering Problems

### Q11 (MCQ – Scenario)

You are an ML Engineer writing a feature pipeline to analyze NYC taxi tips. The pipeline includes a StringIndexer stage to convert textual payment methods into numeric label indices.

During validation on live production streams, a record appears containing a brand new string value ("Crypto") that was never present during the training phase. By default, what will happen to your running streaming pipeline?

A. The pipeline will drop the column value and replace it with a random float number.

B. Spark will crash with a runtime error (SparkException) due to an unseen label category.

C. The row will be automatically routed into a fallback model layer.

D. Spark will instantiate a background thread to re-fit the training dataset.

**Answer: B**

**Explanation:** By default, StringIndexer throws a runtime exception when encountering an unseen string value during transformation. To alter this behavior, you must explicitly set `.setHandleInvalid("skip")` or `"keep"`.

### Q12 (MCQ – Scenario)

An engineer evaluates the following Spark ML configuration block:

```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol="features", labelCol="label")
paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.01]).build()
cv = CrossValidator(estimator=lr, estimatorParamMaps=paramGrid, numFolds=3)
```

If the input training DataFrame has exactly 6 million records, how does CrossValidator compute the final exported model returned after calling `.fit()`?

A. It returns an average ensemble model composed of the 3 fold variations blended together.

B. It selects the parameter set that achieved the best performance metric score, then completely retrains that specific model configuration on the full, original 6 million record training dataset.

C. It returns the exact model instance created during the final fold processing run.

D. It splits the dataset into 18 partitions to compress weights.

**Answer: B**

**Explanation:** After evaluating parameter combinations across folds to determine the best hyperparameter footprint, CrossValidator executes a final step: it applies those winning parameters to fit a new model over the entire original training dataset to leverage all available data.

### Q13 (MCQ – Scenario)

You are building an Apache Beam application to ingest user telemetry metrics. The pipeline needs to process data streams arriving from an unstable cellular network. The records contain event timestamps, but elements often arrive up to 10 minutes out of sequence.

Which architectural design choice will allow you to calculate rolling sums over these late events without dropping data records?

A. Implement a `beam.Map` transform that updates a local static global dictionary.

B. Define a fixed time window coupled with an explicit event-time watermark tracker and a 10-minute Allowed Lateness configuration window.

C. Force the pipeline to use the DirectRunner with processing-time wall-clock configurations.

D. Convert the PCollection into a standard PySpark DataFrame inside a DoFn process block.

**Answer: B**

**Explanation:** To handle out-of-order data, you must track event-time semantics (using watermarks) and define windows with a grace period (allowed_lateness). This tells Beam to update window accumulations when late-arriving records fall within that tolerance window.

## Section 4: Code Diagnostics & Execution Analysis

### Q14 (MCQ – Code Diagnostics)

Observe this attempted Apache Beam pipeline implementation:

```python
import apache_beam as beam

with beam.Pipeline() as p:
    lines = p | "Read" >> beam.Create(["apple,10", "banana,20", "apple,30"])
    mapped = lines | "Parse" >> beam.Map(lambda x: (x.split(",")[0], int(x.split(",")[1])))
    grouped = mapped | "Group" >> beam.GroupByKey()
    result = grouped | "Print" >> beam.Map(print)
```

When this pipeline runs locally under the DirectRunner, what is the exact type of elements passed into the final printing step?

A. A flat string value matching "apple,40".

B. A tuple pair matching (key, iterable_values), specifically structured like `('apple', [10, 30])`.

C. A standard localized multi-index Pandas DataFrame object.

D. A nested key-value dictionary array.

**Answer: B**

**Explanation:** `beam.GroupByKey` requires input elements to be key-value pairs (tuples of length 2). It aggregates all items sharing a key, outputting elements structured as (key, iterable), which means 'apple' will point to an iterable collection of its mapped integers [10, 30].

### Q15 (MCQ – Code Diagnostics)

Review the following PySpark feature pipeline segment:

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler

assembler = VectorAssembler(inputCols=["trip_distance", "fare_amount"], outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)

# Intentional mistake
pipeline = Pipeline(stages=[assembler, scaler, scaler])
```

What will happen when you attempt to invoke `pipeline.fit(training_df)` on this pipeline instance?

A. The cluster executes perfectly because duplicate stages act as computational caches.

B. A runtime crash or exception will occur because a pipeline cannot reuse the exact same stage object instance due to uid uniqueness rules.

C. The target DataFrame columns are deleted to prevent circular loops.

D. Spark bypasses the second scaler stage and replaces it with an identity model step.

**Answer: B**

**Explanation:** Each stage added to a Spark ML Pipeline must have a unique string identifier (uid). Reusing the exact same scaler object instance twice results in duplicate UIDs within the stage graph, which triggers an exception during validation.

### Q16 (MCQ – Code Output)

Analyze the following custom Beam transformation script:

```python
import apache_beam as beam

class FilterHighValue(beam.DoFn):
    def process(self, element):
        item, price = element
        if price > 100:
            yield item
        else:
            return None

with beam.Pipeline() as p:
    data = p | beam.Create([("Laptop", 1200), ("Mouse", 25), ("Monitor", 350)])
    filtered = data | beam.ParDo(FilterHighValue())
    filtered | beam.Map(print)
```

What precise console output is printed during execution?

A. `[('Laptop', 1200), ('Monitor', 350)]`

B. Laptop followed by Monitor on separate lines.

C. `None`

D. A dictionary matching `{"Laptop": True, "Mouse": False, "Monitor": True}`.

**Answer: B**

**Explanation:** The FilterHighValue DoFn inspects the tuple price. For "Laptop" ($1200 > 100$) and "Monitor" ($350 > 100$), it yields the item string components alone. The output elements are flat strings, which are printed sequentially on separate lines.

### Q17 (MCQ – Code Diagnostics)

Analyze this PySpark script:

```python
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
print(lr.getRegParam())
```

What value is printed, and what does this design pattern demonstrate about parameter handling in Spark ML?

A. It prints 0.0 because parameters can only be parsed from data tables.

B. It prints 0.3, demonstrating that Spark ML components maintain explicit internal getter/setter configurations for clear programmatic adjustments.

C. It throws an AttributeError because parameters must be accessed directly via `lr.regParam`.

D. It returns `None` until an active action is executed.

**Answer: B**

**Explanation:** Parameters in Spark ML are explicitly encapsulated using native estimators/transformers setter and getter tracking loops (`getRegParam()`), ensuring clear programmatic configuration access across pipeline configurations.

### Q18 (MCQ – Code Execution)

An engineer evaluates this pipeline topology code block:

```python
from pyspark.ml.feature import StandardScaler
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()
scaler = StandardScaler(inputCol="features", outputCol="scaled")
print(type(scaler))
```

What type of object class is instantiated here before calling any fitting actions?

A. `pyspark.ml.PipelineModel`

B. `pyspark.ml.feature.StandardScaler` (which acts as an Estimator)

C. `pyspark.ml.feature.StandardScalerModel` (which acts as a Transformer)

D. A local single-node Python dictionary layer.

**Answer: B**

**Explanation:** StandardScaler calculates statistical parameters (like variance and mean vectors) from a collection of records. Therefore, it is an Estimator. Once `.fit()` is called on it, it produces a StandardScalerModel, which is the Transformer.

## Section 5: Numerical & Resource Calculations

### Q19 (MCQ – Pipeline Task Math)

You are executing a Spark ML hyperparameter grid search via CrossValidator:

- The parameter grid maps out to exactly 4 distinct parameter combinations.
- The cross-validation loop is configured with 5 folds (`numFolds=5`).
- You explicitly set parallel execution capacity via `.setParallelism(4)`.

How many individual model training iterations must the cluster compute during the cross-validation and final model export phase combined?

A. 20 models

B. 24 models

C. 21 models

D. 4 models

**Answer: C**

**Explanation:** 
1. Cross-validation model count: $\text{Combinations} \times \text{Folds} = 4 \times 5 = 20 \text{ models}$
2. Final export model count: After finding the best combination, a single final model is fit on the full dataset, which adds $1$ model iteration.
3. Total iterations: $20 + 1 = 21 \text{ models}$ (The parallelism parameter controls task concurrency, not the total number of evaluations required).

### Q20 (MCQ – Graph Complexity Calculus)

An advanced analytical workflow is constructed using Apache Beam primitives. The pipeline graph features:

- A collection ingestion source that splits output into 3 parallel downstream branches.
- Each branch applies a sequence of 4 independent sequential PTransforms.
- The branches then converge back together using a single `beam.Flatten()` operation.

How many total PTransform operation execution nodes compose this logical Directed Acyclic Graph (DAG) pipeline layout?

A. 12 nodes

B. 13 nodes

C. 14 nodes

D. 7 nodes

**Answer: B**

**Explanation:** Calculate the nodes stage-by-stage:
- Parallel branches execution tracking: $3 \text{ branches} \times 4 \text{ sequential transforms/branch} = 12 \text{ transform nodes}$
- Convergence node layer: The final `beam.Flatten()` transformation adds exactly $1$ node.
- Total structural transform nodes: $12 + 1 = 13 \text{ nodes}$

### Q21 (MCQ – Cross-Validation Row Ingestion Calculus)

A production machine learning application utilizes CrossValidator with `numFolds=3` to process an input dataset containing exactly 3,000,000 clean records. During a single validation fold step, how many row records compose the training split partition and the validation split partition respectively?

A. Training Split = 1,000,000 rows; Validation Split = 2,000,000 rows

B. Training Split = 2,000,000 rows; Validation Split = 1,000,000 rows

C. Training Split = 3,000,000 rows; Validation Split = 3,000,000 rows

D. Training Split = 1,500,000 rows; Validation Split = 1,500,000 rows

**Answer: B**

**Explanation:** In $k$-fold cross-validation, the data is split evenly into $k$ parts. For each fold, $k-1$ parts are used for training and the remaining 1 part is used for validation. With $k=3$ and 3,000,000 rows, each part has 1,000,000 rows. Training split uses $3-1 = 2$ parts ($2 \times 1,000,000 = 2,000,000 \text{ rows}$), and validation uses 1 part ($1,000,000 \text{ rows}$).

## Section 6: Complex Multi-Select Evaluation Puzzles (MSQs)

### Q22 (MSQ)

When evaluating hyperparameter search models via CrossValidator in a distributed cluster environment, what are the precise operational side-effects of increasing the `.setParallelism(n)` parameter to a high value? (Select ALL correct choices)

- [x] A. It allows multiple parameter combinations to be trained concurrently across cluster nodes, which can accelerate grid search completion.
- [ ] B. It duplicates the input training data arrays permanently onto external disks to prevent cache eviction.
- [x] C. Concurrent evaluations share the same cluster resource pool during execution, which can increase the risk of resource contention or Out-Of-Memory errors if workers are saturated.
- [ ] D. It forces the final chosen model to operate as a multi-node voting ensemble.

**Answer: A, C**

**Explanation:** Setting a higher parallelism value accelerates search times by evaluating model variants concurrently (A). However, since these evaluations share the same underlying cluster memory and CPU cores, saturating worker threads can lead to memory contention issues (C).

### Q23 (MSQ)

Which of the following operations are classified as Transformers in the native Spark ML feature engine stack? (Select ALL correct choices)

- [x] A. VectorAssembler
- [ ] B. LogisticRegression
- [x] C. StandardScalerModel
- [ ] D. StringIndexer

**Answer: A, C**

**Explanation:** VectorAssembler is a transformer because it constructs vectors using purely structural schema transformations. StandardScalerModel is the model generated after fitting, making it a transformer. LogisticRegression and StringIndexer are estimators because they require observing data properties before outputting their respective model transformations.

### Q24 (MSQ)

An engineer wants to optimize an Apache Beam pipeline running on a distributed runner. Which of the following strategies or core architectural concepts can minimize execution overhead or handle late data? (Select ALL correct choices)

- [x] A. Replacing `beam.GroupByKey` with `beam.CombinePerKey` where mathematically feasible to perform local pre-aggregations.
- [x] B. Defining explicit Triggers alongside windowing functions to control exactly when window accumulations are emitted downstream.
- [ ] C. Forcing the input dataset to lose its keys and fall back to plain array objects globally.
- [x] D. Implementing a custom DoFn that yields elements sequentially to avoid buffer overloads.

**Answer: A, B, D**

**Explanation:** Replacing a raw group with a key-based combine reduces network shuffles via local worker aggregations (A). Triggers allow fine-grained control over when data partitions are emitted (B), and custom DoFn steps utilizing Python generators (yield) process records incrementally without buffering large data arrays in worker memory (D).

### Q25 (MSQ)

Which of the statements below correctly identify the runtime behavior and characteristics of a trained Spark ML PipelineModel object instance? (Select ALL correct choices)

- [x] A. It is composed exclusively of a sequence of fitted Transformers.
- [ ] B. It must be retrained via `.fit()` every time it executes a point prediction lookup.
- [x] C. It exposes a `.transform()` method to score incoming target prediction DataFrames.
- [x] D. It preserves the exact sequence of feature engineering transformations required to map raw inputs to predictions.

**Answer: A, C, D**

**Explanation:** When an original Pipeline (a mixture of estimators and transformers) is trained via `.fit()`, it returns a PipelineModel. A PipelineModel contains only fitted transformers (A), uses the `.transform()` method to run inference (C), and preserves the sequence of data transformations across features to avoid training-serving skew (D).

