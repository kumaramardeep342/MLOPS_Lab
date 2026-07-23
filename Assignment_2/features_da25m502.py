"""
Part A: Feature Engineering using Spark (20 Marks)
Implement a Python script named features <roll no>.py.
The script should:
1. Read train.csv and test.csv using Spark.
2. Implement a preprocessing pipeline
(a) Handle missing values.
(b) Encode categorical columns using StringIndexer
(c) Assemble features using VectorAssembler.
(d) Scale numerical features when appropriate for the selected model.
3. Fit the preprocessing pipeline on the training dataset and apply the fitted pipeline to both the
training and test datasets.
4. Create the target label for the training dataset. The processed test dataset should contain only
the transformed features.
5. Save the processed train and test dataset as two separate Parquet files (train processed <roll no>.parquet,
test processed <roll no>.parquet).

Roll No : DA25M502
Name : Amardeep Kumar
spark version : 3.5.6
dataset : https://www.kaggle.com/competitions/playground-series-s5e8/overview
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler

def main():
    # 1. Initialize Spark Session and read datasets
    spark = (
        SparkSession.builder
        .appName("features_da25m502_pipeline")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    try : 
        log_manager = spark.sparkContext._jvm.org.apache.logging.log4j.LogManager
        level = spark.sparkContext._jvm.org.apache.logging.log4j.Level
        log_manager.getLogger(
                "org.apache.spark.sql.execution.streaming.sinks.FileStreamSink"
            ).setLevel(level.ERROR)
    except Exception:
        pass  # If log4j is not available, we skip the logging configuration

    sc = spark.sparkContext

    print(f"Spark version      : {spark.version}")
    print(f"Master             : {spark.sparkContext.master}")
    print(f"Parallel workers   : {sc.defaultParallelism}")


    # Load training and test CSV files
    train_df = spark.read.csv("data/train.csv", header=True, inferSchema=True)
    test_df = spark.read.csv("data/test.csv", header=True, inferSchema=True)

    # Separate target variable and  'id' col
    target_col = "y"
    id_col = "id"
    
    # 2. Preprocessing Configuration
    # Automatically separate feature columns based on data type (excluding id and target)
    categorical_cols = [col for col, dtype in train_df.dtypes 
                        if dtype == "string" and col not in (target_col, id_col)]
    numerical_cols = [col for col, dtype in train_df.dtypes 
                      if dtype in ("int", "double", "bigint") and col not in (target_col, id_col)]

    # --- (a) Handle Missing Values ---
    # We calculate imputation values (median for numeric, mode for categorical) from training set
    impute_values = {}
    
    for col in numerical_cols:
        # Using 50th percentile (median) approximation for robustness against outliers
        median_val = train_df.approxQuantile(col, [0.5], 0.01)[0]
        impute_values[col] = median_val
        
    for col in categorical_cols:
        # Identify the most frequent non-null item (mode)
        mode_val = train_df.groupBy(col).count().orderBy(F.desc("count")).first()[col]
        # Default fallback if the column is entirely null
        impute_values[col] = mode_val if mode_val is not None else "unknown"

    # Apply imputation strategy via direct data fill
    train_imputed = train_df.na.fill(impute_values)
    test_imputed = test_df.na.fill(impute_values)

    # --- Pipeline Stages Construction ---
    stages = []

    # --- (b) Encode Categorical Columns ---
    # Map text strings to index indicator values ordered by frequent appearance
    indexed_categorical_cols = [f"{col}_indexed" for col in categorical_cols]
    string_indexer = StringIndexer(
        inputCols=categorical_cols, 
        outputCols=indexed_categorical_cols,
        handleInvalid="keep"  # Unseen items in test data will gather under a new designated index
    )
    stages.append(string_indexer)

    # --- (c) Assemble Features ---
    # Combine all individual input feature markers into one vector block
    all_features = numerical_cols + indexed_categorical_cols
    assembler = VectorAssembler(inputCols=all_features, outputCol="assembled_features")
    stages.append(assembler)

    # --- (d) Scale Numerical Features ---
    # Scale variables globally to ensure balance (standard score conversion: zero mean, unit variance)
    scaler = StandardScaler(
        inputCol="assembled_features", 
        outputCol="features", 
        withStd=True, 
        withMean=True
    )
    stages.append(scaler)

    # 3. Compile, Fit, and Transform 
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(train_imputed)

    train_transformed = pipeline_model.transform(train_imputed)
    test_transformed = pipeline_model.transform(test_imputed)

    # 4. Filter down final output columns
    # Training set requires features and the target label
    final_train_df = train_transformed.select("features", F.col(target_col).alias("label"))
    
    # Processed test dataset should contain only the transformed features
    final_test_df = test_transformed.select("features")

    # 5. Save the processed datasets as Parquet files
    final_train_df.write.mode("overwrite").parquet("data/train_processed_da25m502.parquet")
    final_test_df.write.mode("overwrite").parquet("data/test_processed_da25m502.parquet")

    print("Pipeline completed successfully! Transformed files saved.")
    spark.stop()

if __name__ == "__main__":
    main()