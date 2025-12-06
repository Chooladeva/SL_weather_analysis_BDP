from pyspark.sql import SparkSession
from pyspark.sql.functions import round as spark_round, col, month, year, to_date, when, avg,  min, max, count
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import builtins

# Start Spark session
spark = SparkSession.builder.appName("Low_ET0_Prediction_May").getOrCreate()

# Load datasets
weather_df = spark.read.option("header", True).option("inferSchema", True).csv("/opt/resources/weatherData.csv")
location_df = spark.read.option("header", True).option("inferSchema", True).csv("/opt/resources/locationData.csv")

# Join Weather and location and and process date
df = weather_df.join(location_df, "location_id")
df = df.withColumn("record_date", to_date(col("record_date"), "d/M/yyyy"))

# Define low ET₀ threshold
# Use 25th percentile of target as "low ET₀"
threshold = df.approxQuantile("et0_fao_evapotranspiration", [0.25], 0.01)[0]
print("Low ET threshold:", threshold)

# Filter for May data only
df_may = df.filter(month(col("record_date")) == 5)
print("May record count:", df_may.count())

# Check target range for imbalanced data
imbalanced_data_stats = df_may.select(
    min(col("et0_fao_evapotranspiration")).alias("min_et0_fao_evapotranspiration"),
    max(col("et0_fao_evapotranspiration")).alias("max_et0_fao_evapotranspiration"))
imbalanced_data_stats = imbalanced_data_stats.withColumn(
    "range",
    spark_round( col("max_et0_fao_evapotranspiration") - col("min_et0_fao_evapotranspiration"), 2))

print("Min, Max, and Range of ET0 for May:")
imbalanced_data_stats.show(truncate=False)

# Select relevant features and target
features = ["precipitation_hours", "sunshine_duration", "wind_speed_10m_max"]
target = "et0_fao_evapotranspiration"

df_may = df_may.select(*features, target)

# Assign weights to handle rare low ET₀ events for Linear Regression Model
df_may = df_may.withColumn(
    "weight",
    when(col(target) < threshold, 3.4).otherwise(0.5)
)

# Assemble features into a vector
assembler = VectorAssembler(inputCols=features, outputCol="features_vec")

# Scale features
scaler = StandardScaler(inputCol="features_vec", outputCol="features", withMean=True, withStd=True)

# Split train/test data
train_data, test_data = df_may.randomSplit([0.8, 0.2], seed=42)
print(f"Training rows: {train_data.count()}, Testing rows: {test_data.count()}")

# Define models
lr = LinearRegression(featuresCol="features", labelCol=target, weightCol="weight")
rf = RandomForestRegressor(featuresCol="features_vec", labelCol=target, seed=42)
gbt = GBTRegressor(featuresCol="features_vec", labelCol=target, seed=42)

# Tree-based models (RF & GBT) do not require feature scaling,
# but Linear Regression requires scaled features.
pipelines = {
    "Linear Regression": Pipeline(stages=[assembler, scaler, lr]),
    "Random Forest": Pipeline(stages=[assembler, rf]),
    "GBT": Pipeline(stages=[assembler, gbt])
}

# Hyperparameter grids
param_grids = {
    "Linear Regression": ParamGridBuilder() \
        .addGrid(lr.regParam, [0.1]) \
        .addGrid(lr.elasticNetParam, [0.0, 1.0]) \
        .build(),

    "Random Forest": ParamGridBuilder() \
        .addGrid(rf.maxDepth, [10]) \
        .addGrid(rf.numTrees, [80]) \
        .build(),

    "GBT": ParamGridBuilder() \
        .addGrid(gbt.maxDepth, [5]) \
        .addGrid(gbt.maxIter, [60]) \
        .build()
}

# Define evaluators
evaluator_rmse = RegressionEvaluator(labelCol=target, predictionCol="prediction", metricName="rmse")
evaluator_mae  = RegressionEvaluator(labelCol=target, predictionCol="prediction", metricName="mae")
evaluator_r2   = RegressionEvaluator(labelCol=target, predictionCol="prediction", metricName="r2")

results = {}
final_models = {}

for name, pipeline in pipelines.items():
    print(f"\n Training: {name}")

    crossval = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grids[name],
        evaluator=evaluator_rmse,
        numFolds=5,
        seed=42
    )

    cv_model = crossval.fit(train_data)
    predictions = cv_model.transform(test_data)

    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    r2  = evaluator_r2.evaluate(predictions)

    results[name] = rmse
    final_models[name] = cv_model

    print(f"\n{name} Results")
    print(f" RMSE: {rmse}")
    print(f" MAE : {mae}")
    print(f" R²  : {r2}")

# Select best model
best_model_name = builtins.min(results.items(), key=lambda x: x[1])[0]
best_model = final_models[best_model_name]
print("\n Best Model Selected:", best_model_name)

# Recreate a clean May dataframe with the same structure as training input
df_may_final = df.filter(month(col("record_date")) == 5).select(*features, target)

# Generate predictions on May dataset
final_predictions = best_model.transform(df_may_final)

# Analyze low ET₀ predictions
low_et_conditions = final_predictions.filter(col("prediction") < threshold) \
    .select(
        *features,
        col(target),
        spark_round(col("prediction"), 2).alias("prediction")
    ) \
    .orderBy("prediction")

print("Top 20 predicted low ET₀ rows:")
low_et_conditions.show(20, truncate=False)

# Calculate min, max, mean for each feature in low ET0 conditions
min_values = low_et_conditions.select(
    *[spark_round(min(f), 2).alias(f"min_{f}") for f in features]
)
print("Recommended Minimum feature values for predicted low ET₀ conditions:")
min_values.show(truncate=False)

max_values = low_et_conditions.select(
    *[spark_round(max(f), 2).alias(f"max_{f}") for f in features]
)
print("Recommended Maximum feature values for predicted low ET₀ conditions:")
max_values.show(truncate=False)

mean_values = low_et_conditions.select(
    *[spark_round(avg(f), 2).alias(f"avg_{f}") for f in features]
)
print("Recommended Mean feature values for predicted low ET₀ conditions:")
mean_values.show(truncate=False)

# Evaluate low ET₀ recall on test set
low_et_predictions = predictions.filter(col("prediction") <= threshold)
actual_low_et_count = test_data.filter(col(target) <= threshold).count()
predicted_low_et_count = low_et_predictions.count()

recall_low_et = predicted_low_et_count / actual_low_et_count if actual_low_et_count != 0 else 0

print(f" Predicted low ET₀ rows: {predicted_low_et_count}")
print(f" Actual low ET₀ rows in test set: {actual_low_et_count}")
print(f" Test-set recall-like proportion : {recall_low_et:.2f}")

# Save Outputs as CSV files
base_path = "/opt/resources/output/sparkML_low_ET0_Prediction"

low_et_conditions.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{base_path}/low_et_predictions")

mean_values.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{base_path}/mean_low_et_features")

max_values.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{base_path}/max_low_et_features")

min_values.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{base_path}/min_low_et_features")

final_predictions_clean = final_predictions.drop("features_vec")

final_predictions_clean.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{base_path}/all_may_predictions")

