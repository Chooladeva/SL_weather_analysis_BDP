from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, year, month, count, sum as _sum, round as _round
from pyspark.sql.types import IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Task1_Shortwave_Radiation_Analysis") \
    .getOrCreate()

# Load CSV files with schema inference
weather_df = spark.read.option("header", True).option("inferSchema", True).csv("/opt/resources/weatherData.csv")
location_df = spark.read.option("header", True).option("inferSchema", True).csv("/opt/resources/locationData.csv")

# Join weather with location to get city_name
df = weather_df.join(location_df, on="location_id", how="left")

# Convert record_date from string to date
df = df.withColumn("record_date", to_date(col("record_date"), "d/M/yyyy"))

# Add year and month columns
df = df.withColumn("year", year(col("record_date"))) \
       .withColumn("month", month(col("record_date")))

# Flag days with shortwave_radiation_sum > 15
df = df.withColumn("high_radiation", (col("shortwave_radiation_sum") > 15).cast(IntegerType()))

# Group by district (city_name), year, month
radiation_summary = df.groupBy("city_name", "year", "month") \
    .agg(
        _round(100 * _sum("high_radiation") / count("high_radiation"), 1).alias("pct_high_radiation")
    ) \
    .orderBy("city_name", "year", "month")

# Show top 50 results
radiation_summary.show(25)

# Save the result to CSV
radiation_summary.coalesce(1).write \
    .option("header", True) \
    .mode("overwrite") \
    .csv("/opt/resources/output/shortwave_radiation_summary")
