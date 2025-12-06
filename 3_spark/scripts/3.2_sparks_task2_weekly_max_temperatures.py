from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, year, month, dayofmonth, ceil, avg, desc, row_number, max
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Task2_Weekly_Max_Temperatures").getOrCreate()

# Load weather data
weather_df = spark.read.option("header", True).option("inferSchema", True).csv("/opt/resources/weatherData.csv")

# Convert record_date column to date type
weather_df = weather_df.withColumn("record_date", to_date(col("record_date"), "d/M/yyyy"))

# Extract year and month
weather_df = weather_df.withColumn("year", year(col("record_date"))) \
                       .withColumn("month", month(col("record_date")))

# Compute monthly mean temperature per year and month
monthly_temp = weather_df.groupBy("year", "month") \
                         .agg(avg(col("temperature_2m_max")).alias("avg_max_temp"))

# Rank months per year by average max temperature
window_spec = Window.partitionBy("year").orderBy(desc("avg_max_temp"))
ranked_months = monthly_temp.withColumn("rank", row_number().over(window_spec))

# Select top 3 hottest months per year
top_months = ranked_months.filter(col("rank") <= 3)

# Join back to original data to get weekly max temperatures
weather_top_months = weather_df.join(top_months, ["year", "month"], "inner")

# Compute week in month and weekly max temperature
weekly_max_temp = weather_top_months \
    .withColumn("week_of_month", ceil(dayofmonth(col("record_date")) / 7)) \
    .groupBy("year", "month", "week_of_month") \
    .agg(max(col("temperature_2m_max")).alias("weekly_max_temp")) \
    .orderBy("year", "month", "week_of_month")

# Show top 25 results
weekly_max_temp.show(25)

# Save the result to CSV
weekly_max_temp.coalesce(1).write \
    .option("header", True) \
    .mode("overwrite") \
    .csv("/opt/resources/output/weekly_max_temp")

