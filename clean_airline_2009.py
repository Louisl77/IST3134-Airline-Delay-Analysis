# -*- coding: utf-8 -*-
"""
Created on Sun Aug 16 21:32:16 2026

@author: Owner
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Airline2009Cleaning") \
    .getOrCreate()

# S3 locations
input_path = "s3://ist3134-airline-delay-yewfeng/raw/2009.csv"
output_path = "s3://ist3134-airline-delay-yewfeng/cleaned/2009/"

# Load raw CSV
df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)

print("===== RAW DATA =====")
print("Rows:", df.count())
print("Columns:", len(df.columns))
df.printSchema()

# Remove empty/unnecessary column if present
if "Unnamed: 27" in df.columns:
    df = df.drop("Unnamed: 27")

# Convert flight date
df = df.withColumn(
    "FL_DATE",
    F.to_date(F.col("FL_DATE"), "yyyy-MM-dd")
)

# Remove exact duplicate rows
df = df.dropDuplicates()

# Create useful variables
df = df.withColumn("YEAR", F.year("FL_DATE")) \
       .withColumn("MONTH", F.month("FL_DATE")) \
       .withColumn("DAY_OF_WEEK", F.dayofweek("FL_DATE")) \
       .withColumn(
           "ROUTE",
           F.concat_ws("-", F.col("ORIGIN"), F.col("DEST"))
       ) \
       .withColumn(
           "DEP_HOUR",
           F.floor(F.col("CRS_DEP_TIME") / 100)
       )

# Create on-time indicator
# Arrival within 15 minutes of schedule = on time
df = df.withColumn(
    "ON_TIME",
    F.when(
        (F.col("CANCELLED") == 0) &
        (F.col("DIVERTED") == 0) &
        (F.col("ARR_DELAY") <= 15),
        1
    ).otherwise(0)
)

print("===== CLEANED DATA =====")
print("Rows:", df.count())
print("Columns:", len(df.columns))

# Save as Parquet
df.write.mode("overwrite").parquet(output_path)

print("Cleaning completed successfully.")
print("Output:", output_path)

spark.stop()