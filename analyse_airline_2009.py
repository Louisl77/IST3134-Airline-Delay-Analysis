# -*- coding: utf-8 -*-
"""
Created on Sun Aug 16 21:53:47 2026

@author: Owner
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Airline2009Analysis") \
    .getOrCreate()

input_path = "s3://ist3134-airline-delay-yewfeng/cleaned/2009/"
output_base = "s3://ist3134-airline-delay-yewfeng/output/2009/"

# --------------------------------------------------
# 1. Load cleaned Parquet data
# --------------------------------------------------
df = spark.read.parquet(input_path)

print("Total rows:", df.count())
print("Total columns:", len(df.columns))

# --------------------------------------------------
# 2. OVERALL SUMMARY
# --------------------------------------------------
overall = df.agg(
    F.count("*").alias("TOTAL_FLIGHTS"),
    F.round(F.avg("DEP_DELAY"), 2).alias("AVG_DEP_DELAY"),
    F.round(F.avg("ARR_DELAY"), 2).alias("AVG_ARR_DELAY"),
    F.sum("CANCELLED").alias("CANCELLED_FLIGHTS"),
    F.round(F.avg("CANCELLED") * 100, 2).alias("CANCELLATION_RATE")
)

print("===== OVERALL SUMMARY =====")
overall.show(truncate=False)

overall.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(output_base + "overall_summary")

# --------------------------------------------------
# 3. AIRLINE PERFORMANCE
# --------------------------------------------------
airline = df.groupBy("OP_CARRIER").agg(
    F.count("*").alias("TOTAL_FLIGHTS"),
    F.round(F.avg("DEP_DELAY"), 2).alias("AVG_DEP_DELAY"),
    F.round(F.avg("ARR_DELAY"), 2).alias("AVG_ARR_DELAY"),
    F.sum("CANCELLED").alias("CANCELLED_FLIGHTS"),
    F.round(F.avg("CANCELLED") * 100, 2).alias("CANCELLATION_RATE"),
    F.round(F.avg("ON_TIME") * 100, 2).alias("ON_TIME_RATE")
).orderBy(F.desc("AVG_ARR_DELAY"))

print("===== AIRLINE PERFORMANCE =====")
airline.show(30, truncate=False)

airline.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(output_base + "airline_performance")

# --------------------------------------------------
# 4. AIRPORT PERFORMANCE
# --------------------------------------------------
airport = df.filter(
    (F.col("CANCELLED") == 0) &
    (F.col("DIVERTED") == 0) &
    F.col("ARR_DELAY").isNotNull()
).groupBy("ORIGIN").agg(
    F.count("*").alias("TOTAL_FLIGHTS"),
    F.round(F.avg("DEP_DELAY"), 2).alias("AVG_DEP_DELAY"),
    F.round(F.avg("ARR_DELAY"), 2).alias("AVG_ARR_DELAY")
).filter(
    F.col("TOTAL_FLIGHTS") >= 10000
).orderBy(F.desc("AVG_ARR_DELAY"))

print("===== AIRPORT PERFORMANCE =====")
airport.show(30, truncate=False)

airport.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(output_base + "airport_performance")

# --------------------------------------------------
# 5. MONTHLY TREND
# --------------------------------------------------
monthly = df.groupBy("MONTH").agg(
    F.count("*").alias("TOTAL_FLIGHTS"),
    F.round(F.avg("DEP_DELAY"), 2).alias("AVG_DEP_DELAY"),
    F.round(F.avg("ARR_DELAY"), 2).alias("AVG_ARR_DELAY"),
    F.round(F.avg("CANCELLED") * 100, 2).alias("CANCELLATION_RATE")
).orderBy("MONTH")

print("===== MONTHLY TREND =====")
monthly.show(12, truncate=False)

monthly.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(output_base + "monthly_trend")

# --------------------------------------------------
# 6. DELAY CAUSES
# --------------------------------------------------
delay_causes = df.agg(
    F.round(F.sum(F.coalesce(F.col("CARRIER_DELAY"), F.lit(0))), 2)
        .alias("CARRIER_DELAY"),
    F.round(F.sum(F.coalesce(F.col("WEATHER_DELAY"), F.lit(0))), 2)
        .alias("WEATHER_DELAY"),
    F.round(F.sum(F.coalesce(F.col("NAS_DELAY"), F.lit(0))), 2)
        .alias("NAS_DELAY"),
    F.round(F.sum(F.coalesce(F.col("SECURITY_DELAY"), F.lit(0))), 2)
        .alias("SECURITY_DELAY"),
    F.round(F.sum(F.coalesce(F.col("LATE_AIRCRAFT_DELAY"), F.lit(0))), 2)
        .alias("LATE_AIRCRAFT_DELAY")
)

print("===== DELAY CAUSES =====")
delay_causes.show(truncate=False)

delay_causes.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(output_base + "delay_causes")

# --------------------------------------------------
# 7. CANCELLATION REASONS
# --------------------------------------------------
cancellation_reasons = df.filter(
    F.col("CANCELLED") == 1
).groupBy("CANCELLATION_CODE").agg(
    F.count("*").alias("TOTAL_CANCELLATIONS")
).orderBy(F.desc("TOTAL_CANCELLATIONS"))

print("===== CANCELLATION REASONS =====")
cancellation_reasons.show(truncate=False)

cancellation_reasons.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(output_base + "cancellation_reasons")

# --------------------------------------------------
# 8. ROUTE PERFORMANCE
# --------------------------------------------------
route = df.filter(
    (F.col("CANCELLED") == 0) &
    (F.col("DIVERTED") == 0) &
    F.col("ARR_DELAY").isNotNull()
).groupBy("ROUTE").agg(
    F.count("*").alias("TOTAL_FLIGHTS"),
    F.round(F.avg("ARR_DELAY"), 2).alias("AVG_ARR_DELAY")
).filter(
    F.col("TOTAL_FLIGHTS") >= 5000
).orderBy(F.desc("AVG_ARR_DELAY"))

print("===== ROUTE PERFORMANCE =====")
route.show(30, truncate=False)

route.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(output_base + "route_performance")

print("===== ANALYSIS COMPLETED =====")

spark.stop()