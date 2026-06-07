import sys
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg, count, unix_timestamp, when, broadcast

if len(sys.argv) < 3:
    print("Usage: q5_all.py <parquet_yellow_2024_path> <output_base_path>")
    sys.exit(-1)

input_yellow_path = sys.argv[1]
output_base = sys.argv[2]

spark = SparkSession.builder \
    .appName("Q5_Airports_and_Borough_Flows") \
    .getOrCreate()

app_id = spark.sparkContext.applicationId

# Διαβάζουμε τα Parquet δεδομένα
df_yellow = spark.read.parquet(input_yellow_path)
# Υποθέτουμε ότι το taxi_zone_lookup είναι στον ίδιο base φάκελο Parquet
df_zone = spark.read.parquet(f"{output_base}/data/parquet/taxi_zone_lookup")

# 1. ΕΦΑΡΜΟΓΗ ΕΞΑΤΟΜΙΚΕΥΣΗΣ (ΑΜ: 2122303)
# Ημέρες: 16, 17, 18. Ώρες: [15, 19)
df_filtered = df_yellow.filter(
    (col("pickup_day").isin(16, 17, 18)) &
    (col("pickup_hour") >= 15) & (col("pickup_hour") < 19) &
    (col("total_amount") > 0) & (col("trip_distance") > 0)
).withColumn(
    "duration_minutes", 
    (unix_timestamp("dropoff_ts") - unix_timestamp("pickup_ts")) / 60.0
).filter(col("duration_minutes") > 0)

# Προετοιμασία για διπλό Join (Pickup Zone και Dropoff Zone)
zone_pu = df_zone.withColumnRenamed("LocationID", "pu_location_id") \
                 .withColumnRenamed("Borough", "pu_borough") \
                 .withColumnRenamed("Zone", "pu_zone")

zone_do = df_zone.withColumnRenamed("LocationID", "do_location_id") \
                 .withColumnRenamed("Borough", "do_borough") \
                 .withColumnRenamed("Zone", "do_zone")

# =========================================================================
# ΔΙΑΤΑΞΗ 1: DATAFRAME DEFAULT JOIN
# =========================================================================
start_df_default = time.time()

df_joined_1 = df_filtered.join(zone_pu, "pu_location_id", "inner") \
                         .join(zone_do, "do_location_id", "inner")

# Προσθήκη σήμανσης για αεροδρόμια
# (Ζώνες που περιέχουν τη λέξη Airport ή Newark)
df_analytics_1 = df_joined_1.withColumn(
    "is_airport",
    when((col("pu_zone").contains("Airport")) | (col("do_zone").contains("Airport")) |
         (col("pu_borough") == "EWR") | (col("do_borough") == "EWR"), lit("Airport_Trip"))
    .otherwise(lit("Regular_Trip"))
)

# Υπολογισμός Ροών (Top-14)
df_flows_1 = df_analytics_1.groupBy("pu_borough", "do_borough") \
                           .agg(count("*").alias("trips")) \
                           .orderBy(col("trips").desc()).limit(14)

# Σύγκριση Αεροδρομίων vs Regular
df_comparison_1 = df_analytics_1.groupBy("is_airport").agg(
    avg("trip_distance").alias("avg_distance"),
    avg("duration_minutes").alias("avg_duration"),
    avg("total_amount").alias("avg_total_amount")
)

# Αποθήκευση Catalyst Plan 1
df_plan_1 = df_flows_1._jdf.queryExecution().toString()
spark.range(1).select(lit(df_plan_1).alias("value")) \
    .write.mode("overwrite").text(f"{output_base}/plans/q5_df_default_plan")

df_flows_1.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/results/q5_df_default_flows")
df_comparison_1.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/results/q5_df_default_compare")

end_df_default = time.time()
t_df_default = end_df_default - start_df_default

# =========================================================================
# ΔΙΑΤΑΞΗ 2: SPARK SQL
# =========================================================================
start_sql = time.time()

df_filtered.createOrReplaceTempView("trips_raw")
df_zone.createOrReplaceTempView("zones_lookup")

sql_query_flows = """
    WITH JoinedTrips AS (
        SELECT 
            t.*,
            z_pu.Borough as pu_borough,
            z_do.Borough as do_borough
        FROM trips_raw t
        INNER JOIN zones_lookup z_pu ON t.pu_location_id = z_pu.LocationID
        INNER JOIN zones_lookup z_do ON t.do_location_id = z_do.LocationID
    )
    SELECT pu_borough, do_borough, COUNT(*) as trips
    FROM JoinedTrips
    GROUP BY pu_borough, do_borough
    ORDER BY trips DESC
    LIMIT 14
"""

sql_query_compare = """
    WITH JoinedTrips AS (
        SELECT 
            t.*,
            z_pu.Borough as pu_borough, z_pu.Zone as pu_zone,
            z_do.Borough as do_borough, z_do.Zone as do_zone
        FROM trips_raw t
        INNER JOIN zones_lookup z_pu ON t.pu_location_id = z_pu.LocationID
        INNER JOIN zones_lookup z_do ON t.do_location_id = z_do.LocationID
    ),
    TaggedTrips AS (
        SELECT *,
            CASE WHEN pu_zone LIKE '%Airport%' OR do_zone LIKE '%Airport%' OR pu_borough = 'EWR' OR do_borough = 'EWR'
                 THEN 'Airport_Trip' ELSE 'Regular_Trip' END as is_airport
        FROM JoinedTrips
    )
    SELECT is_airport,
           AVG(trip_distance) as avg_distance,
           AVG(duration_minutes) as avg_duration,
           AVG(total_amount) as avg_total_amount
    FROM TaggedTrips
    GROUP BY is_airport
"""

df_sql_flows = spark.sql(sql_query_flows)
df_sql_compare = spark.sql(sql_query_compare)

# Αποθήκευση Catalyst Plan 2
sql_plan = df_sql_flows._jdf.queryExecution().toString()
spark.range(1).select(lit(sql_plan).alias("value")) \
    .write.mode("overwrite").text(f"{output_base}/plans/q5_sql_plan")

df_sql_flows.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/results/q5_sql_flows")
df_sql_compare.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/results/q5_sql_compare")

end_sql = time.time()
t_sql = end_sql - start_sql

# =========================================================================
# ΔΙΑΤΑΞΗ 3: DATAFRAME ALTERNATIVE JOIN STRATEGY (BROADCAST JOIN)
# =========================================================================
# Αναγκάζουμε το Spark να κάνει Broadcast το μικρό lookup dataframe των ζωνών
start_df_alt = time.time()

df_joined_3 = df_filtered.join(broadcast(zone_pu), "pu_location_id", "inner") \
                         .join(broadcast(zone_do), "do_location_id", "inner")

df_analytics_3 = df_joined_3.withColumn(
    "is_airport",
    when((col("pu_zone").contains("Airport")) | (col("do_zone").contains("Airport")) |
         (col("pu_borough") == "EWR") | (col("do_borough") == "EWR"), lit("Airport_Trip"))
    .otherwise(lit("Regular_Trip"))
)

df_flows_3 = df_analytics_3.groupBy("pu_borough", "do_borough") \
                           .agg(count("*").alias("trips")) \
                           .orderBy(col("trips").desc()).limit(14)

df_comparison_3 = df_analytics_3.groupBy("is_airport").agg(
    avg("trip_distance").alias("avg_distance"),
    avg("duration_minutes").alias("avg_duration"),
    avg("total_amount").alias("avg_total_amount")
)

# Αποθήκευση Catalyst Plan 3
df_plan_3 = df_flows_3._jdf.queryExecution().toString()
spark.range(1).select(lit(df_plan_3).alias("value")) \
    .write.mode("overwrite").text(f"{output_base}/plans/q5_df_broadcast_plan")

df_flows_3.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/results/q5_df_broadcast_flows")

end_df_alt = time.time()
t_df_alt = end_df_alt - start_df_alt

# =========================================================================
# ΑΠΟΘΗΚΕΥΣΗ ΜΕΤΡΙΚΩΝ ΧΡΟΝΟΥ (JSON)
# =========================================================================
metrics_data = {
    "spark_application_id": app_id,
    "student_id": "2122303",
    "question": "Q5",
    "dataframe_default_join_seconds": t_df_default,
    "sql_join_seconds": t_sql,
    "dataframe_broadcast_join_seconds": t_df_alt
}

json_string = json.dumps(metrics_data, indent=4)
spark.range(1).select(lit(json_string).alias("value")) \
    .write.mode("overwrite").text(f"{output_base}/metrics/q5_metrics_json")

print("=== Q5 AIRPORTS AND FLOWS EXECUTED SUCCESSFULLY ===")
print(f"Default Join Time  : {t_df_default:.2f} seconds")
print(f"Spark SQL Time     : {t_sql:.2f} seconds")
print(f"Broadcast Join Time: {t_df_alt:.2f} seconds")

spark.stop()
