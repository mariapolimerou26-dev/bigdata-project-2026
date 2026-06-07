import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, when

if len(sys.argv) < 4:
    print("Usage: spark-submit q5_join_repartition.py <trips_path> <zones_path> <output_base_path>")
    sys.exit(-1)

trips_path = sys.argv[1]
zones_path = sys.argv[2]
output_base = sys.argv[3]

START_HOUR = 7
END_HOUR = 8

start_time = time.time()

# Απενεργοποίηση του Broadcast Join για να αναγκάσουμε Shuffle Sort Merge Join
spark = SparkSession.builder \
    .appName("Taxi_Q5_SortMergeJoin_Ablation") \
    .master("local[*]") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

print("--- CONF: autoBroadcastJoinThreshold is set to -1 ---")

# 1. Φόρτωση Δεδομένων
df_trips = spark.read.parquet(trips_path)
df_zones = spark.read.option("header", "true").csv(zones_path)

# 2. Καθαρισμός & Φιλτράρισμα
df_cleaned = df_trips.withColumn(
    "duration_minutes", (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0
).filter(
    (col("tpep_pickup_datetime").isNotNull()) & (col("tpep_dropoff_datetime").isNotNull()) &
    (col("duration_minutes") > 0) & (col("trip_distance") > 0) & (col("total_amount") > 0)
)

df_window = df_cleaned.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
                      .filter((col("pickup_hour") >= START_HOUR) & (col("pickup_hour") <= END_HOUR))

# 3. Προετοιμασία Lookups
df_zones_tagged = df_zones.withColumn(
    "is_airport",
    when(
        (col("Zone").contains("JFK")) | (col("Zone").contains("LaGuardia")) | 
        (col("Zone").contains("Newark")) | (col("Zone").contains("Airport")) |
        (col("Borough") == "EWR"), 1
    ).otherwise(0)
)

df_pu_zones = df_zones_tagged.select(
    col("LocationID").alias("PU_ID"), col("Borough").alias("pu_borough")
)
df_do_zones = df_zones_tagged.select(
    col("LocationID").alias("DO_ID"), col("Borough").alias("do_borough")
)

# 4. Εκτέλεση των Joins (Sort Merge Join)
df_joined = df_window \
    .join(df_pu_zones, col("PULocationID") == col("PU_ID"), "inner") \
    .join(df_do_zones, col("DOLocationID") == col("DO_ID"), "inner")

print("\n=== EXPLAIN PHYSICAL PLAN (SORT MERGE JOIN) ===")
df_joined.explain()
print("================================================\n")

df_flow_test = df_joined.groupBy("pu_borough", "do_borough").count()
df_flow_test.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/smj_flows_test_csv")

end_time = time.time()
print("=== Q5 SORT-MERGE-JOIN SUCCESS ===")
print(f"Execution Time: {end_time - start_time} seconds")

spark.stop()
