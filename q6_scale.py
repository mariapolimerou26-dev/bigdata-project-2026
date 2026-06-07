import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs as _abs, coalesce, lit

if len(sys.argv) < 3:
    print("Usage: spark-submit q6_scale.py <trips_path> <output_base_path>")
    sys.exit(-1)

trips_path = sys.argv[1]
output_base = sys.argv[2]

# Θεωρούμε 265 total zones, το μισό είναι 132
HALF_ZONE_ID = 132

def run_imbalance_analysis(df_source, desc):
    start = time.time()
    
    # 1. Καθαρισμός (ΧΩΡΙΣ φίλτρο ώρας - για όλο το έτος)
    df_filtered = df_source.filter(
        col("tpep_pickup_datetime").isNotNull() & col("tpep_dropoff_datetime").isNotNull()
    )
    
    # 2. Pickups
    df_pickups = df_filtered.groupBy(col("PULocationID").alias("PU_ZoneID")).count().withColumnRenamed("count", "pickups")
    
    # 3. Dropoffs
    df_dropoffs = df_filtered.groupBy(col("DOLocationID").alias("DO_ZoneID")).count().withColumnRenamed("count", "dropoffs")
    
    # 4. Join
    df_imbalance = df_pickups.alias("p").join(
        df_dropoffs.alias("d"),
        col("p.PU_ZoneID") == col("d.DO_ZoneID"),
        "outer"
    ).select(
        coalesce(col("p.PU_ZoneID"), col("d.DO_ZoneID")).alias("ZoneID"),
        coalesce(col("p.pickups"), lit(0)).alias("total_pickups"),
        coalesce(col("d.dropoffs"), lit(0)).alias("total_dropoffs")
    ).withColumn("absolute_imbalance", _abs(col("total_pickups") - col("total_dropoffs")))
    
    # 5. Trigger Action για να μετρήσουμε πραγματικό χρόνο (π.χ. count)
    result_count = df_imbalance.count()
    end = time.time()
    
    print(f"=== {desc} ===")
    print(f"Processed Rows Count: {result_count}")
    print(f"Execution Time: {end - start} seconds\n")
    return end - start

# Εκκίνηση Spark
spark = SparkSession.builder \
    .appName("Taxi_Q6_Scaling_OD_Halves") \
    .master("local[*]") \
    .getOrCreate()

# Φόρτωση πλήρους έτους
df_all_trips = spark.read.parquet(trips_path)

print("--- STARTING SCALING EXPERIMENT (OD-HALVES) ---")

# Πείραμα Α: Μόνο το 1ο Μισό των Origin Zones (PULocationID <= 132)
df_first_half = df_all_trips.filter(col("PULocationID") <= HALF_ZONE_ID)
time_half = run_imbalance_analysis(df_first_half, "PART 1: FIRST HALF OF ZONES")

# Πείραμα Β: Όλο το έτος (Πλήρες Dataset)
time_full = run_imbalance_analysis(df_all_trips, "PART 2: FULL DATASET")

print("=== SCALING BENCHMARK SUMMARY ===")
print(f"First Half Time: {time_half:.2f} s")
print(f"Full Dataset Time: {time_full:.2f} s")
print(f"Scale Factor (Full / Half): {time_full / time_half:.2f}x")

spark.stop()
