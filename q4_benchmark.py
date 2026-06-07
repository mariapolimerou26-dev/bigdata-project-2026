import sys
import time
from pyspark.sql import SparkSession

if len(sys.argv) < 3:
    print("Usage: spark-submit q4_benchmark.py <input_parquet_path> <input_csv_path>")
    sys.exit(-1)

parquet_path = sys.argv[1]
csv_path = sys.argv[2]

# --- ΠΡΟΣΩΠΙΚΟ ΠΑΡΑΘΥΡΟ ΜΑΡΙΑΣ ---
START_HOUR = 7
END_HOUR = 8
# ---------------------------------

spark = SparkSession.builder \
    .appName("Taxi_Q4_Format_Benchmark") \
    .master("local[*]") \
    .getOrCreate()

# ----------------------------------------------------
# ΠΕΙΡΑΜΑ 1: ΕΚΤΕΛΕΣΗ ΠΑΝΩ ΣΕ PARQUET
# ----------------------------------------------------
print("--- Starting Parquet Benchmark ---")
start_parquet = time.time()

df_parquet = spark.read.parquet(parquet_path)
df_parquet.createOrReplaceTempView("trips_parquet")

query = f"""
SELECT 
    HOUR(tpep_pickup_datetime) as pickup_hour,
    payment_type,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare
FROM trips_parquet
WHERE tpep_pickup_datetime IS NOT NULL 
  AND HOUR(tpep_pickup_datetime) >= {START_HOUR} AND HOUR(tpep_pickup_datetime) <= {END_HOUR}
  AND payment_type IN (1, 2)
GROUP BY HOUR(tpep_pickup_datetime), payment_type
"""
spark.sql(query).coalesce(1).write.mode("overwrite").parquet("tmp_benchmark_parquet")
end_parquet = time.time()
time_parquet = end_parquet - start_parquet
print(f"Parquet Execution Time: {time_parquet} seconds")

# ----------------------------------------------------
# ΠΕΙΡΑΜΑ 2: ΕΚΤΕΛΕΣΗ ΠΑΝΩ ΣΕ CSV
# ----------------------------------------------------
print("--- Starting CSV Benchmark ---")
start_csv = time.time()

# Φορτώνουμε το CSV (ορίζουμε schema=True ή inferSchema για να αναγνωριστούν οι τύποι δεδομένων)
df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
df_csv.createOrReplaceTempView("trips_csv")

query_csv = f"""
SELECT 
    HOUR(tpep_pickup_datetime) as pickup_hour,
    payment_type,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare
FROM trips_csv
WHERE tpep_pickup_datetime IS NOT NULL 
  AND HOUR(tpep_pickup_datetime) >= {START_HOUR} AND HOUR(tpep_pickup_datetime) <= {END_HOUR}
  AND payment_type IN (1, 2)
GROUP BY HOUR(tpep_pickup_datetime), payment_type
"""
spark.sql(query_csv).coalesce(1).write.mode("overwrite").parquet("tmp_benchmark_csv")
end_csv = time.time()
time_csv = end_csv - start_csv
print(f"CSV Execution Time: {time_csv} seconds")

print("\n==========================================")
print("=== Q4 FORMAT BENCHMARK RESULTS ===")
print(f"Parquet Format: {time_parquet:.2f} seconds")
print(f"CSV Format:     {time_csv:.2f} seconds")
print(f"Speedup Factor: {time_csv / time_parquet:.2f}x faster with Parquet!")
print("==========================================")

spark.stop()
