import time
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F

# ΕΛΕΓΧΟΣ ΟΡΙΣΜΑΤΩΝ (Βάσει Ενότητας 2 της εκφώνησης)
if len(sys.argv) < 3:
    print("Usage: prepare_parquet.py <input_base_path> <output_base_path>")
    sys.exit(-1)

input_base = sys.argv[1]   # π.χ. hdfs://hdfs-namenode.default.svc.cluster.local:9000/data
output_base = sys.argv[2]  # π.χ. hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/data

# 1. ΕΚΚΙΝΗΣΗ SPARK SESSION
spark = SparkSession.builder \
    .appName("taxi-data-preparation-maria") \
    .getOrCreate()

start_time = time.time()
metrics = {}

# =====================================================================
# ΣΥΝΟΛΟ ΔΕΔΟΜΕΝΩΝ 1: YELLOW TRIPDATA 2024
# =====================================================================
schema_2024 = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

print("=== Έναρξη ανάγνωσης του CSV αρχείου 2024... ===")
df_2024_raw = spark.read.format("csv").option("header", "true").schema(schema_2024).load(
    f"{input_base}/yellow_tripdata_2024.csv"
)

raw_count_2024 = df_2024_raw.count()
print(f"Αρχικές γραμμές στο CSV 2024: {raw_count_2024}")

df_2024 = df_2024_raw.withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime")) \
                     .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))

null_ts_2024 = df_2024.filter(F.col("pickup_ts").isNotNull()).count()

df_2024_filtered = df_2024.filter(F.col("pickup_ts").isNotNull() & (F.year("pickup_ts") == 2024))
wrong_year_2024 = raw_count_2024 - df_2024_filtered.count()

df_2024_final = df_2024_filtered.withColumn("pickup_date", F.to_date("pickup_ts")) \
                                .withColumn("pickup_day", F.dayofmonth("pickup_ts")) \
                                .withColumn("pickup_hour", F.hour("pickup_ts")) \
                                .withColumn("duration_minutes", (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60) \
                                .withColumn("trip_distance_km", F.col("trip_distance") * 1.60934) \
                                .withColumnRenamed("PULocationID", "pu_location_id") \
                                .withColumnRenamed("DOLocationID", "do_location_id") \
                                .withColumnRenamed("VendorID", "vendor_id")

timestamps_2024 = df_2024_final.select(F.min("pickup_ts"), F.max("pickup_ts")).collect()[0]

print("=== Εγγραφή Parquet 2024 στο HDFS (Partition: pickup_day)... ===")
path_2024 = f"{output_base}/parquet/yellow_tripdata_2024"
df_2024_final.write.mode("overwrite").partitionBy("pickup_day").parquet(path_2024)

metrics["yellow_tripdata_2024"] = {
    "raw_rows": raw_count_2024,
    "prepared_rows": df_2024_final.count(),
    "null_or_unreadable_timestamps": raw_count_2024 - null_ts_2024,
    "rejected_wrong_year": wrong_year_2024,
    "min_pickup_ts": str(timestamps_2024[0]),
    "max_pickup_ts": str(timestamps_2024[1]),
    "partition_columns": ["pickup_day"],
    "output_path": path_2024
}

# =====================================================================
# ΣΥΝΟΛΟ ΔΕΔΟΜΕΝΩΝ 2: YELLOW TRIPDATA 2015
# =====================================================================
schema_2015 = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("RateCodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

print("=== Έναρξη ανάγνωσης του CSV αρχείου 2015... ===")
df_2015_raw = spark.read.format("csv").option("header", "true").schema(schema_2015).load(
    f"{input_base}/yellow_tripdata_2015.csv"
)

raw_count_2015 = df_2015_raw.count()
print(f"Αρχικές γραμμές στο CSV 2015: {raw_count_2015}")

df_2015 = df_2015_raw.withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime")) \
                     .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))

null_ts_2015 = df_2015.filter(F.col("pickup_ts").isNotNull()).count()

df_2015_filtered = df_2015.filter(F.col("pickup_ts").isNotNull() & (F.year("pickup_ts") == 2015))
wrong_year_2015 = raw_count_2015 - df_2015_filtered.count()

df_2015_final = df_2015_filtered.withColumn("pickup_date", F.to_date("pickup_ts")) \
                                .withColumn("pickup_day", F.dayofmonth("pickup_ts")) \
                                .withColumn("pickup_hour", F.hour("pickup_ts")) \
                                .withColumn("duration_minutes", (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60) \
                                .withColumn("trip_distance_km", F.col("trip_distance") * 1.60934) \
                                .withColumnRenamed("VendorID", "vendor_id")

timestamps_2015 = df_2015_final.select(F.min("pickup_ts"), F.max("pickup_ts")).collect()[0]

print("=== Εγγραφή Parquet 2015 στο HDFS (Partition: pickup_hour)... ===")
path_2015 = f"{output_base}/parquet/yellow_tripdata_2015"
df_2015_final.write.mode("overwrite").partitionBy("pickup_hour").parquet(path_2015)

metrics["yellow_tripdata_2015"] = {
    "raw_rows": raw_count_2015,
    "prepared_rows": df_2015_final.count(),
    "null_or_unreadable_timestamps": raw_count_2015 - null_ts_2015,
    "rejected_wrong_year": wrong_year_2015,
    "min_pickup_ts": str(timestamps_2015[0]),
    "max_pickup_ts": str(timestamps_2015[1]),
    "partition_columns": ["pickup_hour"],
    "output_path": path_2015
}

# =====================================================================
# ΣΥΝΟΛΟ ΔΕΔΟΜΕΝΩΝ 3: TAXI ZONE LOOKUP
# =====================================================================
schema_lookup = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])

print("=== Έναρξη ανάγνωσης του Taxi Zone Lookup... ===")
# Διαβάζουμε το lookup από το σωστό input path
df_lookup_raw = spark.read.format("csv").option("header", "true").schema(schema_lookup).load(
    f"{input_base}/taxi_zone_lookup.csv"
)

raw_count_lookup = df_lookup_raw.count()

print("=== Εγγραφή Parquet Lookup στο HDFS... ===")
path_lookup = f"{output_base}/parquet/taxi_zone_lookup"
df_lookup_raw.write.mode("overwrite").parquet(path_lookup)

metrics["taxi_zone_lookup"] = {
    "raw_rows": raw_count_lookup,
    "prepared_rows": raw_count_lookup,
    "null_or_unreadable_timestamps": 0,
    "rejected_wrong_year": 0,
    "min_pickup_ts": "N/A",
    "max_pickup_ts": "N/A",
    "partition_columns": "none",
    "output_path": path_lookup
}

# =====================================================================
# ΕΞΑΓΩΓΗ ΜΕΤΡΙΚΩΝ ΣΕ JSON ΜΕΣΑ ΣΤΟ HDFS (ΓΙΑ ΝΑ ΜΗ ΧΑΝΟΝΤΑΙ)
# =====================================================================
end_time = time.time()
total_duration_seconds = end_time - start_time

metrics["global_metrics"] = {
    "total_conversion_time_seconds": total_duration_seconds,
    "total_conversion_time_minutes": total_duration_seconds / 60
}

metrics_json = json.dumps(metrics, indent=4)
print("\n=== ΣΤΑΤΙΣΤΙΚΑ ΚΑΙ ΜΕΤΡΙΚΕΣ ΠΡΟΕΤΟΙΜΑΣΙΑΣ ===")
print(metrics_json)

# Δημιουργούμε ένα RDD με το JSON string και το σώζουμε στο HDFS ως JSON αρχείο
metrics_rdd = spark.sparkContext.parallelize([metrics_json])
metrics_output_path = f"{output_base}/data_preparation_metrics"
metrics_rdd.coalesce(1).saveAsTextFile(metrics_output_path)

print(f"\n=== ΟΙ ΜΕΤΡΙΚΕΣ ΣΩΘΗΚΑΝ ΣΤΟ HDFS: {metrics_output_path} ===")
spark.stop()
