import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, abs as _abs, coalesce, lit

if len(sys.argv) < 4:
    print("Usage: spark-submit q6_df.py <trips_path> <zones_path> <output_base_path>")
    sys.exit(-1)

trips_path = sys.argv[1]
zones_path = sys.argv[2]
output_base = sys.argv[3]

START_HOUR = 7
END_HOUR = 8

start_time = time.time()

spark = SparkSession.builder \
    .appName("Taxi_Q6_Imbalance_DataFrame") \
    .master("local[*]") \
    .getOrCreate()

# 1. Φόρτωση Δεδομένων
df_trips = spark.read.parquet(trips_path)
df_zones = spark.read.option("header", "true").csv(zones_path)

# 2. Φιλτράρισμα ώρας και βασικός καθαρισμός
df_filtered = df_trips.filter(
    col("tpep_pickup_datetime").isNotNull() & 
    col("tpep_dropoff_datetime").isNotNull()
).withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
 .filter((col("pickup_hour") >= START_HOUR) & (col("pickup_hour") <= END_HOUR))

# 3. Υπολογισμός Pickups (μετονομασία της στήλης ID για αποφυγή conflicts)
df_pickups = df_filtered.groupBy("pickup_hour", col("PULocationID").alias("PU_ZoneID")) \
    .count() \
    .withColumnRenamed("count", "pickups")

# 4. Υπολογισμός Dropoffs
df_dropoffs = df_filtered.groupBy("pickup_hour", col("DOLocationID").alias("DO_ZoneID")) \
    .count() \
    .withColumnRenamed("count", "dropoffs")

# 5. Full Outer Join χρησιμοποιώντας ρητά ALIASES για την άρση του ambiguity
df_imbalance = df_pickups.alias("p").join(
    df_dropoffs.alias("d"), 
    (col("p.pickup_hour") == col("d.pickup_hour")) & (col("p.PU_ZoneID") == col("d.DO_ZoneID")), 
    "outer"
).select(
    coalesce(col("p.pickup_hour"), col("d.pickup_hour")).alias("hour"),
    coalesce(col("p.PU_ZoneID"), col("d.DO_ZoneID")).alias("ZoneID"),
    coalesce(col("p.pickups"), lit(0)).alias("total_pickups"),
    coalesce(col("d.dropoffs"), lit(0)).alias("total_dropoffs")
)

# Υπολογισμός της απόλυτης διαφοράς (Imbalance)
df_imbalance = df_imbalance.withColumn("absolute_imbalance", _abs(col("total_pickups") - col("total_dropoffs")))

# 6. Join με το taxi_zone_lookup για τα ονόματα
df_final = df_imbalance.join(df_zones, col("ZoneID") == col("LocationID"), "inner") \
    .select("hour", "ZoneID", "Borough", "Zone", "total_pickups", "total_dropoffs", "absolute_imbalance") \
    .orderBy(col("absolute_imbalance").desc())

# Αποθήκευση αποτελεσμάτων
df_final.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/zone_imbalance_df_csv")

end_time = time.time()
print("=== Q6 DATAFRAME SUCCESS ===")
print(f"Execution Time: {end_time - start_time} seconds")

spark.stop()
