import time
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

if len(sys.argv) < 2:
    print("Usage: q2_udf.py <output_base_path>")
    sys.exit(-1)

output_base = sys.argv[1]

spark = SparkSession.builder \
    .appName("taxi-q2-udf-maria") \
    .getOrCreate()

start_time = time.time()

# 1. Ανάγνωση Parquet 2024
parquet_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/data/parquet/yellow_tripdata_2024"
df = spark.read.parquet(parquet_path)

# 2. Ορισμός Python Συναρτήσεων για το UDF
def miles_to_km(miles):
    if miles is None:
        return 0.0
    return float(miles * 1.60934)

def calculate_speed(dist_km, duration_mins):
    if dist_km is None or duration_mins is None or duration_mins <= 0:
        return 0.0
    return float(dist_km / (duration_mins / 60.0))

# Κάνουμε register τα UDFs στο Spark
miles_to_km_udf = F.udf(miles_to_km, DoubleType())
calculate_speed_udf = F.udf(calculate_speed, DoubleType())

# 3. Εφαρμογή των UDFs στο DataFrame
df_dist = df.withColumn("trip_distance_km", miles_to_km_udf(F.col("trip_distance")))
df_speed = df_dist.withColumn("average_speed", calculate_speed_udf(F.col("trip_distance_km"), F.col("duration_minutes")))

# 4. Διαχωρισμός Ακραίων Τιμών (Outliers)
df_outliers = df_speed.filter((F.col("trip_distance_km") > 200.0) | (F.col("duration_minutes") > 300.0))
df_normal = df_speed.filter((F.col("trip_distance_km") <= 200.0) & (F.col("duration_minutes") <= 300.0))

# 5. Ομαδοποίηση και Στατιστικά
df_grouped = df_normal.groupBy("pu_location_id", "do_location_id").agg(
    F.avg("duration_minutes").alias("avg_duration"),
    F.avg("trip_distance_km").alias("avg_distance"),
    F.avg("average_speed").alias("avg_speed")
).orderBy("pu_location_id", "do_location_id")

# 6. Αποθήκευση στο HDFS (σε ξεχωριστό φάκελο udf για να μην σβήσει τα προηγούμενα)
df_outliers.write.mode("overwrite").parquet(f"{output_base}/q2_outliers_udf_output")
df_grouped.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/q2_normal_grouped_udf_output")

end_time = time.time()
print("=== Q2 UDF SUCCESS ===")
print(f"Execution Time: {end_time - start_time} seconds")

spark.stop()
