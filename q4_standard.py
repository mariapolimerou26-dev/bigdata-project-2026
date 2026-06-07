import time
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if len(sys.argv) < 2:
    print("Usage: q4_standard.py <output_base_path>")
    sys.exit(-1)

output_base = sys.argv[1]

spark = SparkSession.builder \
    .appName("taxi-q4-standard-maria") \
    .getOrCreate()

start_time = time.time()

# 1. Ανάγνωση Parquet 2024 και Taxi Zone Lookup CSV (Με το διορθωμένο path)
parquet_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/data/parquet/yellow_tripdata_2024"
lookup_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/taxi_zone_lookup.csv"

df_taxi = spark.read.parquet(parquet_path)
df_lookup = spark.read.option("header", "true").csv(lookup_path)

# 2. Προσθήκη στήλης για τον διαχωρισμό Καθημερινών / Σαββατοκύριακου
df_taxi_days = df_taxi.withColumn("day_of_week", F.date_format(F.col("pickup_date"), "E")) \
                      .withColumn("is_weekend", F.when(F.col("day_of_week").isin("Sat", "Sun"), "Weekend").otherwise("Weekday"))

# 3. Εκτέλεση STANDARD SHUFFLE JOIN (Χωρίς broadcast hint)
df_joined = df_taxi_days.join(df_lookup, df_taxi_days.pu_location_id == df_lookup.LocationID, "inner")

# 4. Υπολογισμός Top 5 για Καθημερινές (Weekdays)
df_weekdays = df_joined.filter(F.col("is_weekend") == "Weekday") \
                       .groupBy("Zone") \
                       .count() \
                       .orderBy(F.col("count").desc()) \
                       .limit(5)

# 5. Υπολογισμός Top 5 για Σαββατοκύριακα (Weekends)
df_weekends = df_joined.filter(F.col("is_weekend") == "Weekend") \
                       .groupBy("Zone") \
                       .count() \
                       .orderBy(F.col("count").desc()) \
                       .limit(5)

# 6. Αποθήκευση αποτελεσμάτων στο HDFS σε ξεχωριστό φάκελο standard
df_weekdays.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/q4_standard_weekdays_output")
df_weekends.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/q4_standard_weekends_output")

end_time = time.time()
print("=== Q4 STANDARD SUCCESS ===")
print(f"Execution Time: {end_time - start_time} seconds")

spark.stop()
