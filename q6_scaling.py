
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, avg

if len(sys.argv) < 3:
    print("Usage: q6_scaling.py <parquet_2024_path> <output_base_path>")
    sys.exit(-1)

input_path = sys.argv[1]
output_base = sys.argv[2]

spark = SparkSession.builder \
    .appName("Q6_Scaling_OD_Halves") \
    .getOrCreate()

# 1. Φίλτρα εγκυρότητας σε όλο το 2024
df_all = spark.read.parquet(input_path)
df_valid = df_all.filter((col("trip_distance") > 0) & (col("fare_amount") > 0)) \
                 .withColumn("duration_minutes", (col("dropoff_ts").cast("long") - col("pickup_ts").cast("long")) / 60.0) \
                 .filter(col("duration_minutes") > 0)

# 2. Διαχωρισμός σε H1 και H2
df_h1 = df_valid.filter(col("pickup_day") <= 15)
df_h2 = df_valid.filter(col("pickup_day") > 15)

# 3. Ομαδοποίηση ανά ημίχρονο
df_g1 = df_h1.groupBy("pu_location_id", "do_location_id", "pickup_hour") \
             .agg(count("*").alias("trips_h1"),
                  avg("fare_amount").alias("avg_fare_h1"),
                  avg("total_amount").alias("avg_total_h1"))

df_g2 = df_h2.groupBy("pu_location_id", "do_location_id", "pickup_hour") \
             .agg(count("*").alias("trips_h2"),
                  avg("fare_amount").alias("avg_fare_h2"),
                  avg("total_amount").alias("avg_total_h2"))

# 4. Inner Join
df_joined = df_g1.join(df_g2, ["pu_location_id", "do_location_id", "pickup_hour"], "inner")

# 5. Υπολογισμός ποσοστιαίων αλλαγών
df_final = df_joined.withColumn("trips_change_pct", ((col("trips_h2") - col("trips_h1")) * 100.0) / col("trips_h1")) \
                    .withColumn("fare_change_pct", ((col("avg_fare_h2") - col("avg_fare_h1")) * 100.0) / col("avg_fare_h1"))

# Φιλτράρισμα στατιστικά ασήμαντων (π.χ. activity και στα δύο ημίχρονα > 10 διαδρομές)
df_significant = df_final.filter((col("trips_h1") >= 10) & (col("trips_h2") >= 10))

top_increase = df_significant.orderBy(col("trips_change_pct").desc()).limit(14)
top_decrease = df_significant.orderBy(col("trips_change_pct").asc()).limit(14)

# 6. Trigger Execution και Μέτρηση Χρόνου
start_time = time.time()

# Force write για να μετρήσουμε τον πραγματικό χρόνο εκτέλεσης του Join
top_increase.write.mode("overwrite").format("noop").save()

execution_time = time.time() - start_time
print(print(f"SCALING_TIME_MARKER: {execution_time:.2f} seconds"))

# Εκτύπωση Formatted Explain για το Report
df_significant.explain("formatted")

spark.stop()
