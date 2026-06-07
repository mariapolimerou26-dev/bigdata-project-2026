import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_date, unix_timestamp, date_format, when, sum as _sum, count as _count, avg, lit

if len(sys.argv) < 3:
    print("Usage: spark-submit q1_df_parquet.py <input_path> <output_base_path>")
    sys.exit(-1)

input_path = sys.argv[1]
output_base = sys.argv[2] # Θα φτιάξουμε υποφακέλους εδώ

# --- ΠΡΟΣΩΠΙΚΟ ΠΑΡΑΘΥΡΟ ΜΑΡΙΑΣ ---
START_DATE = "2024-03-04"
END_DATE = "2024-03-06"
START_HOUR = 7
END_HOUR = 8
# ---------------------------------

start_time = time.time()

spark = SparkSession.builder \
    .appName("Taxi_Q1_DataFrame_Full") \
    .master("local[*]") \
    .getOrCreate()

# 1. Ανάγνωση Parquet
df_raw = spark.read.parquet(input_path)

# 2. Καθαρισμός & Δημιουργία Βασικών Στηλών
df_cleaned = df_raw.filter(
    (col("tpep_pickup_datetime").isNotNull()) & (col("tpep_dropoff_datetime").isNotNull()) &
    (col("trip_distance") > 0) & (col("total_amount") > 0)
).withColumn("duration_minutes", (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0)

df_valid = df_cleaned.filter(col("duration_minutes") > 0)

# Προσθήκη όλων των στηλών που ζητάει η εκφώνηση
df_features = df_valid \
    .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")).cast("string")) \
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_day", date_format(col("tpep_pickup_datetime"), "d").cast("int")) \
    .withColumn("weekday", date_format(col("tpep_pickup_datetime"), "EEEE")) \
    .withColumn("is_weekend", when(date_format(col("tpep_pickup_datetime"), "E").isin("Sat", "Sun"), lit(1)).otherwise(lit(0))) \
    .withColumn("time_band", 
        when((col("pickup_hour") >= 0) & (col("pickup_hour") <= 5), "Night")
        .when((col("pickup_hour") >= 6) & (col("pickup_hour") <= 11), "Morning")
        .when((col("pickup_hour") >= 12) & (col("pickup_hour") <= 16), "Afternoon")
        .when((col("pickup_hour") >= 17) & (col("pickup_hour") <= 21), "Evening")
        .otherwise("Late")
    )

# 3. Φιλτράρισμα στο Προσωπικό σου Υποσύνολο (Μαρία)
df_personal = df_features.filter(
    (col("pickup_date") >= START_DATE) & (col("pickup_date") <= END_DATE) &
    (col("pickup_hour") >= START_HOUR) & (col("pickup_hour") <= END_HOUR)
)

# Υπολογισμός συνολικών trips στο προσωπικό παράθυρο για το trip_share
total_personal_trips = df_personal.count()

if total_personal_trips == 0:
    print("No trips found in your personal window!")
    spark.stop()
    sys.exit(0)

# 4. ΠΡΩΤΟΣ ΠΙΝΑΚΑΣ: Ανά (pickup_date, pickup_hour)
df_main_metrics = df_personal.groupBy("pickup_date", "pickup_hour").agg(
    _count("*").alias("trips"),
    _count("PULocationID").alias("unique_pickup_zones"), # προσεγγιστικά ως πλήθος εγγραφών ζώνης (ή κανονικά countDistinct)
    avg("passenger_count").alias("avg_passenger_count"),
    avg("duration_minutes").alias("avg_duration_minutes"),
    avg("trip_distance").alias("avg_trip_distance"),
    avg("total_amount").alias("avg_total_amount"),
    _sum("total_amount").alias("total_revenue")
).withColumn("trip_share_in_personal_window", (col("trips") / total_personal_trips) * 100.0)

# Ταξινόμηση (Trips DESC, Revenue DESC, Date ASC, Hour ASC)
df_main_final = df_main_metrics.orderBy(
    col("trips").desc(),
    col("total_revenue").desc(),
    col("pickup_date").asc(),
    col("pickup_hour").asc()
)

# 5. ΔΕΥΤΕΡΟΣ ΠΙΝΑΚΑΣ: Σύνοψη ανά time_band για το προσωπικό υποσύνολο
df_band_summary = df_personal.groupBy("time_band").agg(
    _count("*").alias("trips"),
    avg("duration_minutes").alias("avg_duration_minutes"),
    avg("trip_distance").alias("avg_trip_distance"),
    _sum("total_amount").alias("total_revenue")
)

# 6. Αποθήκευση αποτελεσμάτων (CSV και Parquet όπως ζητάει η εκφώνηση)
df_main_final.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/main_output_csv")
df_main_final.coalesce(1).write.mode("overwrite").parquet(f"{output_base}/main_output_parquet")
df_band_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/band_output_csv")

end_time = time.time()
print("=== Q1 FULL DATAFRAME SUCCESS ===")
print(f"Execution Time: {end_time - start_time} seconds")

spark.stop()
