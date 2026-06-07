import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, avg, sum as _sum, count as _count, when

if len(sys.argv) < 4:
    print("Usage: spark-submit q5_df.py <trips_path> <zones_path> <output_base_path>")
    sys.exit(-1)

trips_path = sys.argv[1]
zones_path = sys.argv[2]
output_base = sys.argv[3]

# --- ΠΡΟΣΩΠΙΚΟ ΠΑΡΑΘΥΡΟ ΜΑΡΙΑΣ ---
START_HOUR = 7
END_HOUR = 8
# ---------------------------------

start_time = time.time()

spark = SparkSession.builder \
    .appName("Taxi_Q5_DataFrame_Airports") \
    .master("local[*]") \
    .getOrCreate()

# 1. Φόρτωση Δεδομένων
df_trips = spark.read.parquet(trips_path)
df_zones = spark.read.option("header", "true").csv(zones_path)

# 2. Καθαρισμός & Φιλτράρισμα βάσει κανόνων Q5
df_cleaned = df_trips.withColumn(
    "duration_minutes", (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0
).filter(
    (col("tpep_pickup_datetime").isNotNull()) & (col("tpep_dropoff_datetime").isNotNull()) &
    (col("duration_minutes") > 0) & (col("trip_distance") > 0) & (col("total_amount") > 0)
)

# Φιλτράρισμα στο ωριαίο παράθυρο
df_window = df_cleaned.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
                      .filter((col("pickup_hour") >= START_HOUR) & (col("pickup_hour") <= END_HOUR))

# 3. Προετοιμασία των Zone Lookups με Flag Αεροδρομίου
# Λογική: Ύπαρξη keywords στο όνομα της ζώνης ή Borough = 'EWR'
df_zones_tagged = df_zones.withColumn(
    "is_airport",
    when(
        (col("Zone").contains("JFK")) | 
        (col("Zone").contains("LaGuardia")) | 
        (col("Zone").contains("Newark")) | 
        (col("Zone").contains("Airport")) |
        (col("Borough") == "EWR"), 1
    ).otherwise(0)
)

df_pu_zones = df_zones_tagged.select(
    col("LocationID").alias("PU_ID"),
    col("Borough").alias("pu_borough"),
    col("Zone").alias("pu_zone"),
    col("is_airport").alias("pu_is_airport")
)

df_do_zones = df_zones_tagged.select(
    col("LocationID").alias("DO_ID"),
    col("Borough").alias("do_borough"),
    col("Zone").alias("do_zone"),
    col("is_airport").alias("do_is_airport")
)

# 4. Διπλό Join
df_joined = df_window \
    .join(df_pu_zones, col("PULocationID") == col("PU_ID"), "inner") \
    .join(df_do_zones, col("DOLocationID") == col("DO_ID"), "inner")

# Προσθήκη γενικού flag αν η διαδρομή αφορά αεροδρόμιο (είτε pickup είτε dropoff)
df_final = df_joined.withColumn(
    "is_airport_trip",
    when((col("pu_is_airport") == 1) | (col("do_is_airport") == 1), 1).otherwise(0)
)

# 5. Υπολογισμός Ροών Borough-to-Borough
df_borough_flows = df_final.groupBy("pu_borough", "do_borough").agg(
    _count("*").alias("trips"),
    _sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_total_amount"),
    avg("trip_distance").alias("avg_trip_distance"),
    avg("duration_minutes").alias("avg_duration_minutes"),
    (_sum("is_airport_trip") / _count("*") * 100.0).alias("airport_trip_share")
).orderBy(col("trips").desc())

# Υπολογισμός συνολικών trips για το trip_share
total_trips_count = df_final.count()
df_borough_flows = df_borough_flows.withColumn("trip_share", (col("trips") / total_trips_count) * 100.0)

# 6. Ξεχωριστή ανάλυση για τις διαδρομές αεροδρομίου (Top-K routes pu_zone -> do_zone)
df_airport_routes = df_final.filter(col("is_airport_trip") == 1) \
    .groupBy("pu_zone", "do_zone").agg(
        _count("*").alias("trips"),
        avg("Airport_fee").alias("avg_airport_fee"),
        avg("total_amount").alias("avg_total_amount"),
        avg("duration_minutes").alias("avg_duration_minutes"),
        avg("trip_distance").alias("avg_trip_distance")
    ).orderBy(col("trips").desc())

# Αποθήκευση αποτελεσμάτων
df_borough_flows.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/borough_flows_csv")
df_airport_routes.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/airport_top_routes_csv")

end_time = time.time()
print("=== Q5 DATAFRAME SUCCESS ===")
print(f"Execution Time: {end_time - start_time} seconds")

spark.stop()
