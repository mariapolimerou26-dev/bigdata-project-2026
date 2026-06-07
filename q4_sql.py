import sys
import time
from pyspark.sql import SparkSession

if len(sys.argv) < 3:
    print("Usage: spark-submit q4_sql.py <input_parquet_path> <output_base_path>")
    sys.exit(-1)

input_path = sys.argv[1]
output_base = sys.argv[2]

# --- ΠΡΟΣΩΠΙΚΟ ΠΑΡΑΘΥΡΟ ΜΑΡΙΑΣ ---
START_HOUR = 7
END_HOUR = 8
# ---------------------------------

start_time = time.time()

spark = SparkSession.builder \
    .appName("Taxi_Q4_SparkSQL") \
    .master("local[*]") \
    .getOrCreate()

# Φόρτωση Parquet 2024 και δημιουργία Temporary View
df_raw = spark.read.parquet(input_path)
df_raw.createOrReplaceTempView("trips_2024")

sql_query = f"""
WITH cleaned_trips AS (
    SELECT 
        VendorID,
        tpep_pickup_datetime,
        HOUR(tpep_pickup_datetime) as pickup_hour,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        trip_distance,
        (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0 as duration_minutes
    FROM trips_2024
    WHERE tpep_pickup_datetime IS NOT NULL 
      AND tpep_dropoff_datetime IS NOT NULL
      -- Υπολογισμός inline στο WHERE για να μην χτυπάει σφάλμα Resolution
      AND ((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0) > 0
      AND trip_distance > 0 
      AND fare_amount > 0 
      AND total_amount > 0
      AND payment_type IN (1, 2)
),
filtered_window AS (
    SELECT * FROM cleaned_trips
    WHERE pickup_hour >= {START_HOUR} AND pickup_hour <= {END_HOUR}
),
hourly_totals AS (
    SELECT 
        pickup_hour,
        COUNT(*) as total_trips_in_hour
    FROM filtered_window
    GROUP BY pickup_hour
),
metrics_by_payment AS (
    SELECT 
        f.pickup_hour,
        f.payment_type,
        COUNT(*) as trips,
        (COUNT(*) / FIRST(h.total_trips_in_hour)) * 100.0 as payment_share_in_hour,
        AVG(f.fare_amount) as avg_fare_amount,
        AVG(f.total_amount) as avg_total_amount,
        AVG(f.tip_amount) as avg_tip_amount,
        AVG(f.tip_amount / f.fare_amount) * 100.0 as tip_rate,
        (SUM(CASE WHEN f.tip_amount = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100.0 as zero_tip_share,
        AVG(f.trip_distance) as avg_trip_distance,
        AVG(f.duration_minutes) as avg_duration_minutes
    FROM filtered_window f
    JOIN hourly_totals h ON f.pickup_hour = h.pickup_hour
    GROUP BY f.pickup_hour, f.payment_type
)
SELECT * FROM metrics_by_payment ORDER BY pickup_hour, payment_type
"""

# Εκτέλεση του κυρίου ερωτήματος
df_q4_summary = spark.sql(sql_query)
df_q4_summary.coalesce(1).write.mode("overwrite").parquet(f"{output_base}/hourly_payment_metrics_parquet")

# 2ο Ερώτημα: Σύνοψη ανά Vendor και Payment Type (Απαίτηση εμπλοκής γ)
sql_vendor = f"""
WITH filtered_window AS (
    SELECT VendorID, payment_type, total_amount, fare_amount, tip_amount
    FROM trips_2024
    WHERE HOUR(tpep_pickup_datetime) >= {START_HOUR} AND HOUR(tpep_pickup_datetime) <= {END_HOUR}
      AND ((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0) > 0
      AND trip_distance > 0 AND fare_amount > 0 AND total_amount > 0
      AND payment_type IN (1, 2)
)
SELECT 
    VendorID,
    payment_type,
    COUNT(*) as total_trips,
    SUM(total_amount) as total_revenue,
    AVG(tip_amount / fare_amount) * 100.0 as avg_tip_rate
FROM filtered_window
GROUP BY VendorID, payment_type
"""
df_vendor_summary = spark.sql(sql_vendor)
df_vendor_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/vendor_payment_summary_csv")

end_time = time.time()
print("=== Q4 SPARK SQL SUCCESS ===")
print(f"Execution Time: {end_time - start_time} seconds")

spark.stop()
