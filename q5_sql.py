import sys
import time
from pyspark.sql import SparkSession

if len(sys.argv) < 4:
    print("Usage: spark-submit q5_sql.py <trips_path> <zones_path> <output_base_path>")
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
    .appName("Taxi_Q5_SparkSQL_Airports") \
    .master("local[*]") \
    .getOrCreate()

# 1. Φόρτωση Δεδομένων και Δημιουργία Temp Views
df_trips = spark.read.parquet(trips_path)
df_trips.createOrReplaceTempView("trips_2024")

df_zones = spark.read.option("header", "true").csv(zones_path)
df_zones.createOrReplaceTempView("taxi_zones")

sql_query = f"""
WITH cleaned_trips AS (
    SELECT 
        VendorID,
        PULocationID,
        DOLocationID,
        trip_distance,
        total_amount,
        Airport_fee,
        (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0 as duration_minutes
    FROM trips_2024
    WHERE tpep_pickup_datetime IS NOT NULL 
      AND tpep_dropoff_datetime IS NOT NULL
      AND ((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0) > 0
      AND trip_distance > 0 
      AND total_amount > 0
      AND HOUR(tpep_pickup_datetime) >= {START_HOUR} 
      AND HOUR(tpep_pickup_datetime) <= {END_HOUR}
),
zones_tagged AS (
    SELECT 
        LocationID,
        Borough,
        Zone,
        CASE WHEN Zone LIKE '%JFK%' OR Zone LIKE '%LaGuardia%' OR Zone LIKE '%Newark%' OR Zone LIKE '%Airport%' OR Borough = 'EWR'
             THEN 1 ELSE 0 END as is_airport
    FROM taxi_zones
),
joined_data AS (
    SELECT 
        t.*,
        z_pu.Borough as pu_borough,
        z_pu.Zone as pu_zone,
        z_do.Borough as do_borough,
        z_do.Zone as do_zone,
        CASE WHEN z_pu.is_airport = 1 OR z_do.is_airport = 1 THEN 1 ELSE 0 END as is_airport_trip
    FROM cleaned_trips t
    JOIN zones_tagged z_pu ON t.PULocationID = z_pu.LocationID
    JOIN zones_tagged z_do ON t.DOLocationID = z_do.LocationID
),
total_count AS (
    SELECT COUNT(*) as grand_total FROM joined_data
),
borough_flows AS (
    SELECT 
        j.pu_borough,
        j.do_borough,
        COUNT(*) as trips,
        (COUNT(*) / FIRST(t.grand_total)) * 100.0 as trip_share,
        SUM(j.total_amount) as total_revenue,
        AVG(j.total_amount) as avg_total_amount,
        AVG(j.trip_distance) as avg_trip_distance,
        AVG(j.duration_minutes) as avg_duration_minutes,
        (SUM(j.is_airport_trip) / COUNT(*)) * 100.0 as airport_trip_share
    FROM joined_data j
    CROSS JOIN total_count t
    GROUP BY j.pu_borough, j.do_borough
    ORDER BY trips DESC
)
SELECT * FROM borough_flows
"""

# Εκτέλεση και αποθήκευση των ροών δήμων
df_sql_flows = spark.sql(sql_query)
df_sql_flows.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/borough_flows_sql_csv")

end_time = time.time()
print("=== Q5 SPARK SQL SUCCESS ===")
print(f"Execution Time: {end_time - start_time} seconds")

spark.stop()
