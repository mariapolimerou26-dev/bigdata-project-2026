import sys
import time
from pyspark.sql import SparkSession

if len(sys.argv) < 4:
    print("Usage: spark-submit q6_sql.py <trips_path> <zones_path> <output_base_path>")
    sys.exit(-1)

trips_path = sys.argv[1]
zones_path = sys.argv[2]
output_base = sys.argv[3]

START_HOUR = 7
END_HOUR = 8

start_time = time.time()

spark = SparkSession.builder \
    .appName("Taxi_Q6_Imbalance_SparkSQL") \
    .master("local[*]") \
    .getOrCreate()

# 1. Φόρτωση Δεδομένων και Δημιουργία Temp Views
df_trips = spark.read.parquet(trips_path)
df_trips.createOrReplaceTempView("trips_2024")

df_zones = spark.read.option("header", "true").csv(zones_path)
df_zones.createOrReplaceTempView("taxi_zones")

sql_query = f"""
WITH filtered_trips AS (
    SELECT 
        PULocationID,
        DOLocationID,
        HOUR(tpep_pickup_datetime) as pickup_hour
    FROM trips_2024
    WHERE tpep_pickup_datetime IS NOT NULL 
      AND tpep_dropoff_datetime IS NOT NULL
      AND HOUR(tpep_pickup_datetime) >= {START_HOUR} 
      AND HOUR(tpep_pickup_datetime) <= {END_HOUR}
),
pickups_agg AS (
    SELECT pickup_hour, PULocationID as ZoneID, COUNT(*) as pickups
    FROM filtered_trips
    GROUP BY pickup_hour, PULocationID
),
dropoffs_agg AS (
    SELECT pickup_hour, DOLocationID as ZoneID, COUNT(*) as dropoffs
    FROM filtered_trips
    GROUP BY pickup_hour, DOLocationID
),
imbalance_base AS (
    SELECT 
        COALESCE(p.pickup_hour, d.pickup_hour) as hour,
        COALESCE(p.ZoneID, d.ZoneID) as ZoneID,
        COALESCE(p.pickups, 0) as total_pickups,
        COALESCE(d.dropoffs, 0) as total_dropoffs
    FROM pickups_agg p
    FULL OUTER JOIN dropoffs_agg d 
      ON p.pickup_hour = d.pickup_hour AND p.ZoneID = d.ZoneID
)
SELECT 
    i.hour,
    i.ZoneID,
    z.Borough,
    z.Zone,
    i.total_pickups,
    i.total_dropoffs,
    ABS(i.total_pickups - i.total_dropoffs) as absolute_imbalance
FROM imbalance_base i
JOIN taxi_zones z ON i.ZoneID = z.LocationID
ORDER BY absolute_imbalance DESC
"""

# Εκτέλεση και αποθήκευση
df_sql_result = spark.sql(sql_query)
df_sql_result.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/zone_imbalance_sql_csv")

end_time = time.time()
print("=== Q6 SPARK SQL SUCCESS ===")
print(f"Execution Time: {end_time - start_time} seconds")

spark.stop()
