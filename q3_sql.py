import time
import sys
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
    print("Usage: q3_sql.py <output_base_path>")
    sys.exit(-1)

output_base = sys.argv[1]

spark = SparkSession.builder \
    .appName("taxi-q3-sql-maria") \
    .getOrCreate()

start_time = time.time()

# 1. Ανάγνωση Parquet 2024
parquet_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/data/parquet/yellow_tripdata_2024"
df = spark.read.parquet(parquet_path)

# 2. Δημιουργία Προσωρινού SQL View
df.createOrReplaceTempView("yellow_v2024")

# 3. SQL Query με Window Function (DENSE_RANK)
# Φιλτράρουμε payment_type = 1 (Credit Card) και pu_location_id IS NOT NULL
# Εξάγουμε τον μήνα από το pickup_date
sql_query = """
WITH MonthlyAverages AS (
    SELECT 
        MONTH(pickup_date) as trip_month,
        pu_location_id,
        AVG(tip_amount) as avg_tip_amount
    FROM yellow_v2024
    WHERE payment_type = 1 AND pu_location_id IS NOT NULL
    GROUP BY MONTH(pickup_date), pu_location_id
),
RankedZones AS (
    SELECT 
        trip_month,
        pu_location_id,
        avg_tip_amount,
        DENSE_RANK() OVER (PARTITION BY trip_month ORDER BY avg_tip_amount DESC) as rank
    FROM MonthlyAverages
)
SELECT trip_month, pu_location_id, avg_tip_amount, rank
FROM RankedZones
WHERE rank <= 5
ORDER BY trip_month ASC, rank ASC
"""

df_result = spark.sql(sql_query)

# 4. Αποθήκευση στο HDFS σε μορφή CSV
output_path = f"{output_base}/q3_sql_output"
df_result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

end_time = time.time()
print("=== Q3 SQL SUCCESS ===")
print(f"Execution Time: {end_time - start_time} seconds")

spark.stop()
