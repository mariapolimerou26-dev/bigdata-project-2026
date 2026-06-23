import sys
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, hour, count, avg, when,sum
from pyspark.sql.window import Window

if len(sys.argv) < 3:
    print("Usage: q4_all.py <input_parquet_path> <output_base_path>")
    sys.exit(-1)

input_path = sys.argv[1]
output_base = sys.argv[2]

# Αρχικοποίηση του Spark Session
spark = SparkSession.builder \
    .appName("marpolymerou_Q4_All_Interfaces") \
    .getOrCreate()

app_id = spark.sparkContext.applicationId
print(f"=== Starting Q4 Execution for AM 2122303 (AppID: {app_id}) ===")

# Εξατομικευμένο παράθυρο βάσει ΑΜ (2122303 -> Τελευταίο ψηφίο 3 -> 3*2 = 6 έως 6+6 = 12)
START_HOUR = 6
END_HOUR = 12

# ==========================================
# 1. ΚΟΙΝΗ ΠΡΟΕΤΟΙΜΑΣΙΑ & ΚΑΘΑΡΙΣΜΟΣ ΔΕΔΟΜΕΝΩΝ
# ==========================================
df_raw = spark.read.parquet(input_path)

df_cleaned = df_raw.withColumn(
    "duration_minutes", (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0
).filter(
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    (col("duration_minutes") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("total_amount") > 0) &
    (col("payment_type").isin(1, 2))
)

# Εφαρμογή του χρονικού παραθύρου
df_window = df_cleaned.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
                      .filter((col("pickup_hour") >= START_HOUR) & (col("pickup_hour") <= END_HOUR))

# Κάνουμε cache το φιλτραρισμένο dataframe για δίκαιη σύγκριση χρόνων μεταξύ των APIs
df_window.cache()
df_window.count() # Force εκτέλεση του cache

# ==========================================
# 2. ΥΛΟΠΟΙΗΣΗ ΜΕ DATAFRAME API
# ==========================================
start_df_time = time.time()

# Group by ανά ώρα και τύπο πληρωμής
df_grouped = df_window.groupBy("pickup_hour", "payment_type").agg(
    count("*").alias("trips"),
    avg("fare_amount").alias("avg_fare_amount"),
    avg("total_amount").alias("avg_total_amount"),
    avg("tip_amount").alias("avg_tip_amount"),
    # Tip rate μόνο για Credit Cards (payment_type == 1)
    avg(when(col("payment_type") == 1, (col("tip_amount") / col("fare_amount")) * 100.0)).alias("tip_rate")
)

# Window Function για τον υπολογισμό του share ανά ώρα
window_spec = Window.partitionBy("pickup_hour")
df_final = df_grouped.withColumn(
    "payment_share_in_hour",
    (col("trips") / sum("trips").over(window_spec)) * 100.0
).orderBy("pickup_hour", "payment_type")

# --- ΔΙΟΡΘΩΜΕΝΗ ΚΑΤΑΓΡΑΦΗ CATALYST PLAN (DATAFRAME) ---
from pyspark.sql.functions import lit
df_plan_string = df_final._jdf.queryExecution().toString()
spark.range(1).select(lit(df_plan_string).alias("value")) \
    .write.mode("overwrite").text(f"{output_base}/plans/q4_df_parquet_plan")

# Αποθήκευση αποτελεσμάτων DataFrame
df_final.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/results/q4_df_output")

end_df_time = time.time()
df_execution_time = end_df_time - start_df_time                                                             

# ==========================================
# 3. ΥΛΟΠΟΙΗΣΗ ΜΕ SPARK SQL
# ==========================================
start_sql_time = time.time()

# Δημιουργία προσωρινού View από το ήδη φιλτραρισμένο/cached dataframe
df_window.createOrReplaceTempView("taxi_window_data")

sql_query = """
    WITH GroupedData AS (
        SELECT
            pickup_hour,
            payment_type,
            COUNT(*) as trips,
            AVG(fare_amount) as avg_fare_amount,
            AVG(total_amount) as avg_total_amount,
            AVG(tip_amount) as avg_tip_amount,
            AVG(CASE WHEN payment_type = 1 THEN (tip_amount / fare_amount) * 100.0 ELSE NULL END) as tip_rate
        FROM taxi_window_data
        GROUP BY pickup_hour, payment_type
    )
    SELECT
        pickup_hour,
        payment_type,
        trips,
        (trips / SUM(trips) OVER(PARTITION BY pickup_hour)) * 100.0 as payment_share_in_hour,
        avg_fare_amount,
        avg_total_amount,
        avg_tip_amount,
        tip_rate
    FROM GroupedData
    ORDER BY pickup_hour, payment_type
"""

df_sql_result = spark.sql(sql_query)

# --- ΔΙΟΡΘΩΜΕΝΗ ΚΑΤΑΓΡΑΦΗ CATALYST PLAN (SQL) ---
sql_plan_string = df_sql_result._jdf.queryExecution().toString()
spark.range(1).select(lit(sql_plan_string).alias("value")) \
    .write.mode("overwrite").text(f"{output_base}/plans/q4_sql_parquet_plan")

# Αποθήκευση αποτελεσμάτων SQL
df_sql_result.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/results/q4_sql_output")

end_sql_time = time.time()
sql_execution_time = end_sql_time - start_sql_time

# ==========================================
# 4. ΚΟΙΝΗ ΑΠΟΘΗΚΕΥΣΗ ΜΕΤΡΙΚΩΝ (JSON)
# ==========================================
metrics_data = {
    "spark_application_id": app_id,
    "student_id": "2122303",
    "question": "Q4",
    "dataframe_execution_time_seconds": df_execution_time,
    "sql_execution_time_seconds": sql_execution_time,
    "df_output_path": f"{output_base}/results/q4_df_output",
    "sql_output_path": f"{output_base}/results/q4_sql_output"
}

# --- ΔΙΟΡΘΩΜΕΝΗ ΑΠΟΘΗΚΕΥΣΗ JSON ΧΩΡΙΣ PARALLELIZE ---
import json
json_string = json.dumps(metrics_data, indent=4)
spark.range(1).select(lit(json_string).alias("value")) \
    .write.mode("overwrite").text(f"{output_base}/metrics/q4_metrics_json")

print("=== Q4 ALL INTERFACES EXECUTED SUCCESSFULLY ===")
print(f"DataFrame Time: {df_execution_time:.2f} seconds")
print(f"Spark SQL Time: {sql_execution_time:.2f} seconds")

spark.stop()
