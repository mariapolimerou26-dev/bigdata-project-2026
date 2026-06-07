import sys
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, abs, coalesce, avg

if len(sys.argv) < 3:
    print("Usage: q6_imbalance.py <parquet_2024_path> <output_base_path>")
    sys.exit(-1)

input_path = sys.argv[1]
output_base = sys.argv[2]

spark = SparkSession.builder \
    .appName("Q6_Imbalance_Analysis") \
    .getOrCreate()

app_id = spark.sparkContext.applicationId

# Διαβάζουμε τα Parquet δεδομένα
df_yellow = spark.read.parquet(input_path)
df_zone = spark.read.parquet(f"{output_base}/data/parquet/taxi_zone_lookup")

# Φιλτράρισμα βάσει AM: 2122303 (Ημέρες 16,17,18 και Ώρες 15:00 έως 19:00)
df_filtered = df_yellow.filter(
    (col("pickup_day").isin(16, 17, 18)) &
    (col("pickup_hour") >= 15) & (col("pickup_hour") < 19) &
    (col("total_amount") > 0)
).withColumn(
    "duration_minutes", 
    (col("dropoff_ts").cast("long") - col("pickup_ts").cast("long")) / 60.0
).filter(col("duration_minutes") > 0)

# =========================================================================
# ΔΙΑΤΑΞΗ 1: DATAFRAME API
# =========================================================================
start_df_time = time.time()

# 1. Σύνολο Pickups
df_pickups = df_filtered.groupBy("pickup_hour", col("pu_location_id").alias("location_id")) \
                        .agg(count("*").alias("pickups"))

# 2. Σύνολο Dropoffs (με βάση την ώρα επιβίβασης pickup_hour όπως ζητάει η εκφώνηση!)
df_dropoffs = df_filtered.groupBy("pickup_hour", col("do_location_id").alias("location_id")) \
                         .agg(count("*").alias("dropoffs"))

# 3. Full Outer Join
df_joined = df_pickups.join(df_dropoffs, ["pickup_hour", "location_id"], "full")

# 4. Αντικατάσταση null με 0 και υπολογισμός μετρικών
df_metrics = df_joined.withColumn("pickups", coalesce(col("pickups"), lit(0))) \
                      .withColumn("dropoffs", coalesce(col("dropoffs"), lit(0))) \
                      .withColumn("activity", col("pickups") + col("dropoffs")) \
                      .withColumn("net_pickups", col("pickups") - col("dropoffs")) \
                      .withColumn("imbalance_ratio", col("net_pickups") / col("activity")) \
                      .withColumn("abs_imbalance_ratio", abs(col("imbalance_ratio")))

# 5. Join με taxi_zone_lookup
df_final_df = df_metrics.join(df_zone, df_metrics.location_id == df_zone.LocationID, "inner")

# 6. Φιλτράρισμα με όριο activity >= 30
df_active = df_final_df.filter(col("activity") >= 30)

# 7. Εξαγωγή Top-14 (K=14)
top_pos_net = df_active.orderBy(col("net_pickups").desc()).limit(14)
top_neg_net = df_active.orderBy(col("net_pickups").asc()).limit(14)
top_abs_imbalance = df_active.orderBy(col("abs_imbalance_ratio").desc()).limit(14)

# 8. Σύνοψη ανά ώρα
df_hourly_summary = df_active.groupBy("pickup_hour").agg(
    count("*").alias("total_zones"),
    avg(abs(col("net_pickups"))).alias("avg_abs_net_pickups"), # διορθώθηκε σε avg για σταθερότητα ορισμού
    avg("abs_imbalance_ratio").alias("mean_abs_imbalance_ratio")
).orderBy("pickup_hour")

# Αποθήκευση Catalyst Plan DataFrame
df_plan_string = df_active._jdf.queryExecution().toString()
spark.range(1).select(lit(df_plan_string).alias("value")) \
    .write.mode("overwrite").text(f"{output_base}/plans/q6_imbalance_df_plan")

# Αποθήκευση Αποτελεσμάτων
top_pos_net.coalesce(1).write.mode("overwrite").option("header","true").csv(f"{output_base}/results/q6_df_top_pos_net")
top_neg_net.coalesce(1).write.mode("overwrite").option("header","true").csv(f"{output_base}/results/q6_df_top_neg_net")
top_abs_imbalance.coalesce(1).write.mode("overwrite").option("header","true").csv(f"{output_base}/results/q6_df_top_abs_imbalance")
df_hourly_summary.coalesce(1).write.mode("overwrite").option("header","true").csv(f"{output_base}/results/q6_df_hourly_summary")

end_df_time = time.time()
t_df = end_df_time - start_df_time

# =========================================================================
# ΔΙΑΤΑΞΗ 2: SPARK SQL
# =========================================================================
start_sql_time = time.time()

df_filtered.createOrReplaceTempView("raw_window_trips")
df_zone.createOrReplaceTempView("zone_lookup")

sql_query = """
    WITH PickupsCTE AS (
        SELECT pickup_hour, pu_location_id as location_id, COUNT(*) as pickups
        FROM raw_window_trips
        GROUP BY pickup_hour, pu_location_id
    ),
    DropoffsCTE AS (
        SELECT pickup_hour, do_location_id as location_id, COUNT(*) as dropoffs
        FROM raw_window_trips
        GROUP BY pickup_hour, do_location_id
    ),
    FullJoined AS (
        SELECT 
            COALESCE(p.pickup_hour, d.pickup_hour) as pickup_hour,
            COALESCE(p.location_id, d.location_id) as location_id,
            COALESCE(p.pickups, 0) as pickups,
            COALESCE(d.dropoffs, 0) as dropoffs
        FROM PickupsCTE p
        FULL OUTER JOIN DropoffsCTE d ON p.pickup_hour = d.pickup_hour AND p.location_id = d.location_id
    ),
    MetricsCTE AS (
        SELECT *,
               (pickups + dropoffs) as activity,
               (pickups - dropoffs) as net_pickups,
               (pickups - dropoffs) / (pickups + dropoffs) as imbalance_ratio,
               ABS((pickups - dropoffs) / (pickups + dropoffs)) as abs_imbalance_ratio
        FROM FullJoined
    )
    SELECT m.*, z.Borough, z.Zone, z.service_zone
    FROM MetricsCTE m
    INNER JOIN zone_lookup z ON m.location_id = z.LocationID
    WHERE (pickups + dropoffs) >= 30
"""

df_sql_active = spark.sql(sql_query)
df_sql_active.createOrReplaceTempView("active_metrics")

df_sql_pos = spark.sql("SELECT * FROM active_metrics ORDER BY net_pickups DESC LIMIT 14")
df_sql_neg = spark.sql("SELECT * FROM active_metrics ORDER BY net_pickups ASC LIMIT 14")
df_sql_abs = spark.sql("SELECT * FROM active_metrics ORDER BY abs_imbalance_ratio DESC LIMIT 14")

# Αποθήκευση Catalyst Plan SQL
sql_plan_string = df_sql_active._jdf.queryExecution().toString()
spark.range(1).select(lit(sql_plan_string).alias("value")) \
    .write.mode("overwrite").text(f"{output_base}/plans/q6_imbalance_sql_plan")

df_sql_pos.coalesce(1).write.mode("overwrite").option("header","true").csv(f"{output_base}/results/q6_sql_top_pos_net")

end_sql_time = time.time()
t_sql = end_sql_time - start_sql_time

# =========================================================================
# ΜΕΤΡΙΚΕΣ JSON
# =========================================================================
metrics_data = {
    "spark_application_id": app_id,
    "student_id": "2122303",
    "question": "Q6_Imbalance",
    "dataframe_execution_time_seconds": t_df,
    "sql_execution_time_seconds": t_sql
}
import json
json_string = json.dumps(metrics_data, indent=4)
spark.range(1).select(lit(json_string).alias("value")) \
    .write.mode("overwrite").text(f"{output_base}/metrics/q6_imbalance_metrics_json")

print("=== Q6 IMBALANCE PART EXECUTED SUCCESSFULLY ===")
spark.stop()
