import sys
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, hour, count, avg, when
from pyspark.sql.window import Window

if len(sys.argv) < 3:
    print("Usage: q4_df.py <input_parquet_path> <output_base_path>")
    sys.exit(-1)

input_path = sys.argv[1]
output_base = sys.argv[2]

spark = SparkSession.builder \
    .appName("marpolymerou_Q4_DataFrame") \
    .getOrCreate()

# Κρατάμε το Application ID για τα metrics
app_id = spark.sparkContext.applicationId
start_time = time.time()

# Ορισμός του δικού σου εξατομικευμένου παραθύρου (βάλε τις δικές σου ώρες)
START_HOUR = 6  # Παράδειγμα
END_HOUR = 12    # Παράδειγμα

# 1. Ανάγνωση Parquet
df_raw = spark.read.parquet(input_path)

# 2. Καθαρισμός & Φιλτράρισμα βάσει κανόνων εκφώνησης
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

# Φιλτράρισμα στο προσωπικό χρονικό παράθυρο
df_window = df_cleaned.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
                      .filter((col("pickup_hour") >= START_HOUR) & (col("pickup_hour") <= END_HOUR))

# 3. Υπολογισμός με βάση τις απαιτήσεις: Group By (pickup_hour, payment_type)
df_grouped = df_window.groupBy("pickup_hour", "payment_type").agg(
    count("*").alias("trips"),
    avg("fare_amount").alias("avg_fare_amount"),
    avg("total_amount").alias("avg_total_amount"),
    avg("tip_amount").alias("avg_tip_amount"),
    # Υπολογισμός tip_rate ΜΟΝΟ για κάρτες (payment_type == 1), αλλιώς null/0
    avg(when(col("payment_type") == 1, (col("tip_amount") / col("fare_amount")) * 100.0)).alias("tip_rate")
)

# Χρήση Window Function για να βρούμε το payment_share_in_hour %
# Αθροίζει τα trips όλων των payment_types μέσα στην ίδια pickup_hour
window_spec = Window.partitionBy("pickup_hour")
df_final = df_grouped.withColumn(
    "payment_share_in_hour", 
    (col("trips") / spark.functions.sum("trips").over(window_spec)) * 100.0
).orderBy("pickup_hour", "payment_type")

# 4. Καταγραφή και αποθήκευση του Catalyst Execution Plan (ΠΡΙΝ το action αποθήκευσης)
# Χρησιμοποιούμε string representation για να το γράψουμε σε αρχείο
# Σημείωση: Στο cluster μπορείς να το σώσεις τοπικά ή στο HDFS
# Εδώ το τυπώνουμε για να αποθηκευτεί στα logs ή το γράφεις σε αρχείο
# df_final.explain("formatted")
# Παίρνουμε το physical plan ως string μέσω του Java Virtual Machine (JVM) του Spark
extended_plan_string = df_final._jdf.queryExecution().toString()

# Το μετατρέπουμε σε RDD και το αποθηκεύουμε ως text αρχείο στο HDFS
plan_rdd = spark.sparkContext.parallelize([extended_plan_string])
plan_rdd.coalesce(1).saveAsTextFile(f"{output_base}/plans/q4_df_parquet_plan")

# 5. Αποθήκευση αποτελεσμάτων στο HDFS
df_final.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_base}/results/q4_complete_output")

end_time = time.time()
execution_timnano q4_df.pye = end_time - start_time

# 6. Αποθήκευση Metrics σε μορφή JSON στο HDFS
metrics_data = {
    "spark_application_id": app_id,
    "student_id": "2122303",  # Το ΑΜ σου
    "question": "Q4",
    "api": "DataFrame_Native_Parquet",
    "execution_time_seconds": execution_time,
    "output_data_path": f"{output_base}/results/q4_complete_output"
}

# Δημιουργία RDD για να σωθεί το JSON απευθείας στο HDFS
metrics_rdd = spark.sparkContext.parallelize([json.dumps(metrics_data)])
metrics_rdd.coalesce(1).saveAsTextFile(f"{output_base}/metrics/q4_df_parquet_metrics")

spark.stop()

