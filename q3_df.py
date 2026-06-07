import time
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

if len(sys.argv) < 2:
    print("Usage: q3_df.py <output_base_path>")
    sys.exit(-1)

output_base = sys.argv[1]

spark = SparkSession.builder \
    .appName("taxi-q3-df-maria") \
    .getOrCreate()

start_time = time.time()

# 1. Ανάγνωση Parquet 2024
parquet_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/data/parquet/yellow_tripdata_2024"
df = spark.read.parquet(parquet_path)

# 2. Φιλτράρισμα και Εξαγωγή Μήνα
# payment_type = 1 (Credit Card)
df_filtered = df.filter((F.col("payment_type") == 1) & (F.col("pu_location_id").isNotNull())) \
                .withColumn("trip_month", F.month(F.col("pickup_date")))

# 3. Υπολογισμός Μέσου Όρου Tips ανά Μήνα και Ζώνη
df_grouped = df_filtered.groupBy("trip_month", "pu_location_id") \
                        .agg(F.avg("tip_amount").alias("avg_tip_amount"))

# 4. Εφαρμογή Window Function (DENSE_RANK)
window_spec = Window.partitionBy("trip_month").orderBy(F.col("avg_tip_amount").desc())

df_ranked = df_grouped.withColumn("rank", F.dense_rank().over(window_spec))

# === 1. ΚΑΤΑΓΡΑΦΗ SPARK APPLICATION ID ===
app_id = spark.sparkContext.applicationId
print(f"Spark Application ID: {app_id}")

# === 2. ΑΠΟΘΗΚΕΥΣΗ ΦΥΣΙΚΟΥ ΣΧΕΔΙΟΥ CATALYST (ΚΑΝΟΝΙΚΟ) ===
import os
os.makedirs("results/plans", exist_ok=True)
with open("results/plans/q3_df_plan.txt", "w", encoding="utf-8") as f:
    f.write(df_result._jdf.queryExecution().toString())

# 6. Αποθήκευση κανονικού αποτελέσματος στο HDFS
output_path = f"{output_base}/q3_df_output"
df_result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

end_time = time.time()
execution_time_normal = end_time - start_time
print(f"Normal Execution Time: {execution_time_normal} seconds")


# === 3. ΠΕΙΡΑΜΑ ΑΦΑΙΡΕΣΗΣ ΔΙΑΜΕΡΙΣΜΟΥ (Q3 ABLATION RUN) ===
print("\n--- Starting Q3 Ablation Run (No Partition Pruning) ---")
start_ablation = time.time()

# Διαβάζουμε το unpartitioned raw αρχείο CSV ή Parquet χωρίς φίλτρο ημερομηνιών 
# ώστε να αναγκάσουμε το Spark να κάνει full scan χωρίς PartitionFilters
df_raw = spark.read.parquet(f"{output_base}/parquet/yellow_tripdata_2024_unpartitioned") 
# Σημείωση: Αν δεν έχεις unpartitioned, διαβάζεις το κανονικό αλλά χωρίς να εφαρμόσεις 
# το φίλτρο του AM (δηλαδή για όλο το έτος), ώστε να μην γίνει pruning.

df_filtered_ablation = df_raw.filter((F.col("payment_type") == 1) & (F.col("pu_location_id").isNotNull())) \
                             .withColumn("trip_month", F.month(F.col("pickup_date")))

df_grouped_ablation = df_filtered_ablation.groupBy("trip_month", "pu_location_id").agg(F.avg("tip_amount").alias("avg_tip_amount"))
df_ranked_ablation = df_grouped_ablation.withColumn("rank", F.dense_rank().over(window_spec))
df_result_ablation = df_ranked_ablation.filter(F.col("rank") <= 5).orderBy("trip_month", "rank")

# Αποθήκευση σχεδίου για το Ablation
with open("results/plans/q3_df_ablation_plan.txt", "w", encoding="utf-8") as f:
    f.write(df_result_ablation._jdf.queryExecution().toString())

output_path_ablation = f"{output_base}/q3_df_ablation_output"
df_result_ablation.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path_ablation)

end_ablation = time.time()
execution_time_ablation = end_ablation - start_ablation
print(f"Ablation Execution Time: {execution_time_ablation} seconds")


# === 4. ΑΠΟΘΗΚΕΥΣΗ ΜΕΤΡΙΚΩΝ ΚΑΙ ΓΙΑ ΤΑ ΔΥΟ ΣΕ JSON ===
os.makedirs("results/metrics", exist_ok=True)
metrics = {
    "query": "Q3_DataFrame_and_Ablation",
    "application_id": app_id,
    "normal_execution_time_seconds": execution_time_normal,
    "ablation_execution_time_seconds": execution_time_ablation,
    "output_hdfs_path": output_path,
    "ablation_output_hdfs_path": output_path_ablation
}
with open("results/metrics/q3_df_metrics.json", "w", encoding="utf-8") as mj:
    json.dump(metrics, mj, indent=4)
print("Metrics saved to JSON successfully.")

spark.stop()
