from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json

def main():
    spark = SparkSession.builder \
        .appName("EDA-Maria-Local") \
        .getOrCreate()

    # 1. Φόρτωση των Parquet απευθείας από το HDFS του εργαστηρίου
    base_hdfs = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/data/parquet"
    
    print("Reading data from HDFS...")
    df_2015 = spark.read.parquet(f"{base_hdfs}/yellow_tripdata_2015")
    df_2024 = spark.read.parquet(f"{base_hdfs}/yellow_tripdata_2024")

    metrics = {}

    # 2. Υπολογισμός Ωριαίας Κατανομής
    print("Calculating hourly distributions...")
    h_15 = df_2015.groupBy(F.hour("tpep_pickup_datetime").alias("hour")).count().collect()
    h_24 = df_2024.groupBy(F.hour("tpep_pickup_datetime").alias("hour")).count().collect()
    
    metrics["hourly_distribution_2015"] = {str(r["hour"]): r["count"] for r in h_15 if r["hour"] is not None}
    metrics["hourly_distribution_2024"] = {str(r["hour"]): r["count"] for r in h_24 if r["hour"] is not None}

    # 3. Υπολογισμός Ημερήσιας Κατανομής 2024
    print("Calculating daily distribution...")
    d_24 = df_2024.groupBy(F.dayofmonth("tpep_pickup_datetime").alias("day")).count().collect()
    metrics["daily_distribution_2024"] = {str(r["day"]): r["count"] for r in d_24 if r["day"] is not None}

    # 4. Ιστόγραμμα Trip Distance 2024 (Floor σε ακέραια μίλια)
    print("Calculating distance buckets...")
    dist_24 = df_2024.withColumn("bucket", F.floor("trip_distance")) \
                     .groupBy("bucket").count().collect()
    metrics["trip_distance_buckets_2024"] = {str(int(r["bucket"])): r["count"] for r in dist_24 if r["bucket"] is not None and r["bucket"] >= 0}

    # 5. Ιστόγραμμα Total Amount 2024 (Βήματα των $10)
    print("Calculating amount buckets...")
    amt_24 = df_2024.withColumn("bucket", F.floor(F.col("total_amount") / 10) * 10) \
                    .groupBy("bucket").count().collect()
    metrics["total_amount_buckets_2024"] = {str(int(r["bucket"])): r["count"] for r in amt_24 if r["bucket"] is not None and r["bucket"] >= 0}

    # Καταγραφή αρνητικών τιμών (ανωμαλίες)
    neg_amt = df_2024.filter(F.col("total_amount") < 0).count()
    metrics["negative_total_amount_count_2024"] = neg_amt

    # 6. ΑΠΟΘΗΚΕΥΣΗ ΤΟΠΙΚΑ ΣΤΟ WSL ΣΑΝ ΚΑΝΟΝΙΚΟ JSON
    output_local_path = "eda_metrics.json"
    print(f"Saving metrics locally to {output_local_path}...")
    with open(output_local_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=4, ensure_ascii=False)

    print("EDA completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
