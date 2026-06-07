#!/usr/bin/env python3
import argparse
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, when, avg, sum as _sum, count as _count, expr

def parse_arguments():
    parser = argparse.ArgumentParser(description="Taxi Data Analysis - Q2 Native DataFrame")
    parser.add_argument("--student-id", type=str, required=True, help="The student ID to compute custom windows")
    parser.add_argument("--input-path", type=str, required=True, help="The HDFS input path to the taxi data")
    parser.add_argument("--output-base", type=str, required=True, help="The HDFS base path for outputs")
    parser.add_argument("--format", type=str, choices=["csv", "parquet"], required=True, help="Input data format")
    return parser.parse_args()

def calculate_custom_parameters(student_id):
    id_num = int(student_id)
    y_val = id_num % 2
    target_year = 2015 if y_val == 1 else 2024
    z_val = id_num % 7
    # Παράθυρο ωρών για το Q2 (π.χ. Z+5 έως Z+8)
    custom_hours = [z_val + 5, z_val + 6, z_val + 7, z_val + 8]
    k_limit = (id_num % 15) + 10
    return target_year, custom_hours, k_limit

def main():
    args = parse_arguments()
    target_year, custom_hours, K = calculate_custom_parameters(args.student_id)
    
    # Ορισμός ορίων ώρας βάσει της λίστας
    start_hour = min(custom_hours)
    end_hour = max(custom_hours)

    spark = SparkSession.builder \
        .appName(f"taxi-q2-df-native-{args.format}-maria") \
        .getOrCreate()

    app_id = spark.sparkContext.applicationId
    output_data_path = f"{args.output_base}/results/q2_df_native_{args.format}_output"
    output_metrics_path = f"{args.output_base}/metrics/q2_df_native_{args.format}_metrics"

    start_time = time.time()

    # 1. Φόρτωση δεδομένων ανάλογα με το format
    if args.format == "csv":
        df_raw = spark.read.option("header", "true").csv(args.input_path)
    else:
        df_raw = spark.read.parquet(args.input_path)

    # 2. Καθαρισμός εγγραφών
    # Έλεγχος ύπαρξης στηλών PULocationID/DOLocationID (στα CSV του 2015 μπορεί να λείπουν ή να έχουν άλλα ονόματα, βάζουμε προστασία)
    pu_col = "PULocationID" if "PULocationID" in df_raw.columns else "trip_distance" # fallback αν δεν υπάρχουν
    do_col = "DOLocationID" if "DOLocationID" in df_raw.columns else "trip_distance"

    df_cleaned = df_raw.filter(
        (col("tpep_pickup_datetime").isNotNull()) & (col("tpep_dropoff_datetime").isNotNull()) &
        (col("trip_distance").cast("double") > 0) & (col("total_amount").cast("double") > 0)
    )

    # Υπολογισμός διάρκειας και φιλτράρισμα ώρας
    df_with_time = df_cleaned.withColumn(
        "duration_minutes", (unix_timestamp(col("tpep_dropoff_datetime").cast("timestamp")) - unix_timestamp(col("tpep_pickup_datetime").cast("timestamp"))) / 60.0
    ).withColumn(
        "duration_hours", col("duration_minutes") / 60.0
    ).filter(col("duration_minutes") > 0)

    df_window = df_with_time.withColumn("pickup_hour", hour(col("tpep_pickup_datetime").cast("timestamp"))) \
                            .filter((col("pickup_hour") >= start_hour) & (col("pickup_hour") <= end_hour))

    # 3. Μετρήσεις ταχύτητας και συμφόρησης
    df_metrics = df_window \
        .withColumn("trip_distance_km", col("trip_distance").cast("double") * 1.60934) \
        .withColumn("speed_kmh", col("trip_distance_km") / col("duration_hours")) \
        .withColumn("duration_per_km", col("duration_minutes") / col("trip_distance_km"))

    df_final_features = df_metrics.withColumn(
        "congestion_candidate",
        when((col("speed_kmh") < 10.0) & (col("trip_distance_km") >= 1.0), 1).otherwise(0)
    )

    # 4. Σύνοψη ανά ώρα
    df_hourly_summary = df_final_features.groupBy("pickup_hour").agg(
        _count("*").alias("trips"),
        avg("duration_minutes").alias("avg_duration_minutes"),
        expr("percentile_approx(duration_minutes, 0.5)").alias("median_duration_minutes"),
        expr("percentile_approx(duration_minutes, 0.9)").alias("p90_duration_minutes"),
        avg("speed_kmh").alias("avg_speed_kmh"),
        (_sum("trip_distance_km") / _sum("duration_hours")).alias("agg_speed_kmh"),
        (avg("congestion_candidate") * 100.0).alias("congestion_candidate_share")
    ).orderBy("pickup_hour")

    # Αποθήκευση κύριων αποτελεσμάτων
    df_hourly_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_data_path)

    end_time = time.time()
    execution_time = end_time - start_time

    # 5. Αποθήκευση Μετρικών JSON
    metrics = {
        "spark_application_id": app_id,
        "student_id": args.student_id,
        "question": "Q2",
        "api": "DataFrame_Native",
        "format_input": args.format,
        "custom_year": target_year,
        "custom_hours": custom_hours,
        "execution_time_seconds": execution_time,
        "output_data_path": output_data_path
    }

    metrics_json = json.dumps(metrics, indent=4)
    spark.sparkContext.parallelize([metrics_json], 1).saveAsTextFile(output_metrics_path)

    print(f"\n=== Q2 NATIVE DF {args.format.upper()} COMPLETED SUCCESSFULLY ===")
    print(f"Execution Time: {execution_time:.2f} seconds\n")
    df_hourly_summary.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
