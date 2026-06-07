#!/usr/bin/env python3
import argparse
import time
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def parse_arguments():
    parser = argparse.ArgumentParser(description="Taxi Data Analysis - Q2 Alternative Native")
    parser.add_argument("--student-id", type=str, required=True, help="The student ID")
    parser.add_argument("--input-path", type=str, required=True, help="The HDFS input path")
    parser.add_argument("--output-base", type=str, required=True, help="The HDFS base path")
    parser.add_argument("--format", type=str, choices=["csv", "parquet"], required=True, help="Input format")
    return parser.parse_args()

def calculate_custom_parameters(student_id):
    id_num = int(student_id)
    y_val = id_num % 2
    target_year = 2015 if y_val == 1 else 2024
    z_val = id_num % 7
    custom_hours = [z_val + 5, z_val + 6, z_val + 7, z_val + 8]
    return target_year, custom_hours

def main():
    args = parse_arguments()
    target_year, custom_hours = calculate_custom_parameters(args.student_id)
    
    spark = SparkSession.builder \
        .appName(f"taxi-q2-native-alt-{args.format}-maria") \
        .getOrCreate()

    app_id = spark.sparkContext.applicationId
    output_data_path = f"{args.output_base}/results/q2_native_alt_{args.format}_output"
    output_metrics_path = f"{args.output_base}/metrics/q2_native_alt_{args.format}_metrics"

    start_time = time.time()

    if args.format == "csv":
        df = spark.read.option("header", "true").csv(args.input_path)
    else:
        df = spark.read.parquet(args.input_path)

    # Δυναμική προσαρμογή στηλών για Grouping (Προστασία για το schema του 2015)
    if "pu_location_id" in df.columns:
        pu_col = "pu_location_id"
        do_col = "do_location_id"
    elif "PULocationID" in df.columns:
        pu_col = "PULocationID"
        do_col = "DOLocationID"
    else:
        pu_col = "vendor_id" if "vendor_id" in df.columns else "VendorID"
        do_col = pu_col

    df_dist = df.withColumn("trip_distance_km", F.col("trip_distance").cast("double") * 1.60934)

    if "duration_minutes" not in df.columns:
        df_dist = df_dist.withColumn(
            "duration_minutes", 
            (F.unix_timestamp(F.col("tpep_dropoff_datetime").cast("timestamp")) - F.unix_timestamp(F.col("tpep_pickup_datetime").cast("timestamp"))) / 60.0
        )

    df_speed = df_dist.withColumn(
        "average_speed",
        F.when(F.col("duration_minutes").cast("double") > 0, F.col("trip_distance_km") / (F.col("duration_minutes").cast("double") / 60.0)).otherwise(0.0)
    )

    # Διορθωμένο φίλτρο με σωστά κλεισμένες παρενθέσεις
    df_normal = df_speed.filter((F.col("trip_distance_km") <= 200.0) & (F.col("duration_minutes").cast("double") <= 300.0))

    # Ομαδοποίηση και Στατιστικά ανά ζώνη
    if pu_col == do_col:
        df_grouped = df_normal.groupBy(pu_col).agg(
            F.avg("duration_minutes").alias("avg_duration"),
            F.avg("trip_distance_km").alias("avg_distance"),
            F.avg("average_speed").alias("avg_speed")
        ).withColumn("pu_location_id", F.col(pu_col)).withColumn("do_location_id", F.col(pu_col))
    else:
        df_grouped = df_normal.groupBy(pu_col, do_col).agg(
            F.avg("duration_minutes").alias("avg_duration"),
            F.avg("trip_distance_km").alias("avg_distance"),
            F.avg("average_speed").alias("avg_speed")
        )

    df_grouped.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_data_path)

    end_time = time.time()
    execution_time = end_time - start_time

    metrics = {
        "spark_application_id": app_id,
        "student_id": args.student_id,
        "question": "Q2_Alt",
        "api": "DataFrame_Native_Alt",
        "format_input": args.format,
        "custom_year": target_year,
        "execution_time_seconds": execution_time,
        "output_data_path": output_data_path
    }

    metrics_json = json.dumps(metrics, indent=4)
    spark.sparkContext.parallelize([metrics_json], 1).saveAsTextFile(output_metrics_path)

    print(f"\n=== Q2 NATIVE ALT {args.format.upper()} COMPLETED SUCCESSFULLY ===")
    spark.stop()

if __name__ == "__main__":
    main()
