#!/usr/bin/env python3
import argparse
import time
import json
from pyspark.sql import SparkSession

def parse_arguments():
    parser = argparse.ArgumentParser(description="Taxi Data Analysis - Q1 Spark SQL")
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
    custom_hours = [z_val + 5, z_val + 6, z_val + 7, z_val + 8]
    custom_days = [z_val, z_val + 1, z_val + 2]
    k_limit = (id_num % 15) + 10
    return target_year, custom_hours, custom_days, k_limit

def main():
    args = parse_arguments()
    target_year, custom_hours, custom_days, K = calculate_custom_parameters(args.student_id)

    # Αρχικοποίηση SparkSession
    spark = SparkSession.builder \
        .appName(f"taxi-q1-sql-{args.format}-maria") \
        .getOrCreate()

    app_id = spark.sparkContext.applicationId
    output_data_path = f"{args.output_base}/results/q1_sql_{args.format}_output"
    output_metrics_path = f"{args.output_base}/metrics/q1_sql_{args.format}_metrics"

    start_time = time.time()

    # Ανάγνωση δεδομένων (Αφαιρούμε το inferSchema για το CSV για να μην κρασάρει από μνήμη!)
    if args.format == "csv":
        df = spark.read.option("header", "true").csv(args.input_path)
    else:
        df = spark.read.parquet(args.input_path)

    df.createOrReplaceTempView("raw_trips")

    # SQL Query που κάνει το parsing on-the-fly όπως το DataFrame
    # Μετατρέπουμε το tpep_pickup_datetime σε timestamp και βγάζουμε year, dayofmonth, hour
    hours_str = ", ".join(map(str, custom_hours))
    days_str = ", ".join(map(str, custom_days))

    query = f"""
    SELECT 
        pickup_day,
        pickup_hour,
        COUNT(*) as trips,
        SUM(CAST(total_amount AS DOUBLE)) as total_revenue
    FROM (
        SELECT 
            CAST(total_amount AS DOUBLE),
            YEAR(CAST(tpep_pickup_datetime AS TIMESTAMP)) as pickup_year,
            DAYOFMONTH(CAST(tpep_pickup_datetime AS TIMESTAMP)) as pickup_day,
            HOUR(CAST(tpep_pickup_datetime AS TIMESTAMP)) as pickup_hour
        FROM raw_trips
    )
    WHERE pickup_year = {target_year}
      AND pickup_day IN ({days_str})
      AND pickup_hour IN ({hours_str})
    GROUP BY pickup_day, pickup_hour
    ORDER BY trips DESC, total_revenue DESC
    """

    df_result = spark.sql(query)

    # Εκτύπωση Catalyst Physical Plan στα Logs
    print("\n=== Q1 SPARK SQL PHYSICAL PLAN ===")
    df_result.explain(True)
    print("===================================\n")

    # Εγγραφή των αποτελεσμάτων στο HDFS (coalesce(1) για να βγει ένα καθαρό αρχείο)
    df_result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_data_path)

    end_time = time.time()
    execution_time = end_time - start_time

    # Δημιουργία δομής JSON μετρικών
    metrics = {
        "spark_application_id": app_id,
        "student_id": args.student_id,
        "question": "Q1",
        "api": "SQL",
        "format_input": args.format,
        "custom_year": target_year,
        "custom_hours": custom_hours,
        "custom_days": custom_days,
        "K_limit": K,
        "execution_time_seconds": execution_time,
        "output_data_path": output_data_path
    }

    metrics_json = json.dumps(metrics, indent=4)
    spark.sparkContext.parallelize([metrics_json], 1).saveAsTextFile(output_metrics_path)

    print(f"\n=== Q1 SQL {args.format.upper()} COMPLETED SUCCESSFULLY ===")
    print(f"Execution Time: {execution_time:.2f} seconds\n")
    df_result.show(K, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
