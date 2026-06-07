#!/usr/bin/env python3
import argparse
import time
import json
from pyspark.sql import SparkSession

def parse_arguments():
    parser = argparse.ArgumentParser(description="Taxi Data Analysis - Q2 Spark SQL")
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
    k_limit = (id_num % 15) + 10
    return target_year, custom_hours, k_limit

def main():
    args = parse_arguments()
    target_year, custom_hours, K = calculate_custom_parameters(args.student_id)
    
    start_hour = min(custom_hours)
    end_hour = max(custom_hours)

    spark = SparkSession.builder \
        .appName(f"taxi-q2-sql-{args.format}-maria") \
        .getOrCreate()

    app_id = spark.sparkContext.applicationId
    output_data_path = f"{args.output_base}/results/q2_sql_{args.format}_output"
    output_metrics_path = f"{args.output_base}/metrics/q2_sql_{args.format}_metrics"

    start_time = time.time()

    # 1. Φόρτωση δεδομένων και Temp View
    if args.format == "csv":
        df_raw = spark.read.option("header", "true").csv(args.input_path)
    else:
        df_raw = spark.read.parquet(args.input_path)
        
    df_raw.createOrReplaceTempView("trips_raw")

    # Δυναμικό φιλτράρισμα στηλών τοποθεσίας ανάλογα με το αν υπάρχουν (Προστασία για το 2015)
    location_filter = ""
    if "PULocationID" in df_raw.columns and "DOLocationID" in df_raw.columns:
        location_filter = "AND PULocationID IS NOT NULL AND DOLocationID IS NOT NULL"

    # 2. Ενιαίο SQL Query
    sql_query = f"""
    WITH cleaned_trips AS (
        SELECT
            CAST(trip_distance AS DOUBLE) as trip_distance,
            CAST(total_amount AS DOUBLE) as total_amount,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            HOUR(CAST(tpep_pickup_datetime AS TIMESTAMP)) as pickup_hour,
            (unix_timestamp(CAST(tpep_dropoff_datetime AS TIMESTAMP)) - unix_timestamp(CAST(tpep_pickup_datetime AS TIMESTAMP))) / 60.0 as duration_minutes
        FROM trips_raw
        WHERE tpep_pickup_datetime IS NOT NULL
          AND tpep_dropoff_datetime IS NOT NULL
          {location_filter}
    ),
    filtered_window AS (
        SELECT
            *,
            (duration_minutes / 60.0) as duration_hours,
            (trip_distance * 1.60934) as trip_distance_km
        FROM cleaned_trips
        WHERE pickup_hour >= {start_hour} AND pickup_hour <= {end_hour}
          AND duration_minutes > 0
          AND trip_distance > 0
          AND total_amount > 0
    ),
    metrics_calculated AS (
        SELECT
            *,
            (trip_distance_km / duration_hours) as speed_kmh,
            CASE WHEN (trip_distance_km / duration_hours) < 10.0 AND trip_distance_km >= 1.0 THEN 1 ELSE 0 END as congestion_candidate
        FROM filtered_window
    )
    SELECT
        pickup_hour,
        COUNT(*) as trips,
        AVG(duration_minutes) as avg_duration_minutes,
        percentile_approx(duration_minutes, 0.5) as median_duration_minutes,
        percentile_approx(duration_minutes, 0.9) as p90_duration_minutes,
        AVG(speed_kmh) as avg_speed_kmh,
        (SUM(trip_distance_km) / SUM(duration_hours)) as agg_speed_kmh,
        (AVG(congestion_candidate) * 100.0) as congestion_candidate_share
    FROM metrics_calculated
    GROUP BY pickup_hour
    ORDER BY pickup_hour
    """

    df_sql_summary = spark.sql(sql_query)

    # 3. Αποθήκευση αποτελεσμάτων
    df_sql_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_data_path)

    end_time = time.time()
    execution_time = end_time - start_time

    # 4. Αποθήκευση Μετρικών JSON
    metrics = {
        "spark_application_id": app_id,
        "student_id": args.student_id,
        "question": "Q2",
        "api": "Spark_SQL",
        "format_input": args.format,
        "custom_year": target_year,
        "custom_hours": custom_hours,
        "execution_time_seconds": execution_time,
        "output_data_path": output_data_path
    }

    metrics_json = json.dumps(metrics, indent=4)
    spark.sparkContext.parallelize([metrics_json], 1).saveAsTextFile(output_metrics_path)

    print(f"\n=== Q2 SPARK SQL {args.format.upper()} COMPLETED SUCCESSFULLY ===")
    print(f"Execution Time: {execution_time:.2f} seconds\n")
    df_sql_summary.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
