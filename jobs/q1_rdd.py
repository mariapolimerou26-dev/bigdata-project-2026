#!/usr/bin/env python3
import argparse
import time
import json
import datetime
from pyspark.sql import SparkSession

def parse_arguments():
    parser = argparse.ArgumentParser(description="Taxi Data Analysis - Q1 RDD API")
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

    spark = SparkSession.builder \
        .appName(f"taxi-q1-rdd-{args.format}-maria") \
        .getOrCreate()

    app_id = spark.sparkContext.applicationId
    output_data_path = f"{args.output_base}/results/q1_rdd_{args.format}_output"
    output_metrics_path = f"{args.output_base}/metrics/q1_rdd_{args.format}_metrics"

    start_time = time.time()

    # Φόρτωση δεδομένων
    if args.format == "csv":
        # Διαβάζουμε το CSV με header, χωρίς inferSchema
        df = spark.read.option("header", "true").csv(args.input_path)
    else:
        df = spark.read.parquet(args.input_path)

    # Μετατροπή σε RDD
    raw_rdd = df.rdd

    # Σετ για γρήγορη αναζήτηση (O(1)) μέσα στα closures του RDD
    hours_set = set(custom_hours)
    days_set = set(custom_days)

    def process_row(row):
        try:
            # Έλεγχος αν οι βασικές στήλες είναι κενές
            if row["tpep_pickup_datetime"] is None or row["total_amount"] is None:
                return None
            
            # Parsing του timestamp
            ts_str = row["tpep_pickup_datetime"]
            # Το Spark μπορεί να επιστρέψει datetime object ή string ανάλογα με το format
            if isinstance(ts_str, str):
                # Υποστήριξη για διαφορετικά formats ημερομηνιών
                if "T" in ts_str:
                    dt = datetime.datetime.strptime(ts_str.split(".")[0], "%Y-%m-%dT%H:%M:%S")
                else:
                    dt = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            else:
                dt = ts_str

            # Εξαγωγή κρίσιμων τιμών
            pickup_year = dt.year
            pickup_day = dt.day
            pickup_hour = dt.hour

            # Εφαρμογή Φίλτρων
            if pickup_year != target_year:
                return None
            if pickup_day not in days_set:
                return None
            if pickup_hour not in hours_set:
                return None

            # Φίλτρα εγκυρότητας ποσού
            total_amount = float(row["total_amount"])
            if total_amount <= 0:
                return None

            # Key: (day, hour), Value: (trips_count, total_revenue)
            return ((pickup_day, pickup_hour), (1, total_amount))
        except Exception:
            return None

    # MapReduce Pipeline
    result_rdd = raw_rdd.map(process_row) \
        .filter(lambda x: x is not None) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1])) \
        .sortBy(lambda x: (-x[2], -x[3])) # Φθίνουσα σειρά ως προς trips και revenue

    # Μετατροπή σε CSV String γραμμές με header
    header = "pickup_day,pickup_hour,trips,total_revenue"
    formatted_lines = result_rdd.map(lambda x: f"{x[0]},{x[1]},{x[2]},{x[3]}")
    
    # Προσθήκη του header και αποθήκευση στο HDFS
    spark.sparkContext.parallelize([header], 1) \
        .union(formatted_lines) \
        .coalesce(1) \
        .saveAsTextFile(output_data_path)

    end_time = time.time()
    execution_time = end_time - start_time

    # Δημιουργία και αποθήκευση JSON μετρικών
    metrics = {
        "spark_application_id": app_id,
        "student_id": args.student_id,
        "question": "Q1",
        "api": "RDD",
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

    print(f"\n=== Q1 RDD {args.format.upper()} COMPLETED SUCCESSFULLY ===")
    print(f"Execution Time: {execution_time:.2f} seconds\n")
    
    # Εμφάνιση των K πρώτων αποτελεσμάτων
    for row in result_rdd.take(K):
        print(f"Day: {row[0]}, Hour: {row[1]} -> Trips: {row[2]}, Revenue: {row[3]:.2f}")

    spark.stop()

if __name__ == "__main__":
    main()
