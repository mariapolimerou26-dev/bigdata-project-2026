#!/usr/bin/env python3
import argparse
import time
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def parse_arguments():
    parser = argparse.ArgumentParser(description="Taxi Data Analysis - Q1 DataFrame API")
    parser.add_argument("--student-id", type=str, required=True, help="The student ID to compute custom windows")
    parser.add_argument("--input-path", type=str, required=True, help="The HDFS or local input path to the taxi data")
    parser.add_argument("--output-base", type=str, required=True, help="The HDFS base path for outputs (results/metrics)")
    parser.add_argument("--format", type=str, choices=["csv", "parquet"], required=True, help="Input data format")
    return parser.parse_args()

def calculate_custom_parameters(student_id):
    """
    Υπολογισμός παραμέτρων βάσει της εκφώνησης:
    X = (id % 3) + 1 -> 2122303 % 3 + 1 = 0 + 1 = 1 (Άρα Yellow Taxi)
    Y = (id % 2) -> 2122303 % 2 = 1 (Άρα Έτος 2015)
    Z = (id % 7) -> 2122303 % 7 = 2
    """
    try:
        id_num = int(student_id)
    except ValueError:
        raise ValueError("Student ID must be a valid integer numeric string.")

    # Υπολογισμός Έτους (Y = id % 2)
    # Y = 0 -> 2024, Y = 1 -> 2015
    y_val = id_num % 2
    target_year = 2015 if y_val == 1 else 2024

    # Υπολογισμός Z (Z = id % 7)
    z_val = id_num % 7
    
    # Ώρες: [Z+5, Z+6, Z+7, Z+8] -> [2+5, 2+6, 2+7, 2+8] = [7, 8, 9, 10]
    custom_hours = [z_val + 5, z_val + 6, z_val + 7, z_val + 8]
    
    # Ημέρες: [Z, Z+1, Z+2] -> [2, 2+1, 2+2] = [2, 3, 4]
    custom_days = [z_val, z_val + 1, z_val + 2]

    # K_limit = (id % 15) + 10 -> 2122303 % 15 + 10 = 13 + 10 = 23
    k_limit = (id_num % 15) + 10

    return target_year, custom_hours, custom_days, k_limit

def main():
    # 1. Parse arguments
    args = parse_arguments()

    # 2. Υπολογισμός εξατομικευμένων παραμέτρων βάσει Μητρώου
    target_year, custom_hours, custom_days, K = calculate_custom_parameters(args.student_id)

    # 3. Αρχικοποίηση SparkSession
    spark = SparkSession.builder \
        .appName(f"taxi-q1-df-{args.format}-maria") \
        .getOrCreate()

    # Ανάγνωση του Application ID
    app_id = spark.sparkContext.applicationId

    # 4. Ορισμός Paths εξόδου
    output_data_path = f"{args.output_base}/results/q1_df_{args.format}_output"
    output_metrics_path = f"{args.output_base}/metrics/q1_df_{args.format}_metrics"

    # 5. Καταγραφή χρόνου έναρξης
    start_time = time.time()

    # 6. Ανάγνωση δεδομένων ανάλογα με το format
    if args.format == "csv":
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input_path)
    else:
        df = spark.read.parquet(args.input_path)

    # 7. Μετατροπή ημερομηνιών και εξαγωγή Year, Day, Hour
    # Προσαρμογή ονόματος στήλης βάσει του Yellow Taxi dataset
    df_with_time = df.withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime")) \
                     .withColumn("pickup_year", F.year("pickup_ts")) \
                     .withColumn("pickup_day", F.dayofmonth("pickup_ts")) \
                     .withColumn("pickup_hour", F.hour("pickup_ts"))

    # 8. Εφαρμογή Φίλτρων (Έτος, Ημέρες, Ώρες)
    df_filtered = df_with_time.filter(
        (F.col("pickup_year") == target_year) &
        (F.col("pickup_day").isin(custom_days)) &
        (F.col("pickup_hour").isin(custom_hours))
    )

    # 9. Ομαδοποίηση και Aggregations (Count, Sum)
    # Χρήση F.count("*") για σωστή καταμέτρηση γραμμών και στήλης total_amount για έσοδα
    df_aggregated = df_filtered.groupBy("pickup_day", "pickup_hour") \
        .agg(
            F.count("*").alias("trips"),
            F.sum("total_amount").alias("total_revenue")
        )

    # 10. Ταξινόμηση (Sort)
    # Πρώτα φθίνουσα σειρά ως προς trips και μετά ως προς total_revenue
    df_result = df_aggregated.sort(
        F.col("trips").desc(),
        F.col("total_revenue").desc()
    )

    # 11. Εγγραφή των αποτελεσμάτων στο HDFS
    if args.format == "csv":
        df_result.write.mode("overwrite").option("header", "true").csv(output_data_path)
    else:
        df_result.write.mode("overwrite").parquet(output_data_path)

    # Καταγραφή χρόνου λήξης
    end_time = time.time()
    execution_time = end_time - start_time

    # 12. Εξαγωγή και Αποθήκευση του Catalyst Physical Plan σε αρχείο κειμένου (.txt)
    # Χρήση του σωστού PySpark API που τυπώνει το formatted plan
    try:
        import sys
        orig_stdout = sys.stdout
        with open("q1_df_physical_plan.txt", "w", encoding="utf-8") as f:
            sys.stdout = f
            df_result.explain(mode="formatted")
            sys.stdout = orig_stdout
    except Exception as e:
        # Fallback αν η έκδοση Spark δεν υποστηρίζει το mode="formatted"
        with open("q1_df_physical_plan.txt", "w", encoding="utf-8") as f:
            sys.stdout = f
            df_result.explain(extended=True)
            sys.stdout = orig_stdout

    # 13. Δημιουργία δομής JSON μετρικών
    metrics = {
        "spark_application_id": app_id,
        "student_id": args.student_id,
        "question": "Q1",
        "api": "DataFrame",
        "format_input": args.format,
        "custom_year": target_year,
        "custom_hours": custom_hours,
        "custom_days": custom_days,
        "K_limit": K,
        "execution_time_seconds": execution_time,
        "output_data_path": output_data_path
    }

    # Αποθήκευση των μετρικών σε JSON στο HDFS
    metrics_json = json.dumps(metrics, indent=4)
    spark.sparkContext.parallelize([metrics_json], 1).saveAsTextFile(output_metrics_path)

    # 14. Τύπωμα στην οθόνη για επιβεβαίωση στο log του driver pod
    print(f"\n=== Q1 DF {args.format.upper()} COMPLETED SUCCESSFULLY ===")
    print(f"Application ID: {app_id}")
    print(f"Execution Time: {execution_time:.2f} seconds\n")
    df_result.show(K, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()

