#!/usr/bin/env python3
import argparse
import time
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def parse_arguments():
    parser = argparse.ArgumentParser(description="Taxi Data Analysis - Q3 Complete Suite")
    parser.add_argument("--student-id", type=str, required=True, help="The student ID")
    parser.add_argument("--input-parquet", type=str, required=True, help="The HDFS input path for Parquet 2024")
    parser.add_argument("--input-csv", type=str, required=True, help="The HDFS input path for raw CSV 2024")
    parser.add_argument("--zone-lookup", type=str, required=True, help="The HDFS path for taxi_zone_lookup.csv")
    parser.add_argument("--output-base", type=str, required=True, help="The HDFS base path")
    return parser.parse_args()

def calculate_custom_parameters(student_id):
    id_num = int(student_id)
    
    # 1. Υπολογισμός 4 ωρών (κυκλικό παράθυρο)
    h_start = id_num % 24
    custom_hours = [(h_start + i) % 24 for i in range(4)]
    
    # 2. Υπολογισμός 3 συνεχόμενων ημερών (βάσει τυπικού αλγορίθμου εκφώνησης)
    d_start = (id_num % 28) + 1
    custom_days = [d_start, d_start + 1, d_start + 2]
    
    return custom_days, custom_hours

def main():
    args = parse_arguments()
    custom_days, custom_hours = calculate_custom_parameters(args.student_id)
    
    spark = SparkSession.builder \
        .appName("taxi-q3-complete-suite-maria") \
        .getOrCreate()

    app_id = spark.sparkContext.applicationId
    
    # Paths εξόδου
    out_metrics_native = f"{args.output_base}/metrics/q3_df_native_metrics"
    out_metrics_sql = f"{args.output_base}/metrics/q3_sql_metrics"
    out_metrics_ablation = f"{args.output_base}/metrics/q3_ablation_csv_metrics"
    out_data_path = f"{args.output_base}/results/q3_final_output"

    # Φόρτωση του Zone Lookup
    zones_df = spark.read.option("header", "true").csv(args.zone_lookup) \
        .select(F.col("LocationID").alias("zone_loc_id"), F.col("Borough"), F.col("Zone"))

    # =========================================================================
    # ΥΛΟΠΟΙΗΣΗ Α: DataFrame API - Parquet (Με Partition Pruning & Threshold)
    # =========================================================================
    start_native = time.time()
    
    df_parquet = spark.read.parquet(args.input_parquet)
    
    # Εφαρμογή φίλτρων ποιότητας και εξατομίκευσης
    df_pq_filtered = df_parquet.filter(
        (F.col("duration_minutes") > 0.0) & 
        (F.col("trip_distance") > 0.0) & 
        (F.col("fare_amount") > 0.0) & 
        (F.col("total_amount") > 0.0) &
        (F.dayofmonth(F.col("tpep_pickup_datetime")).isin(custom_days)) &
        (F.hour(F.col("tpep_pickup_datetime")).isin(custom_hours))
    )
    
    # Προσδιορισμός σωστού PULocationID (Case-insensitive check)
    pu_col = "PULocationID" if "PULocationID" in df_pq_filtered.columns else "pu_location_id"

    # GroupBy και υπολογισμός των 4 βασικών μετρικών
    df_pq_grouped = df_pq_filtered.groupBy(pu_col).agg(
        F.count("*").alias("trips"),
        F.sum("fare_amount").alias("total_revenue"),
        (F.sum("fare_amount") / F.sum("trip_distance")).alias("revenue_per_mile"),
        (F.sum("fare_amount") / F.sum("duration_minutes")).alias("revenue_per_minute")
    )
    
    # Εφαρμογή Ορίου Υποστήριξης (Support Threshold >= 10 διαδρομές)
    df_pq_final = df_pq_grouped.filter(F.col("trips") >= 10)
    
    # Join με τον πίνακα ζωνών για να πάρουμε τα ονόματα
    df_pq_with_zones = df_pq_final.join(zones_df, df_pq_final[pu_col] == zones_df["zone_loc_id"], "inner") \
        .select("zone_loc_id", "Borough", "Zone", "trips", "total_revenue", "revenue_per_mile", "revenue_per_minute")

    # Trigger action για μέτρηση καθαρού χρόνου
    df_pq_with_zones.cache()
    count_pq = df_pq_with_zones.count()
    end_native = time.time()
    
    # Καταγραφή Physical Plan
    native_plan = df_pq_with_zones._jdf.queryExecution().toString()

    # =========================================================================
    # ΥΛΟΠΟΙΗΣΗ Β: Spark SQL - Parquet
    # =========================================================================
    start_sql = time.time()
    
    df_parquet.createOrReplaceTempView("v_parquet_trips")
    zones_df.createOrReplaceTempView("v_zones")
    
    # Κατασκευή SQL Query που αντικατοπτρίζει ακριβώς τα παραπάνω βήματα
    days_str = ",".join(map(str, custom_days))
    hours_str = ",".join(map(str, custom_hours))
    
    sql_query = f"""
        SELECT 
            z.zone_loc_id, z.Borough, z.Zone,
            COUNT(*) as trips,
            SUM(t.fare_amount) as total_revenue,
            SUM(t.fare_amount) / SUM(t.trip_distance) as revenue_per_mile,
            SUM(t.fare_amount) / SUM(t.duration_minutes) as revenue_per_minute
        FROM v_parquet_trips t
        INNER JOIN v_zones z ON t.{pu_col} = z.zone_loc_id
        WHERE t.duration_minutes > 0 AND t.trip_distance > 0 AND t.fare_amount > 0 AND t.total_amount > 0
          AND dayofmonth(t.tpep_pickup_datetime) IN ({days_str})
          AND hour(t.tpep_pickup_datetime) IN ({hours_str})
        GROUP BY z.zone_loc_id, z.Borough, z.Zone
        HAVING COUNT(*) >= 10
    """
    
    df_sql_final = spark.sql(sql_query)
    df_sql_final.cache()
    count_sql = df_sql_final.count()
    end_sql = time.time()
    
    sql_plan = df_sql_final._jdf.queryExecution().toString()

    # =========================================================================
    # ΥΛΟΠΟΙΗΣΗ Γ: Ablation Run - Ακατέργαστο CSV (Χωρίς Partition Pruning)
    # =========================================================================
    start_ablation = time.time()
    
    df_csv_raw = spark.read.option("header", "true").csv(args.input_csv)
    
    # Στο CSV πρέπει να υπολογίσουμε τη διάρκεια χειροκίνητα και να κάνουμε cast
    df_csv_prep = df_csv_raw.withColumn(
        "duration_minutes",
        (F.unix_timestamp(F.col("tpep_dropoff_datetime")) - F.unix_timestamp(F.col("tpep_pickup_datetime"))) / 60.0
    ).withColumn("trip_distance", F.col("trip_distance").cast("double")) \
     .withColumn("fare_amount", F.col("fare_amount").cast("double")) \
     .withColumn("total_amount", F.col("total_amount").cast("double"))
     
    # Εφαρμογή των ίδιων ακριβώς φίλτρων
    df_csv_filtered = df_csv_prep.filter(
        (F.col("duration_minutes") > 0.0) & 
        (F.col("trip_distance") > 0.0) & 
        (F.col("fare_amount") > 0.0) & 
        (F.col("total_amount") > 0.0) &
        (F.dayofmonth(F.col("tpep_pickup_datetime")).isin(custom_days)) &
        (F.hour(F.col("tpep_pickup_datetime")).isin(custom_hours))
    )
    
    pu_csv_col = "PULocationID" if "PULocationID" in df_csv_filtered.columns else "pu_location_id"
    
    df_csv_grouped = df_csv_filtered.groupBy(pu_csv_col).agg(
        F.count("*").alias("trips"),
        F.sum("fare_amount").alias("total_revenue"),
        (F.sum("fare_amount") / F.sum("trip_distance")).alias("revenue_per_mile"),
        (F.sum("fare_amount") / F.sum("duration_minutes")).alias("revenue_per_minute")
    ).filter(F.col("trips") >= 10)
    
    df_csv_final = df_csv_grouped.join(zones_df, df_csv_grouped[pu_csv_col] == zones_df["zone_loc_id"], "inner")
    
    df_csv_final.cache()
    count_ablation = df_csv_final.count()
    end_ablation = time.time()

    # =========================================================================
    # ΔΙΕΡΕΥΝΗΣΗ ΜΙΚΡΩΝ ΔΕΙΓΜΑΤΩΝ (Χωρίς Threshold για την Αναφορά)
    # =========================================================================
    print("\n=== TOP 5 HIGHEST REVENUE PER MINUTE (WITHOUT SUPPORT THRESHOLD) ===")
    df_pq_grouped.join(zones_df, df_pq_grouped[pu_col] == zones_df["zone_loc_id"], "inner") \
        .orderBy(F.col("revenue_per_minute").desc()) \
        .select("Zone", "trips", "total_revenue", "revenue_per_minute").show(5, truncate=False)

    # =========================================================================
    # ΕΚΤΥΠΩΣΗ ΤΩΝ TOP 10 ΖΩΝΩΝ (Με Threshold) ΓΙΑ ΤΑ ΣΥΜΠΕΡΑΣΜΑΤΑ ΣΟΥ
    # =========================================================================
    print("\n=== TOP 10 ZONES BY TOTAL REVENUE ===")
    df_pq_with_zones.orderBy(F.col("total_revenue").desc()).show(10, truncate=False)

    print("\n=== TOP 10 ZONES BY REVENUE PER MILE ===")
    df_pq_with_zones.orderBy(F.col("revenue_per_mile").desc()).show(10, truncate=False)

    # =========================================================================
    # ΑΠΟΘΗΚΕΥΣΗ ΔΕΔΟΜΕΝΩΝ, ΜΕΤΡΙΚΩΝ ΚΑΙ ΠΛΑΝΩΝ
    # =========================================================================
    df_pq_with_zones.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_data_path)

    def write_json_metrics(path, api_name, exec_time):
        m = {
            "spark_application_id": app_id,
            "student_id": args.student_id,
            "question": "Q3",
            "api": api_name,
            "execution_time_seconds": exec_time,
            "output_data_path": out_data_path
        }
        spark.sparkContext.parallelize([json.dumps(m, indent=4)], 1).saveAsTextFile(path)

    write_json_metrics(out_metrics_native, "DataFrame_Native_Parquet", end_native - start_native)
    write_json_metrics(out_metrics_sql, "Spark_SQL_Parquet", end_sql - start_sql)
    write_json_metrics(out_metrics_ablation, "DataFrame_Ablation_CSV", end_ablation - start_ablation)

    spark.sparkContext.parallelize([native_plan], 1).saveAsTextFile(f"{args.output_base}/plans/q3_native_plan.txt")
    spark.sparkContext.parallelize([sql_plan], 1).saveAsTextFile(f"{args.output_base}/plans/q3_sql_plan.txt")

    print("\n=== Q3 ALL RUNS AND EXPERIMENTS COMPLETED SUCCESSFULLY ===")
    spark.stop()

if __name__ == "__main__":
    main()
