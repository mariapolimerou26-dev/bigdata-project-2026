#!/usr/bin/env python3
import argparse
import time
import json
import math
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

def parse_arguments():
    parser = argparse.ArgumentParser(description="Taxi Data Analysis - Q2 Complete Suite")
    parser.add_argument("--student-id", type=str, required=True, help="The student ID")
    parser.add_argument("--input-path", type=str, required=True, help="The HDFS input path")
    parser.add_argument("--output-base", type=str, required=True, help="The HDFS base path")
    return parser.parse_args()

def calculate_custom_parameters(student_id):
    id_num = int(student_id)
    h_val = id_num % 24
    L = 4
    # Κατασκευή ωριαίου παραθύρου (κυκλικό αν h+L >= 24)
    hours = []
    for i in range(L):
        hours.append((h_val + i) % 24)
    return hours

# Python συνάρτηση για το UDF Haversine
def haversine_python(lon1, lat1, lon2, lat2):
    if None in (lon1, lat1, lon2, lat2):
        return 0.0
    # Ακτίνα της Γης σε χλμ
    R = 6371.0
    try:
        dlon = math.radians(lon2 - lon1)
        dlat = math.radians(lat2 - lat1)
        a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c
    except:
        return 0.0

def main():
    args = parse_arguments()
    custom_hours = calculate_custom_parameters(args.student_id)
    
    spark = SparkSession.builder \
        .appName(f"taxi-q2-complete-suite-maria") \
        .getOrCreate()

    app_id = spark.sparkContext.applicationId
    
    # Διαδρομές εξόδου
    out_metrics_native = f"{args.output_base}/metrics/q2_df_native_metrics"
    out_metrics_udf = f"{args.output_base}/metrics/q2_df_udf_metrics"
    out_metrics_sql = f"{args.output_base}/metrics/q2_sql_metrics"
    
    out_data_path = f"{args.output_base}/results/q2_complete_output"

    # 1. Ανάγνωση δεδομένων (Parquet 2015)
    df_raw = spark.read.parquet(args.input_path)

    # Φιλτράρισμα βάσει έτους και εξατομικευμένου ωριαίου παραθύρου
    df_filtered = df_raw.filter(
        (F.year(F.col("tpep_pickup_datetime")) == 2015) & 
        (F.hour(F.col("tpep_pickup_datetime")).isin(custom_hours))
    )

    # Καθαρισμός/Προετοιμασία: Υπολογισμός διάρκειας και φιλτράρισμα outliers
    df_prep = df_filtered.withColumn(
        "duration_minutes",
        (F.unix_timestamp(F.col("tpep_dropoff_datetime")) - F.unix_timestamp(F.col("tpep_pickup_datetime"))) / 60.0
    ).filter(
        (F.col("duration_minutes") > 0.0) & (F.col("duration_minutes") <= 300.0) &
        (F.col("trip_distance") > 0.0) & (F.col("trip_distance") <= 200.0)
    )

    # Μετατροπή μιλίων σε χιλιόμετρα
    df_prep = df_prep.withColumn("trip_distance_km", F.col("trip_distance").cast("double") * 1.60934)

    # =========================================================================
    # ΥΛΟΠΟΙΗΣΗ Α: DataFrame API - Native (Built-in Expressions)
    # =========================================================================
    start_native = time.time()
    
    # Υπολογισμός Haversine με Spark SQL functions
    R = 6371.0
    dlon = F.radians(F.col("dropoff_longitude") - F.col("pickup_longitude"))
    dlat = F.radians(F.col("dropoff_latitude") - F.col("pickup_latitude"))
    
    a = F.sin(dlat / 2)**2 + F.cos(F.radians(F.col("pickup_latitude"))) * F.cos(F.radians(F.col("dropoff_latitude"))) * F.sin(dlon / 2)**2
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1.0 - a))
    df_native = df_prep.withColumn("haversine_distance_km", R * c)

    # Υπολογισμός μέσης ταχύτητας (χλμ/ώρα)
    df_native = df_native.withColumn(
        "average_speed_kmh",
        F.col("trip_distance_km") / (F.col("duration_minutes") / 60.0)
    )

    # Ένδειξη συμφόρησης: average_speed_kmh < 10 χλμ/ώρα
    df_native = df_native.withColumn(
        "is_congestion_candidate",
        F.when(F.col("average_speed_kmh") < 10.0, 1).otherwise(0)
    )

    # Ομαδοποίηση ανά ώρα
    df_native_grouped = df_native.groupBy(F.hour(F.col("tpep_pickup_datetime")).alias("pickup_hour")).agg(
        F.avg("duration_minutes").alias("avg_duration_minutes"),
        F.avg("trip_distance_km").alias("avg_trip_distance_km"),
        F.avg("average_speed_kmh").alias("avg_speed_kmh"),
        F.avg("haversine_distance_km").alias("avg_haversine_distance_km"),
        (F.sum("is_congestion_candidate") / F.count("*")).alias("congestion_candidate_share")
    ).orderBy("pickup_hour")

    # Trigger action και καταγραφή πλάνου
    df_native_grouped.cache()
    count_native = df_native_grouped.count()
    end_native = time.time()

    native_plan = df_native_grouped._jdf.queryExecution().toString()

    # =========================================================================
    # ΥΛΟΠΟΙΗΣΗ Β: DataFrame API - Python UDF
    # =========================================================================
    start_udf = time.time()
    
    haversine_udf = F.udf(haversine_python, DoubleType())
    
    df_udf = df_prep.withColumn(
        "haversine_distance_km", 
        haversine_udf(F.col("pickup_longitude"), F.col("pickup_latitude"), F.col("dropoff_longitude"), F.col("dropoff_latitude"))
    )
    df_udf = df_udf.withColumn("average_speed_kmh", F.col("trip_distance_km") / (F.col("duration_minutes") / 60.0))
    df_udf = df_udf.withColumn("is_congestion_candidate", F.when(F.col("average_speed_kmh") < 10.0, 1).otherwise(0))

    df_udf_grouped = df_udf.groupBy(F.hour(F.col("tpep_pickup_datetime")).alias("pickup_hour")).agg(
        F.avg("duration_minutes").alias("avg_duration_minutes"),
        F.avg("trip_distance_km").alias("avg_trip_distance_km"),
        F.avg("average_speed_kmh").alias("avg_speed_kmh"),
        F.avg("haversine_distance_km").alias("avg_haversine_distance_km"),
        (F.sum("is_congestion_candidate") / F.count("*")).alias("congestion_candidate_share")
    ).orderBy("pickup_hour")

    df_udf_grouped.cache()
    count_udf = df_udf_grouped.count()
    end_udf = time.time()

    udf_plan = df_udf_grouped._jdf.queryExecution().toString()

    # =========================================================================
    # ΥΛΟΠΟΙΗΣΗ Γ: Spark SQL
    # =========================================================================
    start_sql = time.time()
    
    df_native.createOrReplaceTempView("v_records")
    
    df_sql_grouped = spark.sql("""
        SELECT 
            hour(tpep_pickup_datetime) as pickup_hour,
            avg(duration_minutes) as avg_duration_minutes,
            avg(trip_distance_km) as avg_trip_distance_km,
            avg(average_speed_kmh) as avg_speed_kmh,
            avg(haversine_distance_km) as avg_haversine_distance_km,
            (sum(is_congestion_candidate) / count(*)) as congestion_candidate_share
        FROM v_records
        GROUP BY hour(tpep_pickup_datetime)
        ORDER BY pickup_hour
    """)
    
    df_sql_grouped.cache()
    count_sql = df_sql_grouped.count()
    end_sql = time.time()

    sql_plan = df_sql_grouped._jdf.queryExecution().toString()

    # =========================================================================
    # ΔΙΕΡΕΥΝΗΣΗ ΑΚΡΑΙΩΝ ΕΓΓΡΑΦΩΝ (5 ταχύτερες / 5 πιο αργές ανά χλμ)
    # =========================================================================
    # Υπολογισμός "χρόνου ανά χιλιόμετρο" (minutes_per_km)
    df_outliers_base = df_native.withColumn("minutes_per_km", F.col("duration_minutes") / F.col("trip_distance_km"))
    
    # Δυναμική επιλογή στήλης Vendor για την εκτύπωση των outliers
    v_col = "VendorID" if "VendorID" in df_outliers_base.columns else "vendor_id"

    print("\n=== TOP 5 FASTEST RECORDS PER KM ===")
    df_outliers_base.orderBy("minutes_per_km").select(
        v_col, "tpep_pickup_datetime", "tpep_dropoff_datetime", 
        "trip_distance_km", "duration_minutes", "haversine_distance_km", "minutes_per_km"
    ).show(5, truncate=False)

    print("\n=== TOP 5 SLOWEST RECORDS PER KM ===")
    df_outliers_base.orderBy(F.col("minutes_per_km").desc()).select(
        v_col, "tpep_pickup_datetime", "tpep_dropoff_datetime", 
        "trip_distance_km", "duration_minutes", "haversine_distance_km", "minutes_per_km"
    ).show(5, truncate=False)
    # =========================================================================
    # ΑΠΟΘΗΚΕΥΣΗ ΔΕΔΟΜΕΝΩΝ ΚΑΙ ΜΕΤΡΙΚΩΝ
    # =========================================================================
    # Αποθήκευση του τελικού αποτελέσματος (Native)
    df_native_grouped.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_data_path)

    # Δημιουργία JSON μετρικών
    def write_metrics(path, api_name, exec_time):
        m = {
            "spark_application_id": app_id,
           "student_id": args.student_id,
            "question": "Q2",
            "api": api_name,
            "execution_time_seconds": exec_time,
            "output_data_path": out_data_path
        }
        spark.sparkContext.parallelize([json.dumps(m, indent=4)], 1).saveAsTextFile(path)

    write_metrics(out_metrics_native, "DataFrame_Native", end_native - start_native)
    write_metrics(out_metrics_udf, "DataFrame_UDF", end_udf - start_udf)
    write_metrics(out_metrics_sql, "Spark_SQL", end_sql - start_sql)

    # Αποθήκευση των σχεδίων εκτέλεσης στο HDFS ως text files
    spark.sparkContext.parallelize([native_plan], 1).saveAsTextFile(f"{args.output_base}/plans/q2_native_plan.txt")
    spark.sparkContext.parallelize([udf_plan], 1).saveAsTextFile(f"{args.output_base}/plans/q2_udf_plan.txt")
    spark.sparkContext.parallelize([sql_plan], 1).saveAsTextFile(f"{args.output_base}/plans/q2_sql_plan.txt")

    print("\n=== Q2 ALL IMPLEMENTATIONS COMPLETED SUCCESSFULLY ===")
    spark.stop()

if __name__ == "__main__":
    main()
