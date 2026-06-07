from pyspark.sql import SparkSession
from pyspark.sql.functions import col, radians, sin, cos, sqrt, atan2, to_timestamp, hour, when

def main():
    # 1. Αρχικοποίηση Spark Session
    spark = SparkSession.builder \
        .appName("TaxiMetricsFix") \
        .get_state() if hasattr(SparkSession, "get_state") else SparkSession.builder.appName("TaxiMetricsFix").getOrCreate()

    # Μονοπάτια HDFS (άλλαξέ τα αν χρειάζεται)
    input_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/data/parquet/yellow_tripdata_2015"
    output_extreme_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/data/q2_extreme_output"

    print("=== ΦΟΡΤΩΣΗ ΔΕΔΟΜΕΝΩΝ ===")
    df = spark.read.parquet(input_path)
    
    # 2. Μετατροπή Ημερομηνιών και Εξαγωγή Ώρας (Αποφυγή NULL)
    df = df.withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("dropoff_ts", to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))
    
    df = df.withColumn("pickup_hour", hour(col("pickup_ts")))

    # 3. Βασικοί Υπολογισμοί (Μετατροπές και Haversine)
    df = df.withColumn("duration_minutes", (col("dropoff_ts").cast("long") - col("pickup_ts").cast("long")) / 60.0) \
           .withColumn("trip_distance_km", col("trip_distance") * 1.60934) \
           .withColumn("duration_hours", col("duration_minutes") / 60.0) \
           .withColumn("speed_kmh", when(col("duration_hours") > 0, col("trip_distance_km") / col("duration_hours")).otherwise(0.0))

    # Υπολογισμός Απόστασης Haversine
    dlat = radians(col("dropoff_latitude") - col("pickup_latitude"))
    dlon = radians(col("dropoff_longitude") - col("pickup_longitude"))
    
    a = sin(dlat / 2.0)**2 + cos(radians(col("pickup_latitude"))) * cos(radians(col("dropoff_latitude"))) * sin(dlon / 2.0)**2
    c = 2.0 * atan2(sqrt(a), sqrt(1.0 - a))
    df = df.withColumn("haversine_km", c * 6371.0)

    # Υπολογισμός επιπλέον ζητούμενων στηλών
    df = df.withColumn("duration_per_km", when(col("trip_distance_km") > 0, col("duration_minutes") / col("trip_distance_km")).otherwise(0.0))

    # 4. Εφαρμογή Φίλτρων (Γεωγραφικά, Ώρες, Εγκυρότητα)
    filtered_geo_df = df.filter(
        (col("trip_distance") > 0.0) & 
        (col("duration_minutes") > 0.0) &
        (col("pickup_longitude") >= -74.25) & (col("pickup_longitude") <= -73.7) &
        (col("dropoff_longitude") >= -74.25) & (col("dropoff_longitude") <= -73.7) &
        (col("pickup_latitude") >= 40.5) & (col("pickup_latitude") <= 40.92) &
        (col("dropoff_latitude") >= 40.5) & (col("dropoff_latitude") <= 40.92) &
        (col("pickup_hour").isin(7, 8, 9, 10)) &
        (col("haversine_km") > 0.2) # Αν συνεχίσει να βγαίνει άδειο, σχολίασε ΜΟΝΟ αυτή τη γραμμή
    )

    # Cache για βελτίωση απόδοσης
    filtered_geo_df.cache()

    # Έλεγχος πλήθους εγγραφών στην οθόνη
    total_rows = filtered_geo_df.count()
    print(f"=== ΣΥΝΟΛΙΚΕΣ ΕΓΓΡΑΦΕΣ ΜΕΤΑ ΤΑ ΦΙΛΤΡΑ: {total_rows} ===")

    if total_rows > 0:
        # 5 Πιο αργές εγγραφές ανά χιλιόμετρο
        slowest_5 = filtered_geo_df.orderBy(col("duration_per_km").desc()).limit(5).select(
            "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "trip_distance_km", "duration_minutes", "speed_kmh", "haversine_km"
        )

        # 5 Πιο γρήγορες εγγραφές (βάσει speed_kmh)
        fastest_5 = filtered_geo_df.orderBy(col("speed_kmh").desc()).limit(5).select(
            "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "trip_distance_km", "duration_minutes", "speed_kmh", "haversine_km"
        )

        print("=== 5 SLOWEST TRIPS PER KM ===")
        slowest_5.show()
        print("=== 5 FASTEST TRIPS ===")
        fastest_5.show()

        # Ένωση και αποθήκευση των 10 ακραίων τιμών σε ένα CSV
        print("=== ΑΠΟΘΗΚΕΥΣΗ ΑΠΟΤΕΛΕΣΜΑΤΩΝ ===")
        slowest_5.union(fastest_5).coalesce(1).write.mode("overwrite").option("header", "true").csv(output_extreme_path)
    else:
        print("⚠️ ΠΡΟΣΟΧΗ: Δεν βρέθηκαν δεδομένα που να ικανοποιούν τα φίλτρα. Η αποθήκευση ακυρώθηκε.")

    spark.stop()

if __name__ == "__main__":
    main()

