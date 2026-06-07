from pyspark.sql import SparkSession

# Ξεκινάμε το Spark Session
spark = SparkSession.builder \
    .appName("CheckParquetSize") \
    .getOrCreate()

print("\n" + "="*50)
print("=== ΕΛΕΓΧΟΣ ΔΕΔΟΜΕΝΩΝ PARQUET ===")

try:
    # Προσπαθούμε να διαβάσουμε απευθείας το Parquet που φτιάξαμε
    # Το Spark ξέρει από μόνο του το configuration του HDFS από το περιβάλλον σου
    df = spark.read.parquet("hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/data/parquet/yellow_tripdata_2024/")
    
    print("\n✅ ΕΠΙΤΥΧΙΑ! Τα Parquet αρχεία διαβάστηκαν κανονικά.")
    print(f"Συνολικές γραμμές στο έτοιμο Parquet: {df.count():,}")
    
    print("\n--- Σχήμα Δεδομένων (Schema) ---")
    df.printSchema()
    
    print("\n--- Δείγμα Δεδομένων (Πρώτες 5 γραμμές) ---")
    df.show(5, truncate=False)

except Exception as e:
    print("\n❌ Σφάλμα κατά την ανάγνωση:")
    print(str(e))

print("="*50 + "\n")
