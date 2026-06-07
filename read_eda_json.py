from pyspark.sql import SparkSession

def main():
    # Αρχικοποίηση του Spark
    spark = SparkSession.builder \
        .appName("Read-EDA-JSON-Maria") \
        .getOrCreate()

    # Διαδρομή του JSON στο HDFS
    # (Αν το αποθήκευσες με άλλο όνομα φακέλου, άλλαξέ το εδώ)
    hdfs_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/eda_output.json"

    print("========================================")
    print("STARTING TO READ EDA METRICS FROM HDFS")
    print("========================================")

    try:
        # Διαβάζουμε το αρχείο ως απλό κείμενο γραμμή-γραμμή
        df = spark.read.text(hdfs_path)
        
        # Συλλέγουμε τις γραμμές και τις τυπώνουμε
        lines = df.collect()
        for row in lines:
            print(row[0])
            
    except Exception as e:
        print("ERROR READING FILE:", str(e))

    print("========================================")
    print("FINISHED READING EDA METRICS")
    print("========================================")

    spark.stop()

if __name__ == "__main__":
    main()

