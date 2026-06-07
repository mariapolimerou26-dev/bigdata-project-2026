from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Find-HDFS-Files").getOrCreate()
    sc = spark.sparkContext
    
    # Ορίζουμε τις πιθανές διαδρομές για έλεγχο
    paths_to_check = [

        "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/marpolymerou/project2026/data/"
    ]
    
    print("\n========================================")
    print("SEARCHING FOR JSON FILES IN HDFS...")
    print("========================================")
    
    for path_str in paths_to_check:
        try:
            print(f"\nChecking directory: {path_str}")
            # Χρήση του εσωτερικού Java FileSystem API του Hadoop
            URI = sc._jvm.java.net.URI
            Path = sc._jvm.org.apache.hadoop.fs.Path
            FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
            
            fs = FileSystem.get(URI.create(path_str), sc._jsc.hadoopConfiguration())
            status = fs.listStatus(Path(path_str))
            
            for file_status in status:
                f_path = file_status.getPath().toString()
                print(f"-> Found: {f_path}")
        except Exception as e:
            print(f"Could not read {path_str}: {str(e)}")
            
    print("\n========================================")
    spark.stop()

if __name__ == "__main__":
    main()
