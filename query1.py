from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("taxi-analysis-q1-maria") \
    .getOrCreate()

print("=== Το Spark Session δημιουργήθηκε επιτυχώς για τη Μαρία! ===")

# Ανάγνωση από το επίσημο HDFS του εργαστηρίου
df = spark.read.csv("hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2024.csv", header=True, inferSchema=True)

# Εδώ βάζεις τον υπόλοιπο κώδικα της ανάλυσής σου (π.χ. df.show(), φιλτραρίσματα κλπ.)
df.show(5)

spark.stop()
