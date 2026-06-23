#!/bin/bash

# Δημιουργία καταλόγου στο HDFS
hdfs dfs -mkdir -p /user/hadoop/data

# Ανέβασμα των αρχείων δεδομένων (CSV/Parquet) από το τοπικό σύστημα στο HDFS
hdfs dfs -put /local/path/to/datasets/* /user/hadoop/data/

echo "Τα δεδομένα ανέβηκαν επιτυχώς στο HDFS!"
