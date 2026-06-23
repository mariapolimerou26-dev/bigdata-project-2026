#!/bin/bash

# Έλεγχος αν δόθηκε το όνομα του script ως όρισμα
if [ -z "$1" ]; then
    echo "Σφάλμα: Παρακαλώ δώστε το όνομα του python script. (π.χ. ./submit_remote_job.sh q1_df.py)"
    exit 1
fi

SCRIPT_NAME=$1

# Εκτέλεση του job στο Spark Cluster
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    --executor-cores 2 \
    --executor-memory 4G \
    /home/hadoop/jobs/$SCRIPT_NAME

echo "Το job $SCRIPT_NAME στάλθηκε για εκτέλεση στο cluster!"
