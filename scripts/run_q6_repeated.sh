#!/bin/bash

echo "Έναρξη επαναλαμβανόμενης εκτέλεσης του Ερωτήματος 6 για μέτρηση επιδόσεων..."

# Ορισμός πλήθους επαναλήψεων
REPEATS=5

for ((i=1; i<=REPEATS; i++))
do
   echo "----------------------------------------"
   echo "Επανάληψη $i από $REPEATS..."
   echo "----------------------------------------"
   
   echo "Εκτέλεση Q6 με DataFrames..."
   ./submit_remote_job.sh q6_df.py
   
   echo "Εκτέλεση Q6 με Spark SQL..."
   ./submit_remote_job.sh q6_sql.py
done

echo "Όλες οι επαναλήψεις του Q6 ολοκληρώθηκαν!"
