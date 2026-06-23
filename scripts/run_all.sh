#!/bin/bash

echo "Έναρξη καθολικής εκτέλεσης των ερωτημάτων του Project..."

# Εκτέλεση του Exploratory Data Analysis
echo "Τρέχει το EDA..."
./submit_remote_job.sh eda.py

# Εκτέλεση του Ερωτήματος 1
echo "Τρέχει το Q1 (DataFrames & SQL)..."
./submit_remote_job.sh q1_df.py

# Εκτέλεση του Ερωτήματος 2 (All-in-one)
echo "Τρέχει το Q2 All..."
./submit_remote_job.sh q2_final_all.py

# Εκτέλεση του Ερωτήματος 3 (All-in-one)
echo "Τρέχει το Q3 All..."
./submit_remote_job.sh q3_final_all.py

# Εκτέλεση του Ερωτήματος 4 (All-in-one)
echo "Τρέχει το Q4 All..."
./submit_remote_job.sh q4_all.py

# Εκτέλεση του Ερωτήματος 5 (All-in-one)
echo "Τρέχει το Q5 All..."
./submit_remote_job.sh q5_all.py

# Εκτέλεση του Ερωτήματος 6 (DataFrames & SQL)
echo "Τρέχει το Q6..."
./submit_remote_job.sh q6_df.py

echo "Όλα τα jobs στάλθηκαν επιτυχώς!"
