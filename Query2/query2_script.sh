#!/bin/bash

# Specify your Spark submit command and options
SPARK_SUBMIT_CMD="spark-submit"
SPARK_EXECUTABLE="--num-executors 4 query2_DF_SQL.py > output_q2_DF_SQL.txt"

# Number of times to run the Spark job
NUM_RUNS=12

echo "Running For DataFrame & SQL"

# Loop to run spark-submit multiple times
for ((i=1; i<=$NUM_RUNS; i++)); do

    echo "Spark Execution - Iteration $i"
    $SPARK_SUBMIT_CMD $SPARK_EXECUTABLE

    sleep 5
done
echo ""

SPARK_EXECUTABLE="--num-executors 4 query2_RDD.py > output_q2_RDD.txt"
echo "Running For RDD"

# Loop to run spark-submit multiple times
for ((i=1; i<=$NUM_RUNS; i++)); do

    echo "Spark Execution - Iteration $i"
    $SPARK_SUBMIT_CMD $SPARK_EXECUTABLE

    sleep 5
done
echo ""

echo "Spark execution completed."

