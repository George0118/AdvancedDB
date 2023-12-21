#!/bin/bash

# Specify your Spark submit command and options
SPARK_SUBMIT_CMD="spark-submit"
SPARK_EXECUTABLE="--num-executors 4 query1_DF.py > output_DF.txt"

# Number of times to run the Spark job
NUM_RUNS=12

echo "Running For DataFrame"

# Loop to run spark-submit multiple times
for ((i=1; i<=$NUM_RUNS; i++)); do

    echo "Spark Execution - Iteration $i"
    $SPARK_SUBMIT_CMD $SPARK_EXECUTABLE

    sleep 5
done
echo ""

SPARK_EXECUTABLE="--num-executors 4 query1_SQL.py > output_SQL.txt"
echo "Running For SQL"

# Loop to run spark-submit multiple times
for ((i=1; i<=$NUM_RUNS; i++)); do

    echo "Spark Execution - Iteration $i"
    $SPARK_SUBMIT_CMD $SPARK_EXECUTABLE

    sleep 5
done
echo ""

echo "Spark execution completed."

