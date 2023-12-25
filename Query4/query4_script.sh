#!/bin/bash

# Number of times to run the Spark job
NUM_RUNS=12

# Specify your Spark submit command and options
SPARK_SUBMIT_CMD="spark-submit"

#####       QUERY4_1A
SPARK_EXECUTABLE="--num-executors 16 --py-files geopy.zip,geographiclib.zip query4_assigned_a.py > output_query4_assigned_a.txt"

cd /home/user/Query4/assigned/a
echo "Running For Query4_1a"

# Loop to run spark-submit multiple times
for ((i=1; i<=$NUM_RUNS; i++)); do

    echo "Spark Execution - Iteration $i"
    $SPARK_SUBMIT_CMD $SPARK_EXECUTABLE
    echo ""
done
echo ""

#####       QUERY4_1B

SPARK_EXECUTABLE="--num-executors 16 --py-files geopy.zip,geographiclib.zip query4_assigned_b.py > output_query4_assigned_b.txt"

cd /home/user/Query4/assigned/b
echo "Running For Query4_1b"

# Loop to run spark-submit multiple times
for ((i=1; i<=$NUM_RUNS; i++)); do

    echo "Spark Execution - Iteration $i"
    $SPARK_SUBMIT_CMD $SPARK_EXECUTABLE
    echo ""
done
echo ""

#####       QUERY4_2A

SPARK_EXECUTABLE="--num-executors 16 --py-files geopy.zip,geographiclib.zip query4_closest_a.py > output_query4_closest_a.txt"

cd /home/user/Query4/closest/a
echo "Running For Query4_2a"

# Loop to run spark-submit multiple times
for ((i=1; i<=$NUM_RUNS; i++)); do

    echo "Spark Execution - Iteration $i"
    $SPARK_SUBMIT_CMD $SPARK_EXECUTABLE
    echo ""
done
echo ""

#####       QUERY4_2B

SPARK_EXECUTABLE="--num-executors 16 --py-files geopy.zip,geographiclib.zip query4_closest_b.py > output_query4_closest_b.txt"

cd /home/user/Query4/closest/b
echo "Running For Query4_2b"

# Loop to run spark-submit multiple times
for ((i=1; i<=$NUM_RUNS; i++)); do

    echo "Spark Execution - Iteration $i"
    $SPARK_SUBMIT_CMD $SPARK_EXECUTABLE
    echo ""
done
echo ""


echo "Spark execution completed."
