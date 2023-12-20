from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Function to define time period
def get_time_period(time):
    if 500 <= time <= 1159:
        return "Morning"
    elif 1200 <= time <= 1659:
        return "Afternoon"
    elif 1700 <= time <= 2059:
        return "Evening"
    elif time >= 2100 or time <= 459:
        return "Night"
    else:
        return "None"

# Create a Spark session
spark = SparkSession.builder.appName("Query 2 RDD").getOrCreate()

columns_to_select = ["DR_NO","Time OCC"]

# Read data from the first file / first node
crimes_rdd1 = spark.read.csv("/user/user/data/Crime_Data_from_2010_to_2019.csv", header=True).select(columns_to_select).rdd
# Read data from the second file / second node
crimes_rdd2 = spark.read.csv("/user/user/data/Crime_Data_from_2020_to_Present.csv", header=True).select(columns_to_select).rdd

# Union the two RDDs
crimes_rdd = crimes_rdd1.union(crimes_rdd2)

# Remove rows with any null values if any
crimes_rdd = crimes_rdd.filter(lambda row: row[1] is not None)

# Cast the "Time OCC" column to integers
crimes_rdd = crimes_rdd.map(lambda row: (row[0], int(row[1])))  # Convert the second element to int

# Define the conditions for each time period
def map_to_time_period(row):
    time_period = get_time_period(row[1])
    return (time_period, 1)

# Use map and reduceByKey to count occurrences for each time period
result_rdd = crimes_rdd.map(map_to_time_period).reduceByKey(lambda x, y: x + y)

# Sort the result in descending order by count
result_rdd = result_rdd.sortBy(lambda x: x[1], ascending=False)

print("The Time Periods with the most crimes in Descending order are:")
for row in result_rdd.collect():
    print(row[0], row[1])
