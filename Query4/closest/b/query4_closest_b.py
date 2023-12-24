### Query 4 Closest B - DataFrame & SQL Implementation

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, desc, col, broadcast
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

import geopy.distance

@F.udf(returnType=FloatType())
def get_distance(a, b):
    return geopy.distance.geodesic(a, b).km

spark = SparkSession \
    .builder \
    .appName("Query 4 closest B DF/SQL") \
    .getOrCreate()

columns_to_select_crimes = ["DR_NO", "Weapon Used Cd", "LAT", "LON"]

# Read data from the first file / first node
crimes_df1 = spark.read.csv("/user/user/data/Crime_Data_from_2010_to_2019.csv", header=True).select(columns_to_select_crimes)

# Read data from the second file / second node
crimes_df2 = spark.read.csv("/user/user/data/Crime_Data_from_2020_to_Present.csv", header=True).select(columns_to_select_crimes)

# Union the two DataFrames
crimes_df = crimes_df1.union(crimes_df2)

# Remove rows with any null values if any and then drop Weapon Code too
crimes_df = crimes_df.na.drop()
crimes_df = crimes_df.drop("Weapon Used Cd")

# Columns to select for LAPD stations
columns_to_select_LAPD_stations = ["DIVISION", "X", "Y"]

# Read data for LAPD stations
LAPD_stations = spark.read.csv("/user/user/data/LAPD_Police_Stations.csv", header=True).select(columns_to_select_LAPD_stations)

####################################################################################################################################

# Clear Null Island rows
crimes_df = crimes_df.filter((col("LAT") != "0") | (col("LON") != "0"))

broadcast_LAPD_stations = broadcast(LAPD_stations)

# Full join and filter afterwards
query_df = crimes_df.crossJoin(broadcast_LAPD_stations)

# Create the Distance Column
query_df = query_df.withColumn(
    "Distance",
    get_distance(F.array("LAT", "LON"), F.array("Y", "X"))
)

# Drop the coordinates columns
query_df = query_df.drop("LAT")
query_df = query_df.drop("LON")
query_df = query_df.drop("X")
query_df = query_df.drop("Y")

# With Window function create the rank column to keep only the min distance and its LAPD Division
window_spec = Window.partitionBy("DR_NO").orderBy("Distance")

query_df = query_df.withColumn(
    "rank", F.rank().over(window_spec)
).filter(
    col("rank") == 1
).drop(
    "rank"
).select(
    "DR_NO", "Distance", "DIVISION"
)

# Group By DIVISION and Count each crime (DESC) and AVG the min distances
query_df = query_df.groupBy("DIVISION").agg(
    count("DR_NO").alias("Crime_Count"),
    avg("Distance").alias("Average_Distance")
).orderBy(desc("Crime_Count"))

# Show the resulting DataFrame
query_df.show(30)