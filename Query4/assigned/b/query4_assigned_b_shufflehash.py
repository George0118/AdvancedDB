### Query 4 Assigned B - DataFrame & SQL Implementation

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, desc, col
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F

import geopy.distance

@F.udf(returnType=FloatType())
def get_distance(a, b):
    return geopy.distance.geodesic(a, b).km

spark = SparkSession \
    .builder \
    .appName("Query 4 assigned B DF/SQL") \
    .getOrCreate()

columns_to_select_crimes = ["DR_NO", "Weapon Used Cd", "LAT", "LON"]

c1 = columns_to_select_crimes + ["AREA "]
c2 = columns_to_select_crimes + ["AREA"]

# Read data from the first file / first node
crimes_df1 = spark.read.csv("/user/user/data/Crime_Data_from_2010_to_2019.csv", header=True).select(c1)
crimes_df1 = crimes_df1.withColumnRenamed("AREA ", "AREA")

# Read data from the second file / second node
crimes_df2 = spark.read.csv("/user/user/data/Crime_Data_from_2020_to_Present.csv", header=True).select(c2)

# Union the two DataFrames
crimes_df = crimes_df1.union(crimes_df2)

# Remove rows with any null values if any and then drop Weapon Code too
crimes_df = crimes_df.na.drop()
crimes_df = crimes_df.drop("Weapon Used Cd")

# Columns to select for LAPD stations
columns_to_select_LAPD_stations = ["DIVISION", "PREC", "X", "Y"]

# Read data for LAPD stations
LAPD_stations = spark.read.csv("/user/user/data/LAPD_Police_Stations.csv", header=True).select(columns_to_select_LAPD_stations)

####################################################################################################################################

# Clear Null Island rows
crimes_df = crimes_df.filter((col("LAT") != "0") | (col("LON") != "0"))

# Remove Duplicate Crimes
crimes_df = crimes_df.dropDuplicates(["DR_NO"])

# Convert "AREA" to int to compare with PREC
crimes_df = crimes_df.withColumn("AREA", col("AREA").cast("int"))

# Create temporary views for both DataFrames
crimes_df.createOrReplaceTempView("crimes_view")
LAPD_stations.createOrReplaceTempView("stations_view")

# Join Crimes and LAPD Station on PREC = AREA
query_df = spark.sql("""
    SELECT /*+ SHUFFLE_HASH(s) */
           c.DR_NO, c.LAT, c.LON, s.DIVISION, s.X, s.Y
    FROM crimes_view c
    JOIN stations_view s ON c.AREA = s.PREC
""")

query_df.explain()

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

query_df = query_df.groupBy("DIVISION").agg(
    count("DR_NO").alias("Crime_Count"),
    avg("Distance").alias("Average_Distance")
).orderBy(desc("Crime_Count"))

# Show the resulting DataFrame
query_df.show(30)