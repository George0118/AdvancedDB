### Query 1 - DataFrame Implementation

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, to_date, month, year, rank
from pyspark.sql.window import Window


spark = SparkSession \
    .builder \
    .appName("Query 1 DF") \
    .getOrCreate()

columns_to_select = ["DR_NO","Date Rptd"]

# Read data from the first file / first node
crimes_df1 = spark.read.csv("/user/user/data/Crime_Data_from_2010_to_2019.csv", header=True).select(columns_to_select)

# Read data from the second file / second node
crimes_df2 = spark.read.csv("/user/user/data/Crime_Data_from_2020_to_Present.csv", header=True).select(columns_to_select)

# Union the two DataFrames
crimes_df = crimes_df1.union(crimes_df2)

##################################################

# Convert the "Date Rptd" column to a timestamp type
crimes_df = crimes_df.withColumn("Timestamp", to_date(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))

# Extract the "Date" and "Month" components
crimes_df = crimes_df.withColumn("Year", year(col("Timestamp")))
crimes_df = crimes_df.withColumn("Month", month(col("Timestamp")))
crimes_df = crimes_df.drop("Date Rptd")
crimes_df = crimes_df.drop("Timestamp")

# Count Crimes by Year and Month
count_result = crimes_df.groupBy("Year", "Month").count()

# Create a window specification to rank months within each year based on counts
window_spec = Window.partitionBy("Year").orderBy(col("count").desc())

# Add a new column "MonthRank" to show the rank of each month within the year
count_result = count_result.withColumn("MonthRank", rank().over(window_spec))

# Filter only the top 3 from each year
top3_per_year = count_result.filter(col("MonthRank") <= 3)

# Show the results
top3_per_year.show(50)