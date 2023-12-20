### DataFrame API

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession \
    .builder \
    .appName("DF Creation") \
    .getOrCreate()

columns_to_select = ["Date Rptd", "DATE OCC", "Vict Age", "LAT", "LON"]

# Read data from the first file / first node
crimes_df1 = spark.read.csv("/user/user/data/Crime_Data_from_2010_to_2019.csv", header=True).select(columns_to_select)

# Read data from the second file / second node
crimes_df2 = spark.read.csv("/user/user/data/Crime_Data_from_2020_to_Present.csv", header=True).select(columns_to_select)

# Union the two DataFrames
crimes_df = crimes_df1.union(crimes_df2)

##################################################

new_data_types = {
    "Date Rptd": StringType(),
    "DATE OCC": StringType(),
    "Vict Age": IntegerType(),
    "LAT": FloatType(),
    "LON": FloatType(),
}

for col_name, new_data_type in new_data_types.items():
    crimes_df = crimes_df.withColumn(col_name, col(col_name).cast(new_data_type))

# Define the date format
date_format = "M/d/yyyy h:mm:ss a"

# Cast the date columns to a date type
crimes_df = crimes_df.withColumn("Date Rptd", to_timestamp(crimes_df["Date Rptd"], date_format))
crimes_df = crimes_df.withColumn("DATE OCC", to_timestamp(crimes_df["DATE OCC"], date_format))

##################################################

print("DataFrame Schema:")

# Print the DataFrame Schema
crimes_df.printSchema()

print("Showing the first 5 items with no NULL values:")

# Print the first 5 rows of the DataFrame 
crimes_df.show(5)

row_count = crimes_df.count() # get the number of rows

print("Number of rows in the DataFrame: {}".format(row_count))