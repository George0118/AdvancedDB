### Query 2 - DataFrame & SQL Implementation

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, to_date, month, year, rank, when
from pyspark.sql.window import Window


spark = SparkSession \
    .builder \
    .appName("Query 2 DF/SQL") \
    .getOrCreate()

columns_to_select = ["DR_NO","Time OCC"]

# Read data from the first file / first node
crimes_df1 = spark.read.csv("/user/user/data/Crime_Data_from_2010_to_2019.csv", header=True).select(columns_to_select)

# Read data from the second file / second node
crimes_df2 = spark.read.csv("/user/user/data/Crime_Data_from_2020_to_Present.csv", header=True).select(columns_to_select)

# Union the two DataFrames
crimes_df = crimes_df1.union(crimes_df2)

##################################################

# Remove rows with any null values if any
crimes_df = crimes_df.na.drop()

# Cast the "Time OCC" column to integers
crimes_df = crimes_df.withColumn("Time OCC", col("Time OCC").cast("int"))

# Define the conditions for each time period
conditions = [
    (col("Time OCC") >= 500) & (col("Time OCC") <= 1159),  # Morning
    (col("Time OCC") >= 1200) & (col("Time OCC") <= 1659),  # Afternoon
    (col("Time OCC") >= 1700) & (col("Time OCC") <= 2059),  # Evening
    (col("Time OCC") >= 2100) | (col("Time OCC") <= 459)    # Night
]

# Define the corresponding values for each time period
values = ["Morning", "Afternoon", "Evening", "Night"]

# Use the when and otherwise functions to create the new column
crimes_df = crimes_df.withColumn("Time_Period", when(conditions[0], values[0])
                                        .when(conditions[1], values[1])
                                        .when(conditions[2], values[2])
                                        .when(conditions[3], values[3])
                                        .otherwise(col("Time OCC").cast("string"))) #last line for debugging the condition should never be met

crimes_df = crimes_df.drop("Time OCC")

# Register the DataFrame as a temporary SQL table
crimes_df.createOrReplaceTempView("crimes_table")

# Use SQL to group by "Time_Period" and show results in descending order
result = spark.sql("SELECT Time_Period, COUNT(*) as count FROM crimes_table GROUP BY Time_Period ORDER BY count DESC")

print("The Time Periods with the most crimes in Descending order are:")

result.show(truncate=False)