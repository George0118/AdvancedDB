### Query 1 - SQL Implementation

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Query 1 SQL") \
    .getOrCreate()

# Read data from the first file / first node
crimes_df1 = spark.read.csv("/user/user/data/Crime_Data_from_2010_to_2019.csv", header=True)
crimes_df1.createOrReplaceTempView("crimes_df1")

# Read data from the second file / second node
crimes_df2 = spark.read.csv("/user/user/data/Crime_Data_from_2020_to_Present.csv", header=True)
crimes_df2.createOrReplaceTempView("crimes_df2")

# Union the two DataFrames using Spark SQL
spark.sql("SELECT DR_NO, to_date(`Date Rptd`, 'MM/dd/yyyy hh:mm:ss a') as Timestamp FROM crimes_df1 UNION SELECT DR_NO, to_date(`Date Rptd`, 'MM/dd/yyyy hh:mm:ss a') as Timestamp FROM crimes_df2").createOrReplaceTempView("crimes_df")

# Extract the "Year" and "Month" components using Spark SQL
spark.sql("SELECT *, year(Timestamp) as Year, month(Timestamp) as Month FROM crimes_df").createOrReplaceTempView("crimes_df")

# Count the occurrences of each Year and Month using Spark SQL
count_result = spark.sql("SELECT Year, Month, COUNT(*) as count FROM crimes_df GROUP BY Year, Month")

# Create a window specification to rank months within each year based on counts using Spark SQL
count_result.createOrReplaceTempView("count_result")
spark.sql("SELECT *, RANK() OVER (PARTITION BY Year ORDER BY count DESC) as MonthRank FROM count_result").createOrReplaceTempView("count_result")

# Select the top 3 per year using Spark SQL
top3_per_year = spark.sql("SELECT * FROM count_result WHERE MonthRank <= 3")

# Show the resulting DataFrame
top3_per_year.show()
