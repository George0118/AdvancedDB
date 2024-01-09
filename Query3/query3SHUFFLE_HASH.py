# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp

# Create a Spark session
spark = SparkSession.builder.appName("Query 3 SHUFFLE_HASH HINT").getOrCreate()

# Define columns to select
columns_to_select1 = ["DR_NO", "Date Rptd", "Vict Descent", "LAT", "LON"]
columns_to_select2 = ["Zip Code", "Estimated Median Income"]
columns_to_select3 = ["LAT", "LON", "ZIPcode"]

# Read data from CSV files
crimes_df1 = spark.read.csv("/user/user/data/Crime_Data_from_2010_to_2019.csv", header=True).select(columns_to_select1)
income_data = spark.read.csv("file:///home/user/data/income/LA_income_2015.csv", header=True).select(columns_to_select2)
reverse_geocoding_data = spark.read.csv("file:///home/user/data/revgecoding.csv", header=True).select(columns_to_select3)


income_data.createOrReplaceTempView("income_data_table")


# Convert "Date Rptd" to a timestamp column
crimes_df1 = crimes_df1.withColumn("Timestamp", to_timestamp("Date Rptd", "MM/dd/yyyy hh:mm:ss a"))

# Extract the year from the timestamp
crimes_df1 = crimes_df1.withColumn("Year", year("Timestamp"))

# Show the DataFrame
# crimes_df1.show(truncate=False)




crimes_df1_2015 = crimes_df1.filter(crimes_df1["Year"] == 2015)


crimes_df1_2015 = crimes_df1_2015.filter(col("Vict Descent").isNotNull())
# count = crimes_df1_2015.count()
# print("Number of rows in crimes_df1_2015:", count)


reverse_geocoding_data = reverse_geocoding_data.dropDuplicates(["LAT", "LON"])


# Write SQL queries to get top 3 and bottom 3 rows
top_income_sql = (
    "SELECT DISTINCT `Zip Code`, CAST(regexp_replace(`Estimated Median Income`, '[$,]', '') AS INT) AS income "
    "FROM income_data_table ORDER BY income DESC LIMIT 3"
)

bottom_income_sql = (
    "SELECT DISTINCT `Zip Code`, CAST(regexp_replace(`Estimated Median Income`, '[$,]', '') AS INT) AS income "
    "FROM income_data_table ORDER BY income ASC LIMIT 3"
)

# Execute SQL queries
top_incomes = spark.sql(top_income_sql)
bottom_incomes = spark.sql(bottom_income_sql)

# Union the top and bottom DataFrames with distinct rows
combined_incomes = top_incomes.union(bottom_incomes).distinct()

# Show the combined DataFrame
# combined_incomes.show(truncate=False)

# First Join
first_join = crimes_df1_2015.join(reverse_geocoding_data.hint("SHUFFLE_HASH"), ["LAT", "LON"], "inner")

first_join.explain()





# Second Join
merged_data = first_join.join(combined_incomes.hint("SHUFFLE_HASH"), first_join["ZIPcode"] == combined_incomes["Zip Code"], "inner")

merged_data.createOrReplaceTempView("merged_data_table")



victim_descent_counts_sql = """
    SELECT 
        CASE 
            WHEN `Vict Descent` = 'A' THEN 'Other Asian'
            WHEN `Vict Descent` = 'B' THEN 'Black'
            WHEN `Vict Descent` = 'C' THEN 'Chinese'
            WHEN `Vict Descent` = 'D' THEN 'Cambodian'
            WHEN `Vict Descent` = 'F' THEN 'Filipino'
            WHEN `Vict Descent` = 'G' THEN 'Guamanian'
            WHEN `Vict Descent` = 'H' THEN 'Hispanic/Latin/Mexican'
            WHEN `Vict Descent` = 'I' THEN 'American Indian/Alaskan Native'
            WHEN `Vict Descent` = 'J' THEN 'Japanese'
            WHEN `Vict Descent` = 'K' THEN 'Korean'
            WHEN `Vict Descent` = 'L' THEN 'Laotian'
            WHEN `Vict Descent` = 'O' THEN 'Other'
            WHEN `Vict Descent` = 'P' THEN 'Pacific Islander'
            WHEN `Vict Descent` = 'S' THEN 'Samoan'
            WHEN `Vict Descent` = 'U' THEN 'Hawaiian'
            WHEN `Vict Descent` = 'V' THEN 'Vietnamese'
            WHEN `Vict Descent` = 'W' THEN 'White'
            WHEN `Vict Descent` = 'X' THEN 'Unknown'
            WHEN `Vict Descent` = 'Z' THEN 'Asian Indian'
            ELSE 'Unknown' 
        END AS Descent_Code,
        COUNT(*) as count 
    FROM merged_data_table
    GROUP BY Descent_Code
    ORDER BY count DESC

"""

# Execute the SQL query
victim_descent_counts = spark.sql(victim_descent_counts_sql)

# Show the result
victim_descent_counts.show(truncate=False)

# Explain the execution plan for the Second Join
print("Second Join - Execution Plan:")
merged_data.explain()


# Stop the Spark session
spark.stop()