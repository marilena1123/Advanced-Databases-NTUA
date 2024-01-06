'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, desc, col, row_number, to_date
from pyspark.sql.window import Window
import time

# Initialize Spark session with 4 executors and legacy time parser policy
spark = SparkSession.builder \
    .appName("Crime Data Analysis") \
    .config("spark.executor.instances", "4") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Read the CSV file into a DataFrame 
crimes_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)

# Correction of the date format
date_format = "MM/dd/yyyy hh:mm:ss a"  # Adjusted date format

# Extract year and month from the date
crimes_df = crimes_df.withColumn("Year", year(to_date(col("DATE OCC"), date_format))) \
                     .withColumn("Month", month(to_date(col("DATE OCC"), date_format)))

# Register the DataFrame as a temporary view for SQL queries
crimes_df.createOrReplaceTempView("crimes")

# Define window specification for ranking
windowSpec = Window.partitionBy("Year").orderBy(desc("count"))

# DataFrame API implementation
start_time_df = time.time()
df_result = crimes_df.groupBy("Year", "Month") \
    .count() \
    .withColumn("MonthRank", row_number().over(windowSpec)) \
    .filter(col("MonthRank") <= 3) \
    .orderBy("Year", desc("count"))
df_result.show()
end_time_df = time.time()
print("DataFrame API execution time: {} seconds".format(end_time_df - start_time_df))

# SQL API implementation
start_time_sql = time.time()
sql_result = spark.sql("""
    SELECT Year, Month, count, MonthRank
    FROM (
        SELECT Year, Month, count(*) as count, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY count(*) DESC) as MonthRank
        FROM crimes
        GROUP BY Year, Month
    ) as ranked
    WHERE MonthRank <= 3
    ORDER BY Year, count DESC
""")
sql_result.show()
end_time_sql = time.time()
print("SQL API execution time: {} seconds".format(end_time_sql - start_time_sql))

# Stop the Spark session
spark.stop()
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, desc, col, row_number, to_date
from pyspark.sql.window import Window
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Crime Data Analysis") \
    .config("spark.executor.instances", "4") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Read the CSV files into DataFrames
crimes_df_2010_2019 = spark.read.csv("hdfs://okeanos-master:54310/user/user/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df_2020_present = spark.read.csv("hdfs://okeanos-master:54310/user/user/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)  # Replace with actual path

# Union the DataFrames
crimes_df = crimes_df_2010_2019.union(crimes_df_2020_present)

# Correction of the date format
date_format = "MM/dd/yyyy hh:mm:ss a"  # Adjusted date format

# Extract year and month from the date
crimes_df = crimes_df.withColumn("Year", year(to_date(col("DATE OCC"), date_format))) \
                     .withColumn("Month", month(to_date(col("DATE OCC"), date_format)))

# Register the DataFrame as a temporary view for SQL queries
crimes_df.createOrReplaceTempView("crimes")

# Define window specification for ranking
windowSpec = Window.partitionBy("Year").orderBy(desc("count"))

# DataFrame API implementation
start_time_df = time.time()
df_result = crimes_df.groupBy("Year", "Month") \
    .count() \
    .withColumn("MonthRank", row_number().over(windowSpec)) \
    .filter(col("MonthRank") <= 3) \
    .orderBy("Year", desc("count"))
df_result.show(n=42, truncate=False)
end_time_df = time.time()
print("DataFrame API execution time: {} seconds".format(end_time_df - start_time_df))

# SQL API implementation
start_time_sql = time.time()
sql_result = spark.sql("""
    SELECT Year, Month, count, MonthRank
    FROM (
        SELECT Year, Month, count(*) as count, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY count(*) DESC) as MonthRank
        FROM crimes
        GROUP BY Year, Month
    ) as ranked
    WHERE MonthRank <= 3
    ORDER BY Year, count DESC
""")
sql_result.show(n=42, truncate=False)
end_time_sql = time.time()
print("SQL API execution time: {} seconds".format(end_time_sql - start_time_sql))

# Stop the Spark session
spark.stop()
