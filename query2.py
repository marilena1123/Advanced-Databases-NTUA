from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc
from pyspark.sql.types import StringType
import time

# Initialize Spark session with 4 executors
spark = SparkSession.builder \
    .appName("Crime Data Analysis") \
    .config("spark.executor.instances", "4") \
    .getOrCreate()

# Read the CSV files into DataFrames
crimes_df_2010_2019 = spark.read.csv("hdfs://okeanos-master:54310/user/user/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df_2020_present = spark.read.csv("hdfs://okeanos-master:54310/user/user/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)  # Replace with the actual path

# Union the DataFrames
crimes_df = crimes_df_2010_2019.union(crimes_df_2020_present)

# Function to classify parts of the day
def classify_time(hour):
    if 5 <= hour < 12:
        return 'Morning'
    elif 12 <= hour < 17:
        return 'Afternoon'
    elif 17 <= hour < 21:
        return 'Evening'
    else:
        return 'Night'

# Register UDF
classify_time_udf = udf(classify_time, StringType())

# Add new column for time classification
crimes_df = crimes_df.withColumn("PartOfDay", classify_time_udf(col("TIME OCC")))

# Rename column 
crimes_df = crimes_df.withColumnRenamed("Premis Desc", "Premis_Desc")

# Register the DataFrame as a temporary view for SQL queries
crimes_df.createOrReplaceTempView("crimes")

# SQL implementation
start_time_sql = time.time()
sql_result = spark.sql("""
    SELECT PartOfDay, COUNT(*) as count
    FROM crimes
    WHERE Premis_Desc = 'STREET'
    GROUP BY PartOfDay
    ORDER BY count DESC
""")
sql_result.show()
end_time_sql = time.time()
print("SQL execution time: {} seconds".format(end_time_sql - start_time_sql))

# DataFrame API implementation
start_time_df = time.time()
df_filtered = crimes_df.filter(crimes_df["Premis_Desc"] == "STREET")
df_result = df_filtered.groupBy("PartOfDay").count().orderBy(desc("count"))
df_result.show()
end_time_df = time.time()
print("DataFrame API execution time: {} seconds".format(end_time_df - start_time_df))

# RDD API implementation
crimes_rdd = crimes_df.rdd

start_time_rdd = time.time()
filtered_rdd = crimes_rdd.filter(lambda row: row['Premis_Desc'] == 'STREET')
rdd_result = filtered_rdd \
    .map(lambda row: (classify_time(int(row['TIME OCC'])), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .collect()
end_time_rdd = time.time()
print("RDD API execution time: {} seconds".format(end_time_rdd - start_time_rdd))

for result in rdd_result:
    print(result)

# Stop the Spark session
spark.stop()
