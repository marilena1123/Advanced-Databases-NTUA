'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

#IT IS NOT CLEAR WHETHER WE SHOULD INCLUDE ALL THE COLUMNS OF THE DATA IN THE DATAFRAME OR ONLY THESE WHICH ARE MENTIONED 
#IF ONLY THE ONES THAT ARE MENTIONED, THE FIRST PART OF CODE SHOULD BE USED
spark = SparkSession.builder \
    .appName("UDF question2") \
    .getOrCreate()

#Definition of the schema without DateType for date columns (reading as strings first)
crimes_schema = StructType([
    StructField("Date Rptd", StringType()), 
    StructField("DATE OCC:", StringType()), 
    StructField("Vict Age", IntegerType()),
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType()),
    # Add other fields here if your CSV has more columns
])

# Read the CSV file
crimes_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/Crime_Data_from_2010_to_2019.csv", header=True, schema=crimes_schema)

# date strings to DateType conversion
crimes_df = crimes_df.withColumn("Date Rptd", to_date(col("Date Rptd"), "MM/dd/yyyy")) \
                    .withColumn("DATE OCC:", to_date(col("DATE OCC:"), "MM/dd/yyyy"))

crimes_df.printSchema()

#print the total number of rows
print("Total number of rows:", crimes_df.count())
'''

#IF WE NEED ALL THE COLUMNS, THE SECOND SHOULD BE USED
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, to_date

# Initialize Spark session
spark = SparkSession.builder.appName("Crime Data Analysis").getOrCreate()

# Define the schema for the DataFrame
crimes_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", StringType()),
    StructField("AREA ", StringType()),
    StructField("AREA NAME", StringType()),
    StructField("Rpt Dist No", StringType()),
    StructField("Part 1-2", StringType()),
    StructField("Crm Cd", StringType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", StringType()),
    StructField("Vict Age", IntegerType()),
    StructField("Vict Sex", StringType()),
    StructField("Vict Descent", StringType()),
    StructField("Premis Cd", StringType()),
    StructField("Premis Desc", StringType()),
    StructField("Weapon Used Cd", StringType()),
    StructField("Weapon Desc", StringType()),
    StructField("Status", StringType()),
    StructField("Status Desc", StringType()),
    StructField("Crm Cd 1", StringType()),
    StructField("Crm Cd 2", StringType()),
    StructField("Crm Cd 3", StringType()),
    StructField("Crm Cd 4", StringType()),
    StructField("LOCATION", StringType()),
    StructField("Cross Street", StringType()),
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType())
])

# Read the CSV file into a DataFrame
crimes_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/Crime_Data_from_2010_to_2019.csv", header=True, schema=crimes_schema)

# Convert date strings to DateType
date_format = "MM/dd/yyyy" 
crimes_df = crimes_df.withColumn("Date Rptd", to_date(col("Date Rptd"), date_format)) \
                     .withColumn("DATE OCC", to_date(col("DATE OCC"), date_format))

# Print the schema of the DataFrame
crimes_df.printSchema()

# Print the total number of rows in the DataFrame
print("Total number of rows:", crimes_df.count())
