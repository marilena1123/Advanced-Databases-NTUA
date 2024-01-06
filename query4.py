from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, udf, avg, count, to_date
from pyspark.sql.types import DoubleType

# Define a UDF to calculate distance
def get_distance(lat1, long1, lat2, long2):
    return ((lat1 - lat2)**2 + (long1 - long2)**2)**0.5

get_distance_udf = udf(get_distance, DoubleType())

#Create spark session
spark = SparkSession.builder.appName("Crime Analysis").getOrCreate()

#Read data from files
crimes_df_2010_2019 = spark.read.csv("hdfs://okeanos-master:54310/user/user/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
crimes_df_2020_present = spark.read.csv("hdfs://okeanos-master:54310/user/user/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
stations_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/LAPD_Police_Stations.csv", header=True, inferSchema=True)

#First edits of the data
stations_df = stations_df.withColumnRenamed("PREC", "AREA").withColumn("AREA", col("AREA").cast("integer"))
stations_df = stations_df.withColumnRenamed("X", "Station_LON").withColumnRenamed("Y", "Station_LAT")

date_format = "MM/dd/yyyy hh:mm:ss a"
crimes_df = crimes_df_2010_2019.union(crimes_df_2020_present)
crimes_df = crimes_df.withColumnRenamed("AREA ", "AREA").withColumn("AREA", col("AREA").cast("integer"))
crimes_df = crimes_df.withColumn("Year", year(to_date(col("DATE OCC"), date_format)))

firearm_crimes = crimes_df.filter((crimes_df["Weapon Used Cd"].startswith("1")) & (crimes_df["LAT"] != 0) & (crimes_df["LON"] != 0))

joined_df = firearm_crimes.join(stations_df, "AREA")
joined_df = joined_df.withColumn("Distance", get_distance_udf(col("LAT"), col("LON"), col("Station_LAT"), col("Station_LON")))

crimes_and_precincts_df=firearm_crimes.crossJoin(stations_df).withColumn("Distance", get_distance_udf(col("LAT"), col("LON"), col("Station_LAT"), col("Station_LON")))

# DataFrame API Analysis
yearly_stats_df = joined_df.groupBy("Year").agg(
    avg("Distance").alias("Average Distance"),
    count("DR_NO").alias("Number of Crimes")
).orderBy("Year")

dept_stats_df = joined_df.groupBy("DIVISION").agg(
    avg("Distance").alias("Average Distance"),
    count("DR_NO").alias("Number of Crimes")
).orderBy(col("Number of Crimes").desc())

# SQL API Analysis
joined_df.createOrReplaceTempView("firearm_crimes_view")
crimes_and_precincts_df.createOrReplaceTempView("closer_view")

yearly_stats_sql = spark.sql("""
    SELECT Year , AVG(Distance) AS `Average Distance`, COUNT(DR_NO) AS `Number of Crimes`
    FROM firearm_crimes_view
    GROUP BY Year
    ORDER BY Year
""")

dept_stats_sql = spark.sql("""
    SELECT DIVISION, AVG(Distance) AS `Average Distance`, COUNT(DR_NO) AS `Number of Crimes`
    FROM firearm_crimes_view
    GROUP BY DIVISION
    ORDER BY `Number of Crimes` DESC
""")


yearly_stats_closer_sql = spark.sql("""
        SELECT Year, AVG(Distance) AS `Average Distance`, COUNT(DISTINCT DR_NO) AS `Number of Crimes`
        FROM (
        SELECT DR_NO , Year, MIN(Distance) AS `Distance`
        FROM closer_view
        GROUP BY DR_NO,Year
        )
        GROUP BY Year
        ORDER BY Year
""")

dept_stats_closer_sql = spark.sql("""
    SELECT DIVISION AS `DIVISION`, AVG(Distancea1) AS `Average Distance`, COUNT(DR_NOa1) AS `Number of Crimes`
    FROM (
        SELECT a1.DIVISION AS DIVISION, a1.Distance AS Distancea1 , a2.Distance, a1.DR_NO AS DR_NOa1, a2.DR_NO FROM
        (SELECT DR_NO , MIN(Distance) AS `Distance`
        FROM closer_view 
        GROUP BY DR_NO) AS `a2`
        INNER JOIN closer_view AS `a1` 
        WHERE (a1.DR_NO=a2.DR_NO AND a1.Distance=a2.Distance)
        )
    GROUP BY DIVISION
    ORDER BY `Number of Crimes` DESC
""")

print("DataFrame API - Year-wise Analysis:")
yearly_stats_df.show()

print("DataFrame API - Police Department-wise Analysis:")
dept_stats_df.show(n=21, truncate=False)

print("SQL API - Year-wise Analysis:")
yearly_stats_sql.show()

print("SQL API - Police Department-wise Analysis:")
dept_stats_sql.show(n=21, truncate=False)

print("SQL API - Year-wise Closer Distance Analysis:")
yearly_stats_closer_sql.show()

print("SQL API - Police Department-wise Closer Distance Analysis:")
dept_stats_closer_sql.show(n=21, truncate=False)

spark.stop()
