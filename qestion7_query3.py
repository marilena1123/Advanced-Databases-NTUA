from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, regexp_replace, upper, desc, to_date, lit, broadcast
from pyspark.sql.types import IntegerType
import time

def main(spark_executors):
    spark = SparkSession.builder.appName("Crime Data Analysis").config("spark.executor.instances", spark_executors).config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()

    # Load and Process datasets
    crimes_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
    income_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/LA_income_2015.csv", header=True, inferSchema=True)
    revgeo_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/revgecoding.csv", header=True, inferSchema=True)

    income_df = income_df.withColumn("Median Income", regexp_replace(col("Estimated Median Income"), "[$,]", "").cast(IntegerType()))
    income_df = income_df.withColumnRenamed("Zip Code", "ZIP Code")

    date_format = "MM/dd/yyyy hh:mm:ss a"
    crimes_2015 = crimes_df.filter((year(to_date(col("DATE OCC"), date_format)) == lit(2015)) & col("Vict Descent").isNotNull())

    crimes_with_zip = crimes_2015.join(revgeo_df, (crimes_2015["LAT"] == revgeo_df["LAT"]) & (crimes_2015["LON"] == revgeo_df["LON"]))
    crimes_with_zip = crimes_with_zip.withColumn("ZIPcode", col("ZIPcode").cast("integer"))

    crimes_2015_with_income = crimes_with_zip.join(income_df, crimes_with_zip["ZIPcode"] == income_df["ZIP Code"])

    top_zip_codes = income_df.orderBy(desc("Median Income")).limit(3).select("ZIP Code")
    bottom_zip_codes = income_df.orderBy("Median Income").limit(3).select("ZIP Code")
    top_bottom_zip_codes = top_zip_codes.union(bottom_zip_codes)

    relevant_crimes = crimes_2015_with_income.join(top_bottom_zip_codes, ["ZIP Code"])

    # Experiment with different join strategies
    # BROADCAST join strategy
    print("BROADCAST Join Strategy:")
    crimes_2015_with_income_broadcast = crimes_with_zip.join(broadcast(income_df), crimes_with_zip["ZIPcode"] == income_df["ZIP Code"])
    crimes_2015_with_income_broadcast.explain()
    
    #MERGE join strategy
    print("SORT MERGE Join Strategy:")
    crimes_with_income_sort_merge = crimes_with_zip.hint("merge").join(income_df, crimes_with_zip["ZIPcode"] == income_df["ZIP Code"])
    crimes_with_income_sort_merge.explain()

    # SHUFFLE_HASH join strategy
    print("SHUFFLE_HASH Join Strategy:")
    crimes_2015_with_income_shuffle_hash = crimes_with_zip.hint("SHUFFLE_HASH").join(income_df, crimes_with_zip["ZIPcode"] == income_df["ZIP Code"])
    crimes_2015_with_income_shuffle_hash.explain()

    # SHUFFLE_REPLICATE_NL join strategy
    print("SHUFFLE_REPLICATE_NL Join Strategy:")
    crimes_2015_with_income_shuffle_replicate_nl = crimes_with_zip.hint("SHUFFLE_REPLICATE_NL").join(income_df, crimes_with_zip["ZIPcode"] == income_df["ZIP Code"])
    crimes_2015_with_income_shuffle_replicate_nl.explain()

    # DataFrame API Analysis
    start_time_df = time.time()
    print("DataFrame API Analysis Result:")
    df_result = relevant_crimes.groupBy(upper(col("Vict Descent")).alias("Vict Descent")).count().orderBy(desc("count"))
    df_result.show()
    end_time_df = time.time()
    print("DataFrame API execution time with {} spark executors : {} seconds".format(spark_executors, (end_time_df - start_time_df)))

    # SQL API Analysis
    start_time_sql = time.time()
    relevant_crimes.createOrReplaceTempView("relevant_crimes")
    print("SQL API Analysis Result:")
    sql_query = """
    SELECT UPPER(`Vict Descent`) AS `Vict Descent`, COUNT(*) as count
    FROM relevant_crimes
    GROUP BY UPPER(`Vict Descent`)
    ORDER BY count DESC
    """
    sql_result = spark.sql(sql_query)
    sql_result.show()
    end_time_sql = time.time()
    print("SQL API execution time with {} spark executors : {} seconds".format(spark_executors, (end_time_sql - start_time_sql)))

    spark.stop()

if __name__ == "__main__":
    main("4")
    main("3")
    main("2")

