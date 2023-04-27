import argparse
from itertools import product
from prefect import task, flow
from pyspark.sql import types
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import hour, month, year
from pyspark import SparkFiles

schema = types.StructType(
    [
        types.StructField("DATE_TIME", types.TimestampType(), True),
        types.StructField("LONGITUDE", types.StringType(), True),
        types.StructField("LATITUDE", types.StringType(), True),
        types.StructField("GEOHASH", types.StringType(), True),
        types.StructField("MINIMUM_SPEED", types.IntegerType(), True),
        types.StructField("MAXIMUM_SPEED", types.IntegerType(), True),
        types.StructField("AVERAGE_SPEED", types.IntegerType(), True),
        types.StructField("NUMBER_OF_VEHICLES", types.IntegerType(), True),
    ]
)


@task(name="spark_session", retries=3)
def spark_session() -> SparkSession:
    """spark session"""
    spark = (
        SparkSession.builder.appName("test")
        .config("spark.sql.broadcastTimeout", "36000")
        .master("local[*]")
        .getOrCreate()
    )
    spark.conf.set("temporaryGcsBucket", f"gs://de-project_{project_id}temp")
    return spark


@task(name="read")
def read_from_web(spark, data_url, file_name):
    """read csv file from web"""
    spark.sparkContext.addFile(data_url)
    spark_df = (
        spark.read.option("sep", ",")
        .csv("file://" + SparkFiles.get(f"{file_name}.csv"), schema=schema)
        .cache()
    )
    spark_df = spark_df.select([F.col(col).alias(col.lower()) for col in spark_df.columns])
    spark_df = (
        spark_df.withColumn("year", year(spark_df.date_time))
        .withColumn("month", month(spark_df.date_time))
        .withColumn("hour", hour(spark_df.date_time))
        .select(
            "date_time",
            "year",
            "month",
            "hour",
            "latitude",
            "longitude",
            "geohash",
            "minimum_speed",
            "maximum_speed",
            "average_speed",
            "number_of_vehicles",
        )
    )

    return spark_df


@task(name="write_gcs", log_prints=True)
def write_to_gcs(spark_df, project_id):
    """write spark dataframe to gcs bucket with append mode"""
    spark_df.write.option("header", True).mode("append").parquet(
        f"gs://de-project_{project_id}/pq/pre-processed/"
    )


@flow(name="dataproc_jobs1", log_prints=True)
def etl_to_gcs(year: int, month: int) -> None:
    """extract data from web"""
    file_name = f"traffic_density_{year}{month:02}"
    data_url = f"https://github.com/yusyel/de-project/releases/download/{year}/{file_name}.csv"
    spark = spark_session()
    spark_df = read_from_web(spark, data_url, file_name)
    write_to_gcs(spark_df, project_id)
    print(f"{year}, {month},{file_name}, is saved")


@flow(name="dataproc_jobs1:parent_flow", log_prints=True)
def etl_parent_flow(years, months):
    """main flow iterates year and month"""
    for year, month in list(product(years, months)):
        print(year, month)
        etl_to_gcs(year, month)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    args = parser.parse_args()
    project_id = args.project_id
    years = [2020, 2021, 2022]
    months = range(1, 13)
    etl_parent_flow(years, months)
