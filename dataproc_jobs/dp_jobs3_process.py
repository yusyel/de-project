import argparse
import os
from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types
import googlemaps


KEY = os.getenv("API")


@task(name="spark_session")
def spark_get(project_id: str):
    """spark session"""
    spark = (
        SparkSession.builder.appName("test")
        .config("spark.sql.broadcastTimeout", "36000")
        .getOrCreate()
    )
    spark.conf.set("temporaryGcsBucket", f"gs://de-project_{project_id}/temp")
    return spark


@task(name="read", log_prints=True)
def read(input_pq: str, spark: SparkSession):
    """reads parquet files from gcs"""
    df_full = spark.read.option("header", "true").option("inferSchema", "true").parquet(input_pq)
    print("df_full obs count:", df_full.count())
    print("df_full obs columns:", df_full.columns)
    df_location = df_full.groupBy("geohash", "latitude", "longitude").count()
    print("df_location count:", df_location.count())
    print("df_location cloums:", df_location.columns)

    return df_full, df_location


@task(name="join", log_prints=True)
def join(df_full, df_location):
    """joins locations"""
    df_full = (
        df_full.join(df_location, df_full.geohash == df_location.geohash, "inner")
        .drop(df_location.geohash)
        .drop(df_location.latitude)
        .drop(df_location.longitude)
    )

    print(df_full.show())
    print(df_full.count())
    print(df_full.columns)
    print("test")
    return df_full, df_location


@task(name="write_gcs", log_prints=True)
def write(df_full, df_location):
    """writes datas to gcs"""
    print("test1")
    df_full = df_full.write.mode("overwrite").parquet(
        f"gs://de-project_{project_id}/pq/processed/full/"
    )
    print("test2")
    df_location = df_location.write.mode("overwrite").parquet(
        f"gs://de-project_{project_id}/pq/processed/location/"
    )

    return df_full, df_location


@flow(name="dataproc_jobs3", log_prints=True)
def main_flow(input_pq: str):
    """main flow"""
    spark = spark_get(project_id)
    df_full, df_location = read(input_pq, spark)

    def get_location(latitude: int, longitude: int):
        """gets full address from google maps api"""
        gmaps = googlemaps.Client(key=KEY)
        result = gmaps.reverse_geocode((latitude, longitude))
        address = result[0]['formatted_address']
        return address

    print(get_location(41.1135864257812, 28.8446044921875))
    get_func = F.udf(get_location, returnType=types.StringType())
    print(df_full.count(), df_location.count())
    df_location = df_location.withColumn(
        "location", get_func(df_location.latitude, df_location.longitude)
    )
    df_full, df_location = join(df_full, df_location)
    df_full, df_location = write(df_full, df_location)
    return df_full, df_location


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_pq", required=True)
    parser.add_argument("--project_id", required=True)
    args = parser.parse_args()
    input_pq = args.input_pq
    project_id = args.project_id
    main_flow(input_pq)
