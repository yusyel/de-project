import argparse
import os
from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import avg
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
    df_location = df_full.groupBy("geohash").agg(
        avg("latitude").alias("latitude"), avg("longitude").alias("longitude")
    )
    df_location = (
        df_location.withColumn(
            "coordinates", F.concat(F.col("latitude"), F.lit(","), F.col("longitude"))
        )
        .drop("latitude")
        .drop("longitude")
    )
    print("df_location count:", df_location.count())
    print("df_location columns:", df_location.columns)
    print(df_location.dtypes)

    return df_full, df_location


@task(name="get_locations")
def location(df_location):
    """get location"""

    def get_location(coordinates: str):
        """gets full address from google maps api"""
        gmaps = googlemaps.Client(key=KEY)
        result = gmaps.reverse_geocode((coordinates))
        address = result[0]['formatted_address']
        return address

    # test
    x = "41.1135864257812, 28.8446044921875"
    print(get_location(x))
    get_func = F.udf(get_location, returnType=types.StringType())
    print(df_location.coordinates)
    df_location = df_location.na.drop("any").withColumn(
        "location", get_func(df_location.coordinates)
    )
    return df_location


@task(name="district", log_prints=True)
def district(df_location, spark: SparkSession):
    """get district/district list"""
    list = [
        "Adalar/İstanbul, Türkiye",
        "Arnavutköy/İstanbul, Türkiye",
        "Ataşehir/İstanbul, Türkiye",
        "Avcılar/İstanbul, Türkiye",
        "Bağcılar/İstanbul, Türkiye",
        "Bahçelievler/İstanbul, Türkiye",
        "Bakırköy/İstanbul, Türkiye",
        "Başakşehir/İstanbul, Türkiye",
        "Bayrampaşa/İstanbul, Türkiye",
        "Beşiktaş/İstanbul, Türkiye",
        "Beykoz/İstanbul, Türkiye",
        "Beylikdüzü/İstanbul, Türkiye",
        "Beyoğlu/İstanbul, Türkiye",
        "Büyükçekmece/İstanbul, Türkiye",
        "Çatalca/İstanbul, Türkiye",
        "Çekmeköy/İstanbul, Türkiye",
        "Esenler/İstanbul, Türkiye",
        "Esenyurt/İstanbul, Türkiye",
        "Eyüp/İstanbul, Türkiye",
        "Fatih/İstanbul, Türkiye",
        "Gaziosmanpaşa/İstanbul, Türkiye",
        "Güngören/İstanbul, Türkiye",
        "Kadıköy/İstanbul, Türkiye",
        "Kâğıthane/İstanbul, Türkiye",
        "Kartal/İstanbul, Türkiye",
        "Küçükçekmece/İstanbul, Türkiye",
        "Maltepe/İstanbul, Türkiye",
        "Pendik/İstanbul, Türkiye",
        "Sancaktepe/İstanbul, Türkiye",
        "Sarıyer/İstanbul, Türkiye",
        "Silivri/İstanbul, Türkiye",
        "Sultanbeyli/İstanbul, Türkiye",
        "Sultangazi/İstanbul, Türkiye",
        "Şile/İstanbul, Türkiye",
        "Şişli/İstanbul, Türkiye",
        "Tuzla/İstanbul, Türkiye",
        "Ümraniye/İstanbul, Türkiye",
        "Üsküdar/İstanbul, Türkiye",
        "Zeytinburnu/İstanbul, Türkiye",
    ]
    list = spark.createDataFrame(list, types.StringType())
    district = list.withColumn("district", list.value).drop(list.value)
    df_location = district.join(
        df_location, df_location.location.contains(district.district), "inner"
    )
    return df_location


@task(name="join", log_prints=True)
def join(df_full, df_location):
    """joins locations"""
    df_full = (
        df_full.join(df_location, df_full.geohash == df_location.geohash, "inner")
        .drop(df_location.geohash)
        .drop(df_full.latitude)
        .drop(df_full.longitude)
    )
    print("test")
    return df_full, df_location


@task(name="write_gcs", log_prints=True)
def write(df_full, df_location):
    """write data to gcs"""
    print("test1")
    df_full = df_full.write.mode("overwrite").parquet(
        f"gs://de-project_{project_id}/pq/processed/full/"
    )
    print("test2")
    df_location = df_location.write.mode("overwrite").parquet(
        f"gs://de-project_{project_id}/pq/processed/location/"
    )

    return df_full, df_location


@flow(name="dataproc_jobs2", log_prints=True)
def main_flow(input_pq: str):
    """main flow"""
    spark = spark_get(project_id)
    df_full, df_location = read(input_pq, spark)
    df_location = location(df_location)
    df_location = district(df_location, spark)
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
