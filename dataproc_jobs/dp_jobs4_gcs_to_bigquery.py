import argparse
from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, hour, avg, col


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


@task(name="read_pq", log_prints=True, retries=2)
def read_file(input_full: str, input_location: str, spark: SparkSession):
    """read files from gcs"""
    df_full = spark.read.option("header", "true").option("inferSchema", "true").parquet(input_full)
    print("df_full obs count:", df_full.count())
    print("df_full columns:", df_full.columns)

    df_location = (
        spark.read.option("header", "true").option("inferSchema", "true").parquet(input_location)
    )
    print("df_location obs count:", df_location.count())
    print("df_locations columns:", df_location.columns)

    return df_full, df_location


@task(name="transform", log_prints=True)
def transform(df_full, df_location):
    """transforms data"""

    df_avg = (
        df_full.withColumn("year", year(df_full.date_time))
        .withColumn("month", month(df_full.date_time))
        .withColumn("hour", hour(df_full.date_time))
        .groupBy("coordinates", "hour", "month", "year")
        .agg(
            avg("number_of_vehicles").alias("avg_number_of_vehicles"),
            avg("minimum_speed").alias("avg_minimum_speed_km_h"),
            avg("maximum_speed").alias("avg_maximum_speed_km_h"),
            avg("average_speed").alias("avg_average_speed_km_h"),
        )
    )
    print(df_avg.count())
    list1 = (
        df_full.groupBy("geohash")
        .agg(
            avg("number_of_vehicles").alias("avg_number_of_vehicles"),
            avg("minimum_speed").alias("avg_minimum_speed"),
            avg("maximum_speed").alias("avg_maximum_speed"),
            avg("average_speed").alias("avg_average_speed"),
        )
        .orderBy(col("avg_number_of_vehicles").desc())
        .limit(100)
        .select("geohash")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    list2 = (
        df_full.groupBy("geohash")
        .agg(
            avg("number_of_vehicles").alias("avg_number_of_vehicles"),
            avg("minimum_speed").alias("avg_minimum_speed"),
            avg("maximum_speed").alias("avg_maximum_speed"),
            avg("average_speed").alias("avg_average_speed"),
        )
        .orderBy(col("avg_number_of_vehicles").asc())
        .limit(100)
        .select("geohash")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    df_most = (
        df_full.groupBy("geohash", "location", "hour")
        .agg(
            avg("number_of_vehicles").alias("avg_number_of_vehicles"),
            avg("minimum_speed").alias("avg_minimum_speed"),
            avg("maximum_speed").alias("avg_maximum_speed"),
            avg("average_speed").alias("avg_average_speed"),
        )
        .filter(df_full.geohash.isin(list1))
    )
    print(df_most.count())
    df_less = (
        df_full.groupBy("geohash", "location", "hour")
        .agg(
            avg("number_of_vehicles").alias("avg_number_of_vehicles"),
            avg("minimum_speed").alias("avg_minimum_speed"),
            avg("maximum_speed").alias("avg_maximum_speed"),
            avg("average_speed").alias("avg_average_speed"),
        )
        .filter(df_full.geohash.isin(list2))
    )
    print(df_less.count())

    df_district = df_full.groupBy("district", "year").agg(
        avg("number_of_vehicles").alias("avg_number_of_vehicles"),
        avg("minimum_speed").alias("avg_minimum_speed_km_h"),
        avg("maximum_speed").alias("avg_maximum_speed_km_h"),
        avg("average_speed").alias("avg_average_speed_km_h"),
    )
    print("df_district:", df_district.count())
    return df_full, df_location, df_district, df_avg, df_most, df_less


@task(name="write_to_bigquery")
def write(df_full, df_location, df_district, df_avg, df_most, df_less):
    """write to bigquery"""

    df_full.write.format("bigquery").option("partitionType", "MONTH").option(
        "partitionField", "date_time"
    ).mode("overwrite").option("table", "dataset.reports-full").option(
        "temporaryGcsBucket", f"de-project_{project_id}temp/big"
    ).save()

    df_district.write.format("bigquery").mode("overwrite").option(
        "table", "dataset.reports-district"
    ).option("temporaryGcsBucket", f"de-project_{project_id}temp/big").save()

    df_location.write.format("bigquery").option("table", "dataset.reports-location").mode(
        "overwrite"
    ).option("temporaryGcsBucket", f"de-project_{project_id}temp").save()

    df_avg.write.format("bigquery").mode("overwrite").option("table", "dataset.reports-avg").option(
        "temporaryGcsBucket", f"de-project_{project_id}temp/big"
    ).save()

    df_most.write.format("bigquery").mode("overwrite").option(
        "table", "dataset.reports-less"
    ).option("temporaryGcsBucket", f"de-project_{project_id}temp/big").save()

    df_less.write.format("bigquery").mode("overwrite").option(
        "table", "dataset.reports-less"
    ).option("temporaryGcsBucket", f"de-project_{project_id}temp/big").save()


@flow(name="dataproc_jobs4", log_prints=True)
def main(input_full: str, input_location: str):
    """writes dataframes to bigquery"""
    spark = spark_get(project_id)
    df_full, df_location = read_file(input_full, input_location, spark)
    df_full, df_location, df_district, df_avg, df_most, df_less = transform(df_full, df_location)
    write(df_full, df_location, df_district, df_avg, df_most, df_less)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_full", required=True)
    parser.add_argument("--input_location", required=True)
    parser.add_argument("--project_id", required=True)
    args = parser.parse_args()
    input_full = args.input_full
    input_location = args.input_location
    project_id = args.project_id
    main(input_full, input_location)
