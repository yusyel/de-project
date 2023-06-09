import argparse
from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


@task(name="spark_session")
def spark_get(project_id: str):
    """spark session"""
    spark = (
        SparkSession.builder.appName("test")
        .config("spark.sql.broadcastTimeout", "36000")
        .getOrCreate()
    )
    spark.conf.set("temporaryGcsBucket", f"gs://de-project_{project_id}")
    return spark


@task(name="read_pq", log_prints=True, retries=2)
def read_file(input_full: str, spark: SparkSession):
    """read files from gcs"""
    df_full = spark.read.option("header", "true").option("inferSchema", "true").parquet(input_full)
    print("df_full obs count:", df_full.count())
    print("df_full columns:", df_full.columns)

    return df_full


@task(name="transform", log_prints=True)
def transform(df_full):
    """transforms data"""

    df_avg = df_full.groupBy("coordinates", "district", "location", "hour", "month", "year").agg(
        avg("number_of_vehicles").alias("avg_number_of_vehicles"),
        avg("minimum_speed").alias("avg_minimum_speed_km_h"),
        avg("maximum_speed").alias("avg_maximum_speed_km_h"),
        avg("average_speed").alias("avg_average_speed_km_h"),
    )
    print("df_avg count", df_avg.count())

    df_district = df_full.groupBy("district", "month", "year").agg(
        avg("number_of_vehicles").alias("avg_number_of_vehicles"),
        avg("minimum_speed").alias("avg_minimum_speed_km_h"),
        avg("maximum_speed").alias("avg_maximum_speed_km_h"),
        avg("average_speed").alias("avg_average_speed_km_h"),
    )
    print("df_district:", df_district.count())

    df_location = df_full.groupBy("location").count()
    print("df_location count:", df_location.count())

    df_overall = df_full.groupBy("year", "month").agg(
        avg("number_of_vehicles").alias("avg_number_of_vehicles"),
        avg("minimum_speed").alias("avg_minimum_speed_km_h"),
        avg("maximum_speed").alias("avg_maximum_speed_km_h"),
        avg("average_speed").alias("avg_average_speed_km_h"),
    )
    print("df_overall count:", df_overall.count())

    return df_full, df_district, df_avg, df_location, df_overall


@task(name="write_to_bigquery")
def write(df_full, df_district, df_avg, df_location, df_overall):
    """write to bigquery"""

    df_full.write.format("bigquery").option("partitionType", "MONTH").option(
        "partitionField", "date_time"
    ).mode("overwrite").option("table", "dataset.reports-full").option(
        "temporaryGcsBucket", f"de-project_{project_id}temp/big"
    ).save()

    df_district.write.format("bigquery").mode("overwrite").option(
        "table", "dataset.reports-district"
    ).option("temporaryGcsBucket", f"de-project_{project_id}temp/big").save()

    df_avg.write.format("bigquery").mode("overwrite").option("table", "dataset.reports-avg").option(
        "temporaryGcsBucket", f"de-project_{project_id}temp/big"
    ).save()

    df_location.write.format("bigquery").mode("overwrite").option(
        "table", "dataset.reports-location"
    ).option("temporaryGcsBucket", f"de-project_{project_id}temp/big").save()

    df_overall.write.format("bigquery").mode("overwrite").option(
        "table", "dataset.reports-overall"
    ).option("temporaryGcsBucket", f"de-project_{project_id}temp/big").save()


@flow(name="dataproc_jobs3", log_prints=True)
def main(input_full: str):
    """writes dataframes to bigquery"""
    spark = spark_get(project_id)
    df_full = read_file(input_full, spark)
    df_full, df_district, df_avg, df_location, df_overall = transform(df_full)
    write(df_full, df_district, df_avg, df_location, df_overall)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_full", required=True)
    parser.add_argument("--project_id", required=True)
    args = parser.parse_args()
    input_full = args.input_full
    project_id = args.project_id
    main(input_full)
