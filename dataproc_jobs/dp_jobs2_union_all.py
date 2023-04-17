import argparse
from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql.functions import hour, month, year


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


def spark_get(project_id: str):
    """spark session"""
    spark = (
        SparkSession.builder.appName("test")
        .config("spark.sql.broadcastTimeout", "36000")
        .getOrCreate()
    )
    spark.conf.set("temporaryGcsBucket", f"gs://de-project_{project_id}/temp")
    return spark


@task(log_prints=True)
def read_df(input_2020: str, input_2021: str, input_2022: str, spark: SparkSession):
    """reads data from gcs"""
    # 2020
    df_2020 = (
        spark.read.option("header", "true")
        .option("inferschema", "true")
        .schema(schema)
        .csv(input_2020)
    )
    df_2020 = df_2020.select([F.col(col).alias(col.lower()) for col in df_2020.columns])
    #### 2021
    df_2021 = (
        spark.read.option("header", "true")
        .option("inferschema", "true")
        .schema(schema)
        .csv(input_2021)
    )
    df_2021 = df_2021.select([F.col(col).alias(col.lower()) for col in df_2021.columns])
    # 2022
    df_2022 = (
        spark.read.option("header", "true")
        .option("inferschema", "true")
        .schema(schema)
        .csv(input_2022)
    )

    df_2022 = df_2022.select([F.col(col).alias(col.lower()) for col in df_2022.columns])

    print(df_2020.columns, df_2021.columns, df_2022.columns)
    print(
        f"2020count: {df_2020.count()},\
         2021count: {df_2021.count()},\
         df_2022count: {df_2022.count()}"
    )
    return df_2020, df_2021, df_2022


@task(log_prints=True)
def union(df_2020, df_2021, df_2022):
    """combining all data"""
    df = df_2020.unionAll(df_2021)
    df_full = df.unionAll(df_2022)
    return df_full


@task(name="reorder & add column")
def reorder(df_full):
    """reorder columns"""
    df_full = (
        df_full.withColumn("year", year(df_full.date_time))
        .withColumn("month", month(df_full.date_time))
        .withColumn("hour", hour(df_full.date_time))
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
    return df_full


@flow(name="write_gcs", log_prints=True)
def write_gcs(input_2020: str, input_2021: str, input_2022: str):
    """writes data to gcs"""
    spark = spark_get(project_id)
    df_2020, df_2021, df_2022 = read_df(input_2020, input_2021, input_2022, spark)
    df_full = union(df_2020, df_2021, df_2022)
    df_full = reorder(df_full)
    df_full.write.option("header", True).mode("overwrite").parquet(
        f"gs://de-project_{project_id}/pq/pre-processed/"
    )
    print("written")
    return df_full


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_2020", required=True)
    parser.add_argument("--input_2021", required=True)
    parser.add_argument("--input_2022", required=True)
    parser.add_argument("--project_id", required=True)
    args = parser.parse_args()
    input_2020 = args.input_2020
    input_2021 = args.input_2021
    input_2022 = args.input_2022
    project_id = args.project_id
    write_gcs(input_2020, input_2021, input_2022)

# WORKING
