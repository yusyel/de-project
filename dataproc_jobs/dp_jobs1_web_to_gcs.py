from itertools import product
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(name="fetch", retries=3)
def fetch(data_url: str) -> pd.DataFrame:
    """fetchs data from web"""
    df = pd.read_csv(data_url)
    return df


@task(name="check", log_prints=True)
def check(df: pd.DataFrame) -> pd.DataFrame:
    """Checks null data"""
    print(df.isnull().sum())
    print(f"rows: {len(df)}")
    return df


@task(name="write_local", log_prints=True)
def write_local(df: pd.DataFrame, year: str, file_name: str) -> Path:
    """writes data local"""
    Path(f"./raw/{year}").mkdir(parents=True, exist_ok=True)
    path = Path(f"./raw/{year}/{file_name}.csv")
    df.to_csv(path, index=False)
    print(f"path is: {path}")
    return path


@task(name="write_gcs", log_prints=True)
def write_gcs(path: Path) -> None:
    """uploads data to gcs"""
    gcs_block = GcsBucket.load("google")
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=600)


@flow(name="dataproc_jobs1", log_prints=True)
def etl_to_gcs(year: int, month: int) -> None:
    """extract data from web"""
    file_name = f"traffic_density_{year}{month:02}"
    data_url = f"https://github.com/yusyel/de-project/releases/download/{year}/{file_name}.csv"

    df = fetch(data_url)
    df_clean = check(df)
    path = write_local(df_clean, year, file_name)
    write_gcs(path)
    print(f"{year}, {month},{file_name}, is saved GCS:{path} location")


@flow(name="dataproc_jobs1:parent_flow", log_prints=True)
def etl_parent_flow(years, months):
    """main flow iterates year and month"""
    for year, month in list(product(years, months)):
        print(year, month)
        etl_to_gcs(year, month)


if __name__ == "__main__":
    years = [2020, 2021, 2022]
    months = range(1, 13)
    etl_parent_flow(years, months)
