import argparse
from google.cloud import dataproc_v1 as dp
from prefect import flow, task
from prefect_gcp import GcpCredentials


@task(name="dataproc_jobs1:web_to_gcs", log_prints=True, timeout_seconds=2700)
def dataproc_jobs1(project_id: str, region: str, cluster_name: str, job1: str):
    """dataproc_jobs1 web to gcs"""
    gcp = GcpCredentials.load("gcp-creds")
    job_client = dp.JobControllerClient(
        credentials=gcp.get_credentials_from_service_account(),
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)},
    )
    jobs = {
        "reference": {"job_id": "custom_job_id"},
        "placement": {"cluster_name": cluster_name},
        "reference": {"project_id": project_id},
        "pyspark_job": {
            "main_python_file_uri": job1,
        },
    }
    res = job_client.submit_job_as_operation(project_id=project_id, region=region, job=jobs)
    result = res.result()
    return result


@task(name="dataproc_jobs2:combining", log_prints=True, timeout_seconds=2700)
def dataproc_jobs2(project_id: str, region: str, cluster_name: str, job2: str):
    """dataproc_jobs2 combining all data"""
    gcp = GcpCredentials.load("gcp-creds")
    job_client = dp.JobControllerClient(
        credentials=gcp.get_credentials_from_service_account(),
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)},
    )

    jobs = {
        "reference": {"job_id": "custom_job_id"},
        "placement": {"cluster_name": cluster_name},
        "reference": {"project_id": project_id},
        "pyspark_job": {
            "main_python_file_uri": job2,
            "args": {
                f"--input_2020=gs://de-project_{project_id}/raw/2020/*/",
                f"--input_2021=gs://de-project_{project_id}/raw/2021/*/",
                f"--input_2022=gs://de-project_{project_id}/raw/2022/*/",
                f"--project_id={project_id}",
            },
        },
    }
    res = job_client.submit_job_as_operation(project_id=project_id, region=region, job=jobs)
    result = res.result()
    return result


@task(name="dataproc_jobs3:process", log_prints=True, timeout_seconds=2700)
def dataproc_jobs3(project_id: str, region: str, cluster_name: str, job3: str):
    """dataproc_jobs3 processes data"""
    gcp = GcpCredentials.load("gcp-creds")
    job_client = dp.JobControllerClient(
        credentials=gcp.get_credentials_from_service_account(),
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)},
    )
    jobs = {
        "reference": {"job_id": "custom_job_id"},
        "placement": {"cluster_name": cluster_name},
        "reference": {"project_id": project_id},
        "pyspark_job": {
            "main_python_file_uri": job3,
            "args": {
                f"--input_pq=gs://de-project_{project_id}/pq/pre-processed/",
                f"--project_id={project_id}",
            },
        },
    }
    res = job_client.submit_job_as_operation(project_id=project_id, region=region, job=jobs)
    result = res.result()
    return result


@task(name="dataproc_job4:gcs_to_bigquery", log_prints=True, timeout_seconds=2700)
def dataproc_jobs4(project_id: str, region: str, cluster_name: str, job4: str, jar_file):
    """dataproc_jobs4 gcs to bigquery"""
    gcp = GcpCredentials.load("gcp-creds")
    job_client = dp.JobControllerClient(
        credentials=gcp.get_credentials_from_service_account(),
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)},
    )
    jobs = {
        "reference": {"job_id": "custom_job_id"},
        "placement": {"cluster_name": cluster_name},
        "reference": {"project_id": project_id},
        "pyspark_job": {
            "main_python_file_uri": job4,
            "args": {
                f"--input_full=gs://de-project_{project_id}/pq/processed/full/*/",
                f"--input_location=gs://de-project_{project_id}/pq/processed/location/",
                f"--project_id={project_id}",
            },
            "jar_file_uris": jar_file,
        },
    }
    res = job_client.submit_job_as_operation(project_id=project_id, region=region, job=jobs)
    result = res.result()
    return result


@flow(name="main_flow", log_prints=True, timeout_seconds=2700)
def main():
    """main prefect flow"""
    result = dataproc_jobs1(project_id, region, cluster_name, job1)
    print(result)
    result = dataproc_jobs2(project_id, region, cluster_name, job2)
    print(result)
    result = dataproc_jobs3(project_id, region, cluster_name, job3)
    print(result)
    result = dataproc_jobs4(project_id, region, cluster_name, job4, jar_file)
    print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--region", required=True)
    args = parser.parse_args()
    project_id = args.project_id
    region = args.region
    job1 = f"gs://de-project_{project_id}/dataproc_jobs/dp_jobs1_web_to_gcs.py"
    job2 = f"gs://de-project_{project_id}/dataproc_jobs/dp_jobs2_union_all.py"
    job3 = f"gs://de-project_{project_id}/dataproc_jobs/dp_jobs3_process.py"
    job4 = f"gs://de-project_{project_id}/dataproc_jobs/dp_jobs4_gcs_to_bigquery.py"
    cluster_name = "cluster"
    jar_file = ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
    main()
