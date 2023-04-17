locals {
  de-project = "de-project"
}

variable "project_id" {}
variable "region"{}
variable "credentials" {}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "bq_dataset" {
  description = "dataset for bigquery"
  type =string
  default="dataset"

}

variable "services" {
  description ="The list of apis necessary for the project"
  type = list(string)
  default = [
    "cloudresourcemanager.googleapis.com",
    "serviceusage.googleapis.com",
    "dataproc.googleapis.com",
    "bigquery.googleapis.com",
    "iam.googleapis.com",
    "geocoding-backend.googleapis.com",
    "storage-api.googleapis.com",
  ]
}

variable "files" {
  type = map(string)
  default = {
    # sourcefile = destfile
    "./dataproc_jobs/dp_jobs1_web_to_gcs.py" = "dataproc_jobs/dp_jobs1_web_to_gcs.py"
    "./dataproc_jobs/dp_jobs2_union_all.py" = "dataproc_jobs/dp_jobs2_union_all.py"
    "./dataproc_jobs/dp_jobs3_process.py" = "dataproc_jobs/dp_jobs3_process.py"
    "./dataproc_jobs/dp_jobs4_gcs_to_bigquery.py" = "dataproc_jobs/dp_jobs4_gcs_to_bigquery.py"
    "./init_script.sh" = "script/init_script.sh"
  }
}
