terraform {
  required_version = ">= 1.0"
  backend "local" {}  #
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project_id
  region = var.region
  credentials = file(var.credentials)
}



#services
resource "google_project_service" "services" {
  for_each = toset(var.services)
  project                    = var.project_id
  service                    = each.key
  disable_dependent_services = false
  disable_on_destroy         = false
}




# GCS BUCKET

resource "google_storage_bucket" "de_zoomcamp" {
  name          = "${local.de-project}_${var.project_id}"
  location      = var.region
  storage_class = var.storage_class
  uniform_bucket_level_access = true
  force_destroy = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }
  timeouts {
    create = "10m"
  }

}

resource "google_storage_bucket" "de_zoomcamp2" {
  name          = "${local.de-project}_${var.project_id}temp"
  location      = var.region
  storage_class = var.storage_class
  uniform_bucket_level_access = true
  force_destroy = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }
  timeouts {
    create = "10m"
  }

}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset
  project    = var.project_id
  location   = var.region
}

# GCS BUCKET UPLOAD SCRIPT



resource "google_storage_bucket_object" "dataproc_jobs" {
  for_each = var.files
  name     = each.value
  source   = "${path.module}/${each.key}"
  bucket   = "${local.de-project}_${var.project_id}"
  depends_on = [
    google_storage_bucket.de_zoomcamp
  ]
}
#darapoc machine
resource "google_dataproc_cluster" "cluster"{
  name     = "cluster"
  region   = var.region
  graceful_decommission_timeout = "600s"
  labels = {
    foo = "bar"
  }
  timeouts {
    create  = "10m"
  }

  cluster_config {
    staging_bucket = "${local.de-project}_${var.project_id}"
    temp_bucket = "${local.de-project}_${var.project_id}temp"

master_config {
      num_instances = 1
      machine_type  = "n2-standard-8"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 100
      }
    }

    preemptible_worker_config {
      num_instances = 0
    }

    # Override or set some custom properties
    software_config {
      image_version = "2.0.35-debian10"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }
    # You can define multiple initialization_action blocks
    initialization_action {
      script      = "gs://${local.de-project}_${var.project_id}/script/init_script.sh"
      timeout_sec = 500
    }
  }
  depends_on = [
    google_storage_bucket.de_zoomcamp,
    google_storage_bucket.de_zoomcamp2,
    google_storage_bucket_object.dataproc_jobs
  ]
}

# BIGQUERY DATASET

#DATAPROC MACHINE
