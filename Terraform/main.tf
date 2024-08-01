terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.35.0"
    }
  }
}

provider "google" {
  credentials = file(var.gcp_credentials)
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "bucket-1" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
  storage_class = var.bucket_storage_class

  public_access_prevention = "enforced"


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "dataset-1" {
  dataset_id = var.bq_dataset_name
  location = var.region

  delete_contents_on_destroy = true
}

