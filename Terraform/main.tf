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
  project     = var.project_id
  region      = var.region
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

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }

}


resource "google_bigquery_dataset" "dataset-1" {
  dataset_id = var.bq_dataset_name
  location   = var.region

  delete_contents_on_destroy = true
}


resource "google_bigquery_table" "default" {
  dataset_id = var.bq_dataset_name
  table_id   = var.bq_table_name

  time_partitioning {
    type = "DAY"
  }

#   schema = <<EOF
# [
#   {
#     "name": "permalink",
#     "type": "STRING",
#     "mode": "NULLABLE",
#     "description": "The Permalink"
#   },
#   {
#     "name": "state",
#     "type": "STRING",
#     "mode": "NULLABLE",
#     "description": "State where the head office is located"
#   }
# ]
# EOF

}