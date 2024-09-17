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
      age = 14
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


  schema = <<EOF
[
  {
    "name": "track_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": ""
  },
  {
    "name": "track_name",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": ""
  },  
  {
    "name": "artists",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": ""
  },  
  {
    "name": "played_at",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": ""
  },
  {
    "name": "duration_ms",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "track_duration",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "spotify_url",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "upload_timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": ""
  },
  {
    "name": "unique_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": ""
  },
  {
    "name": "danceability",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "energy",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "key",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "loudness",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "mode",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "speechiness",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "acousticness",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "instrumentalness",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "liveness",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "time_signature",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "valence",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": ""
  },
    {
    "name": "tempo",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": ""
  }
]
EOF

}