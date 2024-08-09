variable "gcp_credentials" {
  description = "my gcp credentials key"
  default     = "~/.gc/my-creds.json"
}

variable "region" {
  description = "Location for resource"
  default     = "us-west1"
}

variable "project_id" {
  description = "gcp project id"
  default     = "famous-muse-426921-s5"
}

variable "bucket_name" {
  description = "gcp storage bucket ID"
  default     = "spotify_cchow_bucket"
}

variable "bucket_storage_class" {
  description = "gcp storage bucket storage class"
  default     = "STANDARD"
}

variable "bq_dataset_name" {
  description = "gcp dataset ID"
  default     = "spotify_cchow_dataset"
}

variable "bq_table_name" {
  description = "gcp table ID"
  default     = "spotify_cchow_table"
}
