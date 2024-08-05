# main.tf

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project_id
  region      = var.region
}

# ENABLE APIS

# Enable IAM API
resource "google_project_service" "iam" {
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}


# CREATE CLOUD STORAGE BUCKET
resource "google_storage_bucket" "indego_storage" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


# CREATE BIGQUERY DATASET
resource "google_bigquery_dataset" "bq-dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}