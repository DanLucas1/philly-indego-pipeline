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


# CREATE BIGQUERY DATASET
resource "google_bigquery_dataset" "bq-dataset-dev" {
  dataset_id = var.bq_dev_dataset_name
  location   = var.location
}


# CREATE BIGQUERY DATASET
resource "google_bigquery_dataset" "bq-dataset-prod" {
  dataset_id = var.bq_prod_dataset_name
  location   = var.location
}
