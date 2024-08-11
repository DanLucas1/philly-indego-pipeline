variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "credentials" {
  description = "Credentials for Terraform service account"
  default     = "../indego_pipeline/service_acct_creds.json"
}

variable "region" {
  type        = string
  description = "The default compute region"
  default     = "us-central1"
}

variable "project_id" {
  type        = string
  description = "Indego Bikeshare Pipeline"
  default     = "indego-pipeline"
}

# BIGQUERY DWH
variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "indego_tripdata"
}

variable "bq_dev_dataset_name" {
  description = "Dev dataset for BigQuery dbt"
  default     = "indego_dev"
}

variable "bq_prod_dataset_name" {
  description = "Prod dataset for BigQuery dbt"
  default     = "indego_prod"
}

# CLOUD STORAGE
variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "indego_815299289556"
}
