variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-grids.json"
}

variable "project" {
  description = "project"
  default     = "dtc-dez-412015"
}

variable "region" {
  description = "region"
  default     = "dtc-dez-412015"
}

variable "location" {
  description = "project location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Storage Bucket Name"
  default     = "dtc-dez-412015-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

