variable "aws_region" {
  description = "AWS region for the S3 Data Lake bucket and Databricks workspace"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "s3_bucket_name" {
  description = "Globally unique AWS S3 bucket name for Citi Bike raw landing and Unity Catalog external storage"
  type        = string
  default     = "citi-bike-lakehouse-raw-dev"
}

variable "databricks_host" {
  description = "Databricks workspace URL (e.g., https://dbc-xxxxxxxx-xxxx.cloud.databricks.com)"
  type        = string
}

variable "databricks_aws_account_id" {
  description = "Databricks AWS control plane account ID for Unity Catalog IAM role trust policy"
  type        = string
  default     = "414351767826"
}

variable "unity_catalog_name" {
  description = "Unity Catalog name for the Citi Bike Lakehouse"
  type        = string
  default     = "citi_bike_catalog"
}

variable "git_repo_url" {
  description = "Git repository URL containing the pipeline code for Databricks Workflows"
  type        = string
  default     = "https://github.com/your-username/citi_bike_pipeline.git"
}

variable "notification_email" {
  description = "Email address for Databricks Workflow job failure notifications"
  type        = string
  default     = "data-engineering@example.com"
}
