terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.38"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "citi-bike-lakehouse"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

provider "databricks" {
  host = var.databricks_host
}
