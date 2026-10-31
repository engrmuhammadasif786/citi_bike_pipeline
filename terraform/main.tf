# ==============================================================================
# 1. AWS S3 Data Lake Storage & Lifecycle Policies
# ==============================================================================

resource "aws_s3_bucket" "citi_bike_datalake" {
  bucket        = "${var.s3_bucket_name}-${var.environment}"
  force_destroy = var.environment == "dev"
}

resource "aws_s3_bucket_versioning" "citi_bike_datalake_versioning" {
  bucket = aws_s3_bucket.citi_bike_datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "citi_bike_datalake_sse" {
  bucket = aws_s3_bucket.citi_bike_datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "citi_bike_datalake_public_block" {
  bucket                  = aws_s3_bucket.citi_bike_datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "citi_bike_datalake_lifecycle" {
  bucket = aws_s3_bucket.citi_bike_datalake.id

  rule {
    id     = "archive-raw-zip-files"
    status = "Enabled"

    filter {
      prefix = "raw/citibike/tripdata/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }
}

# ==============================================================================
# 2. AWS IAM Role for Databricks Unity Catalog Storage Credential
# ==============================================================================

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

resource "aws_iam_role" "databricks_uc_storage_role" {
  name = "citi-bike-databricks-uc-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = [
            "arn:aws:iam::${var.databricks_aws_account_id}:root",
            "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/citi-bike-databricks-uc-role-${var.environment}"
          ]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "databricks_uc_s3_policy" {
  name = "citi-bike-s3-access-${var.environment}"
  role = aws_iam_role.databricks_uc_storage_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.citi_bike_datalake.arn,
          "${aws_s3_bucket.citi_bike_datalake.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = aws_iam_role.databricks_uc_storage_role.arn
      }
    ]
  })
}

# ==============================================================================
# 3. Databricks Unity Catalog, External Location & Medallion Schemas
# ==============================================================================

resource "databricks_storage_credential" "datalake_credential" {
  name = "citi_bike_s3_credential_${var.environment}"
  aws_iam_role {
    role_arn = aws_iam_role.databricks_uc_storage_role.arn
  }
  comment = "Managed by Terraform for S3 Citi Bike Data Lake access"
}

resource "databricks_external_location" "datalake_external_location" {
  name            = "citi_bike_s3_external_${var.environment}"
  url             = "s3://${aws_s3_bucket.citi_bike_datalake.id}"
  credential_name = databricks_storage_credential.datalake_credential.id
  comment         = "External S3 location for Citi Bike raw landing and Delta tables"
}

resource "databricks_catalog" "citi_bike" {
  name           = var.unity_catalog_name
  storage_root   = "s3://${aws_s3_bucket.citi_bike_datalake.id}/unity-catalog"
  isolation_mode = "OPEN"
  comment        = "NYC Citi Bike Medallion Lakehouse Catalog"
  depends_on     = [databricks_external_location.datalake_external_location]
}

resource "databricks_schema" "medallion_layers" {
  for_each     = toset(["bronze", "silver", "gold"])
  catalog_name = databricks_catalog.citi_bike.id
  name         = each.value
  comment      = "Medallion ${upper(each.value)} layer for NYC Citi Bike analytics"
}

# ==============================================================================
# 4. Databricks Serverless SQL Warehouse (Data Warehouse & BI Serving Layer)
# ==============================================================================

resource "databricks_sql_endpoint" "analytics_warehouse" {
  name                      = "citi-bike-serverless-wh-${var.environment}"
  cluster_size              = "2X-Small"
  max_num_clusters          = 1
  auto_stop_mins            = 10
  enable_serverless_compute = true
  warehouse_type            = "PRO"
}

# ==============================================================================
# 5. Databricks Multi-Task Workflow Job (End-to-End Batch Orchestration)
# ==============================================================================

resource "databricks_job" "citi_bike_medallion_pipeline" {
  name        = "citi_bike_medallion_lakehouse_${var.environment}"
  description = "End-to-end monthly batch pipeline: S3 Ingestion -> PySpark Bronze -> PySpark Silver (Partitioned & Clustered) -> dbt Gold Marts -> Data Quality Validation"
  
  git_source {
    url      = var.git_repo_url
    provider = "gitHub"
    branch   = "main"
  }

  schedule {
    quartz_cron_expression = "0 0 6 5 * ?" # Runs at 06:00 UTC on the 5th of every month
    timezone_id            = "UTC"
    pause_status           = "UNPAUSED"
  }

  email_notifications {
    on_failure = [var.notification_email]
  }

  environment {
    environment_key = "serverless"
    spec {
      client = "1"
    }
  }

  task {
    task_key        = "ingest_citibike_to_s3"
    description     = "Download monthly Citi Bike tripdata archives and upload to S3 Data Lake"
    environment_key = "serverless"

    spark_python_task {
      python_file = "notebooks/01_ingest_to_s3.py"
      source      = "GIT"
      parameters = [
        "--s3-bucket", aws_s3_bucket.citi_bike_datalake.id,
        "--year-months", "202401,202402,202403"
      ]
    }
  }

  task {
    task_key        = "bronze_raw_to_delta"
    description     = "Ingest raw CSVs from S3 into Unity Catalog Bronze Delta table with schema enforcement and audit metadata"
    environment_key = "serverless"
    depends_on {
      task_key = "ingest_citibike_to_s3"
    }

    spark_python_task {
      python_file = "notebooks/02_bronze_ingestion.py"
      source      = "GIT"
      parameters = [
        "--catalog", databricks_catalog.citi_bike.name,
        "--s3-bucket", aws_s3_bucket.citi_bike_datalake.id
      ]
    }
  }

  task {
    task_key        = "silver_clean_and_cluster"
    description     = "Deduplicate, validate, enrich, partition by trip_year_month, and Liquid Cluster/Z-ORDER Silver Delta table"
    environment_key = "serverless"
    depends_on {
      task_key = "bronze_raw_to_delta"
    }

    spark_python_task {
      python_file = "notebooks/03_silver_transformation.py"
      source      = "GIT"
      parameters = [
        "--catalog", databricks_catalog.citi_bike.name
      ]
    }
  }

  task {
    task_key        = "dbt_gold_marts"
    description     = "Execute dbt deps, dbt run, and dbt test to build Gold dimensional and analytical marts"
    environment_key = "serverless"
    depends_on {
      task_key = "silver_clean_and_cluster"
    }

    spark_python_task {
      python_file = "notebooks/04_gold_marts_and_quality.py"
      source      = "GIT"
      parameters = [
        "--catalog", databricks_catalog.citi_bike.name
      ]
    }
  }
}
