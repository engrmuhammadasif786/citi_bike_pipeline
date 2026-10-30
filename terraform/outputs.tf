output "s3_datalake_bucket_name" {
  description = "Provisioned AWS S3 Data Lake bucket name"
  value       = aws_s3_bucket.citi_bike_datalake.id
}

output "s3_datalake_bucket_arn" {
  description = "Provisioned AWS S3 Data Lake bucket ARN"
  value       = aws_s3_bucket.citi_bike_datalake.arn
}

output "unity_catalog_name" {
  description = "Databricks Unity Catalog name"
  value       = databricks_catalog.citi_bike.name
}

output "sql_warehouse_id" {
  description = "Databricks Serverless SQL Warehouse ID for dbt and Streamlit dashboard connections"
  value       = databricks_sql_endpoint.analytics_warehouse.id
}

output "sql_warehouse_jdbc_url" {
  description = "Databricks SQL Warehouse JDBC URL"
  value       = databricks_sql_endpoint.analytics_warehouse.jdbc_url
}

output "databricks_workflow_job_url" {
  description = "URL to the orchestrated Databricks Workflow pipeline"
  value       = databricks_job.citi_bike_medallion_pipeline.url
}
