output "data_lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.telemetry.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.telemetry.name
}
