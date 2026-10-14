# Terraform

Creates the analytical AWS foundation: encrypted/versioned S3 bucket, Glue catalog database, and Athena workgroup.

```bash
terraform init
terraform plan
terraform apply
```

This deliberately excludes MSK/RDS/EMR from the default Terraform path because those services can create material AWS charges. The architecture document describes the production mapping.
