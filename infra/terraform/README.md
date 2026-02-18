Terraform example to deploy the pipeline on AWS:

- VPC with subnets and security groups
- S3 buckets for checkpoints and output
- MSK (Managed Kafka) cluster connected to the VPC
- EMR cluster (Spark) reading from MSK and writing to S3
- ECR repository to host the producer Docker image

This is a minimal, opinionated starting point — review IAM, sizing and
networking before using in production.

Quick start
1. Fill `infra/terraform/terraform.tfvars` with your AWS `region` and `key_name`.
2. Init & plan:
```bash
cd infra/terraform
terraform init
terraform plan -out plan.tfplan
terraform apply plan.tfplan
```
3. Build and push producer image to ECR (see outputs for repository URL).
4. Optionally create an ECS service to run the producer (not included).

Notes
- This example doesn't enable encryption, monitoring, or fine-grained IAM policies.
- Customize instance types, EMR release label and MSK broker config for production.
