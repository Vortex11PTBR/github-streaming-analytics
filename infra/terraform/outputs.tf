output "vpc_id" {
  value = aws_vpc.this.id
}

output "s3_bucket" {
  value = aws_s3_bucket.data_bucket.bucket
}

output "msk_bootstrap_brokers" {
  value = aws_msk_cluster.kafka.bootstrap_brokers
}

output "ecr_repo_url" {
  value = aws_ecr_repository.producer.repository_url
}

output "emr_master_public_dns" {
  value = aws_emr_cluster.emr.master_public_dns
}
