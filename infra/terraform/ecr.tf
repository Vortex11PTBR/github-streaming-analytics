resource "aws_ecr_repository" "producer" {
  name = "${var.name_prefix}-producer"
  image_tag_mutability = "MUTABLE"
  tags = { Name = "${var.name_prefix}-producer" }
}

output "ecr_repo_url" {
  value = aws_ecr_repository.producer.repository_url
}
