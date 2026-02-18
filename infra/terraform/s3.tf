resource "aws_s3_bucket" "data_bucket" {
  bucket = "${var.name_prefix}-data-${random_id.bucket_id.hex}"
  acl    = "private"
  force_destroy = true
  tags = { Name = "${var.name_prefix}-data" }
}

resource "random_id" "bucket_id" {
  byte_length = 4
}

output "s3_bucket" {
  value = aws_s3_bucket.data_bucket.bucket
}
