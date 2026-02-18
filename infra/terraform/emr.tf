resource "aws_iam_role" "emr_service_role" {
  name = "${var.name_prefix}-emr-service-role"
  assume_role_policy = data.aws_iam_policy_document.emr_service_assume.json
}

data "aws_iam_policy_document" "emr_service_assume" {
  statement { actions = ["sts:AssumeRole"] principals { type = "Service" identifiers = ["elasticmapreduce.amazonaws.com"] } }
}

resource "aws_iam_role_policy_attachment" "emr_service_attach" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2_role" {
  name = "${var.name_prefix}-emr-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.emr_ec2_assume.json
}

data "aws_iam_policy_document" "emr_ec2_assume" {
  statement { actions = ["sts:AssumeRole"] principals { type = "Service" identifiers = ["ec2.amazonaws.com"] } }
}

resource "aws_iam_instance_profile" "emr_ec2_profile" {
  name = "${var.name_prefix}-emr-instance-profile"
  role = aws_iam_role.emr_ec2_role.name
}

resource "aws_iam_role_policy_attachment" "emr_ec2_attach" {
  role = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_emr_cluster" "emr" {
  name = "${var.name_prefix}-emr-cluster"
  release_label = "emr-6.11.0"
  applications = ["Hadoop", "Spark"]

  ec2_attributes {
    instance_profile = aws_iam_instance_profile.emr_ec2_profile.arn
    subnet_id = element(aws_subnet.private.*.id, 0)
    key_name = var.key_name != "" ? var.key_name : null
  }

  master_instance_type = "m5.xlarge"
  core_instance_type = "m5.xlarge"
  core_instance_count = 2

  service_role = aws_iam_role.emr_service_role.arn

  visible_to_all_users = true

  configurations_json = jsonencode([])

  bootstrap_action {
    name = "install-kafka-client"
    path = "s3://elasticmapreduce/bootstrap-actions/run-if/"
    args = ["-c", "echo installing" ]
  }

  tags = { Name = "${var.name_prefix}-emr" }
}

output "emr_master_public_dns" {
  value = aws_emr_cluster.emr.master_public_dns
}
