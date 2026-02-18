resource "aws_security_group" "msk_sg" {
  name        = "${var.name_prefix}-msk-sg"
  description = "Allow internal traffic for MSK brokers"
  vpc_id      = aws_vpc.this.id
}

resource "aws_msk_cluster" "kafka" {
  cluster_name = "${var.name_prefix}-msk"
  kafka_version = "3.4.0"

  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type = "kafka.m5.large"
    client_subnets = [for s in aws_subnet.private : s.id]
    security_groups = [aws_security_group.msk_sg.id]
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = null
  }

  tags = { Name = "${var.name_prefix}-msk" }
}

output "msk_bootstrap_brokers" {
  value = aws_msk_cluster.kafka.bootstrap_brokers
}
