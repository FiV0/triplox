resource "aws_instance" "automq" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.automq_instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.automq.id]
  iam_instance_profile   = aws_iam_instance_profile.instance.name

  root_block_device {
    volume_type = "gp3"
    volume_size = var.automq_root_gb
  }

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/templates/automq_user_data.sh.tftpl", {
    region              = var.region
    automq_image        = var.automq_image
    automq_heap_opts    = var.automq_heap_opts
    cluster_id          = var.automq_cluster_id
    automq_data_bucket  = local.automq_data_bucket
    automq_ops_bucket   = local.automq_ops_bucket
    kafka_topic         = var.kafka_topic
    ssm_access_key_name = local.ssm_access_key_name
    ssm_secret_key_name = local.ssm_secret_key_name
  })

  tags = { Name = "${local.name}-automq", Role = "automq" }

  # The buckets, SSM params and S3 endpoint must exist before the broker boots.
  depends_on = [
    aws_s3_bucket.automq_data,
    aws_s3_bucket.automq_ops,
    aws_ssm_parameter.s3_access_key,
    aws_ssm_parameter.s3_secret_key,
    aws_iam_user_policy.s3,
    aws_vpc_endpoint.s3,
  ]
}
