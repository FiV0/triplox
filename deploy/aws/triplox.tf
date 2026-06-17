resource "aws_eip" "triplox" {
  domain = "vpc"
  tags   = { Name = "${local.name}-triplox" }
}

resource "aws_instance" "triplox" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.triplox_instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.triplox.id]
  iam_instance_profile   = aws_iam_instance_profile.instance.name

  root_block_device {
    volume_type = "gp3"
    volume_size = var.triplox_root_gb
  }

  # Dedicated cache volume only when not using instance-store NVMe.
  dynamic "ebs_block_device" {
    for_each = var.triplox_disk_mode == "ebs" ? [1] : []
    content {
      device_name = "/dev/sdf"
      volume_type = "gp3"
      volume_size = var.triplox_cache_ebs_gb
      iops        = var.triplox_cache_ebs_iops
      throughput  = var.triplox_cache_ebs_throughput
    }
  }

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/templates/triplox_user_data.sh.tftpl", {
    region              = var.region
    cache_path          = var.triplox_cache_path
    disk_mode           = var.triplox_disk_mode
    triplox_bucket      = local.triplox_bucket
    automq_private_ip   = aws_instance.automq.private_ip
    kafka_topic         = var.kafka_topic
    triplox_port        = var.triplox_port
    triplox_image       = var.triplox_image
    ssm_access_key_name = local.ssm_access_key_name
    ssm_secret_key_name = local.ssm_secret_key_name
  })

  tags = { Name = "${local.name}-triplox", Role = "triplox" }

  depends_on = [
    aws_s3_bucket.triplox,
    aws_ssm_parameter.s3_access_key,
    aws_ssm_parameter.s3_secret_key,
    aws_iam_user_policy.s3,
    aws_vpc_endpoint.s3,
  ]
}

resource "aws_eip_association" "triplox" {
  instance_id   = aws_instance.triplox.id
  allocation_id = aws_eip.triplox.id
}
