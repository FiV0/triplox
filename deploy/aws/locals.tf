data "aws_availability_zones" "available" {
  state = "available"
}

# Latest Amazon Linux 2023 x86_64 AMI (excludes the -minimal- variants).
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  name = var.name_prefix
  az   = data.aws_availability_zones.available.names[0]

  triplox_bucket     = "${var.name_prefix}-triplox-${random_id.suffix.hex}"
  automq_data_bucket = "${var.name_prefix}-automq-data-${random_id.suffix.hex}"
  automq_ops_bucket  = "${var.name_prefix}-automq-ops-${random_id.suffix.hex}"

  ssm_access_key_name = "/${var.name_prefix}/s3/access_key"
  ssm_secret_key_name = "/${var.name_prefix}/s3/secret_key"

  common_tags = {
    Project   = "triplox"
    Stack     = var.name_prefix
    ManagedBy = "opentofu"
  }
}
