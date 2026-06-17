# Optional load generator (AuctionMark). Default OFF; the image is added later,
# so this only provisions the instance + networking and a ready-to-run helper.
resource "aws_instance" "loadgen" {
  count = var.enable_load_gen ? 1 : 0

  ami                    = data.aws_ami.al2023.id
  instance_type          = var.loadgen_instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.loadgen.id]
  iam_instance_profile   = aws_iam_instance_profile.instance.name

  root_block_device {
    volume_type = "gp3"
    volume_size = 30
  }

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/templates/loadgen_user_data.sh.tftpl", {
    region             = var.region
    loadgen_image      = var.loadgen_image
    triplox_private_ip = aws_instance.triplox.private_ip
    triplox_port       = var.triplox_port
    scale_factor       = var.loadgen_scale_factor
    threads            = var.loadgen_threads
    duration           = var.loadgen_duration
  })

  tags = { Name = "${local.name}-loadgen", Role = "loadgen" }
}

resource "aws_eip" "loadgen" {
  count  = var.enable_load_gen ? 1 : 0
  domain = "vpc"
  tags   = { Name = "${local.name}-loadgen" }
}

resource "aws_eip_association" "loadgen" {
  count         = var.enable_load_gen ? 1 : 0
  instance_id   = aws_instance.loadgen[0].id
  allocation_id = aws_eip.loadgen[0].id
}
