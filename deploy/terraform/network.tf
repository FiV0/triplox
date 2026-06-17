resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${local.name}-vpc" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnet_cidr
  availability_zone       = local.az
  map_public_ip_on_launch = true

  tags = { Name = "${local.name}-public" }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${local.name}-igw" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${local.name}-public" }
}

resource "aws_route" "default" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.igw.id
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Free, high-bandwidth path to S3; both AutoMQ and Triplox are S3-heavy.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]

  tags = { Name = "${local.name}-s3" }
}

# --- Security groups (inbound scoped; egress open for image pulls + DNS + S3) ---

resource "aws_security_group" "automq" {
  name        = "${local.name}-automq"
  description = "AutoMQ broker"
  vpc_id      = aws_vpc.main.id
  tags        = { Name = "${local.name}-automq" }
}

resource "aws_security_group" "triplox" {
  name        = "${local.name}-triplox"
  description = "Triplox server"
  vpc_id      = aws_vpc.main.id
  tags        = { Name = "${local.name}-triplox" }
}

resource "aws_security_group" "loadgen" {
  name        = "${local.name}-loadgen"
  description = "Load generator"
  vpc_id      = aws_vpc.main.id
  tags        = { Name = "${local.name}-loadgen" }
}

# AutoMQ: Kafka from Triplox; controller self-reference for the single-node quorum.
resource "aws_vpc_security_group_ingress_rule" "automq_kafka_from_triplox" {
  security_group_id            = aws_security_group.automq.id
  referenced_security_group_id = aws_security_group.triplox.id
  from_port                    = 9092
  to_port                      = 9092
  ip_protocol                  = "tcp"
  description                  = "Kafka bootstrap/produce/fetch from Triplox"
}

resource "aws_vpc_security_group_ingress_rule" "automq_controller_self" {
  security_group_id            = aws_security_group.automq.id
  referenced_security_group_id = aws_security_group.automq.id
  from_port                    = 9093
  to_port                      = 9093
  ip_protocol                  = "tcp"
  description                  = "KRaft controller quorum (single node)"
}

resource "aws_vpc_security_group_egress_rule" "automq_all" {
  security_group_id = aws_security_group.automq.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "All egress (S3, Docker Hub, DNS)"
}

# Triplox: server port from the load generator, and optionally a direct admin
# CIDR. With admin_cidr empty (default) there is NO public 5490 ingress — reach
# Triplox via SSM port forwarding instead.
resource "aws_vpc_security_group_ingress_rule" "triplox_from_admin" {
  count             = var.admin_cidr != "" ? 1 : 0
  security_group_id = aws_security_group.triplox.id
  cidr_ipv4         = var.admin_cidr
  from_port         = var.triplox_port
  to_port           = var.triplox_port
  ip_protocol       = "tcp"
  description       = "Triplox wire protocol from admin"
}

resource "aws_vpc_security_group_ingress_rule" "triplox_from_loadgen" {
  security_group_id            = aws_security_group.triplox.id
  referenced_security_group_id = aws_security_group.loadgen.id
  from_port                    = var.triplox_port
  to_port                      = var.triplox_port
  ip_protocol                  = "tcp"
  description                  = "Triplox wire protocol from load generator"
}

resource "aws_vpc_security_group_egress_rule" "triplox_all" {
  security_group_id = aws_security_group.triplox.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "All egress (S3, GHCR, DNS, Kafka to AutoMQ)"
}

# Load generator: outbound only (reaches Triplox; SSM for admin).
resource "aws_vpc_security_group_egress_rule" "loadgen_all" {
  security_group_id = aws_security_group.loadgen.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "All egress (Triplox, image registry, DNS)"
}
