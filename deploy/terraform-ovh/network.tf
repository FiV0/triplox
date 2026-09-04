# --- Private network (vRack-backed) the three nodes share for inter-node traffic ---
resource "ovh_cloud_project_network_private" "priv" {
  service_name = var.project_id
  name         = "${local.name}-net"
  regions      = [var.os_region]
  vlan_id      = 0
}

resource "ovh_cloud_project_network_private_subnet" "priv" {
  service_name = var.project_id
  network_id   = ovh_cloud_project_network_private.priv.id
  region       = var.os_region
  network      = var.private_subnet_cidr
  start        = var.private_dhcp_start
  end          = var.private_dhcp_end
  dhcp         = true
  no_gateway   = false
}

# --- SSH key (the only login path; no SSM on OVH) ---
resource "openstack_compute_keypair_v2" "admin" {
  name       = "${local.name}-admin"
  public_key = var.ssh_public_key
}

# --- Security groups (one per role, mirroring the AWS rig) ---
# These are enforced on the private network. On the public network OVH does not
# reliably enforce security groups, so each host also runs a ufw firewall (see
# the user_data templates) — that is the real lock on SSH and the Triplox port.
resource "openstack_networking_secgroup_v2" "automq" {
  name        = "${local.name}-automq"
  description = "AutoMQ broker"
}

resource "openstack_networking_secgroup_v2" "triplox" {
  name        = "${local.name}-triplox"
  description = "Triplox server"
}

resource "openstack_networking_secgroup_v2" "loadgen" {
  name        = "${local.name}-loadgen"
  description = "Load generator"
}

# AutoMQ: Kafka from Triplox; controller self-reference for the single-node quorum.
resource "openstack_networking_secgroup_rule_v2" "automq_kafka_from_triplox" {
  security_group_id = openstack_networking_secgroup_v2.automq.id
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 9092
  port_range_max    = 9092
  remote_group_id   = openstack_networking_secgroup_v2.triplox.id
  description       = "Kafka bootstrap/produce/fetch from Triplox"
}

resource "openstack_networking_secgroup_rule_v2" "automq_controller_self" {
  security_group_id = openstack_networking_secgroup_v2.automq.id
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 9093
  port_range_max    = 9093
  remote_group_id   = openstack_networking_secgroup_v2.automq.id
  description       = "KRaft controller quorum (single node)"
}

resource "openstack_networking_secgroup_rule_v2" "automq_ssh" {
  security_group_id = openstack_networking_secgroup_v2.automq.id
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 22
  port_range_max    = 22
  remote_ip_prefix  = var.admin_cidr
  description       = "SSH from admin"
}

# Triplox: server port from admin and from the load generator; SSH from admin.
resource "openstack_networking_secgroup_rule_v2" "triplox_from_admin" {
  security_group_id = openstack_networking_secgroup_v2.triplox.id
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = var.triplox_port
  port_range_max    = var.triplox_port
  remote_ip_prefix  = var.admin_cidr
  description       = "Triplox wire protocol from admin"
}

resource "openstack_networking_secgroup_rule_v2" "triplox_from_loadgen" {
  security_group_id = openstack_networking_secgroup_v2.triplox.id
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = var.triplox_port
  port_range_max    = var.triplox_port
  remote_group_id   = openstack_networking_secgroup_v2.loadgen.id
  description       = "Triplox wire protocol from load generator"
}

resource "openstack_networking_secgroup_rule_v2" "triplox_ssh" {
  security_group_id = openstack_networking_secgroup_v2.triplox.id
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 22
  port_range_max    = 22
  remote_ip_prefix  = var.admin_cidr
  description       = "SSH from admin"
}

# Load generator: SSH from admin; everything else is outbound (default egress).
resource "openstack_networking_secgroup_rule_v2" "loadgen_ssh" {
  security_group_id = openstack_networking_secgroup_v2.loadgen.id
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 22
  port_range_max    = 22
  remote_ip_prefix  = var.admin_cidr
  description       = "SSH from admin"
}
