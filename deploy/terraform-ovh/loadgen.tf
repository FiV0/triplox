# Optional load generator (AuctionMark). Default OFF; the image is added later,
# so this only provisions the instance + a ready-to-run helper.
resource "openstack_compute_instance_v2" "loadgen" {
  count = var.enable_load_gen ? 1 : 0

  name            = "${local.name}-loadgen"
  flavor_name     = var.loadgen_flavor
  image_id        = data.openstack_images_image_v2.os.id
  key_pair        = openstack_compute_keypair_v2.admin.name
  security_groups = [openstack_networking_secgroup_v2.loadgen.name]

  network {
    name           = "Ext-Net"
    access_network = true
  }

  network {
    name        = ovh_cloud_project_network_private.priv.name
    fixed_ip_v4 = var.loadgen_private_ip
  }

  user_data = templatefile("${path.module}/templates/loadgen_user_data.sh.tftpl", {
    loadgen_image      = var.loadgen_image
    triplox_private_ip = var.triplox_private_ip
    triplox_port       = var.triplox_port
    scale_factor       = var.loadgen_scale_factor
    threads            = var.loadgen_threads
    duration           = var.loadgen_duration
    private_cidr       = var.private_subnet_cidr
    admin_cidr         = var.admin_cidr
  })

  depends_on = [ovh_cloud_project_network_private_subnet.priv]
}
