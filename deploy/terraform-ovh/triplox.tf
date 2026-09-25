# Dedicated cache volume only when not using the flavor's local disk.
resource "openstack_blockstorage_volume_v3" "cache" {
  count       = var.triplox_disk_mode == "volume" ? 1 : 0
  name        = "${local.name}-cache"
  size        = var.triplox_cache_volume_gb
  volume_type = var.triplox_cache_volume_type
}

resource "openstack_compute_volume_attach_v2" "cache" {
  count       = var.triplox_disk_mode == "volume" ? 1 : 0
  instance_id = openstack_compute_instance_v2.triplox.id
  volume_id   = openstack_blockstorage_volume_v3.cache[0].id
}

resource "openstack_compute_instance_v2" "triplox" {
  name            = "${local.name}-triplox"
  flavor_name     = var.triplox_flavor
  image_id        = data.openstack_images_image_v2.os.id
  key_pair        = openstack_compute_keypair_v2.admin.name
  security_groups = [openstack_networking_secgroup_v2.triplox.name]

  network {
    name           = "Ext-Net"
    access_network = true
  }

  network {
    name        = ovh_cloud_project_network_private.priv.name
    fixed_ip_v4 = var.triplox_private_ip
  }

  user_data = templatefile("${path.module}/templates/triplox_user_data.sh.tftpl", {
    cache_path        = var.triplox_cache_path
    disk_mode         = var.triplox_disk_mode
    triplox_bucket    = local.triplox_bucket
    automq_private_ip = var.automq_private_ip
    kafka_topic       = var.kafka_topic
    triplox_port      = var.triplox_port
    triplox_image     = var.triplox_image
    s3_access_key     = ovh_cloud_project_user_s3_credential.s3.access_key_id
    s3_secret_key     = ovh_cloud_project_user_s3_credential.s3.secret_access_key
    s3_endpoint       = local.s3_endpoint
    s3_region         = local.s3_region
    private_cidr      = var.private_subnet_cidr
    admin_cidr        = var.admin_cidr
  })

  depends_on = [
    ovh_cloud_project_network_private_subnet.priv,
    ovh_cloud_project_user_s3_policy.s3,
  ]
}
