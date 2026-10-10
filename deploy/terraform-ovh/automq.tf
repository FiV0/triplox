resource "openstack_compute_instance_v2" "automq" {
  name            = "${local.name}-automq"
  flavor_name     = var.automq_flavor
  image_id        = data.openstack_images_image_v2.os.id
  key_pair        = openstack_compute_keypair_v2.admin.name
  security_groups = [openstack_networking_secgroup_v2.automq.name]

  # Public IP for image pulls; ufw locks inbound down to SSH from admin.
  network {
    name           = "Ext-Net"
    access_network = true
  }

  # Private network with a fixed IP, so advertised.listeners is known up front.
  network {
    name        = ovh_cloud_project_network_private.priv.name
    fixed_ip_v4 = var.automq_private_ip
  }

  user_data = templatefile("${path.module}/templates/automq_user_data.sh.tftpl", {
    automq_image       = var.automq_image
    automq_heap_opts   = var.automq_heap_opts
    cluster_id         = var.automq_cluster_id
    automq_data_bucket = local.automq_data_bucket
    automq_ops_bucket  = local.automq_ops_bucket
    kafka_topic        = var.kafka_topic
    s3_access_key      = ovh_cloud_project_user_s3_credential.s3.access_key_id
    s3_secret_key      = ovh_cloud_project_user_s3_credential.s3.secret_access_key
    s3_endpoint        = local.s3_endpoint
    s3_region          = local.s3_region
    private_ip         = var.automq_private_ip
    private_cidr       = var.private_subnet_cidr
    admin_cidr         = var.admin_cidr
  })

  # The buckets, S3 keys and private subnet must exist before the broker boots.
  depends_on = [
    ovh_cloud_project_network_private_subnet.priv,
    ovh_cloud_project_user_s3_policy.s3,
  ]
}
