# Latest matching OVH public image (e.g. "Debian 12") in the compute region.
data "openstack_images_image_v2" "os" {
  name        = var.image_name
  visibility  = "public"
  most_recent = true
}

locals {
  name = var.name_prefix

  # S3 SDK region is lowercase ("gra"); ovh_cloud_project_storage wants uppercase
  # ("GRA"); OpenStack wants the full region ("GRA11"). Three different casings.
  s3_region   = lower(var.storage_region)
  s3_endpoint = "https://s3.${local.s3_region}.io.cloud.ovh.net"

  triplox_bucket     = "${var.name_prefix}-triplox-${random_id.suffix.hex}"
  automq_data_bucket = "${var.name_prefix}-automq-data-${random_id.suffix.hex}"
  automq_ops_bucket  = "${var.name_prefix}-automq-ops-${random_id.suffix.hex}"
}
