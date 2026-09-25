resource "random_id" "suffix" {
  byte_length = 3
}

# Three S3-compatible buckets (Triplox SlateDB + AutoMQ data/ops). Nested
# versioning/encryption use attribute syntax (= {}) in ovh provider v2.
resource "ovh_cloud_project_storage" "triplox" {
  service_name = var.project_id
  region_name  = var.storage_region
  name         = local.triplox_bucket
  encryption   = { sse_algorithm = "AES256" }
}

resource "ovh_cloud_project_storage" "automq_data" {
  service_name = var.project_id
  region_name  = var.storage_region
  name         = local.automq_data_bucket
  encryption   = { sse_algorithm = "AES256" }
}

resource "ovh_cloud_project_storage" "automq_ops" {
  service_name = var.project_id
  region_name  = var.storage_region
  name         = local.automq_ops_bucket
  encryption   = { sse_algorithm = "AES256" }
}
