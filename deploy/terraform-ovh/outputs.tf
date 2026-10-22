output "triplox_endpoint" {
  description = "Triplox wire-protocol endpoint (reachable from admin_cidr)."
  value       = "${openstack_compute_instance_v2.triplox.access_ip_v4}:${var.triplox_port}"
}

output "triplox_public_ip" {
  description = "Public IP of the Triplox server (changes if the instance is rebuilt)."
  value       = openstack_compute_instance_v2.triplox.access_ip_v4
}

output "triplox_private_ip" {
  description = "Private IP of the Triplox server."
  value       = var.triplox_private_ip
}

output "automq_public_ip" {
  description = "Public IP of the AutoMQ broker (egress + admin SSH only)."
  value       = openstack_compute_instance_v2.automq.access_ip_v4
}

output "automq_private_ip" {
  description = "AutoMQ broker private IP (Kafka bootstrap target for Triplox)."
  value       = var.automq_private_ip
}

output "loadgen_public_ip" {
  description = "Public IP of the optional load generator (null when disabled)."
  value       = try(openstack_compute_instance_v2.loadgen[0].access_ip_v4, null)
}

output "s3_endpoint" {
  description = "S3 endpoint the nodes use for object storage."
  value       = local.s3_endpoint
}

output "s3_buckets" {
  description = "Provisioned S3 buckets."
  value = {
    triplox     = local.triplox_bucket
    automq_data = local.automq_data_bucket
    automq_ops  = local.automq_ops_bucket
  }
}

output "s3_access_key_id" {
  description = "S3 access key id (also baked into the nodes' user_data)."
  value       = ovh_cloud_project_user_s3_credential.s3.access_key_id
}

output "s3_secret_access_key" {
  description = "S3 secret key. Read with: tofu output -raw s3_secret_access_key"
  value       = ovh_cloud_project_user_s3_credential.s3.secret_access_key
  sensitive   = true
}

output "ssh" {
  description = "SSH onto a node (the only login path; no SSM on OVH)."
  value = {
    automq  = "ssh ${var.ssh_user}@${openstack_compute_instance_v2.automq.access_ip_v4}"
    triplox = "ssh ${var.ssh_user}@${openstack_compute_instance_v2.triplox.access_ip_v4}"
    loadgen = try("ssh ${var.ssh_user}@${openstack_compute_instance_v2.loadgen[0].access_ip_v4}", null)
  }
}
