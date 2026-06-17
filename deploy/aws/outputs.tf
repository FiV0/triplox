output "triplox_endpoint" {
  description = "Triplox wire-protocol endpoint (reachable from admin_cidr)."
  value       = "${aws_eip.triplox.public_ip}:${var.triplox_port}"
}

output "triplox_public_ip" {
  description = "Public IP of the Triplox server."
  value       = aws_eip.triplox.public_ip
}

output "triplox_private_ip" {
  description = "Private IP of the Triplox server."
  value       = aws_instance.triplox.private_ip
}

output "automq_private_ip" {
  description = "AutoMQ broker private IP (Kafka bootstrap target for Triplox)."
  value       = aws_instance.automq.private_ip
}

output "loadgen_public_ip" {
  description = "Public IP of the optional load generator (null when disabled)."
  value       = try(aws_eip.loadgen[0].public_ip, null)
}

output "s3_buckets" {
  description = "Provisioned S3 buckets."
  value = {
    triplox     = aws_s3_bucket.triplox.id
    automq_data = aws_s3_bucket.automq_data.id
    automq_ops  = aws_s3_bucket.automq_ops.id
  }
}

output "ssm_sessions" {
  description = "Open a shell on a node via Session Manager (no SSH)."
  value = {
    automq  = "aws ssm start-session --region ${var.region} --target ${aws_instance.automq.id}"
    triplox = "aws ssm start-session --region ${var.region} --target ${aws_instance.triplox.id}"
    loadgen = try("aws ssm start-session --region ${var.region} --target ${aws_instance.loadgen[0].id}", null)
  }
}
