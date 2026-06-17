resource "random_id" "suffix" {
  byte_length = 3
}

# force_destroy = true so `tofu destroy` works without manually emptying buckets
# (rig convenience; S3 is the source of truth, but this is a throwaway stack).
resource "aws_s3_bucket" "triplox" {
  bucket        = local.triplox_bucket
  force_destroy = true
  tags          = { Name = local.triplox_bucket }
}

resource "aws_s3_bucket" "automq_data" {
  bucket        = local.automq_data_bucket
  force_destroy = true
  tags          = { Name = local.automq_data_bucket }
}

resource "aws_s3_bucket" "automq_ops" {
  bucket        = local.automq_ops_bucket
  force_destroy = true
  tags          = { Name = local.automq_ops_bucket }
}

locals {
  bucket_ids = {
    triplox     = aws_s3_bucket.triplox.id
    automq_data = aws_s3_bucket.automq_data.id
    automq_ops  = aws_s3_bucket.automq_ops.id
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = local.bucket_ids
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.bucket_ids
  bucket   = each.value

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
