# Triplox's object store requires explicit S3 access keys, so we create a Public
# Cloud user and mint S3 credentials for it. AutoMQ reuses the same keys for
# parity with the dev compose setup.
#
# Two planes, both required: the objectstore_operator role is the OVH control
# plane; the s3_policy below is the S3 data plane. Keys without a policy can do
# nothing.
resource "ovh_cloud_project_user" "s3" {
  service_name = var.project_id
  description  = "${local.name}-s3"
  role_names   = ["objectstore_operator"]
}

resource "ovh_cloud_project_user_s3_credential" "s3" {
  service_name = var.project_id
  user_id      = ovh_cloud_project_user.s3.id
}

# Bucket-scoped read/write. OVH's policy dialect is an AWS subset: only
# Statement/Sid/Effect/Action/Resource (no Principal, no Condition).
resource "ovh_cloud_project_user_s3_policy" "s3" {
  service_name = var.project_id
  user_id      = ovh_cloud_project_user.s3.id

  policy = jsonencode({
    Statement = [
      {
        Sid    = "ListBuckets"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          "arn:aws:s3:::${local.triplox_bucket}",
          "arn:aws:s3:::${local.automq_data_bucket}",
          "arn:aws:s3:::${local.automq_ops_bucket}",
        ]
      },
      {
        Sid    = "Objects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = [
          "arn:aws:s3:::${local.triplox_bucket}/*",
          "arn:aws:s3:::${local.automq_data_bucket}/*",
          "arn:aws:s3:::${local.automq_ops_bucket}/*",
        ]
      },
    ]
  })

  depends_on = [
    ovh_cloud_project_storage.triplox,
    ovh_cloud_project_storage.automq_data,
    ovh_cloud_project_storage.automq_ops,
  ]
}
