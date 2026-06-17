# --- Static S3 credentials ---
# Triplox's object store requires explicit access keys (no instance-role/IMDS
# path in the code), so we provision an IAM user scoped to the three buckets.
# AutoMQ reuses the same keys for parity with the dev compose setup.
resource "aws_iam_user" "s3" {
  name = "${local.name}-s3"
}

resource "aws_iam_access_key" "s3" {
  user = aws_iam_user.s3.name
}

data "aws_iam_policy_document" "s3" {
  statement {
    sid     = "ListBuckets"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.triplox.arn,
      aws_s3_bucket.automq_data.arn,
      aws_s3_bucket.automq_ops.arn,
    ]
  }

  statement {
    sid = "Objects"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = [
      "${aws_s3_bucket.triplox.arn}/*",
      "${aws_s3_bucket.automq_data.arn}/*",
      "${aws_s3_bucket.automq_ops.arn}/*",
    ]
  }
}

resource "aws_iam_user_policy" "s3" {
  name   = "${local.name}-s3"
  user   = aws_iam_user.s3.name
  policy = data.aws_iam_policy_document.s3.json
}

# --- SSM SecureString storage so the keys never sit in plaintext user_data ---
resource "aws_ssm_parameter" "s3_access_key" {
  name  = local.ssm_access_key_name
  type  = "SecureString"
  value = aws_iam_access_key.s3.id
}

resource "aws_ssm_parameter" "s3_secret_key" {
  name  = local.ssm_secret_key_name
  type  = "SecureString"
  value = aws_iam_access_key.s3.secret
}

# --- Shared instance role: SSM Session Manager + read the two SSM params ---
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  name               = "${local.name}-instance"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

# Enables Session Manager shell access (no inbound SSH needed).
resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "ssm_read" {
  statement {
    sid     = "ReadS3Keys"
    actions = ["ssm:GetParameter", "ssm:GetParameters"]
    resources = [
      aws_ssm_parameter.s3_access_key.arn,
      aws_ssm_parameter.s3_secret_key.arn,
    ]
  }

  # SecureString uses the account-default aws/ssm KMS key; scope decrypt to SSM.
  statement {
    sid       = "DecryptViaSsm"
    actions   = ["kms:Decrypt"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.${var.region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "ssm_read" {
  name   = "${local.name}-ssm-read"
  role   = aws_iam_role.instance.id
  policy = data.aws_iam_policy_document.ssm_read.json
}

resource "aws_iam_instance_profile" "instance" {
  name = "${local.name}-instance"
  role = aws_iam_role.instance.name
}
