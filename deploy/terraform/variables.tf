# --- Identity / region ---
variable "region" {
  description = "AWS region for the deployment."
  type        = string
  default     = "eu-central-1"
}

variable "name_prefix" {
  description = "Prefix for resource names and tags."
  type        = string
  default     = "triplox-bench"
}

# --- Networking ---
variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "public_subnet_cidr" {
  description = "CIDR block for the single public subnet."
  type        = string
  default     = "10.42.1.0/24"
}

variable "admin_cidr" {
  description = "Optional CIDR allowed to reach Triplox on port 5490 directly. Empty (default) opens NO public ingress — reach the server via SSM port forwarding or from in-VPC. The wire protocol is unauthenticated, so only set this to a tight CIDR (your IP/32) when you need direct public access."
  type        = string
  default     = ""

  validation {
    condition     = var.admin_cidr != "0.0.0.0/0"
    error_message = "admin_cidr must not be 0.0.0.0/0: the Triplox wire protocol is unauthenticated. Leave it empty (no public ingress; use SSM port forwarding) or set a tight CIDR such as your-ip/32."
  }
}

# --- Images ---
variable "triplox_image" {
  description = "Triplox image to deploy. The published image includes the kafka feature; pin to a version tag for reproducible stress runs."
  type        = string
  default     = "ghcr.io/fiv0/triplox:snapshot"
}

variable "automq_image" {
  description = "AutoMQ (Kafka-on-S3) broker image."
  type        = string
  default     = "automqinc/automq:1.7.0-strimzi"
}

variable "loadgen_image" {
  description = "Load-generator (AuctionMark) image. Leave empty until the image is published; the load-gen host writes a ready-to-run helper instead."
  type        = string
  default     = ""
}

# --- Instance types (x86_64; Graviton/arm64 is a later swap) ---
variable "automq_instance_type" {
  description = "Instance type for the AutoMQ broker (memory-leaning)."
  type        = string
  default     = "r6i.large"
}

variable "triplox_instance_type" {
  description = "Instance type for the Triplox server. With triplox_disk_mode=\"nvme\" this must be a *d type with instance-store NVMe."
  type        = string
  default     = "m6id.xlarge"
}

variable "loadgen_instance_type" {
  description = "Instance type for the optional load-generator."
  type        = string
  default     = "c6i.xlarge"
}

# --- Disks ---
variable "automq_root_gb" {
  description = "Root volume size (GiB) for the AutoMQ node."
  type        = number
  default     = 40
}

variable "triplox_root_gb" {
  description = "Root volume size (GiB) for the Triplox node."
  type        = number
  default     = 50
}

variable "triplox_disk_mode" {
  description = "Where the Triplox cache lives: \"nvme\" (instance-store, needs a *d instance type) or \"ebs\" (a dedicated gp3 volume)."
  type        = string
  default     = "nvme"

  validation {
    condition     = contains(["nvme", "ebs"], var.triplox_disk_mode)
    error_message = "triplox_disk_mode must be \"nvme\" or \"ebs\"."
  }
}

variable "triplox_cache_ebs_gb" {
  description = "Size (GiB) of the dedicated cache volume when triplox_disk_mode = \"ebs\"."
  type        = number
  default     = 200
}

variable "triplox_cache_ebs_iops" {
  description = "Provisioned IOPS for the cache volume when triplox_disk_mode = \"ebs\"."
  type        = number
  default     = 6000
}

variable "triplox_cache_ebs_throughput" {
  description = "Provisioned throughput (MiB/s) for the cache volume when triplox_disk_mode = \"ebs\"."
  type        = number
  default     = 500
}

# --- AutoMQ tuning ---
variable "automq_cluster_id" {
  description = "Kafka KRaft cluster id (any valid base64 UUID)."
  type        = string
  default     = "rZdE0DjZSrqy96PXrMUZVw"
}

variable "automq_heap_opts" {
  description = "KAFKA_HEAP_OPTS for the AutoMQ broker JVM."
  type        = string
  default     = "-Xms1g -Xmx4g -XX:MetaspaceSize=96m -XX:MaxDirectMemorySize=1G"
}

# --- Triplox / Kafka topic ---
variable "kafka_topic" {
  description = "Transaction-log topic name. Triplox validates it is single-partition with infinite retention and LogAppendTime."
  type        = string
  default     = "triplox-tx-log"
}

variable "triplox_port" {
  description = "Triplox server port."
  type        = number
  default     = 5490
}

variable "triplox_cache_path" {
  description = "On-disk root for the SlateDB object-store cache and dbsp scratch on the Triplox node."
  type        = string
  default     = "/var/lib/triplox/disk"
}

# --- Load-gen (default OFF) ---
variable "enable_load_gen" {
  description = "Provision the optional load-generator instance."
  type        = bool
  default     = false
}

variable "loadgen_scale_factor" {
  description = "AuctionMark scale factor for the load-generator."
  type        = number
  default     = 0.1
}

variable "loadgen_threads" {
  description = "Worker threads for the load-generator."
  type        = number
  default     = 8
}

variable "loadgen_duration" {
  description = "Benchmark duration (seconds) for the load-generator."
  type        = number
  default     = 120
}
