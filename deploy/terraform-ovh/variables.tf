# --- Identity / providers ---
variable "project_id" {
  description = "OVH Public Cloud project id (service_name). Also settable via OVH_CLOUD_PROJECT_SERVICE."
  type        = string
}

variable "ovh_endpoint" {
  description = "OVH API endpoint (ovh-eu, ovh-us, ovh-ca, ...). This is the API region, not the datacenter."
  type        = string
  default     = "ovh-eu"
}

variable "os_auth_url" {
  description = "OpenStack Keystone (Identity v3) URL. The same URL serves every OVH region."
  type        = string
  default     = "https://auth.cloud.ovh.net/v3"
}

variable "name_prefix" {
  description = "Prefix for resource names."
  type        = string
  default     = "triplox-bench"
}

# --- Regions (note the three casings) ---
variable "os_region" {
  description = "OpenStack region for compute + private network, e.g. GRA11, SBG5, DE1."
  type        = string
  default     = "GRA11"
}

variable "storage_region" {
  description = "OVH object-storage region (uppercase, e.g. GRA, SBG, DE). Lowercased for the S3 endpoint/SDK region. Use the same location as os_region."
  type        = string
  default     = "GRA"
}

# --- Access ---
variable "ssh_public_key" {
  description = "SSH public key registered on every node (the only login path; OVH has no SSM equivalent)."
  type        = string
}

variable "ssh_user" {
  description = "Default cloud-image login user (debian for Debian, ubuntu for Ubuntu)."
  type        = string
  default     = "debian"
}

variable "admin_cidr" {
  description = "CIDR allowed to reach SSH (22) and the Triplox port. The wire protocol is unauthenticated, so tighten this to your own IP/32. Enforced by the host firewall (ufw), since OVH security groups are not reliably enforced on the public network."
  type        = string
  default     = "0.0.0.0/0"
}

variable "image_name" {
  description = "OVH public image to boot. apt-based; user_data assumes Debian/Ubuntu."
  type        = string
  default     = "Debian 12"
}

# --- Images (containers) ---
variable "triplox_image" {
  description = "Kafka-enabled Triplox image to deploy. Must be built with the kafka feature (see .github/workflows/docker-publish.yml)."
  type        = string
  default     = "ghcr.io/fiv0/triplox:snapshot-kafka"
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

# --- Flavors (OpenStack flavor names; b3 = general purpose, r3 = RAM, c3 = CPU) ---
variable "automq_flavor" {
  description = "Flavor for the AutoMQ broker (memory-leaning)."
  type        = string
  default     = "r3-16"
}

variable "triplox_flavor" {
  description = "Flavor for the Triplox server (the unit under test). Swap to an i1-* flavor for instance-local NVMe (then set triplox_disk_mode=\"local\")."
  type        = string
  default     = "b3-16"
}

variable "loadgen_flavor" {
  description = "Flavor for the optional load-generator."
  type        = string
  default     = "b3-8"
}

# --- Triplox cache disk ---
variable "triplox_disk_mode" {
  description = "Where the Triplox cache lives: \"volume\" (a dedicated Block Storage volume) or \"local\" (the flavor's local disk; pick an i1-* flavor for NVMe)."
  type        = string
  default     = "volume"

  validation {
    condition     = contains(["volume", "local"], var.triplox_disk_mode)
    error_message = "triplox_disk_mode must be \"volume\" or \"local\"."
  }
}

variable "triplox_cache_volume_gb" {
  description = "Size (GiB) of the dedicated cache volume when triplox_disk_mode = \"volume\"."
  type        = number
  default     = 200
}

variable "triplox_cache_volume_type" {
  description = "Block Storage class for the cache volume: classic, high-speed, or high-speed-gen2 (NVMe-backed)."
  type        = string
  default     = "high-speed-gen2"
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

# --- Private network ---
variable "private_subnet_cidr" {
  description = "CIDR for the vRack-backed private network the three nodes share."
  type        = string
  default     = "192.168.42.0/24"
}

variable "private_dhcp_start" {
  description = "First DHCP-pool address. Keep above the static node IPs below."
  type        = string
  default     = "192.168.42.100"
}

variable "private_dhcp_end" {
  description = "Last DHCP-pool address."
  type        = string
  default     = "192.168.42.200"
}

variable "automq_private_ip" {
  description = "Static private IP for the AutoMQ broker (advertised.listeners + controller). Must be inside private_subnet_cidr and outside the DHCP pool."
  type        = string
  default     = "192.168.42.10"
}

variable "triplox_private_ip" {
  description = "Static private IP for the Triplox server."
  type        = string
  default     = "192.168.42.11"
}

variable "loadgen_private_ip" {
  description = "Static private IP for the optional load-generator."
  type        = string
  default     = "192.168.42.12"
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
