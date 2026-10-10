terraform {
  required_version = ">= 1.6"

  required_providers {
    # Account-plane: object storage, S3 users/keys, private network.
    ovh = {
      source  = "ovh/ovh"
      version = "~> 2.0"
    }
    # Compute-plane: OVH Public Cloud is OpenStack underneath. The instances,
    # keypair, security groups and block volumes live here.
    openstack = {
      source  = "terraform-provider-openstack/openstack"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

# Endpoint selects the OVH *API* (ovh-eu / ovh-us / ovh-ca), NOT the datacenter.
# Credentials come from OVH_APPLICATION_KEY / OVH_APPLICATION_SECRET /
# OVH_CONSUMER_KEY (or OVH_CLIENT_ID / OVH_CLIENT_SECRET for OAuth2) in the env.
provider "ovh" {
  endpoint = var.ovh_endpoint
}

# OVH Keystone is one URL for every region; the region picks the datacenter.
# Credentials come from OS_* env vars, clouds.yaml, or
# OS_APPLICATION_CREDENTIAL_ID / OS_APPLICATION_CREDENTIAL_SECRET in the env.
provider "openstack" {
  auth_url = var.os_auth_url
  region   = var.os_region
}
