terraform {
  # backend "gcs" {
  #   bucket = "geely-tfstate"
  #   prefix = "clients/cffc/terraform-state"
  # }
}

resource "google_storage_bucket" "tfstate" {
  name          = "moe-tfstate"
  location      = "AUSTRALIA-SOUTHEAST1"
  project       = "moe-main"
  force_destroy = false

  versioning {
    enabled = true
  }
}
