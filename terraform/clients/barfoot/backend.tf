terraform {
  # backend "gcs" {
  #   bucket = "volvo-tfstate"
  #   prefix = "clients/volvo/terraform-state"
  # }
}

resource "google_storage_bucket" "tfstate" {
  name          = "barfoot-tfstate"
  location      = "AUSTRALIA-SOUTHEAST1"
  project       = "barfoot-and-thompson-main"
  force_destroy = false

  versioning {
    enabled = true
  }
}
