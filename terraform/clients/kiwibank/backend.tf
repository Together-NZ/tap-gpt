terraform {
  # backend "gcs" {
  #   bucket = "volvo-tfstate"
  #   prefix = "clients/volvo/terraform-state"
  # }
}

resource "google_storage_bucket" "tfstate" {
  name          = "kiwibank-tfstate"
  location      = "AUSTRALIA-SOUTHEAST1"
  project       = "kiwibank-main"
  force_destroy = false

  versioning {
    enabled = true
  }
}
