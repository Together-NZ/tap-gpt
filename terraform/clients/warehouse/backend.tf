terraform {
  # backend "gcs" {
  #   bucket = "warehouse-tfstate1"
  #   prefix = "clients/warehouse/terraform-state"
  # }
}

import {
  to = google_storage_bucket.tfstate
  id = "warehouse-tfstate1"
}

resource "google_storage_bucket" "tfstate" {
  name          = "warehouse-tfstate1"
  location      = "AUSTRALIA-SOUTHEAST1"
  project       = "warehouse-main"
  force_destroy = false

  versioning {
    enabled = true
  }

  depends_on = [
    google_project_service.storage,
  ]
}
