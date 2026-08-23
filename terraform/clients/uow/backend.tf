resource "google_storage_bucket" "tfstate" {
  name          = "uow-tfstate"
  location      = "AUSTRALIA-SOUTHEAST1"
  project       = "uowaikato-main"
  force_destroy = false

  versioning {
    enabled = true
  }
}
