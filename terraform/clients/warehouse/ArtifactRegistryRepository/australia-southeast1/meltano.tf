resource "google_artifact_registry_repository" "meltano" {
  description = "Meltano repository"
  format      = "DOCKER"

  labels = {
    managed-by-cnrm = "true"
  }

  location      = "australia-southeast1"
  mode          = "STANDARD_REPOSITORY"
  project       = "warehouse-main"
  repository_id = "meltano"
}
