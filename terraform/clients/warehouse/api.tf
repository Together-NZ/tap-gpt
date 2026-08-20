resource "google_project_service" "artifact_registry" {
  project = "warehouse-main"
  service = "artifactregistry.googleapis.com"

  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  project = "warehouse-main"
  service = "bigquery.googleapis.com"

  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  project = "warehouse-main"
  service = "storage.googleapis.com"

  disable_on_destroy = false
}

resource "google_project_service" "iam" {
  project = "warehouse-main"
  service = "iam.googleapis.com"

  disable_on_destroy = false
}
