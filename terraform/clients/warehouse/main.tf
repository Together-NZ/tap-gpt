provider "google" {
  project = "warehouse-main"
  region  = "australia-southeast1"
  zone    = "australia-southeast1-a"
}

module "google_iam_service_account" {
  source = "./IamServiceAccount"

  depends_on = [
    google_project_service.iam,
  ]
}

module "bigquerydataset" {
  source = "./BigQueryDataset"

  depends_on = [
    google_project_service.bigquery,
  ]
}

module "google_artifact_registry_repository" {
  source = "./ArtifactRegistryRepository/australia-southeast1"

  depends_on = [
    google_project_service.artifact_registry,
  ]
}
