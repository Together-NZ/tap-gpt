resource "google_project_iam_member" "sa_artifact_registry_writer" {
  project = "moe-main"
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:impact-ci-ci-cd-service-accoun@together-internal.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "sa_cloud_object_write" {
    project="moe-main"
    role="roles/composer.environmentAndStorageObjectViewer"
    member="serviceAccount:impact-ci-ci-cd-service-accoun@together-internal.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "sa_object_admin" {
    project="moe-main"
    role="roles/storage.objectAdmin"
    member = "serviceAccount:impact-ci-ci-cd-service-accoun@together-internal.iam.gserviceaccount.com"
}