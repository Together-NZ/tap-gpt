resource "google_project_iam_member" "sa_artifact_registry_writer" {
  project = "warehouse-main"
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:impact-ci-ci-cd-service-accoun@together-internal.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "sa_cloud_object_write" {
  project = "warehouse-main"
  role    = "roles/composer.environmentAndStorageObjectViewer"
  member  = "serviceAccount:impact-ci-ci-cd-service-accoun@together-internal.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "sa_object_admin" {
  project = "warehouse-main"
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:impact-ci-ci-cd-service-accoun@together-internal.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "meltano_bigquery_data_editor" {
  project = "warehouse-main"
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:together-meltano@together-internal.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "meltano_bigquery_data_viewer" {
  project = "warehouse-main"
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:together-meltano@together-internal.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "meltano_bigquery_job_user" {
  project = "warehouse-main"
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:together-meltano@together-internal.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "meltano_artifact_registry_reader" {
  project = "warehouse-main"
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:together-meltano@together-internal.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "meltano_storage_object_admin" {
  project = "warehouse-main"
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:together-meltano@together-internal.iam.gserviceaccount.com"
}
