resource "google_bigquery_dataset" "pinterest_transformed__twhs" {
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "OWNER"
    user_by_email = "peter@wearetogether.co.nz"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }

  access {
    role          = "WRITER"
    user_by_email = "together-meltano@together-internal.iam.gserviceaccount.com"
  }
  dataset_id                 = "pinterest_transformed__twhs"
  delete_contents_on_destroy = false

  labels = {
    managed-by-cnrm = "true"
  }

  location              = "australia-southeast1"
  max_time_travel_hours = "168"
  project               = "warehouse-main"
}
