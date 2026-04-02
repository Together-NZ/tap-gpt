resource "google_bigquery_dataset" "google_ads_search_transformed__everyday_banking_retail_deposit" {
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

  dataset_id                 = "google_ads_search_transformed__everyday_banking_retail_deposit"
  delete_contents_on_destroy = false

  labels = {
    managed-by-cnrm = "true"
  }

  location              = "australia-southeast1"
  max_time_travel_hours = "168"
  project               = "kiwibank-main"
}
