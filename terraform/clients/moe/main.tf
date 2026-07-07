provider "google" {
  project = "together-internal"
  region  = "australia-southeast1"
  zone    = "australia-southeast1-a"  # Sydney zone
}




module "google_iam_service_account" {
  source = "./IamServiceAccount"
}

module "bigquerydataset" {
  source = "./BigQueryDataset"
}