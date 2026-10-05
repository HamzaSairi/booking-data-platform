resource "google_bigquery_dataset" "datasets" {
  for_each                        = var.datasets
  dataset_id                      = each.key
  location                        = var.bq_location
  default_table_expiration_ms     = var.bq_sandbox_expiration_ms
  default_partition_expiration_ms = var.bq_sandbox_expiration_ms
  delete_contents_on_destroy      = var.delete_contents_on_destroy

  labels = {
    project    = "booking-data-platform"
    managed_by = "terraform"
  }
}

resource "google_service_account" "pipeline" {
  account_id   = "booking-sa"
  display_name = "Booking pipeline"
}

# dataEditor au niveau DATASET : plus fin que le niveau projet
resource "google_bigquery_dataset_iam_member" "editor" {
  for_each   = google_bigquery_dataset.datasets
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

# jobUser n'existe qu'au niveau projet (lancer des requêtes)
resource "google_project_iam_member" "job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}