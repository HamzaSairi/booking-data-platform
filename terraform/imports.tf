import {
  for_each = var.datasets
  to       = google_bigquery_dataset.datasets[each.key]
  id       = "projects/${var.project_id}/datasets/${each.key}"
}

import {
  to = google_service_account.pipeline
  id = "projects/${var.project_id}/serviceAccounts/booking-sa@${var.project_id}.iam.gserviceaccount.com"
}