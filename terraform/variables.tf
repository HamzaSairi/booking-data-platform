variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "bq_location" {
  type    = string
  default = "EU"
}

variable "datasets" {
  type    = set(string)
  default = ["raw_booking", "staging_booking", "marts_booking"]
}

variable "delete_contents_on_destroy" {
  description = "Autorise destroy à supprimer des datasets non vides"
  type        = bool
  default     = false
}

variable "bq_sandbox_expiration_ms" {
  description = "Expiration imposée par le BigQuery sandbox (60 j). null si la facturation est activée."
  type        = number
  default     = 5184000000
}
