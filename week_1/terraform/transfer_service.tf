# Used with reference: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_transfer_job

data "google_storage_transfer_project_service_account" "default" {
  project = var.project
}

resource "google_storage_bucket" "transfer-service-terraform" {
  # Need to create a unique name for the bucket
  # Joining names from variable.tf
  name          = "${local.data_lake_bucket}_${var.project}_transfer_service_terraform"
  storage_class = "STANDARD"
  project       = var.project
  location      = "EU"
}

# I can refer to resource above as "transfer-service-terraform"
# Don't need a full name as above in name declaration
resource "google_storage_bucket_iam_member" "transfer-service-terraform-iam" {
  bucket     = google_storage_bucket.transfer-service-terraform.name
  role       = "roles/storage.admin"
  member     = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
  depends_on = [google_storage_bucket.transfer-service-terraform]
}

resource "google_storage_transfer_job" "s3-bucket-nightly-backup" {
  description = "Execute a cloud transfer job via Terraform"
  project     = var.project

  transfer_spec {
    transfer_options {
      delete_objects_unique_in_sink = false
    }
    aws_s3_data_source {
      bucket_name = "nyc-tlc"
      aws_access_key {
        access_key_id     = var.aws_access_key
        secret_access_key = var.aws_secret_key
      }
    }
    gcs_data_sink {
      bucket_name = google_storage_bucket.transfer-service-terraform.name
      path        = ""
    }
  }

  schedule {
    schedule_start_date {
      year  = 2023
      month = 1
      day   = 22
    }
    schedule_end_date {
      year  = 2023
      month = 1
      day   = 22
    }
  }

  depends_on = [google_storage_bucket_iam_member.transfer-service-terraform-iam]
}