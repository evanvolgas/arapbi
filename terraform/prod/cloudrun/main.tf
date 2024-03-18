
terraform {
  backend "gcs" {
    bucket = "arapbi_terraform"
    prefix = "prod/cloudrun/"
  }
}


resource "google_cloud_run_v2_job" "alldividendshistory" {
  name     = "tf-alldividendshistory"
  location = var.region

  template {
    template {
      containers {
        image = var.image
        args  = ["all_dividends_history"]
        resources {
          limits = {
            cpu    = 2
            memory = "1Gi"
          }
        }
        env {
          name = "POLYGON_API_KEY"
          value_source {
            secret_key_ref {
              secret  = var.polygon_secret
              version = var.polygon_secret_version
            }
          }
        }
      }
      timeout         = var.timeout
      service_account = var.service_account
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}

resource "google_cloud_run_v2_job" "allcryptohistory" {
  name     = "tf-allcryptohistory"
  location = var.region

  template {
    template {
      containers {
        image = var.image
        args  = ["all_crypto_history"]
        resources {
          limits = {
            cpu    = 2
            memory = "1Gi"
          }
        }
        env {
          name = "POLYGON_API_KEY"
          value_source {
            secret_key_ref {
              secret  = var.polygon_secret
              version = var.polygon_secret_version
            }
          }
        }
      }
      timeout         = var.timeout
      service_account = var.service_account
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}


resource "google_cloud_run_v2_job" "alltickertypes" {
  name     = "tf-alltickertypes"
  location = var.region

  template {
    template {
      containers {
        image = var.image
        args  = ["all_ticker_types"]
        resources {
          limits = {
            cpu    = 1
            memory = "1Gi"
          }
        }
        env {
          name = "POLYGON_API_KEY"
          value_source {
            secret_key_ref {
              secret  = var.polygon_secret
              version = var.polygon_secret_version
            }
          }
        }
      }
      timeout         = "900s"
      service_account = var.service_account
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}

resource "google_cloud_run_v2_job" "alltickers" {
  name     = "tf-alltickers"
  location = var.region

  template {
    template {
      containers {
        image = var.image
        args  = ["all_ticker_types"]
        resources {
          limits = {
            cpu    = 1
            memory = "2Gi"
          }
        }
        env {
          name = "POLYGON_API_KEY"
          value_source {
            secret_key_ref {
              secret  = var.polygon_secret
              version = var.polygon_secret_version
            }
          }
        }
      }
      timeout         = var.timeout
      service_account = var.service_account
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}


resource "google_cloud_run_v2_job" "alltickersdetail" {
  name     = "tf-alltickersdetail"
  location = var.region

  template {
    template {
      containers {
        image = var.image
        args  = ["all_tickers_detail"]
        resources {
          limits = {
            cpu    = 1
            memory = "4Gi"
          }
        }
        env {
          name = "POLYGON_API_KEY"
          value_source {
            secret_key_ref {
              secret  = var.polygon_secret
              version = var.polygon_secret_version
            }
          }
        }
      }
      timeout         = var.timeout
      service_account = var.service_account
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}


resource "google_cloud_run_v2_job" "alltickershistory" {
  name     = "tf-alltickershistory"
  location = var.region

  template {
    template {
      containers {
        image = var.image
        args  = ["all_tickers_history"]
        resources {
          limits = {
            cpu    = 2
            memory = "8Gi"
          }
        }
        env {
          name = "POLYGON_API_KEY"
          value_source {
            secret_key_ref {
              secret  = var.polygon_secret
              version = var.polygon_secret_version
            }
          }
        }
      }
      timeout         = var.timeout
      service_account = var.service_account
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}

resource "google_cloud_run_v2_job" "alltickersfinancials" {
  name     = "tf-alltickersfinancials"
  location = var.region

  template {
    template {
      containers {
        image = var.image
        args  = ["all_financials"]
        resources {
          limits = {
            cpu    = 1
            memory = "2Gi"
          }
        }
        env {
          name = "POLYGON_API_KEY"
          value_source {
            secret_key_ref {
              secret  = var.polygon_secret
              version = var.polygon_secret_version
            }
          }
        }
      }
      timeout         = "900s"
      service_account = var.service_account
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}


resource "google_cloud_run_v2_job" "dbtrun" {
  name     = "tf-dbtrun"
  location = var.region

  template {
    template {
      volumes {
        name = "secret-volume"
        secret {
          secret       = "dbt_credentials"
          default_mode = 292 # 0444
         items {
            version = "latest"
            path    = "dbt_credentials"
            mode    = 256 # 0400
          }
        }
      }
      containers {
        image = var.image
        volume_mounts {
          name       = "secret-volume"
          mount_path = "/secrets"
        }
        args = ["run"]
        resources {
          limits = {
            cpu    = 1
            memory = "2Gi"
          }
        }
        env {
          name  = "GOOGLE_APPLICATION_CREDENTIALS"
          value = "/secrets/dbt_credentials"
        }
      }
      timeout         = "900s"
      service_account = var.service_account
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}

resource "google_cloud_run_v2_job" "dbttest" {
  name     = "tf-dbttest"
  location = var.region

  template {
    template {
      volumes {
        name = "secret-volume"
        secret {
          secret       = "dbt_credentials"
          default_mode = 292 # 0444
          items {
            version = "latest"
            path    = "dbt_credentials"
            mode    = 256 # 0400
          }
        }
      }
      containers {
        image = var.image
        volume_mounts {
          name       = "secret-volume"
          mount_path = "/secrets"
        }
        args = ["test"]
        resources {
          limits = {
            cpu    = 1
            memory = "2Gi"
          }
        }
        env {
          name  = "GOOGLE_APPLICATION_CREDENTIALS"
          value = "/secrets/dbt_credentials"
        }
      }
      timeout         = "900s"
      service_account = var.service_account
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}