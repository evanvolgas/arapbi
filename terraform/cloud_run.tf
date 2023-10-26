
resource "google_cloud_run_v2_job" "alldividendshistory" {
  name     = "tf-alldividendshistory"
  location = "us-west1"

  template {
    template {
      containers {
        image = "us-west1-docker.pkg.dev/new-life-400922/arapbi/arapbi:latest"
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
              secret  = "polygon"
              version = "latest"
            }
          }
        }
      }
      timeout         = "1800s"
      service_account = "admin-509@new-life-400922.iam.gserviceaccount.com"
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
  location = "us-west1"

  template {
    template {
      containers {
        image = "us-west1-docker.pkg.dev/new-life-400922/arapbi/arapbi:latest"
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
              secret  = "polygon"
              version = "latest"
            }
          }
        }
      }
      timeout         = "1800s"
      service_account = "admin-509@new-life-400922.iam.gserviceaccount.com"
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
  location = "us-west1"

  template {
    template {
      containers {
        image = "us-west1-docker.pkg.dev/new-life-400922/arapbi/arapbi:latest"
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
              secret  = "polygon"
              version = "latest"
            }
          }
        }
      }
      timeout         = "900s"
      service_account = "admin-509@new-life-400922.iam.gserviceaccount.com"
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
  location = "us-west1"

  template {
    template {
      containers {
        image = "us-west1-docker.pkg.dev/new-life-400922/arapbi/arapbi:latest"
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
              secret  = "polygon"
              version = "latest"
            }
          }
        }
      }
      timeout         = "1800s"
      service_account = "admin-509@new-life-400922.iam.gserviceaccount.com"
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
  location = "us-west1"

  template {
    template {
      containers {
        image = "us-west1-docker.pkg.dev/new-life-400922/arapbi/arapbi:latest"
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
              secret  = "polygon"
              version = "latest"
            }
          }
        }
      }
      timeout         = "1800s"
      service_account = "admin-509@new-life-400922.iam.gserviceaccount.com"
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
  location = "us-west1"

  template {
    template {
      containers {
        image = "us-west1-docker.pkg.dev/new-life-400922/arapbi/arapbi:latest"
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
              secret  = "polygon"
              version = "latest"
            }
          }
        }
      }
      timeout         = "1800s"
      service_account = "admin-509@new-life-400922.iam.gserviceaccount.com"
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
  location = "us-west1"

  template {
    template {
      containers {
        image = "us-west1-docker.pkg.dev/new-life-400922/arapbi/arapbi:latest"
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
              secret  = "polygon"
              version = "latest"
            }
          }
        }
      }
      timeout         = "900s"
      service_account = "admin-509@new-life-400922.iam.gserviceaccount.com"
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
  location = "us-west1"

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
        image = "us-west1-docker.pkg.dev/new-life-400922/arapbi/arapbi:latest"
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
      service_account = "admin-509@new-life-400922.iam.gserviceaccount.com"
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
  location = "us-west1"

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
        image = "us-west1-docker.pkg.dev/new-life-400922/arapbi/arapbi:latest"
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
      service_account = "admin-509@new-life-400922.iam.gserviceaccount.com"
    }
  }

  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
}