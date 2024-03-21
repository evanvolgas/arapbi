terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "new-life-400922"
  region  = var.region
  zone    = var.region
}

