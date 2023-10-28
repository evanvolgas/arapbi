terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = file("terraform.json")

  project = "new-life-400922"
  region  = "us-west1"
  zone    = "us-west1"
}

resource "google_compute_network" "vpc_network" {
  name = "terraform-network"
}


terraform {
  backend "gcs" {
    bucket = "arapbi-tf-state-prod"
    prefix = "terraform/state"
  }
}

