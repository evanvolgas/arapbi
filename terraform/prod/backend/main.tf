terraform {
  backend "gcs" {
    bucket = "arapbi_terraform"
    prefix = "global/backend/"
  }
}

resource "google_compute_network" "vpc_network" {
  name = "arapbi-network"
}




