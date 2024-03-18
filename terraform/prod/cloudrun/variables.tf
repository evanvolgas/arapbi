variable "region" {
    description = "Which region to run Cloudrun jobs"
    type        = string
    default     = "us-west1"
}

variable "image" {
    description = "The docker image to run"
    type        = string
    default     = "us-west1-docker.pkg.dev/new-life-400922/arapbi/arapbi:latest"
}

variable "timeout" {
    description = "The timeout for the Cloudrun job"
    type        = string
    default     = "1800s"
}

variable "service_account" {
    description = "The service account to run the Cloudrun job"
    type        = string
    default     = "admin-509@new-life-400922.iam.gserviceaccount.com"
}

variable polygon_secret {
    description = "The secret containing the polygon secret"
    type        = string
    default     = "polygon"
}

    variable polygon_secret_version {
    description = "The secret containing the polygon data secret version"
    type        = string
    default     = "latest"
}