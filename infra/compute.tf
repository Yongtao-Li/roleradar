resource "google_compute_address" "static" {
  name   = "${var.instance_name}-ip"
  region = var.region
}

resource "google_compute_disk" "data" {
  name = "${var.instance_name}-data"
  type = "pd-standard"
  zone = var.zone
  size = var.data_disk_size_gb
}

resource "google_compute_instance" "app" {
  name         = var.instance_name
  machine_type = var.machine_type
  zone         = var.zone
  tags         = ["roleradar-web"]

  boot_disk {
    initialize_params {
      image = "projects/cos-cloud/global/images/family/cos-stable"
      size  = 10
      type  = "pd-standard"
    }
  }

  attached_disk {
    source      = google_compute_disk.data.id
    device_name = google_compute_disk.data.name
  }

  network_interface {
    subnetwork = google_compute_subnetwork.main.id
    access_config {
      nat_ip = google_compute_address.static.address
    }
  }

  metadata_startup_script = templatefile("${path.module}/startup.sh", {
    fqdn             = var.fqdn
    duckdns_subdomain = var.duckdns_subdomain
    duckdns_token    = var.duckdns_token
    image            = var.ghcr_image
    ghcr_username    = var.ghcr_username
    ghcr_token       = var.ghcr_token
    data_disk_name   = google_compute_disk.data.name
  })
}
