resource "google_compute_network" "main" {
  name                    = "${var.instance_name}-net"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "main" {
  name          = "${var.instance_name}-subnet"
  region        = var.region
  network       = google_compute_network.main.id
  ip_cidr_range = "10.10.0.0/24"
}

resource "google_compute_firewall" "allow_web" {
  name    = "${var.instance_name}-allow-web"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["80", "443"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["roleradar-web"]
}

resource "google_compute_firewall" "allow_ssh_iap" {
  count   = var.allow_ssh_iap ? 1 : 0
  name    = "${var.instance_name}-allow-ssh-iap"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = var.ssh_source_ranges
  target_tags   = ["roleradar-web"]
}
