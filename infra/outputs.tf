output "instance_ip" {
  value       = google_compute_address.static.address
  description = "External IP address of the VM"
}

output "app_url" {
  value       = "https://${var.fqdn}"
  description = "Public URL"
}
