# RoleRadar

A lightweight job-tracking tool that monitors selected company career pages
and highlights new postings with keyword and location filters.

## Why
LinkedIn is noisy and does not reliably show what is new.
This tool provides deterministic, daily visibility into roles that matter.

## Features
- Multi-company tracking (e.g. MathWorks, Amazon)
- Daily diff of new postings
- Keyword search
- Location filtering (multi-select)
- Simple SQLite storage
- Streamlit UI

## Tech Stack
- Python
- SQLite
- Streamlit

## Status
Feature-frozen. Intended for personal use and experimentation.

## Deploy on GCP Free Tier (Terraform + DuckDNS)

This setup mirrors the architecture from https://github.com/eatonc/actual-gcp:
- GCE e2-micro on Container-Optimized OS
- Docker containers for app + Caddy + DuckDNS
- Persistent disk for SQLite data

### Prerequisites
- GCP account with billing enabled
- DuckDNS subdomain (roleradar.duckdns.org) and token
- Terraform installed
- gcloud CLI installed and authenticated
- GitHub repo with GHCR build (workflow included in this repo)

### 1) Create a GCP project
Create a project (example ID: roleradar-486220) and attach billing in the GCP console.

Enable the Compute Engine API:
```bash
gcloud services enable compute.googleapis.com --project roleradar-486220
```

Authenticate ADC for Terraform:
```bash
gcloud auth application-default login
```

### 2) Build and publish the app image to GHCR
This repo includes a GitHub Actions workflow that builds and publishes:

`ghcr.io/bunnybryna/roleradar:latest`

Push to the `master` branch to trigger a build.

If the repository is private, set the image package to public or provide GHCR credentials in Terraform (see below).

### 3) Configure Terraform variables
Create `infra/sensitive.auto.tfvars` (gitignored) with your values:

```hcl
project_id       = "roleradar-486220"
region           = "us-east1"
zone             = "us-east1-b"
fqdn             = "roleradar.duckdns.org"
duckdns_subdomain = "roleradar"
duckdns_token    = "REPLACE_ME"

# Optional (only for private GHCR images)
# ghcr_username = "bunnybryna"
# ghcr_token    = "REPLACE_ME"
```

### 4) Deploy
```bash
cd infra
terraform init
terraform plan
terraform apply
```

After apply completes, Terraform outputs the VM IP and app URL.

### 5) Verify
- Open `https://roleradar.duckdns.org`
- Validate containers (optional):

```bash
gcloud compute ssh roleradar --zone us-east1-b --project roleradar-486220
docker ps
```

You should see three containers: `roleradar`, `caddy`, and `duckdns`.

### Data persistence
SQLite files are stored on a persistent disk mounted to `/mnt/disks/roleradar-data` on the VM and mounted into the container at `/app/data`.

### Updating the app
1) Push to `master` to publish a new image.
2) On the VM, pull and restart:

```bash
docker pull ghcr.io/bunnybryna/roleradar:latest
sudo systemctl restart roleradar
```

### Troubleshooting
If SSH times out, enable IAP TCP forwarding in Terraform:

```hcl
allow_ssh_iap = true
```

Then apply and connect using:

```bash
gcloud compute ssh roleradar --zone us-east1-b --project roleradar-486220 --tunnel-through-iap
```
