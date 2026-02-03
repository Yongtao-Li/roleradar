#!/bin/bash
set -euo pipefail

DATA_DISK="/dev/disk/by-id/google-${data_disk_name}"
MOUNT_DIR="/mnt/disks/roleradar-data"

if [ ! -e "$DATA_DISK" ]; then
  echo "Data disk not found: $DATA_DISK"
  exit 1
fi

if ! blkid "$DATA_DISK" >/dev/null 2>&1; then
  mkfs.ext4 -F "$DATA_DISK"
fi

mkdir -p "$MOUNT_DIR"
if ! grep -q "$MOUNT_DIR" /etc/fstab; then
  echo "$DATA_DISK $MOUNT_DIR ext4 defaults 0 2" >> /etc/fstab
fi
mount -a
chmod 777 "$MOUNT_DIR"

mkdir -p /var/lib/caddy /var/lib/caddy/data /var/lib/caddy/config
mkdir -p /var/lib/duckdns

cat > /var/lib/caddy/Caddyfile <<EOF
${fqdn} {
  encode gzip
  reverse_proxy roleradar:8501
}
EOF

docker network inspect roleradar >/dev/null 2>&1 || docker network create roleradar

if [ -n "${ghcr_token}" ] && [ -n "${ghcr_username}" ]; then
  echo "${ghcr_token}" | docker login ghcr.io -u "${ghcr_username}" --password-stdin
fi

docker pull ${image} || true
docker pull caddy:2.8 || true
docker pull linuxserver/duckdns:latest || true

cat > /etc/systemd/system/roleradar.service <<EOF
[Unit]
Description=RoleRadar container
After=docker.service
Requires=docker.service

[Service]
Restart=always
ExecStartPre=-/usr/bin/docker rm -f roleradar
ExecStart=/usr/bin/docker run --name roleradar --restart=unless-stopped --network roleradar -p 127.0.0.1:8501:8501 -v $${MOUNT_DIR}:/app/data ${image}
ExecStop=/usr/bin/docker stop roleradar

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/caddy.service <<EOF
[Unit]
Description=Caddy reverse proxy
After=docker.service roleradar.service
Requires=docker.service

[Service]
Restart=always
ExecStartPre=-/usr/bin/docker rm -f caddy
ExecStart=/usr/bin/docker run --name caddy --restart=unless-stopped --network roleradar -p 80:80 -p 443:443 -v /var/lib/caddy/Caddyfile:/etc/caddy/Caddyfile -v /var/lib/caddy/data:/data -v /var/lib/caddy/config:/config caddy:2.8
ExecStop=/usr/bin/docker stop caddy

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/duckdns.service <<EOF
[Unit]
Description=DuckDNS updater
After=docker.service
Requires=docker.service

[Service]
Restart=always
ExecStartPre=-/usr/bin/docker rm -f duckdns
ExecStart=/usr/bin/docker run --name duckdns --restart=unless-stopped -e SUBDOMAINS=${duckdns_subdomain} -e TOKEN=${duckdns_token} -e LOG_FILE=true -v /var/lib/duckdns:/config linuxserver/duckdns:latest
ExecStop=/usr/bin/docker stop duckdns

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now roleradar.service caddy.service duckdns.service
