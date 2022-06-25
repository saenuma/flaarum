#! /bin/bash
echo "Installing Dependencies"
sudo apt update
sudo apt install nano

echo "Updating PATH variable"
sudo echo 'export PATH="$PATH:/opt/saenuma/flaarum"' >> /etc/environment

echo "Fetching Assets"
mkdir -p /opt/saenuma/flaarum
wget -q https://storage.googleapis.com/pandolee/flaarum/1/flaarum.tar.xz -O /opt/saenuma/flaarum.tar.xz
tar -xf /opt/saenuma/flaarum.tar.xz -C /opt/saenuma/flaarum

sudo chmod +x /opt/saenuma/flaarum/flcli
sudo chmod +x /opt/saenuma/flaarum/fldaemon
sudo chmod +x /opt/saenuma/flaarum/flgcpasr
sudo chmod +x /opt/saenuma/flaarum/flprogs
sudo chmod +x /opt/saenuma/flaarum/flstore
sudo chmod +x /opt/saenuma/flaarum/flstatsr

sudo cp /opt/saenuma/flaarum/fldaemon.service /etc/systemd/system/fldaemon.service
sudo cp /opt/saenuma/flaarum/flgcpasr.service /etc/systemd/system/flgcpasr.service
sudo cp /opt/saenuma/flaarum/flstore.service /etc/systemd/system/flstore.service
sudo cp /opt/saenuma/flaarum/flstatsr.service /etc/systemd/system/flstatsr.service

echo "Starting Services"
sudo systemctl daemon-reload
sudo systemctl start flstore
sudo systemctl start fldaemon
