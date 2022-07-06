#! /bin/bash
echo "Installing Dependencies"
sudo apt update
sudo apt install nano

echo "Fetching Assets"
rm -rf /opt/saenuma/flaarum
mkdir -p /opt/saenuma/flaarum
wget -q https://storage.googleapis.com/pandolee/flaarum/3/flaarum.tar.xz -O /opt/saenuma/flaarum.tar.xz
tar -xf /opt/saenuma/flaarum.tar.xz -C /opt/saenuma/flaarum

sudo chmod +x /opt/saenuma/flaarum/bin/flcli
sudo chmod +x /opt/saenuma/flaarum/bin/flprod
sudo chmod +x /opt/saenuma/flaarum/bin/flstore

sudo cp /opt/saenuma/flaarum/bin/flstore.service /etc/systemd/system/flstore.service

sudo cp /opt/saenuma/flaarum/bin/flcli /usr/local/bin/
sudo cp /opt/saenuma/flaarum/bin/flprod /usr/local/bin/

echo "Starting Services"
sudo systemctl daemon-reload
sudo systemctl start flstore
sudo systemctl start fldaemon
