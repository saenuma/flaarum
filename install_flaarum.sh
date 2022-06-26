#! /bin/bash
echo "Installing Dependencies"
sudo apt update
sudo apt install nano

echo "Updating PATH variable"
if [ ! -d "/opt/saenuma/flaarum/" ]
then
  sudo echo 'PATH="$PATH:/opt/saenuma/flaarum/bin/"' >> /etc/environment
fi

echo "Fetching Assets"
rm -rf /opt/saenuma/flaarum
mkdir -p /opt/saenuma/
wget -q https://storage.googleapis.com/pandolee/flaarum/2/flaarum.tar.xz -O /opt/saenuma/flaarum.tar.xz
tar -xf /opt/saenuma/flaarum.tar.xz -C /opt/saenuma/flaarum

sudo chmod +x /opt/saenuma/flaarum/bin/flcli
sudo chmod +x /opt/saenuma/flaarum/bin/fldaemon
sudo chmod +x /opt/saenuma/flaarum/bin/flprogs
sudo chmod +x /opt/saenuma/flaarum/bin/flstore

sudo cp /opt/saenuma/flaarum/bin/fldaemon.service /etc/systemd/system/fldaemon.service
sudo cp /opt/saenuma/flaarum/bin/flstore.service /etc/systemd/system/flstore.service

echo "Starting Services"
sudo systemctl daemon-reload
sudo systemctl start flstore
sudo systemctl start fldaemon
