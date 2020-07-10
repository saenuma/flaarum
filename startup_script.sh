#! /bin/bash

sudo snap install flaarum
sudo mkfs.btrfs /dev/sdb
sudo mkdir /var/snap/flaarum/current/data
sudo mount -o discard,defaults /dev/sdb /var/snap/flaarum/current/data
sudo chmod a+w /var/snap/flaarum/current/data
sudo snap stop flaarum.store && sudo snap stop flaarum.tindexer
sudo flaarum.prod c && sudo flaarum.prod mpr
sudo snap start flaarum.store && sudo snap start flaarum.tindexer