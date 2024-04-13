#!/bin/bash

# Update and Upgrade
sudo apt update && sudo apt upgrade -y

# Install Required Packages
sudo apt install curl git -y

# Install Docker
sudo apt-get remove docker docker-engine docker.io containerd runc
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

#postgressql gui
sudo apt install postgresql-client

echo "System setup is complete. Please log out and log back in if you added your user to the docker group."
