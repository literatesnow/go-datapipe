#!/usr/bin/env bash

export DEBIAN_FRONTEND=noninteractive
export DEBCONF_NONINTERACTIVE_SEEN=true

UBUNTU_MIRROR=nz.archive.ubuntu.com

VAGRANT_CACHE_DIR=/vagrant/files/cache
GO_FILE=$VAGRANT_CACHE_DIR/go1.7.linux-amd64.tar.gz

[ ! -d "$VAGRANT_CACHE_DIR" ] && mkdir -p "$VAGRANT_CACHE_DIR"

echo "Install packages..."
if [ -n "$UBUNTU_MIRROR" ]; then
  sed -i'' "s/http:\/\/archive.ubuntu.com/http:\/\/${UBUNTU_MIRROR}/g" /etc/apt/sources.list || exit 1
fi
apt-get -y update || exit 1
apt-get -y install \
  locales curl || exit 1

echo "Setup locales"
locale-gen en_NZ.UTF-8 || exit 1
dpkg-reconfigure locales || exit 1
echo "LANG=\"en_NZ.UTF-8\"" > /etc/default/locale || exit 1

echo "Setup timezone"
timedatectl set-timezone 'Pacific/Auckland' || exit 1
timedatectl status || exit 1

echo "Install go"
[ ! -f "$GO_FILE" ] && curl -LsSk -o "$GO_FILE" https://storage.googleapis.com/golang/go1.7.linux-amd64.tar.gz
sha256sum "$GO_FILE" | grep "702ad90f705365227e902b42d91dd1a40e48ca7f67a2f4b2fd052aaa4295cd95" || exit 1
tar -C /usr/local -xzf "$GO_FILE" || exit 1

echo "Setup environment"
echo "# Vagrant settings
export PATH=\$PATH:/opt/go/bin:/usr/local/go/bin
export GOPATH=/opt/go

alias ll='ls -l'
alias vim='vi'

cd /opt/go/src/github.com/literatesnow/go-datapipe
" >> /home/ubuntu/.profile || exit 1
sed -i'' 's/"$color_prompt" = yes/"$color_prompt" = _vagrant_disabled_/' /home/ubuntu/.bashrc

echo "Set permissions"
chown -R ubuntu:ubuntu /home/ubuntu/ || exit 1
