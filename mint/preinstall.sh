#!/bin/bash -e
#
#

export APT="apt --quiet --yes"
export WGET="wget --quiet --no-check-certificate"

# install nodejs source list
if ! $WGET --output-document=- https://deb.nodesource.com/setup_14.x | bash -; then
    echo "unable to set nodejs repository"
    exit 1
fi

$APT install apt-transport-https

if ! $WGET --output-document=packages-microsoft-prod.deb https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb | bash -; then
    echo "unable to download dotnet packages"
    exit 1
fi

dpkg -i packages-microsoft-prod.deb
rm -f packages-microsoft-prod.deb

$APT update
$APT install gnupg ca-certificates

# download and install golang
GO_VERSION="1.16"
GO_INSTALL_PATH="/usr/local"
download_url="https://storage.googleapis.com/golang/go${GO_VERSION}.linux-amd64.tar.gz"
if ! $WGET --output-document=- "$download_url" | tar -C "${GO_INSTALL_PATH}" -zxf -; then
    echo "unable to install go$GO_VERSION"
    exit 1
fi

xargs --arg-file="${MINT_ROOT_DIR}/install-packages.list" apt --quiet --yes install

# set python 3.6 as default
update-alternatives --install /usr/bin/python python /usr/bin/python3.6 1

sync
