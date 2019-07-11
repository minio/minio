FROM ubuntu:16.04

ENV DEBIAN_FRONTEND noninteractive

ENV LANG C.UTF-8

ENV GOROOT /usr/local/go

ENV GOPATH /usr/local

ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

RUN apt-get --yes update && apt-get --yes upgrade && \
    apt-get --yes --quiet install wget jq curl git dnsmasq && \
    git clone https://github.com/minio/minio.git /minio && \
    ln -sf /minio/mint /mint && /mint/release.sh

WORKDIR /mint

ENTRYPOINT ["/mint/entrypoint.sh"]
