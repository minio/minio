#-------------------------------------------------------------
# Stage 1: Build and Unit tests
#-------------------------------------------------------------
FROM golang:1.12

COPY . /go/src/github.com/minio/minio
WORKDIR /go/src/github.com/minio/minio

RUN apt-get update && apt-get install -y jq
ENV GO111MODULE=on

RUN git config --global http.cookiefile /gitcookie/.gitcookie

RUN apt-get update && \
      apt-get -y install sudo
RUN touch /etc/sudoers

RUN echo "root ALL=(ALL) ALL" >> /etc/sudoers
RUN echo "ci ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN echo "Defaults    env_reset" >> /etc/sudoers
RUN echo 'Defaults    secure_path="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/go:/usr/local/go/bin"' >> /etc/sudoers

RUN mkdir -p /home/ci/.cache

RUN groupadd -g 999 ci && \
    useradd -r -u 999 -g ci ci && \
    chown -R ci:ci /go /home/ci && \
    chmod -R a+rw /go

USER ci

# -- tests --
RUN make
RUN bash -c 'diff -au <(gofmt -s -d cmd) <(printf "")'
RUN bash -c 'diff -au <(gofmt -s -d pkg) <(printf "")'
RUN for d in $(go list ./... | grep -v browser); do go test -v -race --timeout 15m "$d"; done
RUN make verifiers
RUN make crosscompile
RUN make verify

#-------------------------------------------------------------
# Stage 2: Test Frontend
#-------------------------------------------------------------
FROM node:10.15-stretch-slim

COPY browser /minio/browser
WORKDIR /minio/browser

RUN yarn
RUN yarn test

#-------------------------------------------------------------
# Stage 3: Run Gateway Tests
#-------------------------------------------------------------
FROM ubuntu:16.04

COPY --from=0 /go/src/github.com/minio/minio/minio /usr/bin/minio
COPY buildscripts/gateway-tests.sh /usr/bin/gateway-tests.sh

ENV DEBIAN_FRONTEND noninteractive
ENV LANG C.UTF-8
ENV GOROOT /usr/local/go
ENV GOPATH /usr/local
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

RUN apt-get --yes update && apt-get --yes upgrade && apt-get --yes --quiet install wget jq curl git dnsmasq && \
    git clone https://github.com/minio/mint.git /mint && \
    cd /mint && /mint/release.sh

WORKDIR /mint

RUN /usr/bin/gateway-tests.sh
