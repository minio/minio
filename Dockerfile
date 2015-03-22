
FROM ubuntu:14.04

MAINTAINER Minio Community

ENV GOLANG_TARBALL go1.4.2.linux-amd64.tar.gz

ENV GOROOT /usr/local/go/
ENV GOPATH /go-workspace 
ENV PATH ${GOROOT}/bin:${GOPATH}/bin/:$PATH

RUN apt-get update -y && apt-get install -y -q \
		curl \
		git \
		build-essential \
		ca-certificates \
		yasm \
		python-pip

RUN curl -O -s https://storage.googleapis.com/golang/${GOLANG_TARBALL} && \
		tar -xzf ${GOLANG_TARBALL} -C ${GOROOT%*go*} && \
		rm ${GOLANG_TARBALL} && \
		pip install mkdocs

ADD . ${GOPATH}/src/github.com/minio-io/minio

RUN cd ${GOPATH}/src/github.com/minio-io/minio && \
		make

RUN apt-get remove -y build-essential curl git python-pip && \
        apt-get -y autoremove && \
        rm -rf /var/lib/apt/lists/*

EXPOSE 9000 9001

CMD ["sh", "-c", "${GOPATH}/bin/minio"]

