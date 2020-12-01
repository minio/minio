%define         tag     RELEASE.2020-11-25T22-36-25Z
%define         subver  %(echo %{tag} | sed -e 's/[^0-9]//g')
# git fetch https://github.com/minio/minio.git refs/tags/RELEASE.2020-11-25T22-36-25Z
# git rev-list -n 1 FETCH_HEAD
%define         commitid        91130e884b5df59d66a45a0aad4f48db88f5ca63
Summary:        High Performance, Kubernetes Native Object Storage.
Name:           minio
Version:        0.0.%{subver}
Release:        1
Vendor:         MinIO, Inc.
License:        Apache v2.0
Group:          Applications/File
Source0:        https://dl.minio.io/server/minio/release/linux-amd64/archive/minio.%{tag}
Source1:        https://raw.githubusercontent.com/minio/minio-service/master/linux-systemd/distributed/minio.service
URL:            https://www.min.io/
Requires(pre):  /usr/sbin/useradd, /usr/bin/getent
Requires(postun): /usr/sbin/userdel
BuildRoot:      %{tmpdir}/%{name}-%{version}-root-%(id -u -n)

## Disable debug packages.
%define         debug_package %{nil}

%description
MinIO is a High Performance Object Storage released under Apache License v2.0.
It is API compatible with Amazon S3 cloud storage service. Use MinIO to build
high performance infrastructure for machine learning, analytics and application
data workloads.

%pre
/usr/bin/getent group minio-user || /usr/sbin/groupadd -r minio-user
/usr/bin/getent passwd minio-user || /usr/sbin/useradd -r -d /etc/minio -s /sbin/nologin minio-user

%install
rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT/etc/minio/certs
install -d $RPM_BUILD_ROOT/etc/systemd/system
install -d $RPM_BUILD_ROOT/etc/default
install -d $RPM_BUILD_ROOT/usr/local/bin

cat <<EOF >> $RPM_BUILD_ROOT/etc/default/minio
# Remote volumes to be used for MinIO server.
# Uncomment line before starting the server.
# MINIO_VOLUMES=http://node{1...6}/export{1...32}

# Root credentials for the server.
# Uncomment both lines before starting the server.
# MINIO_ACCESS_KEY=Server-Access-Key
# MINIO_SECRET_KEY=Server-Secret-Key

MINIO_OPTS="--certs-dir /etc/minio/certs"
EOF

install %{_sourcedir}/minio.service $RPM_BUILD_ROOT/etc/systemd/system/minio.service
install -p %{_sourcedir}/%{name}.%{tag} $RPM_BUILD_ROOT/usr/local/bin/minio

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root,755)
%attr(644,root,root) /etc/default/minio
%attr(644,root,root) /etc/systemd/system/minio.service
%attr(644,minio-user,minio-user) /etc/minio
%attr(755,minio-user,minio-user) /usr/local/bin/minio
