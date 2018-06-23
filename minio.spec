%define         tag     RELEASE.2017-04-25T01-27-49Z
%define         subver  %(echo %{tag} | sed -e 's/[^0-9]//g')
# git fetch https://github.com/minio/minio.git refs/tags/RELEASE.2017-02-16T01-47-30Z
# git rev-list -n 1 FETCH_HEAD
%define         commitid        83abb310b4ce3a0dfc6d7faf78e33cb6f9132cfe
Summary:        Cloud Storage Server.
Name:           minio
Version:        0.0.%{subver}
Release:        1
Vendor:         Minio, Inc.
License:        Apache v2.0
Group:          Applications/File
Source0:        https://github.com/minio/minio/archive/%{tag}.tar.gz
URL:            https://www.minio.io/
BuildRequires:  golang >= 1.7
BuildRoot:      %{tmpdir}/%{name}-%{version}-root-%(id -u -n)

## Disable debug packages.
%define         debug_package %{nil}

## Go related tags.
%define         gobuild(o:) go build -ldflags "${LDFLAGS:-}" %{?**};
%define         gopath          %{_libdir}/golang
%define         import_path     github.com/minio/minio

%description
Minio is an object storage server released under Apache License v2.0.
It is compatible with Amazon S3 cloud storage service. It is best
suited for storing unstructured data such as photos, videos, log
files, backups and container / VM images. Size of an object can
range from a few KBs to a maximum of 5TiB.

%prep
%setup -qc
mv %{name}-*/* .

install -d src/$(dirname %{import_path})
ln -s ../../.. src/%{import_path}

%build
export GOPATH=$(pwd)

# setup flags like 'go run buildscripts/gen-ldflags.go' would do
tag=%{tag}
version=${tag#RELEASE.}
commitid=%{commitid}
scommitid=$(echo $commitid | cut -c1-12)
prefix=%{import_path}/cmd

LDFLAGS="
-X $prefix.Version=$version
-X $prefix.ReleaseTag=$tag
-X $prefix.CommitID=$commitid
-X $prefix.ShortCommitID=$scommitid
"

%gobuild -o %{name} %{import_path}

# check that version set properly
./%{name} version | tee v

#Version: 2016-09-11T17-42-18Z
#Release-Tag: RELEASE.2016-09-11T17-42-18Z
#Commit-ID: 85e2d886bcb005d49f3876d6849a2b5a55e03cd3
v=$(awk '/Version:/{print $2}' v)
test "$v" = $version
v=$(awk '/Release-Tag:/{print $2}' v)
test "$v" = $tag
v=$(awk '/Commit-ID:/{print $2}' v)
test "$v" = $commitid

%install
rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT%{_sbindir}
install -p %{name} $RPM_BUILD_ROOT%{_sbindir}

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root,755)
%doc README.md README_ZH.md
%attr(755,root,root) %{_sbindir}/minio
