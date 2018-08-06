Summary: gRPC, A high performance, open-source universal RPC framework
Name: grpc
Version: 1.13.1
Release: 1%{?dist}
License: BSD
URL: http://www.grpc.io/
Source0: https://github.com/grpc/grpc/archive/v%{version}.tar.gz

BuildRequires: pkgconfig gcc-c++
BuildRequires: protobuf-devel protobuf-compiler openssl-devel c-ares-devel

%description
Remote Procedure Calls (RPCs) provide a useful abstraction for
building distributed applications and services. The libraries in this
package provide a concrete implementation of the gRPC protocol,
layered over HTTP/2. These libraries enable communication between
clients and servers using any combination of the supported languages.

%package plugins
Summary: gRPC protocol buffers compiler plugins
Requires: %{name}%{?_isa} = %{version}-%{release}
Requires: protobuf-compiler

%description plugins
Plugins to the protocol buffers compiler to generate gRPC sources.

%package devel
Summary: gRPC library development files
Requires: %{name}%{?_isa} = %{version}-%{release}

%description devel
Development headers and files for gRPC libraries.

%package static
Summary: gRPC library static files
Requires: %{name}-devel%{?_isa} = %{version}-%{release}

%description static
Static libraries for gRPC.

%prep
rm -rf grpc
git clone https://github.com/grpc/grpc
cd grpc
git submodule update --init --recursive
git checkout v%{version}
git checkout -b v1.13.1
%build
cd grpc
export CPPFLAGS="-Wno-error=class-memaccess"
%if 0%{?rhel} == 6
make -j 4 
%else
%make_build
%endif

%check

%install
cd grpc
rm -rf %{buildroot}; mkdir %{buildroot}
make install prefix="%{buildroot}/usr"
%ifarch x86_64
mkdir -p %{buildroot}/usr/lib64
mv %{buildroot}/usr/lib/* %{buildroot}/usr/lib64/
%endif

%clean
rm -rf %{buildroot}

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%files
%{_libdir}/*.so.*
%{_datadir}/grpc

%files plugins
%{_bindir}/*

%files devel
%{_libdir}/*.so
%{_libdir}/pkgconfig/*
%{_includedir}/*

%files static
%{_libdir}/*.a

%changelog
* Fri Jul 27 2018 AJP
- Initial revision
