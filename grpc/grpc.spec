#-------------------------------------------------------------------------------
# When using 'make install' with prefix option,
# the following target 'install-header_cxx' will generate
# a line which is too long, ending in the following error:
#
# > [INSTALL] Installing public C++ headers
# > make: execvp: /bin/sh: Argument list too long
# > make: *** [install-headers_cxx] Error 127
# > Makefile:3021: recipe for target 'install-headers_cxx' failed
#
# Issue tracked at:
# https://github.com/grpc/grpc/pull/14844
#-------------------------------------------------------------------------------

%if 0%{?rhel} == 7
  # CentOS 7 can use ".el7.centos" or ".el7.cern". However, we want to avoid that
  # because keeping the ".cern/centos" part will make the compilation fail
  # due to the issue described in the note above.
  %define dist .el7
%endif

#-------------------------------------------------------------------------------
# Custom strip command for SLC6, CC7
#-------------------------------------------------------------------------------
%define distribution %(/usr/lib/rpm/redhat/dist.sh --distnum)
%if 0%{distribution} == 6 || 0%{distribution} == 7
%global __strip /opt/rh/devtoolset-8/root/usr/bin/strip
%endif

#-------------------------------------------------------------------------------
# Package definitions
#-------------------------------------------------------------------------------
Summary: gRPC, A high performance, open-source universal RPC framework
Name: grpc
Version: 1.19.0
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
git checkout -b %{version} tags/v%{version}
git submodule update --init --recursive
%build
cd grpc
%if %{?fedora}%{!?fedora:0} >= 19
export CPPFLAGS="-Wno-error=class-memaccess -Wno-error=tautological-compare -Wno-error=ignored-qualifiers -Wno-error=stringop-truncation"
export HAS_SYSTEM_PROTOBUF=false
%endif
%if 0%{?rhel} == 6
# Fix for SLC6 compilation and autotools
mkdir third_party/protobuf/third_party/googletest/m4
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
