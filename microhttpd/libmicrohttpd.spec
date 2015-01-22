%define _unpackaged_files_terminate_build 0 
Summary: Lightweight library for embedding a webserver in applications
Name: libmicrohttpd
Version: 0.9.38
Release: eos.ves%{?dist}
Group: Development/Libraries
License: LGPLv2+
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
URL: http://www.gnu.org/software/libmicrohttpd/
Source0: ftp://ftp.gnu.org/gnu/libmicrohttpd/libmicrohttpd-%{version}.tar.gz

BuildRequires:  autoconf, automake, libtool
%if 0%{?rhel} == 5
BuildRequires:	curl-devel
%endif
%if 0%{?fedora} >= 11 || 0%{?rhel} >= 6
BuildRequires:	libcurl-devel
%endif
BuildRequires:  gnutls-devel
BuildRequires:  libgcrypt-devel
BuildRequires:  graphviz
BuildRequires:  doxygen

Requires(post): info
Requires(preun): info

Patch0:         vmem.patch
Patch1:         epoll.patch
Patch2:         fdship.patch

%description
GNU libmicrohttpd is a small C library that is supposed to make it
easy to run an HTTP server as part of another application.
Key features that distinguish libmicrohttpd from other projects are:

* C library: fast and small
* API is simple, expressive and fully reentrant
* Implementation is http 1.1 compliant
* HTTP server can listen on multiple ports
* Support for IPv6
* Support for incremental processing of POST data
* Creates binary of only 25k (for now)
* Three different threading models

%package devel
Summary:        Development files for libmicrohttpd
Group:          Development/Libraries
Requires:       %{name} = %{version}-%{release}

%description devel
Development files for libmicrohttpd

%package doc
Summary:        Documentation for libmicrohttpd
Group:          Documentation
Requires:       %{name} = %{version}-%{release}
%if 0%{?fedora} >= 11 || 0%{?rhel} >= 6
BuildArch:      noarch
%endif

%description doc
Doxygen documentation for libmicrohttpd and some example source code

%prep
%setup -q
%patch0 -p1 
%patch1 -p1 
%patch2 -p1
%build
# Required because patches modify .am files
# autoreconf --force
%if 0%{?rhel} >= 6 || %{?fedora}%{!?fedora:0} >= 18
export CFLAGS="-D MAGIC_MIME_TYPE=1 "
%else
export CFLAGS="-D MAGIC_MIME_TYPE=1 -DONLY_EPOLL_CREATE=1 "
%endif
%configure --disable-static --with-gnutls --disable-examples --disable-spdy
make %{?_smp_mflags}
#doxygen doc/doxygen/libmicrohttpd.doxy


# Disabled for now due to problems reported at
# https://gnunet.org/bugs/view.php?id=1619
#check
#make check %{?_smp_mflags}

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}

rm -f %{buildroot}%{_libdir}/libmicrohttpd.la
rm -f %{buildroot}%{_infodir}/dir

# Install some examples in /usr/share/doc/libmicrohttpd-${version}/examples
mkdir examples
#install -m 644 src/examples/*.c examples

# Install the doxygen documentation in /usr/share/doc/libmicrohttpd-${version}/html
#cp -R doc/doxygen/html html

%clean
rm -rf %{buildroot}

%post doc
#/sbin/install-info %{_infodir}/microhttpd.info.gz %{_infodir}/dir || :
#/sbin/install-info %{_infodir}/microhttpd-tutorial.info.gz %{_infodir}/dir || :

%preun doc
if [ $1 = 0 ] ; then
#/sbin/install-info --delete %{_infodir}/microhttpd.info.gz %{_infodir}/dir || :
#/sbin/install-info --delete %{_infodir}/microhttpd-tutorial.info.gz %{_infodir}/dir || :
fi

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%files
%defattr(-,root,root,-)
%doc COPYING
%{_libdir}/libmicrohttpd.so.*

%files devel
%defattr(-,root,root,-)
%{_includedir}/microhttpd.h
%{_libdir}/libmicrohttpd.so
%{_libdir}/pkgconfig/libmicrohttpd.pc

%files doc
%defattr(-,root,root,-)
%{_mandir}/man3/libmicrohttpd.3.gz
%{_infodir}/libmicrohttpd.info.gz
%{_infodir}/libmicrohttpd-tutorial.info.gz
#%doc AUTHORS README ChangeLog
#%doc examples
#%doc html

%changelog
* Mon Nov 17 2014 Andreas Peters <Andreas.Joachim.Peters@cern.ch> - 0.9.38-vmem
- Add patch to avoid virtual memory explosion for PUT requests
