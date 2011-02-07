%define _unpackaged_files_terminate_build 0
%define __os_install_post       /bin/true

Summary: eos project
Name: eos
Version: 0.0.9e
Release: gcc4.4
Prefix: /opt/eos
License: none
Group: Applications/File
Source: eos-0.0.9e.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

BuildRequires: autoconf, automake, libtool
BuildRequires: xrootd-server >= 3.0.0
BuildRequires: xrootd-devel  >= 3.0.0
BuildRequires: readline-devel, ncurses-devel
BuildRequires: sparsehash
BuildRequires: gcc44, gcc44-c++
BuildRequires: e2fsprogs-devel, zlib-devel, openssl-devel

Requires: xrootd-server >= 3.0.0

%description
eos project

%prep

# TODO: change this explicit path
%setup -n eos-0.0.9e

%build
./bootstrap.sh
CC=/usr/bin/gcc44 CXX=/usr/bin/g++44 ./configure --sysconfdir=/etc/ --with-xrootd=/opt/xrootd/ --prefix=/opt/eos/
%{__make} %{_smp_mflags} 

%install
%{__make} install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%post
%{__mkdir} -p -m 600 /var/eos
%{__chown} daemon:daemon /var/eos/

%files
%defattr(-,root,root)
/opt/eos/bin/*
/opt/eos/lib/*
%config(noreplace) /etc/xrd.cf.fst
%config(noreplace) /etc/xrd.cf.mgm
%config(noreplace) /etc/xrd.cf.mq
%config(noreplace) /etc/xrd.cf.sync
%config(noreplace) /etc/sysconfig/xrd.example
%config(noreplace) /etc/sysconfig/eossync.example
%_sysconfdir/cron.d/xrd-logs
%_sysconfdir/cron.d/xrd-alive
%_sysconfdir/cron.d/eos-health
%_sysconfdir/rc.d/init.d/xrd
%_sysconfdir/rc.d/init.d/cmsd
%_sysconfdir/rc.d/init.d/eossync
%_sysconfdir/rc.d/init.d/eoshealth

%changelog
