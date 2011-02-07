Summary: fuse xrootd eos daemon
Name: eos-fuse
Version: 0.0.2
Release: gcc4.4
License: None
Group: Applications/File
#URL:
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
AutoReqProv: no

BuildRequires: autoconf, automake, libtool
BuildRequires: xrootd-server >= 3.0.0
BuildRequires: xrootd-devel  >= 3.0.0
BuildRequires: fuse-devel
BuildRequires: gcc44, gcc44-c++

Requires: fuse
Requires: xrootd-server >= 3.0.0

%description
The fuse module to mount an EOS based xrootd filesystem

%prep
%setup -q

%build
./bootstrap.sh
CC=/usr/bin/gcc44 CXX=/usr/bin/g++44 ./configure --prefix=/opt/eos/ --with-xrootd=/usr/ --with-fuse-location=/usr --sysconfdir=/etc/
%{__make} %{_smp_mflags}

%install
%{__make} install DESTDIR=$RPM_BUILD_ROOT

%post
test -e /etc/sysconfig/eosd && . /etc/sysconfig/eosd
%{__mkdir} -p ${EOSMOUNTDIR-/eos/}

/sbin/chkconfig --add eosd
/sbin/service eosd condrestart > /dev/null 2>&1 || :

%preun
if [ "$1" = "0" ] ; then  # last deinstall
    /sbin/service eosd stop > > /dev/null 2>&1 || :
    /sbin/chkconfig --del eosd
fi

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
/etc/init.d/eosd
/etc/fuse.conf
/opt/eos/bin/eosd
/opt/eos/bin/eosfs
/opt/eos/bin/eosfs.start

%changelog
* Tue Jul 06 2010 Andreas Joachim Peters <root@pcitsmd01.cern.ch> - eosd-1
- Initial build.
