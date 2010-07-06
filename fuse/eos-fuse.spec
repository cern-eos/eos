Summary: fuse xrootd eos daemon 
Name: eos-fuse
Version: 0.0.1
Release: 1
License: None
Group: Applications/File
#URL: 
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
AutoReqProv: no
Requires: fuse 
Requires: xrootd


%description
The fuse module to mount an EOS based xrootd filesystem
%prep
%setup -q
./configure --prefix=/opt/eos/ --with-fuse-location=/usr
%build
make

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT


%files
/etc/init.d/xcfsd
/etc/fuse.conf
/opt/eos/bin/eosd
/opt/eos/bin/eosfs

%defattr(-,root,root,-)
%doc


%changelog
* Tue Jul 06 2010 Andreas Joachim Peters <root@pcitsmd01.cern.ch> - eosd-1
- Initial build.

%pre

%post
test -e /etc/sysconfig/xcfsd && . /etc/sysconfig/xcfsd

mkdir -p ${XCFSMOUNTDIR-/xcfs/cern.ch}

if [ "$1" = "1" ] ; then  # first install
	/etc/init.d/xcfsd start
fi

if [ "$1" = "2" ] ; then  # upgrade
	/etc/init.d/xcfsd condrestart > /dev/null 2>&1 || :
fi

%preun
if [ "$1" = "0" ] ; then  # last deinstall
	/etc/init.d/xcfsd stop
fi

%postun

if [ "$1" = "0" ]; then # last deinstall
	echo "Done"
fi
echo
