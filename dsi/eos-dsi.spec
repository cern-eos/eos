%define _unpackaged_files_terminate_build 0
%define __os_install_post       /bin/true
Summary: EOS gridftp DSI plugin
Name: eos-dsi
Version: 0.1.0
Release: rc19
License: none
Group: Applications/File
Source0: eos-dsi-0.1.0.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

Requires: xrootd-server >= 3.0.0

%description
EOS gridftp DSI plugin

%prep
%setup -n eos-dsi-0.1.0

%build
./bootstrap.sh
./configure --sysconfdir=/etc/
make -j 4 install

%install
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
/opt/eos/*
/opt/globus/*
%_sysconfdir/init.d/eos-gridftp
%doc


%changelog
* Tue Apr 26 2011 root <root@lxfsrd0304.cern.ch> - dsi.rc3
- Build for eos-0.1.0
%define debug_package %{nil}

