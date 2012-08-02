%define debug_package %{nil}
%define _unpackaged_files_terminate_build 0
%define __os_install_post       /bin/true

Summary: EOS gridftp DSI plugin
Name: eos-dsi
Version: 0.2.6
Release: 1
License: none
Group: Applications/File
Source0: eos-dsi-0.2.6-%{release}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

Requires: xrootd-client >= 3.2.0
Requires: vdt_globus_essentials
Requires: vdt_globus_data_server
BuildRequires: vdt_globus_sdk
BuildRequires: vdt_compile_globus_core
BuildRequires: vdt_globus_essentials
BuildRequires: gpt
#BuildRequires: globus-config
BuildRequires: xrootd-libs-devel >= 3.2.0
BuildRequires: xrootd-client-devel >= 3.2.0
BuildRequires: autoconf, automake, libtool

%description
EOS gridftp DSI plugin

%prep
%setup -n eos-dsi-0.2.6-%{release}

%build
./bootstrap.sh
./configure --sysconfdir=/etc/ --libdir=%{_libdir}
%{__make} %{_smp_mflags}

%install
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{_libdir}/*.so.*
/opt/globus/lib/*
%_sysconfdir/init.d/eos-gridftp
%_sysconfdir/logrotate.d/eos-gridftp
%doc

%post
/sbin/chkconfig --add eos
echo Starting conditional EOS grid-ftp services
/sbin/service eos-gridftp condrestart > /dev/null 2>&1 || :
%preun 
if [ $1 = 0 ]; then
        echo Stopping EOS grid-ftp services
        /sbin/service eos-gridftp stop > /dev/null 2>&1 
        /sbin/chkconfig --del eos
fi


%changelog
* Wed Feb 08 2012 root <root@eosdevsrv1.cern.ch> - dsi 0.2.0-0
- no changes, just new version number
* Tue Jan 17 2012 root <root@eosdevsrv1.cern.ch> - dsi 0.1.1-8
- support for file size preset via eos.bookingsize attribute
* Wed Dec 14 2011 root <root@eosdevsrv1.cern.ch> - dsi 0.1.1-5
* Fri Dec 09 2011 root <root@eosdevsrv1.cern.ch> - dsi 0.1.1-3
* Thu Jun 16 2011 root <root@lxbsu2005.cern.ch> - dsi.rc20
- moving to xrootd 3.0.4
- adding command implementation (mkdir,rmdir,chmod+cksm)
* Tue Apr 26 2011 root <root@lxfsrd0304.cern.ch> - dsi.rc3
- Build for eos-0.1.0


