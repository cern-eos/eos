%define debug_package %{nil}
%define _unpackaged_files_terminate_build 0
%define __os_install_post       /bin/true

Summary: XROOTD gridftp DSI plugin
Name: xrootd-dsi
Version: 0.3.0
Release: 1
License: none
Group: Applications/File
Source0: xrootd-dsi-0.3.0-%{release}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

Requires: xrootd-cl >= 3.2.0
Requires: globus-gridftp-server-progs

BuildRequires: globus-gridftp-server-devel
BuildRequires: xrootd-cl >= 3.2.0
BuildRequires: xrootd-cl-devel >= 3.2.0


%description
XROOTD gridftp DSI plugin

%prep
%setup -n %{name}-%{version}-%{release}

%build
test -e $RPM_BUILD_ROOT && rm -r $RPM_BUILD_ROOT
%if 0%{?rhel} < 6
export CC=/usr/bin/gcc44 CXX=/usr/bin/g++44
%endif

mkdir -p build
cd build
cmake ../ -DRELEASE=%{release} -DCMAKE_BUILD_TYPE=RelWithDebInfo

%install
cd build
%{__make} install DESTDIR=$RPM_BUILD_ROOT
echo "Installed!"

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{_libdir}/*.so*
%_sysconfdir/sysconfig/xrootd-gridftp-server
%_sysconfdir/init.d/xrootd-gridftp
%_sysconfdir/logrotate.d/xrootd-gridftp
%doc

%post
/sbin/chkconfig --add xrootd
echo Starting conditional XROOTD grid-ftp services
/sbin/service xrootd-gridftp condrestart > /dev/null 2>&1 || :
%preun 
if [ $1 = 0 ]; then
        echo Stopping XROOTD grid-ftp services
        /sbin/service xrootd-gridftp stop > /dev/null 2>&1 
        /sbin/chkconfig --del xrootd
fi

%changelog
* Tue Feb 05 2013 <root@eosdevsrv1.cern.ch> - dsi 0.3.0-0 
- changing name to XRootD plugin (eos features are now optional and can be configured in /etc/sysconfig/xrootd-gridftp-server)
- moving to CMake build
- moving to xrootd 3.3.0 ( move from XrdPosix to XrdCl, XrdCl being part of the xrootd package)
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


