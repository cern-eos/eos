%define debug_package %{nil}
%define _unpackaged_files_terminate_build 0
%define __os_install_post       /bin/true

Summary: XROOTD gridftp DSI plugin
Name: xrootd-dsi
Version: 0.5.1
Release: 1
License: none
Group: Applications/File
Source0: xrootd-dsi-0.5.1-%{release}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

%global gftp_version %(rpm -qa --queryformat '%%{name}-%%{version}-%%{release}\\n' | grep "globus-gridftp-server-[0-9]")
%global gftpc_version %(rpm -qa --queryformat '%%{name}-%%{version}-%%{release}\\n' | grep "globus-gridftp-server-control-[0-9]")
%if "%{gftp_version}"==""
%global gftp_version "globus-gridftp-server"
%endif
%if "%{gftpc_version}"==""
%global gftpc_version "globus-gridftp-server-control"
%endif
Requires: %{gftp_version}
Requires: %{gftpc_version}

Requires: xrootd-client
Requires: globus-gridftp-server-progs

BuildRequires: globus-gridftp-server-devel
#BuildRequires: xrootd-client >= 3.3.0
BuildRequires: xrootd-client-devel
BuildRequires: cmake
BuildRequires: xrootd-devel
BuildRequires: xrootd-server-devel
BuildRequires: perl
BuildRequires: globus-gridftp-server-progs
BuildRequires: yum-utils
%if 0%{?rhel} < 6
BuildRequires: boost148-devel
%else
BuildRequires: boost-devel
%endif

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
cmake ../ -DRELEASE=%{release} -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_MODULE_PATH=cmake
# we force the deletion of the globus hack header to avoid any problems with left overs and multiple builds
# this forces the rebuild of the header with currently installed versions of globus packages
rm -f ../src/globus_gfs_internal_hack.h

%install
cd build
%{__make} install DESTDIR=$RPM_BUILD_ROOT
echo "Installed!"

%clean
rm -rf $RPM_BUILD_ROOT 

%files
%defattr(-,root,root,-)
%{_libdir}/*.so*
%_sysconfdir/init.d/xrootd-gridftp
%config %_sysconfdir/sysconfig/xrootd-gridftp-server


%doc

%post
ln -s /usr/lib64/libglobus_gridftp_server_xrootd_gcc64pthr.so /usr/lib64/libglobus_gridftp_server_xrootd_gcc64.so
/sbin/chkconfig --add xrootd-gridftp
echo Starting conditional XROOTD grid-ftp services
/sbin/service xrootd-gridftp condrestart > /dev/null 2>&1 || :
%preun 
if [ $1 = 0 ]; then
        echo Stopping XROOTD grid-ftp services
        /sbin/service xrootd-gridftp stop > /dev/null 2>&1 
        /sbin/chkconfig --del xrootd-gridftp
fi
rm -f /usr/lib64/libglobus_gridftp_server_xrootd_gcc64.so

%changelog
* Fri Jul 28 2015 <geoffray.adde@cern.ch> - dsi 0.5.1-1
- add a mutex to protect the config object from deletion while still being used while transfer is being canceled 
- remove logrotate file because the entry is redundant with xrootd
- add hard dependency on the versions of globus-gridftp-server and globus-gridftp-server-control
- set XRD_RUNFORKHANDLERS in the init script because XRD_ENABLEFORKHANDLERS is not supported by latest versions of XRootD
  with which the rpm was built
* Fri Jun 5 2015 <geoffray.adde@cern.ch> - dsi 0.5.0-1
- rewrite MT locking scheme to allow IPC
- add support for dynamic backend discovery
* Wed May 8 2015 <geoffray.adde@cern.ch> - dsi 0.4.0-1
- add support for frontend/backend setup
* Wed Feb 12 2014 <geoffray.adde@cern.ch> - dsi 0.3.3-2
- fix a possible crash when XROOTD_VMP is not properly formated
* Fri Nov 19 2013 <geoffray.adde@cern.ch> - dsi 0.3.3-1
- add extra support for multiple "gridftp mounting points" -> syntax of XROOTD_VMP changed!
- fix the xrootd id change caused by the check when the dsi plugin is getting initialized
* Fri Oct 25 2013 <geoffray.adde@cern.ch> - dsi 0.3.2-1
- add extra checkings on plugin init to see if XROOTD_VMP is properly set and add error reporting
- fix the the preuninstall script in the rpm
- mark config files as such in the rpm
* Thu Oct 17 2013 <geoffray.adde@cern.ch> - dsi 0.3.1-1
- add protection for memory unallocation against unallocated memory while an unsucessful authentication happens
- fix a bug related to XROOTD_DSI_EOS* environment variables poetentialy not taken into account
* Tue Feb 05 2013 <geoffray.adde@cern.ch> - dsi 0.3.0-1 
- changing name to XRootD plugin (eos features are now optional and can be configured in /etc/sysconfig/xrootd-gridftp-server)
- moving to CMake build
- moving to xrootd 3.3.0 ( move from XrdPosix to XrdCl, XrdCl being part of the xrootd package)
