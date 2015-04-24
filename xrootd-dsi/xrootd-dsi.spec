%define debug_package %{nil}
%define _unpackaged_files_terminate_build 0
%define __os_install_post       /bin/true

Summary: XROOTD gridftp DSI plugin
Name: xrootd-dsi
Version: 0.3.3
Release: 2
License: none
Group: Applications/File
Source0: xrootd-dsi-0.3.3-%{release}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

Requires: xrootd-cl >= 3.3.0
Requires: globus-gridftp-server-progs

BuildRequires: globus-gridftp-server-devel
BuildRequires: xrootd-cl >= 3.3.0
BuildRequires: xrootd-cl-devel >= 3.3.0
BuildRequires: cmake
BuildRequires: xrootd-devel >= 3.3.0
BuildRequires: xrootd-server-devel >= 3.3.0

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
%_sysconfdir/init.d/xrootd-gridftp
%_sysconfdir/logrotate.d/xrootd-gridftp
%config %_sysconfdir/sysconfig/xrootd-gridftp-server


%doc

%post
/sbin/chkconfig --add xrootd
echo Starting conditional XROOTD grid-ftp services
/sbin/service xrootd-gridftp condrestart > /dev/null 2>&1 || :
%preun 
if [ $1 = 0 ]; then
        echo Stopping XROOTD grid-ftp services
        /sbin/service xrootd-gridftp stop > /dev/null 2>&1 
        /sbin/chkconfig --del xrootd-gridftp
fi

%changelog
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
