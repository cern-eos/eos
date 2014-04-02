%{!?perl_sitearch: %define perl_sitearch %(eval "`%{__perl} -V:installsitearch`"; echo $installsitearch)}
%define _unpackaged_files_terminate_build 0
%define __os_install_post       /bin/true
%define debug_package %{nil}

Summary: eos-apmon package
Name: eos-apmon
Version: 1.1.4
Release: 1
URL: none
Source0: %{name}-%{version}.tar.gz
License: OpenSource
Group: Applications/Eos
BuildRoot: %{_tmppath}/%{name}-root

BuildRequires: autoconf, automake, libtool

Requires: perl

%description
This package contains service scripts for ML monitoring in EOS

The service is started via init.d scripts.
/etc/init.d/eosapmond start | stop | status | restart

'eosapmond' service is added to run by default in run level 3,4 and 5.

The initd scripts were done by Andreas-Joachim Peters [CERN] (EMAIL: andreas.joachim.peters@cern.ch).

%prep
%setup -q

%build
rm -rf $RPM_BUILD_ROOT
./bootstrap.sh
./configure --prefix=/usr/
%{__make} %{_smp_mflags}

%install
%{__make} install DESTDIR=$RPM_BUILD_ROOT

%post
/sbin/chkconfig --add eosapmond
/sbin/service eosapmond condrestart > /dev/null 2>&1 || :

%preun
if [ "$1" = "0" ] ; then  # last deinstall
    /sbin/service eosapmond stop >> /dev/null 2>&1 || :
    /sbin/chkconfig --del eosapmond
fi

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
/etc/init.d/eosapmond
/etc/logrotate.d/apmon-logs
/usr/sbin/eos_apmond
/usr/sbin/eos_apmonpl

%{perl_sitearch}/ApMon/

%changelog
* Mon Mar 12 2011 root <peters@pcsmd01.cern.ch> - 1.1.0-0
- Initial build.
* Wed Apr  2 2014 root <root@eosdevsrv1.cern.ch> - 1.1.4-1
- add "_xrootd_" to the instance name
- fix RPM version discovery for EOS and XRootD packages
