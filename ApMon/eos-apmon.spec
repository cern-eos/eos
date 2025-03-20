%{!?perl_sitearch: %define perl_sitearch %(eval "`%{__perl} -V:installsitearch`"; echo $installsitearch)}
%define _unpackaged_files_terminate_build 0
%define __os_install_post       /bin/true
%define debug_package %{nil}

Summary: eos-apmon package
Name: eos-apmon
Version: 1.1.12
Release: 1%{?dist}
URL: none
Source0: %{name}-%{version}.tar.gz
License: OpenSource
Group: Applications/Eos

BuildRequires: systemd-rpm-macros

Requires: perl

%description
This package contains service scripts for ML monitoring in EOS

The service is started via systemd
systemctl start | stop | status | restart eosapmond.service

The initd scripts were done by Andreas-Joachim Peters [CERN] (EMAIL: andreas.joachim.peters@cern.ch).

%prep
%setup -q

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}
%{__make} install DESTDIR=%{buildroot}

%post
%systemd_post eosapmond.service

%preun
%systemd_preun eosapmond.service

%postun
%systemd_postun_with_restart eosapmond.service

%files
%defattr(-,root,root)
/%{_unitdir}/eosapmond.service
/etc/logrotate.d/eosapmond
%{perl_sitearch}/ApMon/
/opt/eos/apmon/eosapmond
/opt/eos/apmon/run.sh

%changelog
* Wed Mar 19 2025 Gianmaria Del Monte <gianmaria.del.monte@cern.ch> - 1.1.12-1
- Move to systemd service

* Fri Jan 26 2024  Volodymyr Yurchenko <volodymyr.yurchenko@cern.ch> - 1.1.11-1
- install systemd unit file compatible with Alma 9

* Wed Aug  4 2021  Elvin Sindrilaru <esindril@cern.ch> - 1.1.10-1
- move the apmon logs out of the EOS FST owned directory and
  place them in /var/log/eos/apmon/
- bump version to 1.1.10

* Fri Dec  6 2019  Cristian Contescu <acontesc@cern.ch> - 1.1.9-1
- add fix for interface detection (fix traffic reporting)

* Wed Apr  2 2014 root <root@eosdevsrv1.cern.ch> - 1.1.4-1
- add "_xrootd_" to the instance name
- fix RPM version discovery for EOS and XRootD packages

* Mon Mar 12 2011 root <peters@pcsmd01.cern.ch> - 1.1.0-0
- Initial build.

