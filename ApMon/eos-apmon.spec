Summary: eos-apmon package
Name: eos-apmon
Version: 1.0.0
Release: 3
URL: none
Source0: %{name}-%{version}.tar.gz
License: OpenSource
Group: Applications/Eos
BuildRoot: %{_tmppath}/%{name}-root

BuildRequires: autoconf, automake, libtool

%description
This package contains service scripts for ML monitoring in EOS

The service is started via init.d scripts.
/etc/init.d/eosapmond start | stop | status | restart

'eosapmond' service is added to run by default in run level 3,4 and 5.

The initd scripts were done by Andreas-Joachim Peters [CERN] (EMAIL: andreas.joachim.peters@cern.ch).

%prep
%setup -q

%build
./bootstrap.sh
./configure --prefix=/opt/eos/
%{__make} %{_smp_mflags}

%install
%{__make} install DESTDIR=$RPM_BUILD_ROOT

%post
/sbin/chkconfig --add eosapmond
/sbin/service eosapmond condrestart > /dev/null 2>&1 || :

%preun
if [ "$1" = "0" ] ; then  # last deinstall
    /sbin/service eosapmond stop > > /dev/null 2>&1 || :
    /sbin/chkconfig --del eosapmond
fi

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
/etc/init.d/eosapmond
/etc/init.d/eosnsapmond
/etc/init.d/eostxapmond
/etc/init.d/eostxlog
/opt/eos/bin/eos_apmond
/opt/eos/bin/eos_ns_apmond
/opt/eos/bin/eos_tx_apmond
/opt/eos/bin/eos_tx_log
/opt/eos/perl/ApMon/ApMon.pm
/opt/eos/perl/ApMon/ApMon/BgMonitor.pm
/opt/eos/perl/ApMon/ApMon/Common.pm
/opt/eos/perl/ApMon/ApMon/ConfigLoader.pm
/opt/eos/perl/ApMon/ApMon/ProcInfo.pm
/opt/eos/perl/ApMon/ApMon/XDRUtils.pm
/opt/eos/perl/ApMon/sendToML.sh
/opt/eos/perl/ApMon/servMon.sh
/opt/eos/perl/eos-ns-monitor.pl
/opt/eos/perl/eos-tx-monitor.pl
/opt/eos/perl/eos-tx-log.pl

%changelog
* Tue Jul 13 2010 root <peters@pcsmd01.cern.ch> - 1.0.0-3
- Initial build.
