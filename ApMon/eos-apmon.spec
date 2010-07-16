Summary: eos-apmon package
Name: eos-apmon
Version: 1.0.0
Release: 1
URL: none
Source0: %{name}-%{version}.tar.gz
License: OpenSource
Group: Applications/Eos
BuildRoot: %{_tmppath}/%{name}-root
#%_enable_debug_packages %{nil}

%description
This package contains service scripts for ML monitoring in EOS

The service is started via init.d scripts.
/etc/init.d/eosapmond start | stop | status | restart  

'EOSapmond' service is added to run by default in run level 3,4 and 5.

The initd scripts were done by Andreas-Joachim Peters [CERN] (EMAIL: andreas.joachim.peters@cern.ch).

%prep
rm -rf $RPM_BUILD_ROOT
%setup -q
%build
./configure --prefix=/opt/eos/
make
%install
make DESTDIR=$RPM_BUILD_ROOT install
%clean
echo rm -rf $RPM_BUILD_ROOT

%files
/etc/init.d/eosapmond
/etc/init.d/eosnsapmond
/opt/eos/bin/eos_apmond
/opt/eos/bin/eos_ns_apmond
/opt/eos/perl/ApMon/ApMon.pm
/opt/eos/perl/ApMon/ApMon/BgMonitor.pm
/opt/eos/perl/ApMon/ApMon/Common.pm
/opt/eos/perl/ApMon/ApMon/ConfigLoader.pm
/opt/eos/perl/ApMon/ApMon/ProcInfo.pm
/opt/eos/perl/ApMon/ApMon/XDRUtils.pm
/opt/eos/perl/ApMon/sendToML.sh
/opt/eos/perl/ApMon/servMon.sh
/opt/eos/perl/eos-ns-monitor.pl
%defattr(-,root,root)

%changelog
* Tue Jul 13 2010 root <peters@pcsmd01.cern.ch>
- Initial build.
V1.0.0

%post
/sbin/chkconfig --add eosapmond
/sbin/service eosapmond condrestart > /dev/null 2>&1

%preun
[ -e /etc/init.d/eosapmond ] && /etc/init.d/eosapmond stop
/sbin/chkconfig --del eosapmond
%postun
echo 

