%define _unpackaged_files_terminate_build 0
%define __os_install_post       /bin/true
%define debug_package {nil} 
Summary: The EOS server installation.
Name: eos-server
Version: 0.1.0
Release: gcc4.4
Prefix: /usr
License: none
Group: Applications/File
Source: eos-0.1.0.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

BuildRequires: autoconf, automake, libtool
BuildRequires: xrootd-server >= 3.0.0
BuildRequires: xrootd-devel  >= 3.0.0
BuildRequires: readline-devel, ncurses-devel
BuildRequires: sparsehash
BuildRequires: gcc44, gcc44-c++
BuildRequires: e2fsprogs-devel, zlib-devel, openssl-devel

Requires: xrootd-server >= 3.0.0

%description
The EOS server installation containing MGM, FST & MQ service.

%prep

%setup -n eos-0.1.0

%build
test -e $RPM_BUILD_ROOT && rm -r $RPM_BUILD_ROOT
export CC=/usr/bin/gcc44 CXX=/usr/bin/g++44 
mkdir build
cd build
cmake ../
cmake ../
%{__make} %{_smp_mflags} 

%{__make} install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%post
%{__mkdir} -p -m 700 /var/eos
%{__chown} daemon:daemon /var/eos/

%files
%defattr(-,root,root)
/usr/lib64/libXrdMqClient.so.0.1.0
/usr/lib64/libXrdMqClient.so.0
/usr/lib64/libXrdMqClient.so
/usr/lib64/libXrdMqOfs.so.0.1.0
/usr/lib64/libXrdMqOfs.so.0
/usr/lib64/libXrdMqOfs.so
/usr/bin/xrdmqdumper
/usr/lib64/libeosCommon.so.0.1.0
/usr/lib64/libeosCommon.so.0
/usr/lib64/libeosCommon.so
/usr/lib64/libXrdEosAuth.so.0.1.0
/usr/lib64/libXrdEosAuth.so.0
/usr/lib64/libXrdEosAuth.so
/usr/lib64/libXrdEosFst.so.0.1.0
/usr/lib64/libXrdEosFst.so.0
/usr/lib64/libXrdEosFst.so
/usr/sbin/eosfstregister
/usr/sbin/eosfstcheck
/usr/sbin/eosfstclean
/usr/sbin/eosfstmgmsync
/usr/lib64/libXrdEosMgm.so.0.1.0
/usr/lib64/libXrdEosMgm.so.0
/usr/lib64/libXrdEosMgm.so
/usr/sbin/eos-log-compact
/usr/sbin/eos-log-repair
%config(noreplace) /etc/xrd.cf.fst
%config(noreplace) /etc/xrd.cf.mgm
%config(noreplace) /etc/xrd.cf.mq
%config(noreplace) /etc/xrd.cf.sync
%config(noreplace) /etc/sysconfig/eos.example
#%config(noreplace) /etc/sysconfig/eossync.example
%attr(-,daemon,daemon) %_sysconfdir/eos.keytab
#%_sysconfdir/cron.d/xrd-logs
#%_sysconfdir/cron.d/xrd-alive
#%_sysconfdir/cron.d/eos-health
%_sysconfdir/rc.d/init.d/eos
#%_sysconfdir/rc.d/init.d/cmsd
#%_sysconfdir/rc.d/init.d/eossync
#%_sysconfdir/rc.d/init.d/eoshealth

#######################################################################################
# the shell client package 
#######################################################################################
%package -n eos-client
#######################################################################################
Summary: The EOS shell client
Group: Applications/File
%description -n eos-client
The EOS shell client.
%files -n eos-client
/usr/bin/eos

%changelog
