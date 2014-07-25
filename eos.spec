%define _unpackaged_files_terminate_build 0
Summary: The EOS project
Name: eos
Version: 0.3.37
Release: rc1
Prefix: /usr
License: none
Group: Applications/File

Source: %{name}-%{version}-%{release}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

%if 0%{?rhel} >= 6 || %{?fedora}%{!?fedora:0} >= 18
BuildRequires: cmake >= 2.8
%define cmake_cmd cmake
%else
BuildRequires: cmake28 >= 2.8
%define cmake_cmd cmake28
%endif

BuildRequires: xrootd >= 3.3.4
BuildRequires: xrootd-server-devel >= 3.3.4
BuildRequires: xrootd-private-devel  >= 3.3.4
BuildRequires: readline-devel, ncurses-devel
BuildRequires: libattr-devel, openldap-devel
BuildRequires: e2fsprogs-devel, zlib-devel, openssl-devel,ncurses-devel, xfsprogs-devel
BuildRequires: fuse-devel, fuse >= 2.5
BuildRequires: xrootd-client-devel
BuildRequires: leveldb-devel
BuildRequires: libmicrohttpd, libmicrohttpd-devel
BuildRequires: git

%if 0%{?rhel} >= 6 || %{?fedora}%{!?fedora:0}
%if %{?fedora}%{!?fedora:0} >= 18
BuildRequires: libuuid-devel,ncurses-static,openssl-static,zlib-static,sparsehash-devel
%else
BuildRequires: libuuid-devel,ncurses-static,openssl-static,zlib-static,sparsehash
%endif
%else
BuildRequires: gcc44, gcc44-c++, sparsehash
%endif

%description
The EOS software package.

#######################################################################################
%package -n eos-server
#######################################################################################
Summary: The EOS server installation
Group: Applications/File
Requires: xrootd >= 3.3.4
Requires: xrootd-client
Requires: eos-client
Requires: libmicrohttpd
Requires: acl
Requires: gdb

%description -n eos-server
The EOS server installation containing MGM, FST & MQ service.

%prep

%setup -n %{name}-%{version}-%{release}

%build
test -e $RPM_BUILD_ROOT && rm -r $RPM_BUILD_ROOT
%if 0%{?rhel} < 6 && %{?fedora}%{!?fedora:0} <= 1
export CC=/usr/bin/gcc44 CXX=/usr/bin/g++44 
%endif

mkdir -p build
cd build
%{cmake_cmd} ../ -DRELEASE=%{release} -DCMAKE_BUILD_TYPE=RelWithDebInfo
%{__make} %{_smp_mflags} 

%install
cd build
%{__make} install DESTDIR=$RPM_BUILD_ROOT
echo "Installed!"
%clean
rm -rf $RPM_BUILD_ROOT

%files -n eos-server
%defattr(-,root,root)
/usr/lib64/libXrdMqOfs.so.0.3.37
/usr/lib64/libXrdMqOfs.so.0
/usr/lib64/libXrdMqOfs.so
/usr/bin/xrdmqdumper
/usr/sbin/eosha
/usr/sbin/eoshapl
/usr/sbin/eosfilesync
/usr/sbin/eosdirsync
/usr/lib64/libeosCommonServer.so.0.3.37
/usr/lib64/libeosCommonServer.so.0
/usr/lib64/libeosCommonServer.so
/usr/lib64/libXrdEosAuth.so.0.3.37
/usr/lib64/libXrdEosAuth.so.0
/usr/lib64/libXrdEosAuth.so
/usr/lib64/libXrdEosFst.so.0.3.37
/usr/lib64/libXrdEosFst.so.0
/usr/lib64/libXrdEosFst.so
/usr/lib64/libEosFstOss.so.0.3.37
/usr/lib64/libEosFstOss.so.0
/usr/lib64/libEosFstOss.so
/usr/sbin/eosfstregister
/usr/sbin/eosfstinfo
/usr/sbin/eosadmin
/usr/sbin/eos-check-blockxs
/usr/sbin/eos-udp-dumper
/usr/sbin/eos-compute-blockxs
/usr/sbin/eos-scan-fs
/usr/sbin/eos-adler32
/usr/sbin/eos-mmap
/usr/sbin/eos-repair-tool
/usr/sbin/eos-ioping
/usr/sbin/eos-iobw
/usr/sbin/eos-iops
/usr/lib64/libXrdEosMgm.so.0.3.37
/usr/lib64/libXrdEosMgm.so.0
/usr/lib64/libXrdEosMgm.so
/usr/sbin/eos-log-compact
/usr/sbin/eos-log-repair
/usr/sbin/eossh-timeout
%attr(700,daemon,daemon) /var/eos
%attr(755,daemon,daemon) /var/log/eos/
%config(noreplace) /etc/xrd.cf.fst
%config(noreplace) /etc/xrd.cf.mgm
%config(noreplace) /etc/xrd.cf.mq
%config(noreplace) /etc/xrd.cf.global-mq
%config(noreplace) /etc/xrd.cf.sync
%config(noreplace) /etc/xrd.cf.fed
%config(noreplace) /etc/xrd.cf.prefix
%config(noreplace) /etc/sysconfig/eos.example
%_sysconfdir/rc.d/init.d/eos
%_sysconfdir/rc.d/init.d/eosha
%_sysconfdir/rc.d/init.d/eossync
%_sysconfdir/rc.d/init.d/eosslave
%_sysconfdir/cron.d/eos-logs
%_sysconfdir/cron.d/eos-reports
%_sysconfdir/logrotate.d/eos-logs

%post -n eos-server
/sbin/chkconfig --add eos
/sbin/chkconfig --add eosslave
echo Starting conditional EOS services
sleep 2
/sbin/service eos condrestart > /dev/null 2>&1 || :
/sbin/service eosd condrestart > /dev/null 2>&1 || :
%preun -n eos-server
if [ $1 = 0 ]; then
        echo Stopping EOS services
        /sbin/service eosha stop > /dev/null 2>&1 
        /sbin/service eosd stop > /dev/null 2>&1 
        /sbin/service eos stop > /dev/null 2>&1 || :
        /sbin/service eossync stop > /dev/null 2>&1 
	/sbin/service eosslave stop > /dev/null 2>&1
        /sbin/chkconfig --del eos
        /sbin/chkconfig --del eosslave	
fi


#######################################################################################
# the shell client package 
#######################################################################################
%package -n eos-client
#######################################################################################
Summary: The EOS shell client
Group: Applications/File
Requires: xrootd-client 
%description -n eos-client
The EOS shell client.
%files -n eos-client
/usr/bin/eos
/usr/bin/eosdropboxd
/usr/bin/eoscp
/usr/lib64/libXrdMqClient.so.0.3.37
/usr/lib64/libXrdMqClient.so.0
/usr/lib64/libXrdMqClient.so
/usr/lib64/libeosCommon.so.0.3.37
/usr/lib64/libeosCommon.so.0
/usr/lib64/libeosCommon.so

############################
# documentation
%doc %_mandir/man1/

#######################################################################################
# the fuse client package 
#######################################################################################
%package -n eos-fuse
#######################################################################################
Summary: The EOS fuse client
Group: Applications/File

Requires: xrootd-client
Requires: eos-client
Requires: fuse

%description -n eos-fuse
The EOS fuse client.
%files -n eos-fuse
/usr/bin/eosfsd
/usr/sbin/eosd
%if %{?fedora:1}%{!?fedora:0}
/etc/fuse.conf.eos
%else
%config(noreplace) /etc/fuse.conf
/etc/fuse.conf.eos
%endif
/etc/rc.d/init.d/eosd
%_sysconfdir/logrotate.d/eos-fuse-logs
%changelog

%post -n eos-fuse
/sbin/chkconfig --add eosd
echo Starting conditional EOS services
sleep 2
/sbin/service eosd condrestart > /dev/null 2>&1 || :
%preun -n eos-fuse
if [ $1 = 0 ]; then
        echo Stopping EOS services
        /sbin/service eosd stop > /dev/null 2>&1 
        /sbin/chkconfig --del eosd
fi

#######################################################################################
# the srm scripts package 
#######################################################################################
%package -n eos-srm
#######################################################################################
Summary: The EOS srm script package for checksumming and space
Group: Applications/File

Requires: eos-client

%description -n eos-srm
The EOS srm package.
%files -n eos-srm
/usr/sbin/eos-srm-used-bytes
/usr/sbin/eos-srm-max-bytes
/usr/sbin/eos-srm-checksum


#######################################################################################
# the keytab test package 
#######################################################################################
%package -n eos-testkeytab
#######################################################################################
Summary: The EOS testkeytab package
Group: Applications/File

Requires: eos-server

%description -n eos-testkeytab
Contains an example keytab file.
%files -n eos-testkeytab
%config(noreplace) %attr(-,daemon,daemon) %_sysconfdir/eos.keytab

#######################################################################################
# the test package 
#######################################################################################
%package -n eos-test
#######################################################################################
Summary: The EOS test package
Group: Applications/File

Requires: eos-server cadaver bc

%description -n eos-test
Contains an instance test script and some test executables
%files -n eos-test
/usr/sbin/eos-instance-test
/usr/sbin/eos-rain-test
/usr/sbin/xrdcpabort
/usr/sbin/xrdcpappend
/usr/sbin/xrdcpposixcache
/usr/sbin/xrdcpextend
/usr/sbin/xrdcpholes
/usr/sbin/xrdcpbackward
/usr/sbin/xrdcpdownloadrandom
/usr/sbin/xrdcprandom
/usr/sbin/xrdcpshrink
/usr/sbin/xrdcptruncate
/usr/sbin/xrdcppartial
/usr/sbin/xrdstress
/usr/sbin/xrdstress.exe
/usr/sbin/eos-io-test
/usr/sbin/eos-io-tool

#######################################################################################
# the cleanup package 
#######################################################################################
%package -n eos-cleanup
#######################################################################################
Summary: The EOS test package
Group: Applications/File

%description -n eos-cleanup
Contains an clean-up script to remove 'left-overs' of an EOS instance for FST/MGM/FUSE etc ...
%files -n eos-cleanup
/usr/sbin/eos-uninstall
/usr/sbin/eos-log-clean
/usr/sbin/eos-fst-clean
/usr/sbin/eos-mgm-clean
