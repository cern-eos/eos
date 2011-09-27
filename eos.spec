%define _unpackaged_files_terminate_build 0
%define __os_install_post       /bin/true
%define debug_package %{nil} 

Summary: The EOS project
Name: eos
Version: 0.1.0
Release: rc31
Prefix: /usr
License: none
Group: Applications/File

Source: %{name}-%{version}-%{release}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

BuildRequires: cmake >= 2.6
BuildRequires: xrootd-server >= 3.0.4
BuildRequires: xrootd-server-devel  >= 3.0.4
BuildRequires: readline-devel, ncurses-devel
BuildRequires: libattr-devel
BuildRequires: sparsehash
BuildRequires: e2fsprogs-devel, zlib-devel, openssl-devel
BuildRequires: fuse-devel, fuse

%if 0%{?rhel} < 6
BuildRequires: gcc44, gcc44-c++
%else
BuildRequires: libuuid-devel
%endif

%description
The EOS software package.

#######################################################################################
# the shell client package 
#######################################################################################
%package -n eos-server
#######################################################################################
Summary: The EOS server installation
Group: Applications/File
%description -n eos-server
The EOS server installation containing MGM, FST & MQ service.


Requires: xrootd-server >= 3.0.4
Requires: eos-client

%prep

%setup -n %{name}-%{version}-%{release}

%build
test -e $RPM_BUILD_ROOT && rm -r $RPM_BUILD_ROOT
%if 0%{?rhel} < 6
export CC=/usr/bin/gcc44 CXX=/usr/bin/g++44 
%endif

mkdir -p build
cd build
cmake ../ -DRELEASE=%{release}
cmake ../ -DRELEASE=%{release}
%{__make} %{_smp_mflags} 

%{__make} install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%post
/sbin/chkconfig --add eos
echo Starting conditional EOS services
/sbin/service eos condrestart > /dev/null 2>&1 || :
/sbin/service eosd condrestart > /dev/null 2>&1 || :
%preun
if [ $1 = 0 ]; then
        echo Stopping EOS services
        /sbin/service eosha stop > /dev/null 2>&1 
        /sbin/service eosd stop > /dev/null 2>&1 
        /sbin/service eos stop > /dev/null 2>&1 || :
        /sbin/service eossync stop > /dev/null 2>&1 
        /sbin/chkconfig --del eos
fi

%files -n eos-server
%defattr(-,root,root)
/usr/lib64/libXrdMqClient.so.0.1.0
/usr/lib64/libXrdMqClient.so.0
/usr/lib64/libXrdMqClient.so
/usr/lib64/libXrdMqOfs.so.0.1.0
/usr/lib64/libXrdMqOfs.so.0
/usr/lib64/libXrdMqOfs.so
/usr/bin/xrdmqdumper
/usr/bin/eosfstcp
/usr/sbin/eosha
/usr/sbin/eoshapl
/usr/sbin/eosfilesync
/usr/sbin/eosdirsync
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
/usr/sbin/eosfstinfo
/usr/sbin/eosfstcheck
/usr/sbin/eosfstclean
/usr/sbin/eosfstmgmsync
/usr/sbin/eosadmin
/usr/sbin/eos-check-blockxs
/usr/sbin/eos-compute-blockxs
/usr/sbin/eos-scan-fs
/usr/sbin/eos-fst-fsck
/usr/sbin/eos-adler32
/usr/sbin/eos-fst-dump
/usr/lib64/libXrdEosMgm.so.0.1.0
/usr/lib64/libXrdEosMgm.so.0
/usr/lib64/libXrdEosMgm.so
/usr/sbin/eos-log-compact
/usr/sbin/eos-log-repair
%attr(700,daemon,daemon) /var/eos
%attr(755,daemon,daemon) /var/log/eos/
%config(noreplace) /etc/xrd.cf.fst
%config(noreplace) /etc/xrd.cf.mgm
%config(noreplace) /etc/xrd.cf.mq
%config(noreplace) /etc/xrd.cf.sync
%config(noreplace) /etc/sysconfig/eos.example
%_sysconfdir/rc.d/init.d/eos
%_sysconfdir/rc.d/init.d/eosha
%_sysconfdir/rc.d/init.d/eossync
%_sysconfdir/cron.d/eos-logs

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
############################
# documentation
%doc %_mandir/man1/eos.1.gz
%doc %_mandir/man1/eos::fs.1.gz
%doc %_mandir/man1/eos::access.1.gz
%doc %_mandir/man1/eos::attr.1.gz
%doc %_mandir/man1/eos::cd.1.gz
%doc %_mandir/man1/eos::chown.1.gz
%doc %_mandir/man1/eos::clear.1.gz
%doc %_mandir/man1/eos::config.1.gz
%doc %_mandir/man1/eos::debug.1.gz
%doc %_mandir/man1/eos::file.1.gz


#######################################################################################
# the fuse client package 
#######################################################################################
%package -n eos-fuse
#######################################################################################
Summary: The EOS fuse client
Group: Applications/File

Requires: xrootd-client >= 3.0.0
Requires: eos-client

%description -n eos-fuse
The EOS fuse client.
%files -n eos-fuse
/usr/bin/eosfsd
/usr/sbin/eosd
/etc/fuse.conf
/etc/rc.d/init.d/eosd
%changelog

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
