%define _unpackaged_files_terminate_build 0
%define __os_install_post       /bin/true
%define debug_package %{nil} 
Summary: The EOS server installation.
Name: eos-server
Version: 0.1.0
Release: rc20
Prefix: /usr
License: none
Group: Applications/File
Source: eos-0.1.0.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

BuildRequires: autoconf, automake, libtool
BuildRequires: xrootd-server >= 3.0.4
BuildRequires: xrootd-server-devel  >= 3.0.4
BuildRequires: readline-devel, ncurses-devel
BuildRequires: sparsehash
BuildRequires: gcc44, gcc44-c++
BuildRequires: e2fsprogs-devel, zlib-devel, openssl-devel
BuildRequires: fuse-devel, fuse

Requires: xrootd-server >= 3.0.4
Requires: eos-client

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
/sbin/chkconfig --add eos
/sbin/service eos condrestart > /dev/null 2>&1 || :
/sbin/service eosd condrestart > /dev/null 2>&1 || :
%preun
if [ $1 = 0 ]; then
        /sbin/service eosha stop > /dev/null 2>&1 
        /sbin/service eos stop > /dev/null 2>&1 || :
        /sbin/chkconfig --del eos
fi

%files
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
%config(noreplace) %attr(-,daemon,daemon) %_sysconfdir/eos.keytab
%_sysconfdir/rc.d/init.d/eos
%_sysconfdir/rc.d/init.d/eosha
%_sysconfdir/rc.d/init.d/eossync


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

