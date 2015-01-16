%define _unpackaged_files_terminate_build 0

%define nginx_user      daemon
%define nginx_group     %{nginx_user}
%define nginx_home      %{_localstatedir}/lib/nginx
%define nginx_home_tmp  /var/spool/nginx/tmp
%define nginx_logdir    %{_localstatedir}/log/nginx
%define nginx_confdir   %{_sysconfdir}/nginx
%define nginx_datadir   %{_datadir}/nginx
%define nginx_webroot   %{nginx_datadir}/html

Name:           eos-nginx
Version:        1.4.2
Release:        8
Summary:        Robust, small and high performance http and reverse proxy server
Group:          System Environment/Daemons
Packager:       Justin Salmon <jsalmon@cern.ch>

# BSD License (two clause)
# http://www.freebsd.org/copyright/freebsd-license.html
License:            BSD
URL:                http://nginx.net/ 
BuildRoot:          %{_tmppath}/%{name}-%{version}-%{release}-%(%{__id_u} -n)

%if 0%{?rhel} >= 6 || %{?fedora}%{!?fedora:0}
BuildRequires:      pcre-devel,zlib-devel,openssl-devel,perl(ExtUtils::Embed),pam-devel,git,e2fsprogs-devel,openldap-devel
Requires:           pcre,zlib,openssl,openldap
%else
BuildRequires:      pcre-devel,zlib-devel,openssl-devel,perl(ExtUtils::Embed),pam-devel,git,e2fsprogs-devel,openldap24-libs-devel
Requires:           pcre,zlib,openssl,openldap24-libs
%endif


# for /user/sbin/useradd
Requires(pre):      shadow-utils
Requires(post):     chkconfig
# for /sbin/service
Requires(preun):    chkconfig, initscripts
Requires(postun):   initscripts
# don't allow this on top of vanilla nginx
Conflicts:          nginx

#Source0:    %{name}-%{version}.tar.gz
Source:     http://nginx.org/download/nginx-%{version}.tar.gz
Source2:    nginx.init
Source3:    nginx.logrotate
Source4:    nginx.sysconfig
Source5:    nginx.eos.conf.template

# removes -Werror in upstream build scripts.  -Werror conflicts with
# -D_FORTIFY_SOURCE=2 causing warnings to turn into errors.
Patch0:     nginx-auto-cc-gcc.patch

# nginx has its own configure/build scripts.  These patches allow nginx
# to install into a buildroot.
Patch1:     nginx-auto-install.patch

# configuration patch to match all the Fedora paths for logs, pid files
# etc.
Patch2:     nginx-auto-options.patch

Patch3:     nginx-auth-ldap.patch

Patch4:     nginx-no-put-body.patch

%description
Nginx [engine x] is an HTTP(S) server, HTTP(S) reverse proxy and IMAP/POP3
proxy server written by Igor Sysoev.

One third party module, ngx_http_auth_spnego has been added.
A second third party modul, nginx-auth-ldap has been added.

%prep
%setup -q -n nginx-%{version}

%patch0 -p0
%patch4 -p1

#%patch1 -p0
#%patch2 -p0

%build

rm -rf %{_builddir}/spnego-http-auth-nginx-module
git clone https://github.com/stnoonan/spnego-http-auth-nginx-module \
          %{_builddir}/spnego-http-auth-nginx-module

rm -rf %{_builddir}/nginx-auth-ldap-module
git clone https://github.com/kvspb/nginx-auth-ldap.git \
          %{_builddir}/nginx-auth-ldap-module

rm -rf %{_builddir}/nginx-auth-pam-module
mkdir -p %{_builddir}/nginx-auth-pam-module
curl http://web.iti.upv.es/~sto/nginx/ngx_http_auth_pam_module-1.3.tar.gz -o - | tar xvzf - -C %{_builddir}/nginx-auth-pam-module
mv %{_builddir}/ngx_http_auth_pam_module-1.3 %{_builddir}/nginx-auth-pam-module

# patches for openldap24
%if 0%{?rhel} >= 6 || %{?fedora}%{!?fedora:0}
( cd %{_builddir}/nginx-auth-ldap-module; echo '### Nothing to patch ###' )
%else
( cd %{_builddir}/nginx-auth-ldap-module; git config --global user.email "info@eos.cern.ch" ; git config --global user.name "EOS" ; git am --signoff < %{_sourcedir}/nginx-auth-ldap.patch )
%endif

# nginx does not utilize a standard configure script.  It has its own
# and the standard configure options cause the nginx configure script
# to error out.  This is is also the reason for the DESTDIR environment
# variable.  The configure script(s) have been patched (Patch1 and
# Patch2) in order to support installing into a build environment.
# --with-http_memcached_module\
# --with-http_ssi_module\


export CFLAGS="-I/usr/include/et/ -I/usr/include/openldap24/"
export CC="cc -L/usr/lib64/openldap24/"
export DESTDIR=%{buildroot}
./configure \
    --user=%{nginx_user} \
    --group=%{nginx_group} \
    --prefix=%{nginx_datadir} \
    --sbin-path=%{_sbindir}/nginx \
    --conf-path=%{nginx_confdir}/nginx.conf  \
    --error-log-path=%{nginx_logdir}/error.log \
    --http-log-path=%{nginx_logdir}/access.log \
    --http-client-body-temp-path=%{nginx_home_tmp}/client_body \
    --http-proxy-temp-path=%{nginx_home_tmp}/proxy \
    --http-fastcgi-temp-path=%{nginx_home_tmp}/fastcgi \
    --pid-path=%{_localstatedir}/run/nginx.pid      \
    --lock-path=%{_localstatedir}/lock/subsys/nginx \
    --with-http_ssl_module              \
    --with-http_gzip_static_module      \
    --with-http_stub_status_module      \
    --with-debug                        \
    --without-select_module             \
    --without-poll_module               \
    --without-http_charset_module       \
    --without-http_autoindex_module     \
    --without-http_geo_module           \
    --without-http_map_module           \
    --without-http_referer_module       \
    --without-http_limit_zone_module    \
    --without-http_empty_gif_module     \
    --without-http_browser_module       \
    --without-mail_imap_module          \
    --without-mail_smtp_module          \
    --add-module=%{_builddir}/spnego-http-auth-nginx-module \
    --add-module=%{_builddir}/nginx-auth-ldap-module \
    --add-module=%{_builddir}/nginx-auth-pam-module 	
    #--with-cc-opt="%{optflags} $(pcre-config --cflags)" \

make %{?_smp_mflags} 

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot} INSTALLDIRS=vendor
find %{buildroot} -type f -name .packlist -exec rm -f {} \;
find %{buildroot} -type f -name perllocal.pod -exec rm -f {} \;
find %{buildroot} -type f -empty -exec rm -f {} \;
find %{buildroot} -type f -exec chmod 0644 {} \;
find %{buildroot} -type f -name '*.so' -exec chmod 0755 {} \;
chmod 0755 %{buildroot}%{_sbindir}/nginx
mkdir %{buildroot}%{nginx_confdir}/conf.d
%{__install} -p -D -m 0755 %{SOURCE2} %{buildroot}%{_initrddir}/nginx
%{__install} -p -D -m 0644 %{SOURCE3} %{buildroot}%{_sysconfdir}/logrotate.d/nginx
%{__install} -p -D -m 0644 %{SOURCE4} %{buildroot}%{_sysconfdir}/sysconfig/nginx
%{__install} -p -D -m 0644 %{SOURCE5} %{buildroot}%{_sysconfdir}/nginx/nginx.eos.conf.template
%{__install} -p -d -m 0755 %{buildroot}%{nginx_confdir}/vhosts
%{__install} -p -d -m 0755 %{buildroot}%{nginx_home}
%{__install} -p -d -m 0755 %{buildroot}%{nginx_home_tmp}
%{__install} -p -d -m 0755 %{buildroot}%{nginx_logdir}
%{__install} -p -d -m 0755 %{buildroot}%{nginx_webroot}

# convert to UTF-8 all files that give warnings.
for textfile in CHANGES
do
    mv $textfile $textfile.old
    iconv --from-code ISO8859-1 --to-code UTF-8 --output $textfile $textfile.old
    rm -f $textfile.old
done

%clean
rm -rf %{buildroot}
rm -rf %{_builddir}/spnego-http-auth-nginx-module
rm -rf %{_builddir}/nginx-auth-ldap-module
rm -rf %{_builddir}/nginx-auth-pam-module

%pre
%{_sbindir}/useradd -c "Nginx user" -s /bin/false -r -d %{nginx_home} %{nginx_user} 2>/dev/null || :

%post
/sbin/chkconfig --add nginx

%preun
if [ $1 = 0 ]; then
    /sbin/service nginx stop >/dev/null 2>&1
    /sbin/chkconfig --del nginx
fi

%postun
if [ $1 -ge 1 ]; then
    /sbin/service nginx condrestart > /dev/null 2>&1 || :
fi

%files
%defattr(-,root,root,-)
%doc LICENSE CHANGES README
%dir %{nginx_datadir}
%{_datadir}/nginx/*/*
%{_sbindir}/nginx
#%{_mandir}/man3/nginx.3pm.gz
%{_initrddir}/nginx
%dir %{nginx_confdir}
%dir %{nginx_confdir}/conf.d
%config(noreplace) %{nginx_confdir}/win-utf
%config(noreplace) %{nginx_confdir}/fastcgi_params
%config(noreplace) %{nginx_confdir}/fastcgi_params.default
%config(noreplace) %{nginx_confdir}/mime.types.default
%config(noreplace) %{nginx_confdir}/nginx.conf.default
%config(noreplace) %{nginx_confdir}/fastcgi.conf
%config(noreplace) %{nginx_confdir}/fastcgi.conf.default
%config(noreplace) %{nginx_confdir}/scgi_params
%config(noreplace) %{nginx_confdir}/scgi_params.default
%config(noreplace) %{nginx_confdir}/uwsgi_params
%config(noreplace) %{nginx_confdir}/uwsgi_params.default
%config(noreplace) %{nginx_confdir}/koi-win
%config(noreplace) %{nginx_confdir}/koi-utf
%config(noreplace) %{nginx_confdir}/nginx.conf
%config(noreplace) %{nginx_confdir}/nginx.eos.conf.template
%config(noreplace) %{nginx_confdir}/mime.types
%config(noreplace) %{_sysconfdir}/logrotate.d/nginx
%config(noreplace) %{_sysconfdir}/sysconfig/nginx
#%dir %{perl_vendorarch}/auto/nginx
#%{perl_vendorarch}/nginx.pm
#%{perl_vendorarch}/auto/nginx/nginx.so
%attr(-,%{nginx_user},%{nginx_group}) %dir %{nginx_home}
%attr(-,%{nginx_user},%{nginx_group}) %dir %{nginx_home_tmp}
%attr(-,%{nginx_user},%{nginx_group}) %dir %{nginx_logdir}


%changelog
* Thu Aug 28 2013 Justin Salmon <jsalmon@cern.ch>
- Bump release version
* Thu Jul 25 2013 Justin Salmon <jsalmon@cern.ch>
- Switch to nginx version 1.4.2
