%define _unpackaged_files_terminate_build 0

%define nginx_user      nginx
%define nginx_group     %{nginx_user}
%define nginx_home      %{_localstatedir}/lib/nginx
%define nginx_home_tmp  /var/spool/nginx/tmp
%define nginx_logdir    %{_localstatedir}/log/nginx
%define nginx_confdir   %{_sysconfdir}/nginx
%define nginx_datadir   %{_datadir}/nginx
%define nginx_webroot   %{nginx_datadir}/html

Name:           nginx
Version:        1.0.15
Release:        1
Summary:        Robust, small and high performance http and reverse proxy server
Group:          System Environment/Daemons

# BSD License (two clause)
# http://www.freebsd.org/copyright/freebsd-license.html
License:            BSD
URL:                http://nginx.net/ 
BuildRoot:          %{_tmppath}/%{name}-%{version}-%{release}-%(%{__id_u} -n)
BuildRequires:      pcre-devel,zlib-devel,openssl-devel,perl(ExtUtils::Embed),pam-devel,git
Requires:           pcre,zlib,openssl
Requires:           perl(:MODULE_COMPAT_%(eval "`%{__perl} -V:version`"; echo $version))
# for /user/sbin/useradd
Requires(pre):      shadow-utils
Requires(post):     chkconfig
# for /sbin/service
Requires(preun):    chkconfig, initscripts
Requires(postun):   initscripts

Source0:    http://nginx.org/download/nginx-%{version}.tar.gz
Source1:    %{name}.init
Source2:    %{name}.logrotate
Source7:    %{name}.sysconfig
Source8:    %{name}.eos.conf

# removes -Werror in upstream build scripts.  -Werror conflicts with
# -D_FORTIFY_SOURCE=2 causing warnings to turn into errors.
Patch0:     nginx-auto-cc-gcc.patch

# nginx has its own configure/build scripts.  These patches allow nginx
# to install into a buildroot.
Patch1:     nginx-auto-install.patch

# configuration patch to match all the Fedora paths for logs, pid files
# etc.
Patch2:	    nginx-auto-options.patch

%description
Nginx [engine x] is an HTTP(S) server, HTTP(S) reverse proxy and IMAP/POP3
proxy server written by Igor Sysoev.

One third party module, ngx_http_auth_pam has been added.

%prep
%setup -q

%patch0 -p0
#%patch1 -p0
#%patch2 -p0

%build

git clone http://github.com/stnoonan/spnego-http-auth-nginx-module \
          %{_builddir}/spnego-http-auth-nginx-module

# nginx does not utilize a standard configure script.  It has its own
# and the standard configure options cause the nginx configure script
# to error out.  This is is also the reason for the DESTDIR environment
# variable.  The configure script(s) have been patched (Patch1 and
# Patch2) in order to support installing into a build environment.
# --with-http_memcached_module\
# --with-http_ssi_module\

export DESTDIR=%{buildroot}
./configure \
    --user=%{nginx_user} \
    --group=%{nginx_group} \
    --prefix=%{nginx_datadir} \
    --sbin-path=%{_sbindir}/%{name} \
    --conf-path=%{nginx_confdir}/%{name}.conf  \
    --error-log-path=%{nginx_logdir}/error.log \
    --http-log-path=%{nginx_logdir}/access.log \
    --http-client-body-temp-path=%{nginx_home_tmp}/client_body \
    --http-proxy-temp-path=%{nginx_home_tmp}/proxy \
    --http-fastcgi-temp-path=%{nginx_home_tmp}/fastcgi \
    --pid-path=%{_localstatedir}/run/%{name}.pid      \
    --lock-path=%{_localstatedir}/lock/subsys/%{name} \
    --with-http_ssl_module              \
    --with-http_gzip_static_module      \
    --with-http_stub_status_module      \
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
    --add-module=%{_builddir}/spnego-http-auth-nginx-module
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
%{__install} -p -D -m 0755 %{SOURCE1} %{buildroot}%{_initrddir}/%{name}
%{__install} -p -D -m 0644 %{SOURCE2} %{buildroot}%{_sysconfdir}/logrotate.d/%{name}
%{__install} -p -D -m 0644 %{SOURCE7} %{buildroot}%{_sysconfdir}/sysconfig/%{name}
%{__install} -p -D -m 0644 %{SOURCE8} %{buildroot}%{_sysconfdir}/%{name}/%{name}.eos.conf
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

%pre
%{_sbindir}/useradd -c "Nginx user" -s /bin/false -r -d %{nginx_home} %{nginx_user} 2>/dev/null || :

%post
/sbin/chkconfig --add %{name}

%preun
if [ $1 = 0 ]; then
    /sbin/service %{name} stop >/dev/null 2>&1
    /sbin/chkconfig --del %{name}
fi

%postun
if [ $1 -ge 1 ]; then
    /sbin/service %{name} condrestart > /dev/null 2>&1 || :
fi

%files
%defattr(-,root,root,-)
%doc LICENSE CHANGES README
%dir %{nginx_datadir}
%{_datadir}/%{name}/*/*
%{_sbindir}/%{name}
#%{_mandir}/man3/%{name}.3pm.gz
%{_initrddir}/%{name}
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
%config(noreplace) %{nginx_confdir}/%{name}.conf
%config(noreplace) %{nginx_confdir}/%{name}.eos.conf
%config(noreplace) %{nginx_confdir}/mime.types
%config(noreplace) %{_sysconfdir}/logrotate.d/%{name}
%config(noreplace) %{_sysconfdir}/sysconfig/%{name}
#%dir %{perl_vendorarch}/auto/%{name}
#%{perl_vendorarch}/%{name}.pm
#%{perl_vendorarch}/auto/%{name}/%{name}.so
%attr(-,%{nginx_user},%{nginx_group}) %dir %{nginx_home}
%attr(-,%{nginx_user},%{nginx_group}) %dir %{nginx_home_tmp}
%attr(-,%{nginx_user},%{nginx_group}) %dir %{nginx_logdir}


%changelog
* Thu Jul 25 2013 Justin Salmon <jsalmon@cern.ch>
- 
