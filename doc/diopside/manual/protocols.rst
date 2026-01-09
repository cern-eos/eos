.. highlight:: rst

.. index::
   single: Protocols


Protocols & APIs
----------------

HTTP access
^^^^^^^^^^^

.. index::
   pair: HTTP; WebDAV

**HTTP** access is provided using the XrdHttp plug-in running on the **MGM** recommended on
port **8443** and on **FSTs** on port **8444**.
Clients are mapped to 'nobody' if the authorization/token are missing.

.. index::
   pair: HTTP; NGINX
   pair: HTTP; Proxy

.. NOTE::

   The XrdHttp configuration is describe under http-configuration_!

Configuring an NGINX Proxy
"""""""""""""""""""""""""""

Preconditions
+++++++++++++
.. note::
   To run the HTTPS proxy you need to have the **eos-nginx** RPM installed.

.. code-block:: bash

   yum install eos-nginx

The configuration for the NGINX HTTPS proxy server is ``/etc/sysconfig/nginx``.
Each field in the configuration file is well documented.

The most important settings you might want to change are described in the following.


 .. index::
   pair: HTTP; Certificates


.. index::
   pair: NGINX; Certificates

Certificates
++++++++++++
Location of host key and host certificate:

.. code-block:: bash

   export EOS_NGINX_SSL_CERTIFICATE=/etc/grid-security/hostcert.pem
   export EOS_NGINX_SSL_KEY=/etc/grid-security/hostkey.pem

Port of the HTTPS server with X509 certifcate authentication:

.. code-block:: bash

   export EOS_NGINX_CLIENT_SSL_PORT=443

.. index::
   pair: NGINX; Kerberos

Kerberos Authentication
+++++++++++++++++++++++
Port of the HTTPS server with Kerberos5 authentication:

.. code-block:: bash

   export EOS_NGINX_CLIENT_SSL_PORT=443

Kerberos REALM and keytab file:

.. code-block:: bash

   export EOS_NGINX_GSS_KEYTAB=/etc/krb5.keytab
   export EOS_NGINX_GSS_REALM=CERN.CH

The kerberos keytab file must be readable by the daemon account!

.. index::
   pair: NGINX; Frontend-Redirecition



Frontend- or Backend- Redirection
+++++++++++++++++++++++++++++++++
NGINX is configured by default to forward redirects to the client.
However many WebDAV clients don't follow redirects. You can enable
internal (backend-) redirection proxying the full traffic like this:

.. code-block:: bash

   export EOS_NGINX_REDIRECT_EXTERNALLY=0

.. index::
   pair: NGINX; Backend-Redirecition

.. index::
   pair: NGINX; Deployment


Deployment on MGM or Gateway machines
+++++++++++++++++++++++++++++++++++++
If you want to run a proxy on a different host than the MGM, you have to modify
``/etc/nginx/nginx.eos.conf.template`` and replace **localhost** with the MGM host
name.

.. warning::
   Make sure to configure appropriate firewall rules for *non-MGM* HTTPS proxy
   deployments!

.. code-block:: bash

                  proxy_pass         http://localhost:8443/;

.. index::
   pair: HTTP; User Mapping


User Mapping
"""""""""""""
The **MGM** HTTP module does the user mapping based on the NGINX added authentication header.
Kerberos names are trivially mapped from their principal name, X509 users are mapped using
the default gridmapfile ``/etc/grid-security/grid-mapfile``.
By default all HTTP(S) traffic is mapped to nobody. To map users according to
their authentication token enable HTTPS mapping in the virtual identity interface:

.. code-block:: bash

   eosdevsrv1 # eos -b vid enable https

.. index::
   pair: HTTP; Log Files

Log Files
"""""""""
If you didn't modifiy the NGINX configuration file, NGINX will produce two log information
files with the access and error log ``/var/log/nginx/access.log`` and ``/var/log/nginx/error.log``.

The **MGM** writes a HTTP related log file under ``/var/log/eos/mgm/Http.log``.

To get more
verbose information you can change the log level:

.. code-block:: bash

   # switch to debug log level on the MGM
   eos debug debug

   # switch back to info log level on the MGM
   eos debug info

.. index::
   pair: HTTP; SE Linux

SE Linux
"""""""""

Make sure that your proxy host is not blocking outgoing connections for NGINX to the backend MGM/FST servers (port 8000/8001). The same is true for the incoming
connections on the HTTPS ports.

Proxy Certificates
""""""""""""""""""

.. warning::
   NGINX supports proxy certificates ony if they are RFC compliant!

You should create them e.g. with **grid-proxy-init** using the **-rfc** flag:

.. code-block:: bash

   grid-proxy-init -rfc

.. index::
   pair: HTTP; File Sharing Links
   pair: HTTP; pre-signed URLs

File Sharing Links
""""""""""""""""""

ROOT or HTTP URLs can be obtained with the EOS shell using

.. code-block:: bash

   eos file share myfile


.. index::
   pair: HTTP; TPC

.. index::
   single: TPC

Third Party Copy TPC
^^^^^^^^^^^^^^^^^^^^

.. index::
   pair: TPC; Delegated Credentials


HTTP and XRootD TPC with delegated credentials
"""""""""""""""""""""""""""""""""""""""""""""""

There are several ways in which a third-party transfer can be triggerred in an
XRootD based system like EOS. Currently EOS supports third-party-copy transfers
for both the XRootD and HTTP protocol.

Depending on the authetication/authorization model there are several ways in which
a third-party-copy transfer can proceed but they fall in the following broad
categories:

* `XRootD TPC without delegated credentials`_
* `XRootD TPC with delegated credentials`_
* `HTTP(S) support and HTTP TPC with token authentication`_

.. index::
   pair: TPC; Credentials

XRootD TPC without delegated credentials
"""""""""""""""""""""""""""""""""""""""""""""""

EOS enforces authentication and authorization of client on the MGM node and
supports the following authentication mechanisms:
* `KRB5 (Kerberos 5) <https://xrootd.slac.stanford.edu/doc/dev49/sec_config.htm#_Toc517294110>`_
* `GSI certificates <https://xrootd.slac.stanford.edu/doc/dev49/sec_config.htm#_Toc517294098>`_
* `SSS (Simple Shared Secret) <https://xrootd.slac.stanford.edu/doc/dev49/sec_config.htm#_Toc517294117>`_

The storage nodes (FSTs) on the other hand, accept **unix** and **SSS**
authentication, relying on the encrypted opaque information that the MGM
provides to the client when redirecting to decide if the transfer is allowed
or not.

By default, all outbound connections from the FST daemon to any other endpoint
have the **SSS** authentication mechanism enforced. Due to this, a TPC transfer
between EOS instances that don't share the same SSS key is impossible. On the
other hand, TPC transfers within the same instance will always work and this
functionality is heavily used internaly for draining, balancing and other
maintenance operations. To relax this constraint and allow non-secure connection
from the FSTs nodes to other endpoints, the service manager can set the following
environment variable to disable **SSS** enforcement.

.. code-block:: bash

  # File /etc/sysconfig/eos_env
  EOS_FST_NO_SSS_ENFORCEMENT=1

While this option can easily be enabled in different EOS services managed by
the same organization, this becomes impossible when one of the TPC endpoints
is not an EOS instance or is managed by a different entity.

The TPC model in XRootD is pull based. Therefore, TPC transfers that have the
EOS endpoint as source of the transfer work no matter the configuration setup,
while TPC transfers with EOS as the destination will fail without disabling the
SSS enforcement. A simple way to trigger a TPC transfer is by using the **xrdcp**
command with the following options:

.. code-block:: bash

   xrdcp --tpc only root://eos1.cern.ch//path/to/source root://eos1.cern.ch//path/to/destination


.. index::
   pair: TPC; Delegated Credentials

XRootD TPC with delegated credentials
"""""""""""""""""""""""""""""""""""""""""""""""

In order to enable more complex scenarios and to provide a viable alternative
to the GridFTP service, the XRootD client starting with version 4.10.0 supports
client credential delegation. Direct transfers with delegated credentials against
EOS instances work out of the box without any configuration changes.

On the other hand, for TPC transfers with delegated credentials to be supported
by an EOS instance there are several modifications needed. All these changes are
need to accomodate the fact that there is no actual authentication of the client
on the FST side, therefore there is no credential information to be delegated.

First of all, the EOS service manager will need to deploy a new XRootD Proxy
service that will act as a gatway for incoming TPC traffic. As mentioned in the
previous section, TPC transfers where EOS is the source work perfectly fine
without any configuration changes. The gateway is a vanilla **XRootD PSS**
service with the following reference configuration:

.. code-block:: bash
   :linenos:

   ofs.osslib  libXrdPss.so
   ofs.ckslib  * libXrdPss.so
   xrootd.chksum  adler32
   xrootd.seclib  libXrdSec.so
   pss.origin  eos-target-instance.cern.ch:1094
   all.export  /eos/
   all.adminpath  /var/spool/xrootd
   all.pidpath  /var/run/xrootd
   sec.protocol  gsi -dlgpxy:1 -exppxy:=creds -crl:1 -moninfo:1 -cert:/etc/grid-security/daemon/gridftp-cert.pem -key:/etc/grid-security/daemon/gridftp-key.pem -gridmap:/etc/grid-security/grid-mapfile -d:1 -gmapopt:2
   sec.protbind  * gsi
   ofs.tpc  autorm fcreds gsi =X509_USER_PROXY ttl 60 60 xfr 9 pgm /usr/local/bin/xrootd-third-party-copy.sh


The only configuratino option to be modified for new setups is the **pss.origin**
that needs to point to the EOS MGM node. Particular care should be taken when
typing the **ofs.tpc** directive to follow the exact format of the options present
in the example above. Support for delegated credentials also requires subtile
changes to the **sec.protocol** directive that are clearly explained in the
XRootD documentation and already present in the provided example.

.. The :ref:`helper script <xrootd-third-party-copy>` refereced in the configuration

The `xrootd-third-party-copy.sh` referenced in the configuration makes use of specific environment variables exported by the XRootD PSS service
in the context of the TPC process doing the transfer.

.. :caption: Contents of the xrootd-third-party-copy.sh file
.. :name: xrootd-third-party-copy

.. code-block:: bash

     #! /usr/bin/env bash
     OPTS=("${@:1:$#-2}")
     shift $(($# - 2))
     SRC=$1
     DST=$2

     if [[ -n "${XRDXROOTD_ORIGIN}" ]]; then
       DST="root://${XRDXROOTD_ORIGIN}/${DST}"
     fi

     xrdcp --server "${OPTS[@]}" "${SRC}" "${DST}"
     STATUS=$?

     if [[ ${STATUS} -ne 0 ]]; then
       logger -p err  -t xrdcp-tpc "transfer: xrdcp --server ${OPTS[*]} ${SRC} ${DST} FAILED [exit code: ${STATUS}]"
     fi

     exit ${STATUS}

Once the XRootD gateway is setup, the EOS MGM configuration needs to be updated
so that any incoming TPC transfers with delegated credentials where EOS is the
destination endpoint are redirected to the gateway node. This is done by adding
the following directive to the default EOS MGM configuration file located in
``/etc/xrd.cf.mgm``:

.. code-block:: bash

   ofs.tpc  redirect delegated eos-gateway-node.cern.ch:1094

In order to trigger a TPC transfer with delegated credentials the user needs to
have a valid X509 certificate that the xrdcp command can use during the transfer.
The xrdcp command will automatically pick up the user certificate by using the
following environment variables:

.. code-block:: bash

   # Set the path for X509 user "foo"
   export X509_USER_CERT=/home/foo/.globus/usercert.pem
   export X509_USER_KEY=/home/foo/.globus/userkey.pem

The xrdcp command can also use a user proxy certificate to trigger a TPC transfer
with delegated credentials. The easiest way for a user to obtain a proxy
certificate is to use the ``voms-proxy-init`` tool form the ``voms-client-cpp``
package.

.. code-block:: bash

   voms-proxy-init
   voms-proxy-info
   subject   : /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=foo/CN=007/CN=Foo Bar/CN=220482279
   issuer    : /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=foo/CN=007/CN=Foo Bar
   identity  : /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=foo/CN=007/CN=Foo Bar
   type      : RFC compliant proxy
   strength  : 512 bits
   path      : /tmp/x509up_u1001
   timeleft  : 11:53:16

To make sure we enforce GSI authentication and trigger the delegation of
credentians we can also set the **XrdSecPROTOCOL** environment variable together
with the following options for the xrdcp command:

.. code-block:: bash

   XrdSecPROTOCOL=gsi,unix xrdcp --tpc delegate only root://eos1.cern.ch//path/to/source root://other.world.com//path/to/destination

The minimum requirements for this setup to work correctly are the following:

  - XRootD PSS gateway >= 4.11.1
  - EOS instance >= 4.6.8
  - User XRootD client triggering the TPC transfer >= 4.11.1


.. index::
   pair: HTTP; Token Authentication

HTTP(S) support and HTTP TPC with token authentication
"""""""""""""""""""""""""""""""""""""""""""""""""""""""

EOS supports HTTP access by making use of the XrdHttp plugin which comes by
default with XRootD. There are several configuration changes that need to be
made both on the MGM side and on the FST side to have this setup working.

.. index::
   pair: HTTP; VOMS


Apart from basic HTTP(S) access with client certificates, EOS also supports
HTTP(S) with token authentication starting with version 4.6.8. There
are several extra packages that need to be installed on the MGM node to
enable this feature:

  - **xrdhttpvoms** package which allows the HTTP module to handle proxy
    certificates from the clients. This can be found in the EPEL repository.
  - **eos-scitokens** and **eos-scitokens-debuginfo** packages to enable
    support for SciTokens in EOS. These packages can be found in the
    `eos-depend repository <http://storage-ci.web.cern.ch/storage-ci/eos/diopside-depend/el-7/x86_64/>`_.
    Starting with EOS 5.0.16 there is no need to install the *eos-scitokens*
    package as EOS can use the default library provided by the XRootD framework,
    namely *libXrdAccSciTokens.so* coming from the *xrootd-server* package.

.. index::
   pair: HTTP; Sci Tokens

The following packages are not mandatory but they provide conveninent tools
for testing the token support against the EOS instance:

  - **x509-scitokens-issuer** and **x509-scitokens-issuer-client** that provide
    tools like **macaroon-init** useful when trying to acquire a macaroons for
    testing purposes. They can be found here: http://koji.chtc.wisc.edu/kojifiles/packages/

Support for HTTP(S) access in EOS is provided through an HTTP external handler
plug-in library which is distributed by default with any EOS version called
**libEosMgmHttp.so**.

Below you can find a reference configuration file that will enable HTTP(S) support
and HTTP TPC with both macaroons and scitokens on the MGM. Each line
contains a description of the functionality provided.

HTTP Configuration
""""""""""""""""""

.. index::
   pair: HTTP; Configuration



.. :caption: Contents of /etc/xrd.cf.mgm file
.. :linenos:

.. _http-configuration:

.. code-block:: bash

   # Load and enable HTTP(S) access on port 9000 on the current instance
   xrd.protocol XrdHttp:9000 /usr/lib64/libXrdHttp.so
   # Directory containing CA certificates to be used by the server
   xrd.tlsca certdir /etc/grid-security/certificates/
   # The x509 server certificate and key
   xrd.tls /etc/grid-security/daemon/hostcert.pem /etc/grid-security/daemon/hostkey.pem
   # Path to the "grid map file" to be used for mapping users to specific identities
   http.gridmap /etc/grid-security/grid-mapfile
   # Load security extractor plugin able to deal with proxy certificates and VOMS credentials
   http.secxtractor libXrdVoms.so
   # Optionally enable tracing on the HTTP plugin
   http.trace all
   # Load the XrdTpc external handler which deals only with COPY and OPTIONS http
   # verbs and provides the default HTTP TPC functionality
   http.exthandler xrdtpc /usr/lib64/libXrdHttpTPC.so
   # Load the EOS specific HTTP external handler libEosMgmHttp.so and also specify
   # the option is HTTP traffic is to be redirected to HTTP(S)
   http.exthandler EosMgmHttp /usr/lib64/libEosMgmHttp.so eos::mgm::http::redirect-to-https=0
   # The following two external library plugins are used to provide support for
   # token based authentication with Macaroons and SciTokens. Presence of the
   # second library is optional. When the SciTokens library is present and the
   # XrdMacaroons can not deal with the request then this is delegated to the
   # SciTokens library.
   # Note: Until eos-5.0.15 one needs to use the EOS specific SciTokens library
   # mgmofs.macaroonslib /usr/lib64/libXrdMacaroons.so /usr/lib64/libEosAccSciTokens.so
   # Starting with eos-5.0.16 one can use the XRootD provided SciTokens library
   mgmofs.macaroonslib /usr/lib64/libXrdMacaroons.so /usr/lib64/libXrdAccSciTokens.so
   # Base64-encoded secret key used for generating macroons. A simple way to
   # generate such a secret key is to use the following command:
   # openssl rand -base64 -out /etc/eos.macaroon.secret 64
   macaroons.secretkey /etc/eos.macaroon.secret
   # Optionally enable tracing for the XrdMacaroons plugin
   macaroons.trace all
   # Mandatory sitename configuration for the XrdMacaroons library which is also
   # embedded in the macaroons attributes
   all.sitename eosdev

A simple method of generating a valid ``/etc/eos.macaroon.secret`` file is:

.. :caption: Generating an /etc/eos.macaroon.secret file

.. code-block:: bash

   openssl rand -base64 -out /etc/eos.macaroon.secret 64


The **XrdAccSciTokens** library relies on the default **XRootD Authorization**
plugin to be loaded, which in turn checks that the file ``/opt/xrd/etc/Authfile``
file exists. Therefore, one needs to ensure the path exists and that the file is
owned by daemon:daemon user under which the MGM service runs. The service
manager also needs to put in place the basic configuration for SciTokens support
that relies on the ``/etc/xrootd/scitokens.cfg`` file. This file contains
information about the IAM (Identity and Access Management) provider that the
client/MGM service will contact for SciTokens support. A reference ``scitokens.cfg``
file is provided below:

.. :caption: Contents of the /etc/xrootd/scitokens.cfg file

.. code-block:: bash

   [Global]
   audience = https://wlcg.cern.ch/jwt/v1/any

   [Issuer OSG-Connect]
   issuer = https://wlcg.cloud.cnaf.infn.it/
   base_path = /
   map_subject = False
   default_user = dteam001

An important configuration option is the **default_user** field which specifies
the local username (i.e. known to the MGM) that any token issued by the given IAM
is mapped to.

Apart from the **MGM**, all the **FST** configurations also need to be updated in
order to support HTTP(XrdHttp) and HTTP TPC access.

.. :caption: Contents of the /etc/xrd.cf.fst file relevant for HTTP config

.. code-block:: bash

   # Enable the XrdHttp plugin and listen on port 9001 for connections
   xrd.protocol XrdHttp:9001 /usr/lib64/libXrdHttp.so
   # Load the libEosFstHttp external handler
   http.exthandler EosFstHttp /usr/lib64/libEosFstHttp.so none
   # Load the XrdTpc external handler which deals with COPY and OPTIONS http
   # verbs and provides the default HTTP TPC functionality
   http.exthandler xrdtpc /usr/lib64/libXrdHttpTPC-4.so

The port specified int the **xrd.protocol** directive is specific to the XrdHttp
plugin implementation and must be properly configured depending on the
environment variable **EOS_FST_HTTP_PORT**. The XrdHttp target port redirection
is advertised from the FST to the MGM and represents the port location
where MGM will redirect incoming clients requesting HTTP(S) access to the data.

This can easily be done by adding a systemd custom configuration file for the
FST service in ``/usr/lib/systemd/system/eos@fst.service.d/custom.conf``.

.. :caption: Contents of the custom.conf file

.. code-block:: bash

   [Service]
   Environment=EOS_FST_HTTP_PORT=9001

After starting the EOS service, one can check for the actual value of the HTTP
port advertised by the individual FSTs by executing the following command:

.. code-block:: bash

   eos fs status 1 | grep http
   stat.http.port 9001

In order to have the identity embedded in the tokens (macaroon/scitoken) properly
mapped to the local identity used in EOS, one also needs to enable the **https vid**
mapping:

.. :caption: Enable vid https mapping

.. code-block:: bash

   eos vid enable https


Practical examples for HTTP(S) transfers
"""""""""""""""""""""""""""""""""""""""""""""""

This section contains several examples of HTTP transfers done against an EOS
instance configured with support for certificates, token authorization and
with HTTP TPC. To trigger such transfers we'll make use of the **curl** command
which one of the most feature rich and reliable tools for testing HTTP access
and is also used in it's turn by other client tools that enable HTTP transfers
like for example **davix**.


.. index::
   pair: HTTP; X509

HTTP transfers with X509 credentials
"""""""""""""""""""""""""""""""""""""""""""""""

The assumption here is that the client has a valid certificate and decoded private
key available. To trigger a simple upload to EOS one can use the following command:

.. code-block:: bash

   curl -L -v --capath /etc/grid-security/certificates --cert ~/.globus/usercert.pem --cacert ~/.globus/usercert.pem --key ~/.globus/userkey.pem https://e0.cern.ch:9000//eos/dev/replica/file1.dat --upload-file /etc/passwd


   [esindril@esdss000 build_clang_ninja]$ sudo eos fileinfo /eos/dev/replica/file1.dat
   File: '/eos/dev/replica/file1.dat'  Flags: 0644
   Size: 3314
   Modify: Wed Jan 29 14:54:20 2020 Timestamp: 1580306060.468009000
   Change: Wed Jan 29 14:54:20 2020 Timestamp: 1580306060.459330223
   Birth : Wed Jan 29 14:54:20 2020 Timestamp: 1580306060.459330223
   CUid: 58602 CGid: 1028  Fxid: 00015ac5 Fid: 88773    Pid: 11   Pxid: 0000000b
   XStype: adler    XS: 74 d7 7c 3a    ETAGs: "23829820735488:74d77c3a"
   Layout: replica Stripes: 2 Blocksize: 4k LayoutId: 00100112
   #Rep: 2
   ┌───┬─────┬───────────┬──────────┬──────────────┬───────┬────────────┬────────┬──────┬──────┐
   │no.│fs-id│       host│schedgroup│          path│   boot│configstatus│   drain│active│geotag│
   └───┴─────┴───────────┴──────────┴──────────────┴───────┴────────────┴────────┴──────┴──────┘
    0       5  e0.cern.ch  default.0 /home/../fst5  booted            rw nodrain  online  elvin
    1       1  e0.cern.ch  default.0 /home/../fst1  booted            rw nodrain  online  elvin

When doing such a transfer the "grid map file" specified in the configuration of
the MGM node is used to map the client DN to a known local identity.

.. index::
   pair: HTTP; Macaroons

HTTP transfers with Macaroon authentication
"""""""""""""""""""""""""""""""""""""""""""""""

To trigger a HTTP transfer using a Macaroon token, we first need to acquire a
Macaroon from the EOS MGM endpoint using our X509 certificate and then use this
macarron to authenticate/authorize the transfer. The macaroon token will embed
the username from the X509 certificate (or the mapped identity from the
"grid map file)" so that when the token request is issued the client identity
on the server side will be mapped to this username.

.. :caption: Requesting a macaroon using a X509 certificate.

.. code-block:: bash

   # Make sure the following environment variables point to the client
   # certificate and private key
   X509_USER_CERT=/home/esindril/.globus/usercert.pem
   X509_USER_KEY=/home/esindril/.globus/userkey.pem
   # Use the macaroon-init tool to request a macaroon
   macaroon-init https://esdss000.cern.ch:9000//eos/ 60 DOWNLOAD,UPLOAD
   MDAxNGxvY2F0aW9uIGVvc2RldgowMDM0aWRlbnRpZmllciBiYzhiZWRmZC0wNzJjLTRmZWEtYjNiYy0wNDJjZjczZDhiYjMKMDAxNmNpZCBuYW1lOmVzaW5kcmlsCjAwMWZjaWQgYWN0aXZpdHk6UkVBRF9NRVRBREFUQQowMDI4Y2lkIGFjdGl2aXR5OkRPV05MT0FELFVQTE9BRCxNQU5BR0UKMDAxM2NpZCBwYXRoOi9lb3MvCjAwMjRjaWQgYmVmb3JlOjIwMjAtMDEtMjlUMTU6MTM6MzVaCjAwMmZzaWduYXR1cmUguNm15NCbrb62KCIvxxDlSgrwgMZKjGPrO7NwxFQwIycK
   # Export the token as an environment variable for easier use later on
   export MACAROON=MDAxNGxvY2F0aW9uIGVvc2RldgowMDM0aWRlbnRpZmllciBiYzhiZWRmZC0wNzJjLTRmZWEtYjNiYy0wNDJjZjczZDhiYjMKMDAxNmNpZCBuYW1lOmVzaW5kcmlsCjAwMWZjaWQgYWN0aXZpdHk6UkVBRF9NRVRBREFUQQowMDI4Y2lkIGFjdGl2aXR5OkRPV05MT0FELFVQTE9BRCxNQU5BR0UKMDAxM2NpZCBwYXRoOi9lb3MvCjAwMjRjaWQgYmVmb3JlOjIwMjAtMDEtMjlUMTU6MTM6MzVaCjAwMmZzaWduYXR1cmUguNm15NCbrb62KCIvxxDlSgrwgMZKjGPrO7NwxFQwIycK
   # Use the curl command to trigger the transfer (download) and properly
   # populate the header information with the authentication information
   curl -v -L -H "Authorization: Bearer $MACAROON" https://esdss000.cern.ch:9000/eos/dev/replica/file1.dat

For debugging purposes or just simple curiosity the client can inspect the
contents of the macaroon if they have access to the ``/etc/eos.macaroon.secret``
file. This can easily be done by installing the **python2-macaroons** package
from EPEL and launching a python shell as follows:

.. :caption: Python script to decode a Macaroon token

.. code-block:: python

   >>> import macaroons
   >>> secret = open("/etc/eos.macaroon.secret", 'r').read()
   >>> mtoken = "MDAxNGxvY2F0aW9uIGVvc2RldgowMDM0aWRlbnRpZmllciBiYzhiZWRmZC0wNzJjLTRmZWEtYjNiYy0wNDJjZjczZDhiYjMKMDAxNmNpZCBuYW1lOmVzaW5kcmlsCjAwMWZjaWQgYWN0aXZpdHk6UkVBRF9NRVRBREFUQQowMDI4Y2lkIGFjdGl2aXR5OkRPV05MT0FELFVQTE9BRCxNQU5BR0UKMDAxM2NpZCBwYXRoOi9lb3MvCjAwMjRjaWQgYmVmb3JlOjIwMjAtMDEtMjlUMTU6MTM6MzVaCjAwMmZzaWduYXR1cmUguNm15NCbrb62KCIvxxDlSgrwgMZKjGPrO7NwxFQwIycK"
   >>> M = macaroons.deserialize(mtoken)
   >>> print M.inspect()
   location eosdev
   identifier bc8bedfd-072c-4fea-b3bc-042cf73d8bb3
   cid name:esindril
   cid activity:READ_METADATA
   cid activity:DOWNLOAD,UPLOAD,MANAGE
   cid path:/eos/
   cid before:2020-01-29T15:13:35Z
   signature b8d9b5e4d09badbeb628222fc710e54a0af080c64a8c63eb3bb370c454302327


.. index::
   pair: HTTP; SciToken


HTTP transfers with SciToken authentication
"""""""""""""""""""""""""""""""""""""""""""""""

HTTP transfers with SciTokens work in a similar way to Macaroon tokens. In order
to get a SciToken, one needs to be registered with an IAM provider and install
the **oidc-agent** package which provides the client tools to register and request
tokens. An RPM package for CentOS7 is already available from the
`GitHub releases page of the project <https://github.com/indigo-dc/oidc-agent/releases>`_.

To configure the **oidc-agent**, you can follow these steps:

.. code-block:: bash

   # Start the oidc-agent in the background
   eval $(oidc-agent)
   oidc-gen WLCG-<your_username> -w device
   # Put as issuer https://wlcg.cloud.cnaf.infn.it/ and configure the set of
   # scopes as "max". Then connect the agent to the IAM provide which will
   # prompt you for the password you set up earlier.
   oidc-add WLCG-<your_username>
   # Request a token from the IAM and save it as an environment variable for
   # later use
   export SCI_TOKEN=`oidc-token WLCG-<your_username>`
   # Trigger a HTTP download using the SciToken information
   curl -v -L -H "Authorization: Bearer $SCI_TOKEN" https://esdss000.cern.ch:9000/eos/dev/replica/file1.dat


To inspect the contents of a SciToken, one can use the following commands:

.. code-block:: bash

    echo $SCI_TOKEN | cut -d. -f2 | base64 --decode | jq .
    {
      "wlcg.ver": "1.0",
      "sub": "faded49c-e1bc-4208-9634-682b2b8d16e5",
      "aud": "https://wlcg.cern.ch/jwt/v1/any",
      "nbf": 1613993622,
      "scope": "address storage.create:/ phone openid offline_access profile storage.read:/ storage.modify:/ email wlcg wlcg.groups",
      "iss": "https://wlcg.cloud.cnaf.infn.it/",
      "exp": 1613997222,
      "iat": 1613993622,
      "jti": "ea07cad1-f504-4c16-9e22-da5de2876ca7",
      "client_id": "710b4313-5ff7-4992-a59d-d404ea9d4ac5",
      "wlcg.groups": [
                "/wlcg",
                "/wlcg/xfers"
       ]
    }

HTTP TPC PULL transfers with CURL
"""""""""""""""""""""""""""""""""""""""""""""""

The following snippet provides the steps necessary for obtaining the necessary tokens for doing a HTTP TPC PULL transfer.

.. code-block:: bash

   export SRC=https://esdss000.cern.ch//eos/opstest/esindril/file.dat
   export DST=https://esdss000.cern.ch//eos/opstest/esindril/file1.dat
   # Get macaroon for source
   export TSRC=$(curl --silent --cert /tmp/x509up_u$(id -u) --key /tmp/x509up_u$(id -u) --cacert /tmp/x509up_u$(id -u) --capath /etc/grid-security/certificates -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:DOWNLOAD"], "validity": "PT3000M"}' "$SRC" | jq -r '.macaroon')
   # Get macaroon for destination
   export TDST=$(curl --silent --cert /tmp/x509up_u$(id -u) --key /tmp/x509up_u$(id -u) --cacert /tmp/x509up_u$(id -u) --capath /etc/grid-security/certificates -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:UPLOAD,DELETE,LIST"], "validity": "PT3000M"}' "$DST" | jq -r '.macaroon')
   # Trigger HTTP TPC PULL
   curl -v --capath /etc/grid-security/certificates -L -X COPY -H 'Secure-Redirection: 1' -H 'X-No-Delegate: 1' -H 'Credentials: none' -H "Authorization: Bearer $TDST" -H "TransferHeaderAuthorization: Bearer $TSRC" -H "TransferHeaderTest: Test" -H "Source: $SRC" "$DST"

The same thing now but for a HTTP TPC PUSH transfer.

.. code-block:: bash

   export SRC=https://esdss000.cern.ch//eos/opstest/esindril/xfile.dat
   export DST=https://esdss000.cern.ch//eos/opstest/esindril/xfile1.dat
   # Get macaroon for source
   export TSRC=$(curl --silent --cert /tmp/x509up_u$(id -u) --key /tmp/x509up_u$(id -u) --cacert /tmp/x509up_u$(id -u) --capath /etc/grid-security/certificates -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:DOWNLOAD"], "validity": "PT3000M"}' "$SRC" | jq -r '.macaroon')
   # Get macaroon for destination
   export TDST=$(curl --silent --cert /tmp/x509up_u$(id -u) --key /tmp/x509up_u$(id -u) --cacert /tmp/x509up_u$(id -u) --capath /etc/grid-security/certificates -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:UPLOAD,DELETE,LIST"], "validity": "PT3000M"}' "$DST" | jq -r '.macaroon')
   # Trigger HTTP TPC PUSH
   curl -v --capath /etc/grid-security/certificates -L -X COPY -H 'Secure-Redirection: 1' -H 'X-No-Delegate: 1' -H 'Credentials: none' -H "Authorization: Bearer $TSRC" -H "TransferHeaderAuthorization: Bearer $TDST" -H "Destination: $DST" "$SRC"


.. only:: adminmode

   HTTP TPC transfer triggered by FTS
   """""""""""""""""""""""""""""""""""""""""""""""

Scitags and packet marking
^^^^^^^^^^^^^^^^^^^^^^^^^^

More information about scitags can be found here: https://scitags.org/

Enable packet marking for SciTags support
"""""""""""""""""""""""""""""""""""""""""

The packet marking needs to be enabled on all the FSTs via the FST configuration file:

.. code-block:: bash

    # in xrd.cf.fst
    xrootd.pmark  use firefly scitag
    xrootd.pmark  domain any
    xrootd.pmark  ffdest <FIREFLY_COLLECTOR_IP>:<FIREFLY_COLLECTOR_PORT>
    xrootd.pmark  defsfile curl https://scitags.docs.cern.ch/api.json

More configuration can be found in the XRootD documentation related to packet marking: https://xrootd.web.cern.ch/doc/dev6/xrd_config.htm#_Toc204020828