.. highlight:: rst

HTTP access
=======================

The plain **HTTP** access is always up and running on the **MGM** on 
port **8000** and on **FSTs** on port **8001**.
Clients are mapped to 'nobody' if the authentication headers are missing 
(e.g. the access did not go via an HTTPS proxy in front). 

You should make sure that access to the **MGM** on port **8000** is only possible from **HTTPS** 
proxies by setting up firewall rules.

Configuration
-------------

Preconditions
+++++++++++++
.. note::
   To run the HTTPS proxy you need to have the **eos-nginx** RPM installed.

.. code-block:: bash

   yum install eos-nginx

The configuration for the NGINX HTTPS proxy server is ``/etc/sysconfig/nginx``.
Each field in the configuration file is well documented.

The most important settings you might want to change are described in the following.
 
Certificates
++++++++++++
Location of host key and host certificate:

.. code-block:: bash

   export EOS_NGINX_SSL_CERTIFICATE=/etc/grid-security/hostcert.pem
   export EOS_NGINX_SSL_KEY=/etc/grid-security/hostkey.pem

Port of the HTTPS server with X509 certifcate authentication:

.. code-block:: bash
  
   export EOS_NGINX_CLIENT_SSL_PORT=443

Kerberos Authentication
+++++++++++++++++++++++
Port of the HTTPS server with Kerberos5 authentication:

.. code-block:: bash
  
   export EOS_NGINX_CLIENT_SSL_PORT=443

Kerberos REALM and keytab file:

.. code-block:: bash
 
   export EOS_NGINX_GSS_KEYTAB=/etc/krb5.keytab
   export EOS_NGINX_GSS_REALM=CERN.CH

Frontend- or Backend- Redirection
+++++++++++++++++++++++++++++++
NGINX is configured by default to forward redirects to the client.  
However many WebDAV clients don't follow redirects. You can enable
internal (backend-) redirection proxying the full traffic like this:

.. code-block:: bash
  
   export EOS_NGINX_REDIRECT_EXTERNALLY=0

Deployment on MGM or Gateway machines
+++++++++++++++++++++++++++++++++++++
If you want to run a proxy on a different host than the MGM, you have to modify
``/etc/nginx/nginx.eos.conf.template`` and replace **localhost** with the MGM host
name. 

.. warning::
   Make sure to configure appropriate firewall rules for *non-MGM* HTTPS proxy
   deployments! 

.. code-block:: bash

                  proxy_pass         http://localhost:8000/;

User Mapping
------------
The **MGM** HTTP module does the user mapping based on the NGINX added authentication header.
Kerberos names are trivially mapped from their principal name, X509 users are mapped using
the default gridmapfile ``/etc/grid-security/grid-mapfile``.
By default all HTTP(S) traffic is mapped to nobody. To map users according to 
their authentication token enable HTTPS mapping in the virtual identity interface:

.. code-block:: bash

   eosdevsrv1 # eos -b vid enable https

Log Files
---------
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

Proxy Certificates
------------------

.. warning::
   NGINX supports proxy certificates ony if they are RFC compliant!
   
You should create them e.g. with **grid-proxy-init** using the **-rfc** flag:

.. code-block:: bash

   grid-proxy-init -rfc

    