.. highlight:: rst

.. index::
   pair: HTTP; WebDAV

HTTP access
=======================

Configuration
-------------

The XrdHttp **HTTP** access has to be configured in ``/etc/xrd.cf.mgm``

To configure HTTP+HTTPS on port 9000 use:

.. code-block:: bash

   if exec xrootd
      xrd.protocol XrdHttp:9000 /usr/lib64/libXrdHttp-4.so
      http.exthandler EosMgmHttp /usr/lib64/libEosMgmHttp.so none
      http.cert /etc/grid-security/daemon/host.cert
      http.key /etc/grid-security/daemon/privkey.pem
      http.cafile /etc/grid-security/daemon/ca.cert
   fi

To configure only HTTP remove the **http.cert** **http.key** and **http.cafile** entries above.

Clients are mapped to 'nobody' if https mapping is not enabled (``vid enable https``), the client cannot get mapped according to the grid certificate (using the gridmapfile ``/etc/grid-security/grid-mapfile`` or the **remote-user** header is not present (http). When the **remote-user** header is used in combination with http, the client machine has to be added as a gateway for https. (``vid add gateway [::ffff:188.184.116.37] https``). The mapping is NOT done by the XrdHttp plug-in.

XrdHttp is currently supported only on the MGM and runs in parallel to the default libmicrohttpd implementation.
