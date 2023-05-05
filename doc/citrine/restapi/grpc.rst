.. highlight:: rst

.. index::
   single: GRPC Server

.. _grpc_reference:

GRPC
=====

GRPC is a high performance open-source universal RPC framework. See https://grpc.io

Server
------

Configuration
+++++++++++++

The EOS MGM runs an embedded GRPC server if not disabled via sysconfig configuration.

The server is configured via `/etc/sysconfig/eos_env` and the following variables:

.. code-block:: text

   # GRPC server port - default is 50051 - 0 disables the GRPC server
   EOS_MGM_GRPC_PORT=50051
   # GRPC security - define to enable SSL server
   EOS_MGM_GRPC_SSL_CERT=/etc/grid-security/daemon/host.cert
   EOS_MGM_GRPC_SSL_KEY=/etc/grid-security/daemon/privkey.pem
   EOS_MGM_GRPC_SSL_CA=/etc/grid-security/daemon/ca.cert

It is not recommended to run the GRPC server without TLS support unless you use 
other measures to restrict access. The server certificate has to match the IPV4 and 
IPV6 host name if applicable.


Identity Handling
+++++++++++++++++

The client mapping is configured using the EOS CLI and the vid interface.

The vid interface allows to map requests to EOS virtual identities. If a GRPC client host
is not explicitely declared as a GRPC gateway, all requests run as user ``nobody``.

To allow a GRPC client to map to any other user than ``nobody`` add the IP as a gateway:

.. code-block:: text

   vid add gateway grpc <IPV4-IP|IPV6-IP>

To map GRPC client requests to a given user, there are two options:

* mapping by certificate common name
* mapping by authorization key

If no authorization key (token) is added to the GRPC request, certificate common name mapping will be tried.
If an authorization key (token) is present in the GRPC request, mapping by key will be used.

To add an authorication key use:

.. code-block:: text

   vid set map -grpc <key:secret-key> vuid:<uid> vid:<gid>

The client has to add this key as the ``authkey`` parameters to each GRPC request.

Client
------

The executable ``eos-grpc-ping`` is available to test the GRPC server and display the access latency.

The syntax of the command options is shown here :

.. code-block:: text

   usage: eos-grpc-ping [--key <ssl-key-file> --cert <ssl-cert-file> --ca <ca-cert-file>] [--endpoint <host:port>] [--token <auth-token>]

   e.g. eos-grpc-ping --key /etc/grid-security/daemon/privkey.pem --cert /etc/grid-security/daemon/host.cert --ca /etc/grid-security/daemon/ca.cert --endpoint foo.bar:50051 --token see_my_token
         

The xecutable ``eos-grpc-md`` is available to get individual meta data in a JSON dump for a file or container or to get a listing of a JSON dump of the parent and all children. 

.. code-block:: text

   usage: eos-grpc-md [ ... TLS parameters see above ] [--endpoint <host:port] [--token <auth-token>] [-l] <eos-path>

   e.g. eos-grpc-ping --key /etc/grid-security/daemon/privkey.pem --cert /etc/grid-security/daemon/host.cert --ca /etc/grid-security/daemon/ca.cert --endpoint foo.bar:50051 -l /eos/
