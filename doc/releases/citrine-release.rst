:orphan:

.. highlight:: rst

.. index::
   single: Citrine-Release


Citrine Release Notes
======================

``Version V4.0.0 Citrine``

Introduction
------------
This is the first release targeted for clients to use EOS with XRootD >= 4.2.X.
It is merged from the Aquamarine Version 0.3.130. Main difference to Aquamarine
is the use of XRootD 4 and the new tree-based scheduling algorithm.

``v4.1.23 Citrine``
+++++++++++++++++++

Bugfix
++++++
* MGM: Take headroom into account when scheduling for placement
* MGM: Add protection in case the bookingsize is explicitly set to 0
* ARCHIVE: Use the MgmOfsAlias consistently otherwise the newly generated archive file will contain invalid JSON lines.


``v4.1.22 Citrine``
+++++++++++++++++++

Bugfix
++++++
* Fix response for xrdfs query checksum to display "adler32" instead of "adler" as checksum type
* Fix launch of the follower thread for the MGM slave


``v4.1.21 Citrine``
+++++++++++++++++++

Bugfix
++++++
* [EOS-1833] - eosfuse.cc uses a free'd fuse_req_t -> segfault
* [EOS-1781] - MGM crash in GeoBalancer
* [EOS-1642] - "Bad address" on EOS FUSE should be "Permission denied"
* [EOS-1830] - Recycle bin list crash when doing full scan (need protection)


Task
++++
* [EOS-1848] - selinux error when uninstalling eos-fuse-core

User Documentation
++++++++++++++++++
* [EOS-1826] - Missing dependencies on the front page

Suggestion
++++++++++
* [EOS-1827] - Ancient version of zmq.hpp causing issues when compiling with new zmq.h (taken from system)
* [EOS-1828] - Utils.hh in qclient #include cannot find header
* [EOS-1831] - CMAKE, microhttpd, and client
* [EOS-1832] - Bug in console/commands/com_fuse.cc with handling of environment variable EOS_FUSE_NO_MT


``v4.1.3 Citrine``
++++++++++

Bugfix
+++++++

* [EOS-1606] - Reading root files error when using eos 4.1.1
* [EOS-1609] - eos -b problem : *** Error in `/usr/bin/eos': free():


``v0.4.30 Citrine``
+++++++++++++++++++++++

Bugfix
+++++++

- FUSE: when using krb5 or x509, allow both krb5/x509 and unix so that authentication
        does not fail on the fst (using only unix) when using XRootD >= 4.4


``v0.4.30 Citrine``
+++++++++++++++++++++++

Bugfix
+++++++

- SPEC: Add workaround in the %posttrans section of the eos-fuse-core package
        to keep all the necessary files and directories when doing an update.
- CMAKE: Remove the /var/eos directory from the eos-fuse-core package and fix
        type in directory name.

``v0.4.29 Citrine``
+++++++++++++++++++++++

Bugfix
+++++++

- MGM: add monitoring switch to space,group status function
- MGM: draing mutex fix and fix double unlock when restarting a drain job
- MGM: fixes in JSON formatting, reencoding of non-http friendly tags/letters like <>?@
- FST: wait for pending async requests in the close method
- SPEC: remove directory creation scripting from spec files

New Features
++++++++++++

- RPM: build one source RPM which creates by default only client RPMs with less dependencies
