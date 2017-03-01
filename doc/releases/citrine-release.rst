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

``v4.1.18 Citrine``
++++++++++

Bugfix
+++++++

* FUSE: don't let 'illegal' responses pass into a string exception in LazyOpen
* FUSE: set the link count always to 1 for files or symbolic links to make things like gzip work
* FUSE: fix double locking issue when sid!=pid after implementation of hashed locking in AuthIdMananger


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
