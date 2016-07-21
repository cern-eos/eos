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

Bug Fixes
---------

Consolidation
-------------

New Features
------------

``V0.4.30 Aquamarine``
+++++++++++++++++++++++

Bug Fix
+++++++

- SPEC: Add workaround in the %posttrans section of the eos-fuse-core package
        to keep all the necessary files and directories when doing an update.
- CMAKE: Remove the /var/eos directory from the eos-fuse-core package and fix
        type in directory name.

``V0.4.29 Aquamarine``
+++++++++++++++++++++++

Bug Fix
+++++++

- MGM: add monitoring switch to space,group status function
- MGM: draing mutex fix and fix double unlock when restarting a drain job
- MGM: fixes in JSON formatting, reencoding of non-http friendly tags/letters like <>?@
- FST: wait for pending async requests in the close method
- SPEC: remove directory creation scripting from spec files

New Features
++++++++++++

- RPM: build one source RPM which creates by default only client RPMs with less dependencies
