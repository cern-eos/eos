:orphan:

.. highlight:: rst

.. index::
   single: Diopside-Release


Diopside Release Notes
=====================

``Version 5 Diopside``

Introduction
------------
This release is based on XRootD V5.

``v5.0.3 Diopside``
===================

2021-10-27


Bug
----

* SPEC: Make sure both libproto* and libXrd* requirements are excluded when
  building the eos packages since these come from internally build rpms like
  eos-xrootd and eos-protobuf3 which don't expose the library so names so that
  they can be installed on a machine along with the official rpms for the
  corresponding packages if they exist.
* MGM: Avoid that a slave MGM applies an fsck configuration change in a loop

Improvements
------------

* EOS-4967: Add ARM64 support for blake3


``v5.0.3 Diopside``
===================

2021-10-27


Note
----

* This version is based on XRootD 5.3.2 that addresses some critical bug observed
  in the previous version for XRootD.

Bug
----

* MGM: Fix GRPC IPv6 parsing
* [EOS-4963] - FST: Reply with 206(PARTIAL_CONTENT) for partial content responses
* [EOS-4962] - MGM: Return FORBIDDEN if there is a public access restriction in PROFIND requests
* [EOS-4950] - FUSEX: fix race conditions in async callbacks with respect to proxy object deletions
*

New features
------------

* [EOS-4670] - FUSEX: implement file obfuscation and encryption


``v5.0.2 Diopside``
===================

2021-09-06

Bug
----

* [EOS-4809] - Make eos5 work with XrdMacaroons from XRootD5
* Includes all the fixes from 4.8.65

Improvements
------------

* WNC: Improvements to the EOS-Drive for fileinfo & health command


``v5.0.1 Diopside``
===================

2021-08-16

New features
-------------

* Comtrade WNC contribution for the server side
* Includes all the fixes from the 4.8.60 release


``v5.0.0 Diopside``
===================

2021-06-11

Major changes
--------------

* Based on XRootD 5.2.0
* Drop support for in-memory namespace
* Drop support for file based configuration
* Drop support for old high-availability setup
* Make fusex classes compatible with the latest protobuf library
* Integrate QuarkDB as part of the eos release process
