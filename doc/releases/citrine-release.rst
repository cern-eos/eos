:orphan:

.. highlight:: rst

.. index::
   single: Citrine-Release


Citrine Release Notes
======================

``Version V4.0.10 Citrine``
- bring in all the updates from the beryl_aquamarine branch
- drop sqlite support since leveldb is now used for the FSTs
- fuse improvements regarding PAM integration and lazy open feature
- fix some merge issues in the MGM and lock ordering

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
