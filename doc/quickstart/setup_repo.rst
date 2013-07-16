.. index::
   single: Setup yum repositories

.. _eos_base_setup_repos:

Setup YUM Repository
====================

For SL5 or SL6 add the following repository
-------------------------------------------

EOS (for sl6 change "slc-5-x86_64" to "slc-6-x86_64"). Create file /etc/yum.repos.d/eos.repo with following content

.. code-block:: text

   [eos-beryll]
   name=EOS 0.3 Version
   baseurl=http://eos.cern.ch/rpms/eos-beryll/slc-5-x86_64/
   gpgcheck=0
   enabled=1

Fedora 18
---------

EOS repo /etc/yum.repos.d/eos.repo with following content

.. code-block:: text

   [eos]
   name=eos
   baseurl=http://eos.cern.ch/rpms/eos-beryll/fedora/repos/eos/$releasever/$basearch
   enabled=1
   gpgcheck=0

.. warning::
   You have to add line "exclude=xrootd*" in fedora.repo and fedora-updates.repo to avoid conflicting xrootd versions. This will be removed in furure.
