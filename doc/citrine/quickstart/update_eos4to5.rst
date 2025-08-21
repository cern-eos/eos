.. index::
   single: Update to EOS 5 from EOS 4

.. _eos_base_update_eos4to5:

Update from EOS 4 to EOS 5
================

.. warning::
   Warning: With EOS5, the MGM is expecting to have its configuration in QuarkDB already, so, before starting the upgrade, it is really important to follow the instructions for exporting the file configuration into QuarkDB: :ref:`quarkdbconfig`.

.. warning::
   Before you start: please make sure you already run in HA mode, or that you start the upgrade process with the active MGM, otherwise you risk of running two active MGMs at the same time, with unknown consequences.

.. warning:: 
   When using HA mode, there might still be an issue (< 5.0.17, April 2022) with FSTs losing symkeys (and by that capability to communicate with MGM) during switch over of the master to another headnode. We advise to run in single MGM mode only (i.e. switching off all other MGM processes on the other slave nodes of the headnode cluster until this issue gets resolved). 

The recipe below is tailored to CERN EOS team puppet manifests, but contains general information useful for any EOS 4 to EOS 5 upgrade. 

After the above has been put in place, one needs to add the right repositories to the manifest:

.. code-block:: text

   eosserver::repo::repo_main_url: https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-7/x86_64/
   eosserver::repo::repo_deps_url: https://storage-ci.web.cern.ch/storage-ci/eos/diopside-depend/el-7/x86_64/


Please cross check the XrdHttp port configuration and mapping to be as in e.g.: `code/manifests/pilot/servers/ns.pp` (or LHCb).  In order to have both XrdHttp and libmicro working side by side (In xrootd 4 the http plugin is installed after the ofs is loaded, while in xrootd 5 the http plugin is installed after the xroot plugin and then the ofs layer will try start libmicro at which point libmiro may fail unless the ports are set correctly). In the sysconfig, you should have: 


.. code-block:: text

   EOS_MGM_HTTP_PORT: 8443
   xrd.protocol: XrdHttp:8444 libXrdHttp.so


and in `code/manifests/pilot/servers/ns.pp`:


.. code-block:: text

  # Redirect 443 to 8444 for XrdHttp traffic
  ['iptables', 'ip6tables'].each |Integer $index, String $provider| {
    firewall { "120-${index} redirect 443 to 8444 for XrdHttp traffic":
      chain    => 'PREROUTING',
      table    => 'nat',
      jump     => 'REDIRECT',
      provider => $provider,
      proto    => 'tcp',
      dport    => 443,
      toports  => 8444,
    }
  firewall { "121-${index} redirect 443 to 8444 for XrdHttp local traffic":
      chain    => 'OUTPUT',
      table    => 'nat',
      jump     => 'REDIRECT',
      provider => $provider,
      outiface => 'lo',
      proto    => 'tcp',
      dport    => 443,
      toports  => 8444,
    }
  }

  # Redirect 8000 to 8443 for Nginx HTTP traffic
  ['iptables', 'ip6tables'].each |Integer $index, String $provider| {
    firewall { "122-${index} redirect 8000 to 8443 for Nginx HTTP traffic":
      chain    => 'PREROUTING',
      table    => 'nat',
      jump     => 'REDIRECT',
      provider => $provider,
      proto    => 'tcp',
      dport    => 8000,
      toports  => 8443,
    }
  }




Important thing to notice is that the quarkdb library comes now with EOS itself `eos-quarkdb`, for this, one needs to have a line like the following one in the manifest which will uninstall old `quarkdb` package and install the new `eos-quarkdb`:  `hg_eos::private::ns_compact::eos5enabled: true`. If not present `eos-quarkdb` will be uninstalled ! This is why we first keep this flag false --> run puppet --> disable puppet --> install new EOS version with `eos-quarkdb` manually (based on eos dependencies) and finally change this flag to true in the manifest and enable puppet again once EOS 5 is actually running. This is to avoid that puppet removed the `quarkdb`/`eos-quarkdb` package while EOS 4/5 is running using it. 

This means, first we change: 

.. code-block:: text

   hg_eos::include::versionlock::eosversion: 4.8.74-1.el7.cern
   hg_eos::include::versionlock::xrootversion: 4.12.8-1.el7



to

.. code-block:: text

   hg_eos::include::versionlock::eosversion: 5.0.9-1.el7.cern
   hg_eos::include::versionlock::xrootversion: 5.3.4-1.el7

 
In addition in servers.yaml (/ns.yaml) change these lines:


.. code-block:: text

   http.cadir: /etc/grid-security/certificates/
   http.cert: /etc/grid-security/daemon//hostcert.pem
   http.key:  /etc/grid-security/daemon/hostkey.pem
   http.gridmap: /etc/grid-security/grid-mapfile
   http.secxtractor: libXrdVoms.so
   mgmofs.macaroonslib: libXrdMacaroons.so /opt/eos/lib64/libXrdAccSciTokens.so


to (for versions < 5.0.16): 

.. code-block:: text

   xrd.tls: /etc/grid-security/daemon/hostcert.pem /etc/grid-security/daemon/hostkey.pem
   xrd.tlsca: certdir /etc/grid-security/certificates/
   http.gridmap: /etc/grid-security/grid-mapfile
   http.secxtractor: libXrdHttpVOMS.so
   mgmofs.macaroonslib: libXrdMacaroons.so libEosAccSciTokens.so



For versions 5.0.16+:


.. code-block:: text

   xrd.tls: /etc/grid-security/daemon/hostcert.pem /etc/grid-security/daemon/hostkey.pem
   xrd.tlsca: certdir /etc/grid-security/certificates/
   http.gridmap: /etc/grid-security/grid-mapfile
   http.secxtractor: libXrdHttpVOMS.so
   mgmofs.macaroonslib: libXrdMacaroons.so libXrdAccSciTokens.so



and make sure the library path states: 

.. code-block:: text

   LD_LIBRARY_PATH: "/opt/eos/xrootd/lib64/:$LD_LIBRARY_PATH"



One need to have `/opt/eos/xrootd/lib64/` in `LD_LIBRARY_PATH` for the `libXrdMacaroons.so` and all the xrootd libs which are loaded when starting by the daemon and searched in the usual locations. On the other hand, e.g. `libEosAccSciTokens.so` is 
already in `/usr/lib64/` by default since everything that we install from eos-server goes there.

And in storage.yaml: 

From:

.. code-block:: text
   
   http.cadir: /etc/grid-security/certificates/



to


.. code-block:: text

   xrd.tls: /etc/grid-security/daemon/hostcert.pem /etc/grid-security/daemon/hostkey.pem
   xrd.tlsca: certdir /etc/grid-security/certificates/


Run puppet and then we disable puppet from running:


.. code-block:: text

   puppet agent -tv 
   puppet agent --disable 'MGM upgrade to EOS 5: avoiding removal of future eos-quarkdb package after upgrade to EOS 5'



Remove few obsolete packaged replaced newly by eos dependencies automatically (this also prevents to pull xrootd4 packages for upgrades from epel which we do not want, the versionlock for xrootd packages can be removed entirely from our manifests later with the caviat of checking the xrootd path for all use-cases, for example for FED functionality of CMS one needs to update the xrootd binary location in the systemd script). 

This is where the instance availability gets affected:


.. code-block:: text
   
   yum remove xrdhttpvoms
   yum remove eos-scitokens


Upgrade `scitokens-cpp` package (will be having strict dependency in EOS releases > 5.0.19 where this should not be necessary to be done explicitly):


.. code-block:: text

   yum upgrade scitokens-cpp


Check that `eos-quarkdb` gets installed based on dependencies resolved in the last command:


.. code-block:: text

   yum upgrade "eos-*" "xrootd-*"


Update puppet manifest again from: 

.. code-block:: text

   hg_eos::private::ns_compact::eos5enabled: false


to

.. code-block:: text

   hg_eos::private::ns_compact::eos5enabled: true


And run puppet:


.. code-block:: text

   puppet agent --enable
   puppet agent -tv 


Check the service status and other usual checks

.. code-block:: text

   systemctl status eos@*
   systemctl status xrootd@quarkdb
   rpm -qa | grep eos
   rpm -qa | grep xroot



One needs to run `yum reinstall eos-grpc` on all headnodes and FSTs before proceeding with the usual procedure.


