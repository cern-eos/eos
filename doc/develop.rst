.. highlight:: rst
.. index::
   single: Developing EOS

Develop
=================================================

.. image:: cpp.jpg
   :scale: 40%
   :align: left


Getting a development machine [CERN specific]:
=================================================

Go to `CERN Openstack <https://openstack.cern.ch/>`_ and find project 'IT EOS development' (if you are not a member ask your colleagues to give you access). Check in the Menu:Project/Compute/Overview if there are still free resources to be used. If not, coordinate with your colleagues on reviewing if all currently existing machines are needed. If yes, inform your section leader that you would like to ask for more resources, then follow `Request Quota <https://clouddocs.web.cern.ch/projects/project_quota_request.html>`_.

To get a decent machine, look at Menu"Project/Compute/Instances and then click "Launch Instance" button. Follow the form and when choosing  "Flavour" look for 'm2.2xlarge'. If not visible, file a ticket to the openstack team asking for making it available to you (not publicly available). Machine creation is possible also from a command line using `CERN Openstack Tools <https://clouddocs.web.cern.ch/index.html>`_.

Once the machine is running, you can log-in from aiadm.cern.ch to your machine (as root for convenience) and upgrade to newest packages/kernel etc. (:code:`yum clean all; yum upgrade; reboot;`).


Source Code
=================================================

Ask EOS developers at CERN to give you write permissions to the `EOS repository <https://gitlab.cern.ch/dss/eos.git>`_.
The current development model is that anyone with permission can directly commit to master branch. Newcomers are encouraged to create a separate branch and make a merge request with review from one of the main developers.

Install git, create your working directory (e.g. :code:`/devwork/local`) and clone the EOS repository:

.. code-block:: bash

   yum install git
   git config --global user.email "your@email.x"
   git config --global user.name "FirstName LastName"

   mkdir -p /devwork/local
   cd /devwork/local

   git clone https://gitlab.cern.ch/dss/eos.git

   cd eos
   git submodule update --recursive --init


Create a build directory ...

.. code-block:: bash

   mkdir build
   cd build


Dependencies
=================================================

.. warning:: Before compilation of the master branch you have to make sure that you installed all required dependencies.

EL7
------------------------------------

There is a convenience scripts to install all dependencies in the EOS source tree:

.. code-block:: bash

   utils/el7-packages.sh

This script might not be up to date. To be sure you are having all the dependencies installed consistently with the version of EOS code you just downloaded, one first needs to define the EOS yum repositories as stated in the `Quick Start Guide/Setup YUM Repository <http://eos-docs.web.cern.ch/eos-docs/quickstart/setup_repo.html#eos-base-setup-repos>`_. One may also look for inspiration at the Dockerfiles in the :code:`eos/gitlab-ci/prebuild_OSbase` of your repository, which follows a similar process we will layout below.

In general
------------------------------------

Ask the developers which cmake version is currently supported (cmake3 as of Dec 2020), then, install the following packages (e.g. ccache for speeding up compiling):

.. code-block:: bash

   yum install --nogpg -y ccache centos-release-scl-rh cmake3 gcc-c++ gdb make rpm-build rpm-sign yum-plugin-priorities && yum clean all


Run cmake3 with the DPACKAGEONLY=1 option and make source rpms:

.. code-block:: bash

   cmake3 ../ -DPACKAGEONLY=1 && make srpm


Now build the EOS dependencies:

.. code-block:: bash

   yum-builddep --nogpgcheck --setopt="cern*.exclude=xrootd*" -y SRPMS/*

This will install among other also the devtoolset-8 required for eos development (as of Dec 2020). Add line ‘source /opt/rh/devtoolset-8/enable’ to your bash profile to load each time you log in. This should be confirmed by getting the right path for the compiler, e.g. :code:`which c++` will show :code:`/opt/rh/devtoolset-8/root/usr/bin/c++` as the output.

You will also need to install *QuarkDB*. Define the yum repository with :code:`/etc/yum.repos.d/quarkdb.repo` file with the following content:

.. code-block:: bash

    [quarkdb-stable]
    name=QuarkDB repository [stable]
    baseurl=http://storage-ci.web.cern.ch/storage-ci/quarkdb/tag/el7/x86_64/
    enabled=1
    gpgcheck=False


Then, run:

.. code-block:: bash

    yum install quarkdb quarkdb-debuginfo redis


Important troubleshooting steps:
++++++++++++++++++++++++++++++++++

During the last command, you may encounter error such as *Error: No Package found for libmicrohttpd-devel >= 0.9.38*, this can be resolved by:

.. code-block:: bash

    yum install gnutls
    yum install libmicrohttpd-devel --disablerepo="*" --enablerepo=eos-citrine-dep
    yum-builddep --nogpgcheck --setopt="cern*.exclude=xrootd*" -y SRPMS/*


If you do not succeed enabling devtoolset-8 it might also be that you are using zsh which is incompatible with `scl-utils <https://stackoverflow.com/questions/62958800/enable-devtoolset-8-for-zsh-on-centos-7>`_.

Make sure you have compatible xrootd version installed (:code:`rpm -qa | grep xroot`), currently the above will install you version 5.0.3 which is not yet compatible with EOS <= 4.8.35 (Dec/Jan 2020). Look at the latest version of xrootd in the [eos-citrine-dep] repository (currently 4.12.6) or ask the developers if in doubt.

.. code-block:: bash

    rpm -qa | grep xroot
    > xrootd-client-libs-5.0.3-2.el7.x86_64
    > xrootd-server-devel-5.0.3-2.el7.x86_64
    > xrootd-selinux-5.0.3-2.el7.noarch
    > xrootd-libs-5.0.3-2.el7.x86_64
    > xrootd-devel-5.0.3-2.el7.x86_64
    > xrootd-server-libs-5.0.3-2.el7.x86_64
    > xrootd-server-5.0.3-2.el7.x86_64
    > xrootd-private-devel-5.0.3-2.el7.x86_64
    > xrootd-client-devel-5.0.3-2.el7.x86_64
    > xrootd-5.0.3-2.el7.x86_64

    # as mentioned above, the version needs to be the latest available in [eos-citrine-dep] repository (defined in steps above); to fix this, do the following:
    yum remove xrootd-*
    # it could also be that xrootd 4.12.6 was renamed to xrootd4 in case you have a mix of xrootd and xrootd4 `yum remove xrootd4-*` and make sure you have the right packages:
    yum install xrootd xrootd-client xrootd-server-devel xrootd-private-devel --disablerepo="*" --enablerepo=eos-citrine-dep
    # yum install xrootd-4.12.6-1.el7 xrootd-client-4.12.6-1.el7 xrootd-server-devel-4.12.6-1.el7 xrootd-private-devel-4.12.6-1.el7 --disablerepo="*" --enablerepo=eos-citrine-dep


It may also currently install *eos-folly-2020.10.05.00-1.el7.cern.x86_64* which (for EOS <=4.8.35) needs to be *2019.11.11.00-1.el7.cern*, fix this by:

.. code-block:: bash

    yum remove eos-folly eos-folly-deps
    yum install eos-folly-2019.11.11.00-1.el7.cern


Optional
++++++++++++++++++++++++++++++++++

.. code-block:: bash

    yum install -y moreutils \
    yum clean all


install moreutils just for 'ts', nice to benchmark the build time.


Compilation
=================================================


EOS is a system of libraries which gets loaded by the xrootd executable. In order to run the version you just cloned (or later modified), you have to compile those libraries and then make sure xrootd loads the correct ones. In order to facilitate the deployment, we install ninja-build package (like :code:`make` but faster):

.. code-block:: bash

    yum install ninja-build


Create a new build directory and try to run cmake (see troubleshooting below):

.. code-block:: bash

    mkdir /devwork/local/eos/build-with-ninja
    cd /devwork/local/eos/build-with-ninja
    cmake3 ../ -G Ninja -DCMAKE_INSTALL_PREFIX=/usr/ -Wno-dev -DCMAKE_C_COMPILER=/opt/rh/devtoolset-8/root/usr/bin/cc -DCMAKE_CXX_COMPILER=/opt/rh/devtoolset-8/root/usr/bin/c++


Compile:
------------------------------------


.. code-block:: bash

    ninja-build
    # run some unit tests; sould finish with [  PASSED  ] XXX tests message
    unit_tests/eos-unit-tests


Troubleshooting:
++++++++++++++++++++++++++++++++++

The following dependencies might not be required (you should be able to ignore these in the cmake3 output):

.. code-block:: bash

    Could NOT find Sphinx
    Could NOT find fuse3
    Could NOT find davix
    Could NOT find GTest


.. warning:: yum can automatically update your packages (in yum history you can see:  "-y --skip-broken update" in such a case) you can remove this package :code:`yum remove yum-autoupdate` to make sure it does not screw up EOS rpms installed.

If when executing the unit tests you have errors about the linker that could not find .so files, you can update your :code:`LD_LIBRARY_PATH` to add to it the :code:`common` and the :code:`mq` directory of your EOS build directory. 

Deployment
=================================================


Use Ninja to install EOS on your development machine:

.. code-block:: bash

    cd /devwork/local/eos/build-with-ninja
    ninja-build install

    # depending on your OS, you can remove the el6 repository
    # to avoid pulling packages and dependencies from there in the future
    rm -rf /etc/yum.repos.d/eos-el6*
    rm -rf /etc/yum.repos.d/eos-el7*


After you finish your deployment configuration (see below) and you start modifying the source code, to deploy it, you can do:

.. code-block:: bash

    sudo systemctl stop eos@*
    cd /devwork/local/eos/build-with-ninja
    # if needed rm files from the build directory and run cmake3 again before the next step
    ninja-build
    ninja-build install
    rm -rf /etc/yum.repos.d/eos-el6*
    cp /etc/xrd.cf.mgm_bkp /etc/xrd.cf.mgm
    cp /etc/xrd.cf.mq_bkp /etc/xrd.cf.mq
    systemctl daemon-reload
    systemctl start eos


Deployment Configuration:
=================================================

We need to configure and run the following set of daemons: MGM, MQ (messaging service between MGM and FSTs), several FSTs and QuarkDB.

QuarkDB
------------------------------------

Create a configuration file :code:`/etc/xrootd/xrootd-quarkdb.cfg` with the following content:

.. code-block:: bash

    xrd.port  7777
    xrd.protocol redis:7777 libXrdQuarkDB.so
    redis.mode  standalone
    redis.database  /var/lib/quarkdb/eosns
    # redis.myself  localhost:7777
    redis.password_file  /etc/eos.keytab


 In production deployment usually raft mode is used instead of standalone (you need at least 2 nodes for such a mode).


Create path to your QuarkDB namespace specified above:

.. code-block:: bash

     install -d -o daemon -g daemon /var/lib/quarkdb

Because the eos service will run as user 'daemon', you will have to make sure that all relevant files have the correct permissions and change their ownership, i.e. run:

.. code-block:: bash

    chown -R daemon:daemon /var/log/xrootd
    chown -R daemon:daemon /var/eos/
    chown -R daemon:daemon /etc/eos.* # + must have permissions 400 !
    chown -R daemon:daemon /var/run/xrootd
    chown -R daemon:daemon /var/lib/quarkdb
    chown -R daemon:daemon /var/spool/xrootd


Because the eos service will run as user "daemon", you should run the QuarkDB as the same user, i.e.:

Create your QuarkDB path (modify eostest and <myhostname>) :

.. code-block:: bash

    UUID=eostest-$(uuidgen); echo $UUID; sudo runuser daemon -s /bin/bash -c "quarkdb-create --path /var/lib/quarkdb/eosns --clusterID $UUID --nodes localhost:7777"


Before starting the service, we will need custom config drop-in script. Create the following file path :code:`/etc/systemd/system/xrootd@quarkdb.service.d/custom.conf` with the following content:

.. code-block:: bash

    [Service]
    User=daemon
    Group=daemon


Before starting the service, check once again that the keytabs :code:`/etc/eos.*` have permission 400. Then start the service (log can be followed in :code:`/var/log/xrootd/quarkdb/xrootd.log`) and check its status:

.. code-block:: bash

    systemctl start xrootd@quarkdb
    systemctl status xrootd@quarkdb


Note: `QuarkDB Installation documentation <https://quarkdb.web.cern.ch/quarkdb/docs/master/installation/>`_ is a very helpful resource, in particular for troubleshooting!

Test if the QuarkDB runs fine by saving and retrieving key-value pair:

.. code-block:: bash

    redis-cli -p 7777
    127.0.0.1:7777> set mykey myval
    OK
    127.0.0.1:7777> get mykey
    "myval"


MGM
------------------------------------


The environment configuration will be loaded from :code:`/etc/sysconfig/eos_env` which you need to create from a provided example.

.. code-block:: bash

    mv /etc/sysconfig/eos_env.example /etc/sysconfig/eos_env


Inside you need to fill in various pices of information:
- :code:`XRD_ROLES="mq mgm fst1 fst2 fst3"`, here depending on how many fst daemons you plan (here 3) to run, you need to specify them here. Drop :code:`fed` and :code:`sync` as they are not used anymore. Also, HOST_TARGET was used for the sync; not needed anymore

.. code-block:: bash

    # XRD_ROLES depends on what you wish to run, each separate mgm, mq or fst needs to be specified, e.g. for 3 fsts daemons we put fst1 fst2 fst3. Drop :code:`fed` and :code:`sync` if they ae present, they are not used anymore.
    XRD_ROLES="mq mgm fst1 fst2 fst3"
    EOS_MGM_HOST=<myhostname>.cern.ch`
    EOS_MGM_HOST_TARGET` # was used for the sync and can be commented out
    EOS_INSTANCE_NAME=eostest` # has to start with "eos" and has the form "eos<name>".
    EOS_MGM_MASTER1=<myhostname>.cern.ch`
    EOS_MGM_MASTER2=<myhostname>.cern.ch`
    EOS_MGM_ALIAS=<myhostname>.cern.ch`
    EOS_MAIL_CC=<email>`
    EOS_GEOTAG="\:\:<anything>"` # needs to be filled
    EOS_NS_ACCOUNTING=1`
    EOS_SYNCTIME_ACCOUNTING=1`
    EOS_USE_SHARED_MUTEX=1`


Then you need security keys on your machine:

.. code-block:: bash

    cp /etc/krb5.keytab /etc/eos.krb5.keytab

There is yet another configuration file you will have to modify e.g. :code:`/etc/xrd.cf.mgm` (note aside: These are configuring the xroot daemons that will be running as a service, this has nothing to do with the config of eos itself which is being saved in QuarkDB.). In this file you can change the security settings as needed, e.g.:

.. code-block:: bash

    sec.protocol unix
    sec.protocol sss -c /etc/eos.keytab -s /etc/eos.keytab
    # Example disable krb5 and gsi
    #sec.protocol krb5
    #sec.protocol gsi

    sec.protbind * only sss unix
    sec.protbind localhost unix sss
    sec.protbind localhost.localdomain unix sss


Note: that the order of sec.protbind matters for host maching (matches from "bottom up", i.e. in reverse order of specification, from most specific to least specific).

Also, activate QuarkDB namespace plugin usage and set other parameters as desired, e.g.:

.. code-block:: bash

    #mgmofs.nslib /usr/lib64/libEosNsInMemory.so
    mgmofs.nslib /usr/lib64/libEosNsQuarkdb.so

    mgmofs.instance eostest
    mgmofs.qdbcluster localhost:7777
    mgmofs.qdbpassword_file /etc/eos.keytab

Once done, backup the result to have it available after the next reinstallation of recompiled EOS:

.. code-block:: bash

    cp /etc/xrd.cf.mgm /etc/xrd.cf.mgm_bkp

Start the service, check the status and log file in :code:`/var/log/eos/mgm/xrdlog.mgm` :

.. code-block:: bash

    systemctl daemon-reload
    systemctl start eos@mgm
    systemctl status eos@mgm


You will see errors *RefreshBrokersEndpoints* this is due to the fact that MQ is not yet running:

.. code-block:: bash

    less /var/log/eos/mgm/xrdlog.mgm
    ...
    210304 11:34:22 time=1614854062.201983 func=RefreshBrokersEndpoints  level=ERROR logid=static.............................. unit=mgm@eos-ccaffy-dev01.cern.ch:1094 tid=00007f324109f700 source=XrdMqClient:495                tident= sec=(null) uid=99 gid=99 name=- geo="" msg="failed to contact broker" url="root://localhost:1097//eos/eos-ccaffy-dev01.cern.ch/mgm_mq_test?xmqclient.advisory.flushbacklog=1&xmqclient.advisory.query=1&xmqclient.advisory.status=1"
    ...

Troubleshooting:
++++++++++++++++++++++++++++++++++

If by looking at the :code:`/var/log/eos/mgm/xrdlog.mgm` log file, you see the following error:

.. code-block:: bash

    Seckrb5: Unable to start sequence on the keytab file FILE:/etc/krb5.keytab; Permission denied

Then do 

.. code-block:: bash
    
    chmod a+r /etc/krb5.keytab

MQ
------------------------------------

Look at :code:`/etc/sysconfig/eos_env` and :code:`/etc/xrd.cf.mq` in case you would want any changes there (this can stay as provided by default).

Start the service, check the status and log file in :code:`/var/log/eos/mq/xrdlog.mq` :

.. code-block:: bash

    systemctl start eos@mq
    systemctl status eos@mq

You will see expected errors in connection queue, this is because MQ can not connect to any running file system daemons (FST).
On the other hand the *RefreshBrokersEndpoints* of the MGM  :code:`/var/log/eos/mgm/xrdlog.mgm` should now disappear.


FST
------------------------------------

Look at :code:`/etc/sysconfig/eos_env` in case you would want any changes there (this can stay as provided by default). For each of XRD_ROLES there has to be a configuration file created. For each the file systems you wish to add, you need to create a new configuration file from a provided template :code:`/etc/xrd.cf.fst`. The template can be used directly, but you might want to change the port number in each:

.. code-block:: bash

    for i in {1..3}; do
        cp /etc/xrd.cf.fst /etc/xrd.cf.fst"${i}"
        sed -i "s/xrd.port 1095/xrd.port 200${i}/g" /etc/xrd.cf.fst"${i}"
    done;

Also make sure the following 2 lines are present in each cfg file created above:

.. code-block:: bash

    fstofs.qdbcluster localhost:7777
    fstofs.qdbpassword_file /etc/eos.keytab


Each file system will run its own FST daemon (which communicates via MQ with the MGM). For each of these daemons, we need to create a special drop-in script specifying the connection port:

.. code-block:: bash

    for i in {1..3}; do
        mkdir -p /usr/lib/systemd/system/eos@fst"${i}".service.d
        cd /usr/lib/systemd/system/eos@fst"${i}".service.d
        echo "[Service]" > custom.conf;
        echo "Environment=EOS_FST_HTTP_PORT=900${i}" >> custom.conf;
    done;


FST is usually represented by a disk server with many disks mounted on it.
For our dev purposes, it is usually enough to just create a directory per fst on the local file system (do not forget to grant "daemon" the ownership). In each of these directories there has to be 2 hidden files containing the ID:code:`.eosfsid` and UUID :code:`.eosfsuuid` of the FST you are adding (define however convenient for you), e.g.:

.. code-block:: bash

    mkdir -p /fst
    cd /fst
    for i in {1..3}; do
        mkdir data$i
        echo $i >  data$i/.eosfsid
        echo fst$i > data$i/.eosfsuuid; 
    done
    chown daemon:daemon -R /fst


Now, start the FST services:

.. code-block:: bash

    systemctl daemon-reload
    for i in {1..3}; do
      sudo systemctl start eos@fst"${i}"
    done;

    for i in {1..3}; do
      sudo systemctl status eos@fst"${i}"
    done;


The :code:`/var/log/eos/fstX/xrdlog.fstX` (replace X with the wanted FST number) you will see lines such as:

.. code-block:: bash

    210116 12:53:13 time=1610797993.062063 func=Storage                  level=INFO  logid=FstOfsStorage unit=fst@<hostname>:2001


Also, you should see all as active and the MQ :code:`/var/log/eos/mq/xrdlog.mq` connecting to the "nodes" (the 3 various ports) and new links being added to quarkDB :code:`/var/log/xrootd/quarkdb/xrootd.log`.

If you now run :code:` eos node ls`, you should see the list of (3) FST nodes as they connected to the MGM.


Troubleshooting:
++++++++++++++++++++++++++++++++++

If you see

.. code-block:: bash

    [QCLIENT - INFO - connectTCP:302] Encountered an error when connecting to <yourmachine>:7777 (IPv4,stream resolved from localhost): Unable to connect (111):Connection refused


in the log file :code:`/var/log/eos/fstX/xrdlog.fst1` open a firewall for port 7777:


.. code-block:: bash

     firewall-cmd --zone=public --add-port=7777/tcp --permanent
     firewall-cmd --reload


EOS namespace configuration
------------------------------------


We have QuarkDB, MQ, MGM, and FST daemons running. Now we need to define the EOS space
to which we will be adding these file systems. Similarly to production we can define "spare"
space, just to keep disks waiting unsused in spare and "default" space which will be in this example from 3 scheduling groups by 1 disk each:

.. code-block:: bash

    eos space define spare 0 0
    eos space set spare on
    eos space define default 1 3
    eos space set default on

    # check the status
    eos space status default
    eos space status spare


Now we need to register the FST disks with EOS (we first put them in "spare"):

.. code-block:: bash

    for i in {1..3}; do eos fs add fst${i} ${HOSTNAME}:200${i} /fst/data${i} spare ;done;
    # to see them added:
    eos fs ls

Then drain the disks:

.. code-block:: bash

    eos fs ls spare | awk '/fst/ {print "eos -b fs config " $3 " configstatus=drain"}' | sh -x


Make sure, they all appear as empty in the output of :code:`eos fs ls` after this operation.

Then boot them:

.. code-block:: bash

    for i in {1..3}; do eos fs boot $i;done;

If they do not boot you can check what is wrong by :code:`eos fs ls -e`.

And finally move them to the default space. They will be distributed to the scheduling groups automatically. If you were to add more fsts for development purposes we do not mind having 2 disks form the same node in the same scheduling group (use the --force option below) which is by default forbidden.

.. code-block:: bash

    eos fs ls spare | awk '/fst/ {print "eos -b fs mv --force " $3 " default"}' | sh -x


After this you will see the file systems booted, empty and online in the output of :code:`eos fs ls -e`.

Set the disks to 'rw' mode:

.. code-block:: bash

    eos fs ls default | awk '/fst/ {print "eos -b fs config " $3 " configstatus=rw"}' | sh -x
    eos fs ls

Now, you should be able to work with your EOS file system:

.. code-block:: bash

    eos ls /eos
    eos mkdir /eos/<name>/devtests
    eos attr ls /eos/<name>/devtests
    xrdcp root://localhost//eos/<name>/proc/whoami -
    cd /tmp;
    cat > hello_eos
    Hello EOS !
    eos cp hello_eos /eos/<name>/devtests/hello_eos
    eos ls -l /eos/<name>/devtests
    rm hello_eos
    xrdcp "root://localhost//eos/<name>/devtests/hello_eos" hello_eos
    cat hello_eos
    eos ns
    # [...]
    # ALL      files created since boot         1
    # ALL      container created since boot     1
    # [...]


If you enabled other authentication mechanisms than sss and unix, you need to enable them, e.g.:

.. code-block:: bash

    eos vid enable gsi
    eos vid enable krb5
