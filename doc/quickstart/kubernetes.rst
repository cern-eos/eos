.. index::
   single: Kubernetes

.. _eos_base_kubernetes:

.. _kubernetes: https://kubernetes.io/docs/home/

EOS Kubernetes Installation
===========================

.. image:: kubernetes-logo.png
   :scale: 50 %
   :align: center   


A Kubernetes installation provides a fully functional distributed EOS instances on top of Kubernetes clusters,
keeping the advantages of a straightforward set up and a short deployment time.
The EOS team provides docker images with all the necessary components installed and ready to use.


Preparation
-----------

.. note::

   Make sure you have `kubectl <https://kubernetes.io/docs/reference/kubectl/overview/>`_ installed and access rights to a Kubernetes cluster on your system.
   To start playing around by using a virtual cluster in your local machine, you can use `Minikube <https://kubernetes.io/docs/tasks/tools/install-minikube>`_.


Run EOS in Kubernetes
---------------------

Checkout the eos-on-k8s project:

.. code-block:: bash

   git clone https://gitlab.cern.ch/faluchet/eos-on-k8s.git
   cd eos-on-k8s


To start a small instance with 7 storage servers (**FST**), 1 namespace server (**MGM**), 1 messaging borker (**MQ**), 1 client and 1 Kerberos **KDC** Kubernetes Pod ready to use, all you have to do is to use the `create-all.sh <https://gitlab.cern.ch/faluchet/eos-on-k8s/blob/master/create-all.sh>`_ script. The only mandatory argument, to be provided with the -n flag, is a namespace of your choice representing the world where the resources will live in. The EOS entities ("roles") will reside on the same network knowing about each other and are configured to be working out-of-the-box.  
We refer the courious reader to the `official documentation <https://kubernetes.io/docs/home/>`_ to deepen his knowledge about Kubernetes concepts. 

.. code-block:: bash

   ./create-all -n <your_namespace>

Wait for the resources creation and EOS setup, and you are ready to go. You can check the cluster state in any moment, i.e. with:  

.. code-block:: bash

   kubectl get nodes # get the cluster node list 
   kubectl get all # get all the Kubernetes resources residing on the "default" namespace.  
   kubectl get all -n <your_namespace>


.. note::

   From now on we will use the namespace "tutorial".  


To connect to EOS using the eos shell CLI running in the MGM container you can do:  

.. code-block:: bash

   kubectl exec -n tutorial -it <mgm_pod_name> -- eos  

It is a bit verbose though very easy getting the name of a Pod of your interest, through the use of easy-to-remeber labels:

.. code-block:: bash

   kubectl get pods -n tutorial --no-headers -o custom-columns=":metadata.name" -l app=eos-mgm

So, all together:  

.. code-block:: bash

   kubectl exec -n tutorial -it $(kubectl get pods -n tutorial --no-headers -o custom-columns=":metadata.name" -l app=eos-mgm) -- eos
   EOS Console [root://localhost] |/> whoami
   whoami
   Virtual Identity: uid=0 (2,99,3,0) gid=0 (99,4,0) [authz:sss] sudo* host=localhost

.. code-block:: bash

   EOS Console [root://localhost] |/> version
   version
   EOS_INSTANCE=eosdockertest
   EOS_SERVER_VERSION=4.4.38 EOS_SERVER_RELEASE=1
   EOS_CLIENT_VERSION=4.4.38 EOS_CLIENT_RELEASE=1

.. code-block:: bash

   EOS Console [root://localhost] |/> node ls
   ┌──────────┬─────────────────────────────────────────────────┬────────────────┬──────────┬────────────┬──────┬──────────┬────────┬────────┬────────────────┬─────┐
   │type      │                                         hostport│          geotag│    status│      status│  txgw│ gw-queued│  gw-ntx│ gw-rate│  heartbeatdelta│ nofs│
   └──────────┴─────────────────────────────────────────────────┴────────────────┴──────────┴────────────┴──────┴──────────┴────────┴────────┴────────────────┴─────┘
    nodesview  eos-fst1.eos-fst1.tutorial.svc.cluster.local:1095     docker::test     online           on    off          0       10      120                2     1 
    nodesview  eos-fst2.eos-fst2.tutorial.svc.cluster.local:1095     docker::test     online           on    off          0       10      120                1     1 
    nodesview  eos-fst3.eos-fst3.tutorial.svc.cluster.local:1095     docker::test     online           on    off          0       10      120                1     1 
    nodesview  eos-fst4.eos-fst4.tutorial.svc.cluster.local:1095     docker::test     online           on    off          0       10      120                1     1 
    nodesview  eos-fst5.eos-fst5.tutorial.svc.cluster.local:1095     docker::test     online           on    off          0       10      120                1     1 
    nodesview  eos-fst6.eos-fst6.tutorial.svc.cluster.local:1095     docker::test     online           on    off          0       10      120                1     1 
    nodesview  eos-fst7.eos-fst7.tutorial.svc.cluster.local:1095     docker::test     online           on    off          0       10      120                1     1 


You can mount EOS to the client Pods using FUSE and KRB5 authentication:

.. code-block:: bash

   kubectl exec -n tutorial -it $(kubectl get pods -n tutorial --no-headers -o custom-columns=":metadata.name" -l app=eos-cli1) -- eos fuse mount /eos

   .... trying to create ... /eos
   ===> Mountpoint   : /eos
   ===> Fuse-Options : max_readahead=131072,max_write=4194304,fsname=eos-mgm1.eoscluster.cern.ch,url=root://eos-mgm1.eoscluster.cern.ch//eos/
   ===> fuse readahead        : 1
   ===> fuse readahead-window : 1048576
   ===> fuse debug            : 0
   ===> fuse low-level debug  : 0
   ===> fuse log-level        : 5
   ===> fuse write-cache      : 1
   ===> fuse write-cache-size : 67108864
   ===> fuse rm level protect : 1
   ===> fuse lazy-open-ro     : 0
   ===> fuse lazy-open-rw     : 1
   ==== fuse multi-threading  : true
   info: successfully mounted EOS [root://eos-mgm1.eoscluster.cern.ch] under /eos

.. code-block:: bash

   kubectl exec -n tutorial -it $(kubectl get pods -n tutorial --no-headers -o custom-columns=":metadata.name" -l app=eos-cli1) -- bash 
   
   ls -la /eos/
   total 4
   drwxrwxr-x.  1 root root    0 Jan  1  1970 .
   drwxr-xr-x. 18 root root 4096 Mar 14 10:16 ..
   drwxrwxr-x.  1 root root    0 Jan  1  1970 dockertest

Or by running the EOS instance testsuite:

.. code-block:: bash

   kubectl exec -n tutorial -i $(kubectl get pods -n tutorial --no-headers -o custom-columns=":metadata.name" -l app=eos-mgm) -- eos-instance-test


Delete and clean
----------------

Use the `delete-all.sh <https://gitlab.cern.ch/faluchet/eos-on-k8s/blob/master/delete-all.sh>`_ script to remove the EOS instance from your system.

.. code-block:: bash

   ./delete-all.sh tutorial


Image Repository
----------------

You can get the images for each automatic build and for each release.
The release images are tagged with the release version. Regular images are tagged with the build id of their originating pipeline.

Docker images are accessible from the project's `registry <https://gitlab.cern.ch/dss/eos/container_registry>`_.

.. code-block:: bash

   docker pull gitlab-registry.cern.ch/dss/eos:<tag>

Example for a build

.. code-block:: bash

   docker pull gitlab-registry.cern.ch/dss/eos:777552

Example for the latest release

.. parsed-literal::

   docker pull gitlab-registry.cern.ch/dss/eos:|version| 


Kubernetes-ready images are available since release version 4.4.37


Selfmade images
---------------

In case you would like to create a different setup, you are welcome to browse and reuse the provided scripts under
the `image_scripts <https://gitlab.cern.ch/eos/eos-docker/tree/master/image_scripts>`_ folder of the eos-docker project to get an idea on how to do it.
