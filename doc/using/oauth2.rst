.. highlight:: rst

.. _oauth2:

Using OAUTH2 for authentication
===============================

To enable OAUTH2 token translation, one has to configure the resource endpoint and enable OAUTH2 mapping:

.. code-block:: bash

   # enable oauth2 mapping
   eos vid enable oauth2
   # allow an oauth2 resource in requests
   eos vid set map -oauth2 key:oauthresource.web.cern.ch/api/User vuid:0

   
All XRootD based clients can add the oauth2 token in the endorsement environment variable for sss authentication.
   
.. code-block:: bash

   XrdSecsssENDORSEMENT=oauth2:<access_token>:<oauth-resource>
 
To use OAUTH2 has to configure/force clients to use sss authentication:

.. code-block:: bash

   # eos CLI/xrdcp etc.
   env XrdSecPROTOCL=sss
   env XrdSecsssENDORSEMENT=oauth2:...
   eos whoami

   # eosxd config file parameter

   "auth" : { 
     "sss" : 1,
     "ssskeytab" : "/etc/eos/fuse.sss.keytab",
    }

    env XrdSecsssENDORSEMENT=oauth2:...
    ls /eos/ 
   


