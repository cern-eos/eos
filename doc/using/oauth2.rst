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
   # allow an oauth2 resource in requests (OIDC infrastructure)
   eos vid set map -oauth2 key:auth.cern.ch/auth/realms/cern/protocol/openid-connect/userinfo vuid:0

If you want to check the audience claim in the ticket, you can add the audience to screen to each oauth2 resource:

.. code-block:: bash

   # allow on oauth2 resource in request for the audience 'eosoauth'
   eos vid set map -oauth2 key:auth.cern.ch/auth/realms/cern/protocol/openid-connect/userinfo@eosatuch vuid:0

   
All XRootD based clients can add the oauth2 token in the endorsement environment variable for sss authentication.
   
.. code-block:: bash

   XrdSecsssENDORSEMENT=oauth2:<access_token>:<oauth-resource>
 
OAUTH2 is enabled by default, but can be explicitly en-/or disabled:

.. code-block:: bash

   # eos CLI/xrdcp etc.
   env XrdSecPROTOCL=sss
   env XrdSecsssENDORSEMENT=oauth2:...
   eos whoami

   # eosxd config file parameter

   "auth" : { 
     "oauth2" : 1, #default
     "ssskeytab" : "/etc/eos/fuse.sss.keytab", #default
    }

    export OAUTH2_TOKEN=FILE:/tmp/oauthtk_1000
    # /tmp/oauthtk_1000 contains oauth2:<token>:<oauth-url>
    ls /eos/ 
   
One has to supply an sss key for this communication, however the sss key user can be banned on the instance:
Client and server should share an sss key for a user, which is actually not authorized to use the instance e.g.

.. code-block:: bash

   ############################
   # client
   ############################
   echo 0 u:nfsnobody g:nfsnobody n:eos-test N:5506672669367468033 c:1282122142 e:0 k:0123456789012345678901234567890123456789012345678901234567890123 > $HOME/.eos.keytab
   # point to keytab file
   export XrdSecSSSKT=$HOME/.eos.keytab
   # enforce sss
   export XrdSecPROTOCOL=sss

   ############################
   #server
   ############################

   # server shares the same keytab entry
   echo 0 u:nfsnobody g:nfsnobody n:eos-test N:5506672669367468033 c:1282122142 e:0 k:0123456789012345678901234567890123456789\012345678901234567890123 >> /etc/eos.keytab

   # server bans user nfsnobody or maybe uses already user allow, which bans this user by default
   eos access ban user nfsnobody
  
   ############################
   # client
   ############################
   
   # exports the token in the environment
   export XrdSecsssENDORSEMENT=oauth2:.....:auth.cern.ch/auth/realms/cern/protocol/openid-connect/userinfo

   # test the ID
   [ ~]$ eos whoami
   Virtual Identity: uid=1234 (1234,65534,99) gid=1234 (1234,99) [authz:oauth2] host=localhost domain=localdomain geo-location=cern key=<oauth2> fullname='Foo Bar' email='foo.bar@cern.ch'





