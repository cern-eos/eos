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

   
All XRootD based clients can add the oauth2 token in the endorsement environment variable for sss authentication.
   
.. code-block:: bash

   XrdSecsssENDORSEMENT=oauth2:<access_token>:<oauth-resource>
 
A requirement to use OAUTH2 is to configure/force clients to use sss authentication:

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
   
One has to create an sss key for this communication, however the sss key user can be banned on the instance:
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
  
   # server issues a scoped token binding to a user/group
   TOKEN=`eos token --path /eos/cms/www/ --permission rwx --expires 1600000000 --owner cmsprod --group zh`
 
   ############################
   # client
   ############################
   
   # exports the token in the environment
   export XrdSecsssENDORSEMENT=oauth2:.....:auth.cern.ch/auth/realms/cern/protocol/openid-connect/userinfo

   # test the ID
   [ ~]$ eos whoami
   Virtual Identity: uid=1234 (1234,65534,99) gid=1234 (1234,99) [authz:oauth2] host=localhost domain=localdomain geo-location=cern key=<oauth2> fullname='Foo Bar' email='foo.bar@cern.ch'





