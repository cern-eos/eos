.. highlight:: rst

.. _tokens:

Using EOS Tokens for Authorization
==================================

We provide a generic EOS mechanism to delegate permissions to a token bearer with s.c. EOS tokens. 

The JSON representation of an EOS token looks like shown here:

.. code-block:: bash

   {
    "token": {
     "permission": "rwx",
     "expires": "1571319146",
     "owner": "",
     "group": "",
     "generation": "1",
     "path": "/eos/dev/token",
     "allowtree": false,
     "vtoken": "",
     "voucher": "baecb618-f0e4-11e9-85d9-fa163eb6b6cf",
     "requester": "[Thu Oct 17 15:47:59 2019] uid:0[root] gid:0[root] tident:root.13809:107@localhost name:daemon dn: prot:sss host:localhost domain:localdomain geo:cern sudo:1",
     "origins": []
    },
    "signature": "daUeOZafRUt6VfQZ+g3FMbR/ZA5WvARELqFwdQxbyFU=",
    "serialized": "CgJyeBDq2qHtBTIJL2Vvcy9kZXYvSiRiYWVjYjYxOC1mMGU0LTExZTktODVkOS1mYTE2M2ViNmI2Y2ZSnAFbVGh1IE9jdCAxNyAxNTo0Nzo1OSAyMDE5XSB1aWQ6MFtyb290XSBnaWQ6MFtyb290XSB0aWRlbnQ6cm9vdC4xMzgwOToxMDdAbG9jYWxob3N0IG5hbWU6ZGFlbW9uIGRuOiBwcm90OnNzcyBob3N0OmxvY2FsaG9zdCBkb21haW46bG9jYWxkb21haW4gZ2VvOmFqcCBzdWRvOjE=",
    "seed": 1399098912
   }


Essentially this token gives the bearer the permission to ``rwx`` for the file /eos/dev/token. The token does not bear an
owner and group information, which means, that the creations will be accounted on the mapped authenticated user using this token or an enforced ``sys.owner.auth`` entry. If the token should map the authenticated user, one can add ``owner`` and ``group`` fields. In practical terms the token removes existing user and system ACL entries and places the token user/group/permission entries as a system ACL.

Tokens are signed, zlib compressed, base64url encoded with a replacement of the '+' and '/' characters with '-' and '_'  and a URL encodign of the '=' character to avoid interferences/confusion with directory and file names.

The ``voucher`` field is tagged on the file when a file has been created and is also used as the logging id for this file upload. The ``requester`` field reports when, by whom and how a token has been generated.


Enabling Token Issuing
----------------------

To enable issuing of tokens, the space configuration value ``token.enegeration`` has to be set unequal to 0.

.. code-block:: bash

   eos space config default space.token.generation=1

   
Token creation
--------------

The CLI interface to create a token is shown here:

.. code-block:: bash

   # create a generic read-only token for a file valid 5 minutes
   EXPIRE=`date +%s; let LATER=$EXPIRE+300

   eos token --path /eos/myfile --expires $LATER
   zteos64:MDAwMDAwNzR4nONS4WIuKq8Q-Dlz-ltWI3H91Pxi~cSsAv2S~OzUPP2SeAgtpMAY7f1e31Ts-od-rgcLZ~a2~bhwcZO9cracyhm1b3c6jpRIEWWOws71Ox6xAABeTC8I

   # create a generic read-only token for a directory - mydir has to end with a '/' - valid 5 minutes
   eos token --path /eos/mydir/ --expires $LATER

   # create a generic read-only token for a directory tree - mytree has to end with a '/' - valid 5 minutes
   eos token --path /eos/mydir/ --tree --expires $LATER

   # create a generic write token for a file - valid 5 minutes
   eos token --path /eos/myfile --permission rwx --expires $LATER

Token inspection
----------------

The CLI interface to show the contents of a token is shown here:

.. code-block:: bash

   eos token --token zteos64:MDAwMDAwNzR4nONS4WIuKq8Q-Dlz-ltWI3H91Pxi_cSsAv2S_OzUPP2SeAgtpMAY7f1e31Ts-od-rgcLZ_a2_bhwcZO9cracyhm1b3c6jpRIEWWOws7

   TOKEN="zteos64:MDAwMDAwNzR4nONS4WIuKq8Q-Dlz-ltWI3H91Pxi_cSsAv2S_OzUPP2SeAgtpMAY7f1e31Ts-od-rgcLZ_a2_bhwcZO9cracy"
   
   env EOSAUTHZ=$TOKEN eos whoami
   Virtual Identity: uid=0 (99,3,0) gid=0 (99,4,0) [authz:unix] sudo* host=localhost domain=localdomain geo-location=ajp
   {
    "token": {
     "permission": "rx",
     "expires": "1600000000",
     "owner": "",
     "group": "",
     "generation": "1",
     "path": "/eos/myfile",
     "allowtree": false,
     "origins": []
    },
   }

Token usage
-----------

A file token can be used in two ways:

* as a filename
* via CGI '?authz=$TOKEN'

.. code-block:: bash

   # as a filename
   xrdcp root://myeos//zteos64:MDAwMDAwNzR4nONS4WIuKq8Q-Dlz-ltWI3H91Pxi_cSsAv2S_OzUPP2SeAgtpMAY7f1e31Ts-od-rgcLZ_a2_bhwcZO9cracy /tmp/

   # via CGI
   xrdcp "root://myeos//eos/myfile?authz=zteos64:MDAwMDAwNzR4nONS4WIuKq8Q-Dlz-ltWI3H91Pxi_cSsAv2S_OzUPP2SeAgtpMAY7f1e31Ts-od+rgcLZ_a2_bhwcZO9cracy" /tmp/

If a token contains a subtree permission, the only way to use it for a file access is to use the CGI form. The filename form is practical to hide the filename for up-/downloads.

Token issuing permission
------------------------

The ``root`` user can issue any token. Everybody else can only issue tokens for files in existing parent directories or directory trees, where the calling user is the current owner.

Token lifetime 
---------------

The token lifetime is given as a unix timestamp during the token creation. 

Token Revocation
----------------

Tokens are issued with a generation entry. The generation value is a globally configured 64-bit unsigned number. In case of emergency all tokens can be revoked by increasing the generation value. The generation value is configured via the key ``token.generation`` in the default space

.. code-block:: bash

   # change the generation value 
   eos config default space.token.generation=256

   # show the generation value
   eos space status default | grep token.generation
   token.generation                 := 256

Token Origin Restrictions
-------------------------

The client location from where a token can be used can be restricted by using the ``origins`` entries.

.. code-block:: bash

   # all machines at CERN authenticating via kerberos as user nobody		
   eos token --path /eos/myfile --origin \*.cern.ch:nobody:krb5"

   # all machines at CERN authenticating via unix as user kubernetes from machine k8s.cern.ch
   eos token --path /eos/myfile --origin "k8s.cern.ch:kubernetes:unix"

   # general syntax is a regexp for origin like <regexp hostname>:<regexp username>:<regexp auth protocol>

The default origin regexp is ``*:*:*`` accepting all origins.


Token via GRPC
--------------

Tokens can be requested and verified using GRPC TokenRequest as shown here with the GRPC CLI. To request a token at least ``path``, ``expires`` and ``permission`` should be defined.


.. code-block:: bash

   [root@ajp mgm]# eos-grpc-ns --acl rwx -p /eos/ajp/xrootd token
   request: 
   {
    "authkey": "",
    "token": {
     "token": {
      "token": {
       "permission": "rwx",
       "expires": "1571226882",
       "owner": "",
       "group": "",
       "generation": "0",
       "path": "/eos/ajp/xrootd",
       "allowtree": false,
       "vtoken": "",
       "origins": []
      },
      "signature": "",
      "serialized": "",
      "seed": 0
     }
    }
   }
   
   reply: 
   {
    "error": {
     "code": "0",
     "msg": "zteos64:MDAwMDAwODR4nOPS4WIuKq8QaOqa85ZVii0vPyk_pVIJShvx66fmF-snZhXoVxTl55ekCCk8KMu4qK4Z7_jNTmF5u0_z5hP1J97v3K3G29cid0O4gv-5FEnmKUyavGstGwCiYjHe"
    }
   }

   request took 6226 micro seconds


To verify a token, the ``vtoken`` field should hold the token to decode.

.. code-block:: bash

   [root@ajp mgm]# eos-grpc-ns --ztoken zteos64:MDAwMDAwODR4nOPS4WIuKq8QaOqa85ZVii0vPyk_pVIJShvx66fmF-snZhXoVxTl55ekCCk8KMu4qK4Z7_jNTmF5u0_z5hP1J97v3K3G29cid0O4gv-5FEnmKUyavGstGwCiYjHe token
   request: 
   {
    "authkey": "",
    "token": {
     "token": {
      "token": {
      "permission": "rx",
       "expires": "1571226893",
       "owner": "",
       "group": "",
       "generation": "0",
       "path": "",
       "allowtree": false,
       "vtoken": "zteos64:MDAwMDAwODR4nOPS4WIuKq8QaOqa85ZVii0vPyk_pVIJShvx66fmF-snZhXoVxTl55ekCCk8KMu4qK4Z7_jNTmF5u0_z5hP1J97v3K3G29cid0O4gv-5FEnmKUyavGstGwCiYjHe",
       "origins": []
      },
      "signature": "",
      "serialized": "",
     "seed": 0
     }
    }
   }

   reply: 
   {
    "error": {
    "code": "0",
    "msg": "{\n \"token\": {\n  \"permission\": \"rwx\",\n  \"expires\": \"1571321093\",\n  \"owner\": \"nobody\",\n  \"group\": \"nobody\",\n  \"generation\": \"0\",\n  \"path\": \"/eos/ajp/xrootd\",\n  \"allowtree\": false,\n  \"vtoken\": \"\",\n  \"voucher\": \"6496c338-f0e6-11e9-b81d-fa163eb6b6cf\",\n  \"requester\": \"[Thu Oct 17 15:59:53 2019] uid:99[nobody] gid:99[nobody] tident:.1:46602@[:1] name: dn: prot:grpc host:[:1] domain:localdomain geo:cern sudo:0\",\n  \"origins\": []\n },\n \"signature\": \"2B8qIUfJ6rTusI2NFXKH70AoXZ55wKUUDijFCK3e2bY=\",\n \"serialized\": \"CgNyd3gQheqh7QUaBm5vYm9keSIGbm9ib2R5Mg8vZW9zL2FqcC94cm9vdGRKJDY0OTZjMzM4LWYwZTYtMTFlOS1iODFkLWZhMTYzZWI2YjZjZlKNAVtUaHUgT2N0IDE3IDE1OjU5OjUzIDIwMTldIHVpZDo5OVtub2JvZHldIGdpZDo5OVtub2JvZHldIHRpZGVudDouMTo0NjYwMkBbOjFdIG5hbWU6IGRuOiBwcm90OmdycGMgaG9zdDpbOjFdIGRvbWFpbjpsb2NhbGRvbWFpbiBnZW86YWpwIHN1ZG86MA==\",\n \"seed\": 844966647\n}\n"
    }
   }

The possible return codes are:

* -EINVAL      : the token cannot be decompressed
* -EINVAL      : the token cannot be parsed
* -EACCES      : the generation number inside the token is not valid anymore
* -EKEYEXPIRED : the token validity has expired
* -EPERM       : the token signature is not correct

Using tokens with SSS security
------------------------------

It is very useful to issue scoped tokens to applications. To avoid the complication of appending tokens to each and every URL  one can use ``sss`` security to forward a generic token for each request via the ``endorsement`` environment variable.

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
   export XrdSecsssENDORSEMENT=zteos64:....

   # test the ID
   eos whoami
   Virtual Identity: uid=5410 (65534,99,5410) gid=1339 (65534,99,1338) [authz:sss] host=localhost domain=localdomain geo-location=ajp key=zteos64:....
   {
     "token": {
     "permission": "rwx",
     "expires": "1000000000",
     "owner": "cmsprod",
     "group": "zh",
     "generation": "0",
     "path": "/eos/cms/www/",
     "allowtree": false,
     "vtoken": "",
     "origins": []
    },
   }





