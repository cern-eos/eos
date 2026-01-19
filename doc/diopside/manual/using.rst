.. index::
   single: Using

.. highlight:: rst

.. _using:

Using EOS
===========

.. index::
   pair: Tokens; EOS Tokens
   pair: Using; EOS Tokens


EOS Tokens for Authorization
----------------------------------

We provide a generic EOS mechanism to delegate permissions to a token
bearer with s.c. EOS tokens.

.. index::
   pair: EOS Tokens; JSON Format


The JSON representation of an EOS token looks like this:

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


Essentially this token gives the bearer the permission to `rwx` for the
file /eos/dev/token. The token might not bear any owner or group
information, which means that the creations will be accounted on the
mapped authenticated user using this token or an enforced
`sys.owner.auth` entry. If the token should map the authenticated user,
one can add `owner` and `group` fields. In practical terms the token
removes existing user and system ACL entries and places the token
user/group/permission entries as a system ACL. If a user creates a token,
the `owner` and `group` fields are always added. To have 'dynamic' user tokens, you need to take the `root` role.

Tokens are signed, zlib compressed, base64url encoded with a replacement
of the `+` and `/` characters with `-` and `_` and a URL
encoding of the `=` character to avoid collision with
directory and file names.

The `voucher` field is tagged on the file when a file has been created
and is also used as the logging id for this file upload. The `requester`
field reports when, by whom and how a token has been generated.

.. index::
   pair: EOS Tokens; Issuing

Enabling Token Issuing
^^^^^^^^^^^^^^^^^^^^^^^

To enable issuing of tokens, the space configuration value
`token.generation` has to be set unequal to 0.

.. code-block:: bash

   eos space config default space.token.generation=1


By default the signing key is derived from the instance sss keytab. If
you want to define your own signature key, you can point to a file
containing the key in **/etc/sysconfig/eos_env**:

.. code-block:: bash

   EOS_MGM_TOKEN_KEYFILE=/etc/eos/token.key


The token key file must be owned by the daemon user and have 400
permission!

Token creation
^^^^^^^^^^^^^^

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


.. index::
   pair: EOS Tokens; Inspection

Token inspection
^^^^^^^^^^^^^^^^^^

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
    "owner": "root",
    "group": "root",
    "generation": "1",
    "path": "/eos/myfile",
    "allowtree": false,
    "origins": []
  },
  ...

.. index::
   pair: EOS Tokens; Usage

Token usage
^^^^^^^^^^^

A file token can be used in two ways:

* as a filename
* via CGI ```?authz=$TOKEN```

.. code-block:: bash


   # as a filename
   xrdcp root://myeos//zteos64:MDAwMDAwNzR4nONS4WIuKq8Q-Dlz-ltWI3H91Pxi_cSsAv2S_OzUPP2SeAgtpMAY7f1e31Ts-od-rgcLZ_a2_bhwcZO9cracy /tmp/

   # via CGI
   xrdcp "root://myeos//eos/myfile?authz=zteos64:MDAwMDAwNzR4nONS4WIuKq8Q-Dlz-ltWI3H91Pxi_cSsAv2S_OzUPP2SeAgtpMAY7f1e31Ts-od+rgcLZ_a2_bhwcZO9cracy" /tmp/


If a token contains a subtree permission, the only way to use it for
file access is to use the CGI form. The filename form is practical to
hide the filename for up-/downloads.

.. index::
   pair: EOS Tokens; Permissions

Token issuing permission
^^^^^^^^^^^^^^^^^^^^^^^^^

The `root` user can issue any token. Everybody else can only issue
tokens for files in existing parent directories or directory trees,
where the calling user is the current owner.

.. index::
   pair: EOS Tokens; Lifetime

Token lifetime
^^^^^^^^^^^^^^^^^^^^^^^^^

The token lifetime is given as a UNIX timestamp during the token
creation.

.. index::
   pair: EOS Tokens; Revoaction

Token Revocation
^^^^^^^^^^^^^^^^^^^^^^^^^

Tokens are issued with a generation entry. The generation value is a
globally configured 64-bit unsigned number. In case of emergency all
tokens can be revoked by increasing the generation value. The generation
value is configured via the key `token.generation` in the default space.

.. code-block:: bash

   # change the generation value
   eos config default space.token.generation=256

   # show the generation value
   eos space status default | grep token.generation
   token.generation                 := 256

.. index::
   pair: EOS Tokens; Origin Restrictions

Token Origin Restrictions
^^^^^^^^^^^^^^^^^^^^^^^^^

The client location from where a token can be used can be restricted by
using the `origins` entries.

.. code-block:: bash

   # general syntax is a regexp for origin like <regexp hostname>#<regexp username>#<regexp auth protocol>
   # all machines at CERN authenticating via kerberos as user nobody
   eos token --path /eos/myfile --origin ".*.cern.ch:nobody#krb5"

   # all machines at CERN authenticating via unix as user kubernetes from machine k8s.cern.ch
   eos token --path /eos/myfile --origin "k8s.cern.ch#kubernetes#unix"


The default origin regexp is `.*#.*#.*` accepting all origins. If the
regex is invalid, the command will return with an error message.

.. index::
   pair: EOS Tokens; GRPC

Multi-path Tokens
^^^^^^^^^^^^^^^^^

EOS supports not multipath-tokens which carry the same owner/group and ACLs for several distinct paths.

To issue a multipath token you concatenate your paths using '://:' as a delimiter e.g.

.. code-block:: bash

   # create a token for a generic EOS path and to call the Tape Rest API endpoitn

   eos token --path /eos/://:/api/

   # allow 'rwx' in /eos/user/ and /eos/group/ to 1234:1234
   eos token --path /eos/user/://:/eos/group/ --permission rwx --owner 1234 --group 1234

.. index::
   pair: EOS Tokens; Multi-path Token




Token Mapping
^^^^^^^^^^^^^

The `tokensudo` functionality can be configured on space level. The purpose is to define, which connections are allowed to use tokens and apply the owner/group information (if embebbed in the token). The default is `always`.

.. code-block:: bash

   eos vid tokensudo always|strong|encrypted|never`

- `always` - identity in the token is always taken into account (all auth protocols)
- `strong` - identity in the token is not taken into account for unix authenticated clients (all but linux))
- `encrypted` - identity in the token is only taken into account for encrypted connections (https,grpc,ztn,sss)
- `never` - identity in the token is never taken into account (never assimilate the identity from the token)

Token via GRPC
^^^^^^^^^^^^^^^^^^^^^^^^^

Tokens can be requested and verified using GRPC TokenRequest as shown
here with the GRPC CLI. To request a token at least `path`, `expires`
and `permission` should be defined.

.. code-block:: bash

    [root@dev mgm]# eos-grpc-ns --acl rwx -p /eos/dev/xrootd token
    request:
    {
    "authkey": "",
    "token": {
      "token": {
      "token": {
        "permission": "rwx",
        "expires": "1571226882",
        "owner": "root",
        "group": "root",
        "generation": "0",
        "path": "/eos/dev/xrootd",
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


To verify a token, the `vtoken` field should hold the token to decode.

.. code-block:: bash


    [root@dev mgm]# eos-grpc-ns --ztoken zteos64:MDAwMDAwODR4nOPS4WIuKq8QaOqa85ZVii0vPyk_pVIJShvx66fmF-snZhXoVxTl55ekCCk8KMu4qK4Z7_jNTmF5u0_z5hP1J97v3K3G29cid0O4gv-5FEnmKUyavGstGwCiYjHe token
    request:
    {
    "authkey": "",
    "token": {
      "token": {
      "token": {
      "permission": "rx",
        "expires": "1571226893",
        "owner": "root",
        "group": "root",
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
    "msg": "{\n \"token\": {\n  \"permission\": \"rwx\",\n  \"expires\": \"1571321093\",\n  \"owner\": \"root\",\n  \"group\": \"root\",\n  \"generation\": \"0\",\n  \"path\": \"/eos/dev/xrootd\",\n  \"allowtree\": false,\n  \"vtoken\": \"\",\n  \"voucher\": \"6496c338-f0e6-11e9-b81d-fa163eb6b6cf\",\n  \"requester\": \"[Thu Oct 17 15:59:53 2019] uid:99[nobody] gid:99[nobody] tident:.1:46602@[:1] name: dn: prot:grpc host:[:1] domain:localdomain geo:cern sudo:0\",\n  \"origins\": []\n },\n \"signature\": \"2B8qIUfJ6rTusI2NFXKH70AoXZ55wKUUDijFCK3e2bY=\",\n \"serialized\": \"CgNyd3gQheqh7QUaBm5vYm9keSIGbm9ib2R5Mg8vZW9zL2FqcC94cm9vdGRKJDY0OTZjMzM4LWYwZTYtMTFlOS1iODFkLWZhMTYzZWI2YjZjZlKNAVtUaHUgT2N0IDE3IDE1OjU5OjUzIDIwMTldIHVpZDo5OVtub2JvZHldIGdpZDo5OVtub2JvZHldIHRpZGVudDouMTo0NjYwMkBbOjFdIG5hbWU6IGRuOiBwcm90OmdycGMgaG9zdDpbOjFdIGRvbWFpbjpsb2NhbGRvbWFpbiBnZW86YWpwIHN1ZG86MA==\",\n \"seed\": 844966647\n}\n"
    }
    }


The possible return codes are:

.. epigraph::

   ============== ============================================================
   Error          Meaning
   ============== ============================================================
   `-EINVAL`      the token cannot be decompressed
   `-EINVAL`      the token cannot be parsed
   `-EACCES`      the generation number inside the token is not valid anymore
   `-EKEYEXPIRED` the token validity has expired
   `-EPERM`       the token signature is not correct
   ============== ============================================================

.. index::
   pair: EOS Tokens; Tokens over SSS

Using tokens with SSS security
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is very useful to issue scoped tokens to applications. To avoid the
complication of appending tokens to each and every URL one can use `sss`
security to forward a generic token for each request via the
`endorsement` environment variable.

Client and server should share an sss key for a user, which is actually
not authorized to use the instance e.g.


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
    Virtual Identity: uid=5410 (65534,99,5410) gid=1339 (65534,99,1338) [authz:sss] host=localhost domain=localdomain geo-location=dev key=zteos64:....
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

.. index::
   pair: EOS Tokens; Token /eos access

Using tokens for scoped eosxd access
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As a user you can create a token e.g. for applications like CIs,
webservices etc. if the EOS instance is configured to issue tokens.

To create a token as a user you do:

.. code-block:: bash

   eos token --path /eos/user/f/foo/ci/ --expires 1654328760 --perm rwx --tree


If you create a token as a user, the token puts the calling role as the
identity into the token.

You can inspect your token to verify that it contains what you want
using:

.. code-block:: bash

   eos token --token zteos64:...


Finally to use the token on a mount client you define only the following
variable:

.. code-block:: bash

    # put the token into your client environment
    export XrdSecsssENDORSEMENT=zteos64:...

    # you should now have rwx permission on this tree
    ls /eos/user/f/foo/ci/


.. index::
   pair: EOS Tokens; Token ZTN auth

Using EOS tokens via ZTN authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since EOS 5.1.15 it is possible to configure ZTN authentication in EOS and use this as a token transport mechanism. This is simpler than using `sss` authentication and distributing shared secrets. However the MGM configuration needs to disable token validation for the default SciToken library setting:

.. code-block:: bash

   sec.protocol ztn -tokenlib none

The XRootD client picks up tokens as documented under Section `2.5.7.1 Default token discovery mechanism and augmentation <https://xrootd.slac.stanford.edu/doc/dev56/sec_config.htm#_Toc119617461>`_

The support for ZTN token transport and eosxd was added with EOS version 5.2.


.. index::
   pair: Tokens; OAUTH2


OAUTH2 for authentication
-------------------------------

To enable OAUTH2 token translation, one has to configure the resource endpoint and enable OAUTH2 mapping:

.. code-block:: bash

   # enable oauth2 mapping
   eos vid enable oauth2
   # allow an oauth2 resource in requests
   eos vid set map -oauth2 key:oauthresource.web.cern.ch/api/User vuid:0
   # allow an oauth2 resource in requests (OIDC infrastructure)
   eos vid set map -oauth2 key:auth.cern.ch/auth/realms/cern/protocol/openid-connect/userinfo vuid:0

If you want to check the audience claim in the ticket, you can add the audience to screen to each OAUTH2 resource:

.. code-block:: bash

   # allow on oauth2 resource in request for the audience 'eosoauth'
   eos vid set map -oauth2 key:auth.cern.ch/auth/realms/cern/protocol/openid-connect/userinfo@eosatuch vuid:0

If you want to use a local account which is mapped in the instance to a local uid, you can define a 'sub' field mapping entry using:

.. code-block:: bash

   # remap the sub '7aa5167f-9c28-4336-8a66-af9145ea847d' to the local user id 1000
   eos vid set map -oauth2 sub:7aa5167f-9c28-4336-8a66-af9145ea847d vuid:1000


All XRootD based clients can add the OAUTH2 token in the endorsement environment variable for sss authentication.

.. code-block:: bash

   XrdSecsssENDORSEMENT=oauth2:<access_token>:<oauth-resource>

OAUTH2 is enabled by default, but can be explicitly enabled or disabled:

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

One has to supply an sss key for this communication, however the sss key user should be banned on the instance as a security precaution.
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


.. index::
   pair: File; Versioning
   pair: Using; Versioning

VOMS Role Mapping
-------------------------------

A VOMS proxy uses X509 extensions which are signed by a VOMS server to attach roles to a proxy certificate. The extraction process on EOS verifies the signatures of these extension using the local VOMS configuration. The roles defined by the extensions can be mapped inside EOS using the *vid* interface. In the following we show a VOMS configuration for the CMS experiment as an example.

Installing VOMS configuration for CMS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   yum localinstall https://linuxsoft.cern.ch/wlcg/centos7/x86_64/wlcg-iam-lsc-cms-2.0.0-1.el7.noarch.rpm -y --nogpgcheck
   yum localinstall https://linuxsoft.cern.ch/wlcg/centos7/x86_64/wlcg-voms-cms-2.0.0-1.el7.noarch.rpm -y --nogpgcheck

Configuring VOMS role extraction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

VOMS extraction is configured as a GSI protocol configuration option in the MGM configuration file adding `-vomsat:1 -vomsfun:default` to `sec.protocol gsi`:

.. code-block:: bash

  sec.protocol  gsi -crl:1 -moninfo:1 -cert:/etc/grid-security/daemon/hostcert.pem -key:/etc/grid-security/daemon/hostkey.pem -gridmap:/etc/grid-security/grid-mapfile -d:1 -gmapopt:1 -vomsat:1 -vomsfun:default


Configuring mappings using the CLI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash

   eos vid set map -voms /cms:production vuid:`id -u cmsprod` vgid:`id -g cmsprod`
   eos vid set map -voms /cms:cmsprod vuid:`id -u cmsprod` vgid:`id -u cmsprod`
   eos vid set map -voms /cms:t1production vuid:`id -u cmsprod` vgid:`id -u cmsprod`
   eos vid set map -voms /cms/muon:production vuid:`id -u cmsprod` vgid:`id -u cmsprod`
   eos vid set map -voms /cms:cmsphedex vuid:`id -u cmsprod` vgid:`id -u cmsprod`
   eos vid set map -voms /cms:lcgadmin vuid:`id -u cmssam` vgid:`id -u cmssam`
   eos vid set map -voms /cms/uscms:lcgadmin vuid:`id -u cmssam` vgid:`id -u cmssam`
   eos vid set map -voms /cms:pilot vuid:`id -u cmspilot` vgid:`id -u cmspilot`
   eos vid set map -voms /cms:uscms:pilot vuid:`id -u cmspilot` vgid:`id -u cmspilot`
   eos vid set map -voms /cms:priorityuser vuid:`id -u cmsuser` vgid:`id -u cmsuser`
   eos vid set map -voms /cms:hiproduction vuid:`id -u cmsuser` vgid:`id -u cmsuser`
   eos vid set map -voms /cms: vuid:`id -u cmsuser` vgid:`id -u cmsuser


Configuring gridmap from IAM
----------------------------

In case the deployment needs to rely on ``/etc/grid-security/gridmapfile``,
these can still be generated from the IAM portal using ``eos-iam-mapfile``
script, which is shipped as a part of ``eos-server`` packages.

.. code-block:: bash

   eos-iam-mapfile -h
   usage: eos-iam-mapfile [-h] [-v [{DEBUG,INFO,WARNING,ERROR,CRITICAL}]] [-s SERVER CLIENT_ID CLIENT_KEY] [-c CONFIG] [-t TARGETS] [-i IFILE] [-o OFILE] [-l LGRIDMAP] [-a ACCOUNT] [-p PATTERN] [-C] [-u] [-f [{MAPFILE,GRIDMAP}]] [--cleanup]
                       [--log-file LOG_FILE]

   GRID Map file generation from IAM Server


For testing the script one needs a client-id and a client-key which has ``scim:read`` permissions.


.. code-block:: bash

   $ echo -e '[myiamserver.ch]\nclient-id=client-1234-uuid\nclient-secret=client-secret123\naccount=acc4usermapping' > myiam.conf
   $ eos-iam-mapfile -c iam.conf
   "/DC=ch/DC=cern/..CN=testuser1" acc4usermapping


Configuring eos-iam-mapfile
^^^^^^^^^^^^^^^^^^^^^^^^^^^

``eos-iam-mapfile`` can be configured via a simple INI file: a ``[DEFAULT]``
section takes global parameters, multiple IAM server sections can follow, which
will be evaluated in the order which they appear in the config. A
``localgridmap`` can be configured which will add & override DNs in the
localgridmap in case they are already mapped from previous IAM server outputs.

An example configuration is given below. Note that these are **NOT** the default config options in case the script isn't configured. The only default is ``log_level``  which is set to WARNING level.

.. code-block:: bash

   [DEFAULT]
   cleanup = True # Cleanup intermediate temporary files
   localgridmap = /etc/localgridmap.conf,/etc/localgridmap2.conf
   log_level = WARNING # choose between DEBUG,INFO,WARNING,ERROR,CRITICAL
   log_file = /var/log/eos/grid/eos-iam-map.log

   [myiamserver1.ch]
   client-id = client-uuid1
   client-secret = secret123
   account = account1


   [myiamserver2.ch]
   client-id = client-uuid2
   client-secret = secret234
   account = account2

File Versioning
---------------

File versioning can be triggered as a per-directory policy using the extended attribute ``sys.versioning=<n>`` or via the ``eos file version`` command.

The parameter ``<n>`` in the extended attribute describes the maximum number of versions which should be kept according to a FIFO policy. In addition, there are 11 predefined timebins, for which additional versions exceeding the versioning parameter ``<n>`` are kept.

Versions are kept in a hidden directory (visible with ``ls -la``) which is composed by ``.sys.v#.<basename>``

.. code-block:: bash

   eos ls -la
   drwxrwxrwx   1 root     root                0 Aug 29 15:33 .sys.v#.myfile
   -rw-r-----   1 root     root             1824 Aug 29 15:33 myfile

The 11 time bins are defined as follows:

.. index::
   pair: Versioning; Timebins

.. epigraph::

   ============= ===================================================
   bin           deletion policy
   ============= ===================================================
   age<1d        the first version entering this bin survives
   1d<=age<2d    the first version entering this bin survives
   2d<=age<3d    the first version entering this bin survives
   3d<=age<4d    the first version entering this bin survives
   4d<=age<5d    the first version entering this bin survives
   5d<=age<6d    the first version entering this bin survives
   6d<=age<1w    the first version entering this bin survives
   1w<=age<2w    the first version entering this bin survives
   2w<=age<3w    the first version entering this bin survives
   3w<=age<1mo   the first version entering this bin survives
   ============= ===================================================


.. index::
   pair: Versioning; Automatic Versioning

Configuration of automatic versioning
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Configure each directory which should apply versioning using the extended attribute ``sys.versioning``:

.. code-block:: bash

   # force 10 versions (FIFO)
   eos attr set sys.versioning=10 version-dir

   # upload initial file
   eos cp /tmp/file /eos/version-dir/file

   # versions are created on the fly with each upload - now 1 version
   eos cp /tmp/file /eos/version-dir/file

   # versions are created on the fly with each upload - now 2 versions
   eos cp /tmp/file /eos/version-dir/file


.. index::
   pair: Versioning; Creating

Creating new versions
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   # force a new version
   eos file version myfile

   # force a new version with max 5 versions
   eos file version myfile 5

.. index::
   pair: Versioning; Listing

List existing versions
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   eos file versions myfile
   -rw-r-----   1 root     root             1824 Aug 29 15:17 1567084675.0014ede6
   -rw-r-----   1 root     root             1824 Aug 29 15:33 1567085591.0014ede7
   -rw-r-----   1 root     root             1824 Aug 29 15:33 1567085591.0014ede8
   -rw-r-----   1 root     root             1824 Aug 29 15:33 1567085592.0014ede9


.. index::
   pair: Versioning; Purging Versions

Purging existing versions
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   # remove all versions
   eos file purge myfile 0

   # keep 5 versions (FIFO)
   eos file purge myfile 5


.. index::
   pair: File; Erasure Coding
   pair: Using; RAIN
   pair: Using; Erasure Coding
   pair: EC; Erasure Coding

RAIN - Erasure Coding
---------------------

.. index::
   pair: EC; RAIN Layout DbMapTypes


ECC Layout Types
^^^^^^^^^^^^^^^^

EOS supports five types of RAIN layouts:

.. epigraph::

   ========== ============= ================================ ====================================
   name       redundancy    algorithm                        description
   ========== ============= ================================ ====================================
   raid5      N+1           single parity raid               can lose 1 disk without data loss
   raiddp     4+2           dual parity raid                 can lose 2 disks without data loss
   raid6      N+2           Erasure Code (Jerasure library)  can lose 2 disks without data loss
   archive    N+3           Erasure Code (Jerasure library)  can lose 3 disks without data loss
   qrain      N+4           Erasure Code (Jerasure library)  can lose 4 disks without data loss
   ========== ============= ================================ ====================================

The layout is set in a namespace tree via ``eos attr -r set default=<name> <tree>``.

The minimum number of stripes is currently 6 for all erasure coding layouts (raiddp, raid6, archive, qrain).

The default layout can be defined using default space policies.


.. index::
   pair: FUSE; eosxd
   pair: Using; FUSE Mounting
   pair: Using; eosxd

FUSE - mounting EOS with eosxd
------------------------------

**eosxd** is the executable responsible for running the EOS FUSE mount. FUSE (Filesystem in Userspace) is a software interface for Unix and Unix-like computer operating systems that lets non-privileged users create their own file systems without editing kernel code. This is achieved by running file system code in user space while the FUSE module provides only a bridge to the actual kernel interfaces.

EOS supports **eosxd** based on `libfuse2` and `libfuse3`. `libfuse3` has some additional bulk interfaces and performance improvements compared to `libfuse`.
The executable for `libfuse3` is called **eosxd3**.

.. warning:: To have eosxd working properly with many writers you have to modify the MGM configuration file /etc/xrd.cf.mgm to configure the nolock option: 'all.export / nolock'


There are two FUSEx client modes available:

.. epigraph::

   =========== ================ ===============================================================
    daemon     daemon user-id   description
   =========== ================ ===============================================================
   eosxd       !root            An end-user private mount which is not shared between users
   eosxd       root             A system-wide mount shared between users
   =========== ================ ===============================================================

.. index::
   pair: FUSE; Mounting by hostname


Mounting an EOS instance
^^^^^^^^^^^^^^^^^^^^^^^^

Mounting by hostname
""""""""""""""""""""
The easiest way to mount EOS is to do:

.. code-block:: bash

   mkdir -p /eos/
   eosxd -ofsname=myeos.cern.ch:/eos/ /eos/

or using `mount`:

.. code-block:: bash

   mount -t fuse eosxd /eos/

.. index::
   pair: FUSE; Mounting by name

Mounting with configuration files
""""""""""""""""""""""""""""""""""
The configuration file name for an unnamed instance is /etc/eos/fuse.conf.
To mount an unnamed instance you do:

.. code-block:: bash

   eosxd /eos/

or using `mount`:

.. code-block:: bash

   mount -t fuse eosxd -ofsname=myeos.cern.ch:/eos/ /eos/


The configuration file for a named instance is `/etc/eos/fuse.<name>.conf`.

You can select a named instance adding `-ofsname=<name>` to the argument list.

.. index::
   pair: FUSE; http-configuration_

Configuration File Syntax
""""""""""""""""""""""""""


The default configuration parameters are shown here:

.. code-block:: bash

    {
      "name" : "",
      "hostport" : "localhost:1094",
      "remotemountdir" : "/eos/",
      "localmountdir" : "/eos/",
      "statisticfile" : "stats",
      "mdcachedir" : "/var/cache/eos/fusex/md",
      "mdzmqtarget" : "tcp://localhost:1100",
      "mdzmqidentity" : "eosxd",
      "appname" : "",
      "options" : {
        "debug" : 1,
        "debuglevel" : 4,
        "jsonstats" : 1,
        "backtrace" : 1,
        "hide-versions" : 1,
        "protect-directory-symlink-loops" : 0,
        "md-kernelcache" : 1,
        "md-kernelcache.enoent.timeout" : 0,
        "md-backend.timeout" : 86400,
        "md-backend.put.timeout" : 120,
        "data-kernelcache" : 1,
        "rename-is-sync" : 1,
        "rmdir-is-sync" : 0,
        "global-flush" : 0,
        "flush-wait-open" : 1, // 1 = flush waits for open when updating - 2 = flush waits for open when creating - 0 flush never waits
        "flush-wait-open-size" : 262144 , // file size for which we force to wait that files are opened on FSTs
        "flush-wait-umount" : 120, // seconds to wait for write-back data to be flushed out before terminating the mount - 0 disables waiting for flush
        "flush-nowait-executables" : [ "/tar", "/touch" ],
        "global-locking" : 1,
        "fd-limit" : 524288,
        "no-fsync" : [ ".db", ".db-journal", ".sqlite", ".sqlite-journal", ".db3", ".db3-journal", "*.o" ],
        "overlay-mode" : "000",
        "rm-is-sync" : 0,
        "rename-is-sync" : 1,
        "rm-rf-protect-levels" : 0,
        "rm-rf-bulk" : 0,
        "show-tree-size" : 0,
        "cpu-core-affinity" : 0,
        "no-xattr" : 0,
        "no-eos-xattr-listing" : 0,
        "no-link" : 0,
        "nocache-graceperiod" : 5,
        "leasetime" : 300,
        "write-size-flush-interval" : 10,
        "submounts" : 0,
        "inmemory-inodes" : 16384
      },
      "auth" : {
        "shared-mount" : 1,
        "krb5" : 1,
        "gsi-first" : 0,
        "sss" : 1,
        "ssskeytab" : "/etc/eos/fuse.sss.keytab",
        "oauth2" : 1,
        "unix" : 0,
        "unix-root" : 0,
        "ignore-containerization" : 0,
        "credential-store" : "/var/cache/eos/fusex/credential-store/"
        "environ-deadlock-timeout" : 100,
        "forknoexec-heuristic" : 1
      },
      "inline" : {
        "max-size" : 0,
        "default-compressor" : "none"
      },
      "fuzzing" : {
        "open-async-submit" : 0,
        "open-async-return" : 0,
        "open-async-submit-fatal" : 0,
        "open-async-return-fatal" : 0,
        "read-async-submit" : 0
      }
    }

You also need to define a local cache directory (location) where small files are cached and an optional journal directory to improve the write speed (journal):

.. code-block:: bash

    "cache" : {
      "type" : "disk",
      "size-mb" : 512,
      "size-ino" : 65536,
      "journal-mb" : 2048,
      "journal-ino" : 65536,
      "clean-threshold" : 85.0,
      "location" : "/var/cache/eos/fusex/cache/",
      "journal" : "/var/cache/eos/fusex/journal/",
      "read-ahead-strategy" : "dynamic",
      "read-ahead-bytes-nominal" : 262144,
      "read-ahead-bytes-max" : 1048576,
      "read-ahead-blocks-max" : 16,
      "read-ahead-sparse-ratio" : 0.0,
      "max-read-ahead-buffer" : 134217728,
      "max-write-buffer" : 134217728
    }


The available read-ahead strategies are `dynamic`, `static` or `none`. `dynamic` read-ahead doubles the read-ahead window from nominal to max if the strategy provides cache hits. The default is a dynamic read-ahead starting with 512kb and using 2,4,8,16 blocks resizing blocks up to 2M.

The daemon automatically appends a directory to the mdcachedir, location and journal path and automatically creates these directories with mode=700 owned by root.

You can modify some of the XrdCl variables, however it is recommended not to change these:

.. code-block:: bash

    "xrdcl" : {
      "TimeoutResolution" : 1,
      "ConnectionWindow": 10,
      "ConnectionRetry" : 0,
      "StreamErrorWindow" : 120,
      "RequestTimeout" : 60,
      "StreamTimeout" : 120,
      "RedirectLimit" : 2,
      "LogLevel" : "None"
    },

The recovery settings are defined in the following section:

.. code-block:: bash

    "recovery" : {
      "read-open" : 1,
      "read-open-noserver" : 1,
      "read-open-noserver-retrywindow" : 15,
      "write-open" : 1,
      "write-open-noserver" : 1,
      "write-open-noserver-retrywindow" : 15,
    }


It is possible to overwrite the settings of any standard config files using a second configuration file: /etc/eos/fuse.local.conf or/etc/eos/fuse.<name>.local.conf. This is useful to ship a standard configuration via a package and gives users the opportunity to change individual parameters.

.. index::
   pair: FUSE; configuration defaults


Configuration default values and avoiding configuration files
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Every configuration value has a corresponding default value. As explained the configuration file name is taken from the fsname option given on the command line:

.. code-block:: bash

    root> eosxd -ofsname=foo #loads /etc/eos/fuse.foo.conf
    root> eosxd              #loads /etc/eos/fuse.conf

    user> eosxd -ofsname=foo #loads $HOME/.eos/fuse.foo.conf


One can avoid using configuration files if the defaults are fine providing the remote host and remote mount directory via the fsname:

.. code-block:: bash

    root> eosxd -ofsname=eos.cern.ch:/eos/ $HOME/eos                  # mounts the /eos/ directory from eos.cern.ch shared under $HOME/eos/
    user> eosxd -ofsname=user@eos.cern.ch:/eos/user/u/user/ $home/eos # mounts /eos/user/u/user from eos.cern.ch private under $HOME/eos/


If this is a user-private mount the syntax `foo@cern.ch` should be used to distinguish private mounts of individual users in the `df` output.

.. NOTE:: Please note, that root mounts are by default shared mounts with kerberos configuration, user mounts are private mounts with kerberos configuration!

.. index::
   pair: FUSE; Log FIles

eosxd Logfile
^^^^^^^^^^^^^

eosxd writes a logfile into the fusex log directory `/var/log/eos/fusex/fuse.<instancename>-<mountdir>.log` . The
default verbosity is **warning** level.

.. index::
   pair: FUSE; Statistics File

eosxd Statistics file
^^^^^^^^^^^^^^^^^^^^^

eosxd writes out a statistics file with an update rate of 1Hz into the
fusex log directory `/var/log/eos/fusex/fuse.<instancename>-<mountdir>.stats` .

Here is an example:

.. code-block:: bash

    ALL     Execution Time                   5.06 +- 16.69 = 5.01s (1270 ops)
    # -----------------------------------------------------------------------------------------------------------------------
    who     command                          sum             5s     1min     5min       1h exec(ms) +- sigma(ms)  = cumul(s)
    # -----------------------------------------------------------------------------------------------------------------------
    ALL     :sum                                     1271     0.00     0.05     0.01     0.00     -NA- +- -NA-       = 0.00
    ALL     access                                      4     0.00     0.00     0.00     0.00  1.82825 +- 1.64279    = 0.01
    ALL     create                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     flush                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     forget                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     fsync                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     getattr                                    17     0.00     0.02     0.00     0.00  1.91859 +- 6.93590    = 0.03
    ALL     getxattr                                   58     0.00     0.03     0.01     0.00  2.42547 +- 18.15372   = 0.14
    ALL     link                                        0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     listxattr                                   0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     lookup                                    342     0.00     0.00     0.00     0.00  0.78381 +- 3.70048    = 0.27
    ALL     mkdir                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     mknod                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     open                                        0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     opendir                                   215     0.00     0.00     0.00     0.00 20.56853 +- 26.64452   = 4.42
    ALL     read                                        0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     readdir                                   416     0.00     0.00     0.00     0.00  0.05781 +- 0.07550    = 0.02
    ALL     readlink                                    1     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     release                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     releasedir                                215     0.00     0.00     0.00     0.00  0.00896 +- 0.00425    = 0.00
    ALL     removexattr                                 0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     rename                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     rm                                          0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     rmdir                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     setattr                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     setattr:chmod                               0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     setattr:chown                               0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     setattr:truncate                            0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     setattr:utimes                              0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     setxattr                                    1     0.00     0.00     0.00     0.00  0.08500 +- -NA-       = 0.00
    ALL     statfs                                      2     0.00     0.00     0.00     0.00 57.74450 +- 48.80550   = 0.12
    ALL     symlink                                     0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     unlink                                      0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    ALL     write                                       0     0.00     0.00     0.00     0.00     -NA- +- -NA-       = 0.00
    # -----------------------------------------------------------------------------------------------------------
    ALL        inodes              := 375
    ALL        inodes stack        := 0
    ALL        inodes-todelete     := 0
    ALL        inodes-backlog      := 0
    ALL        inodes-ever         := 3051
    ALL        inodes-ever-deleted := 0
    ALL        inodes-open         := 0
    ALL        inodes-vmap         := 3051
    ALL        inodes-caps         := 1
    # -----------------------------------------------------------------------------------------------------------
    ALL        threads             := 32
    ALL        visze               := 517.10 Mb
    ALL        rss                 := 35.63 Mb
    ALL        pid                 := 1689
    ALL        log-size            := 409384
    ALL        wr-buf-inflight     := 0 b
    ALL        wr-buf-queued       := 0 b
    ALL        wr-nobuff           := 0
    ALL        ra-buf-inflight     := 0 b
    ALL        ra-buf-queued       := 0 b
    ALL        ra-xoff             := 0
    ALL        ra-nobuff           := 0
    ALL        rd-buf-inflight     := 0 b
    ALL        rd-buf-queued       := 0 b
    ALL        version             := 4.4.17
    ALL        fuseversion         := 28
    ALL        starttime           := 1549548272
    ALL        uptime              := 66989
    ALL        total-mem           := 8201658368
    ALL        free-mem            := 149671936
    ALL        load                := 1313970496
    ALL        total-rbytes        := 0
    ALL        total-wbytes        := 0
    ALL        total-io-ops        := 1270
    ALL        read--mb/s          := 0.00
    ALL        write-mb/s          := 0.00
    ALL        iops                := 0
    ALL        xoffs               := 0
    ALL        instance-url        := myhost.cern.ch:1094
    ALL        client-uuid         := 4af8154c-2ae1-11e9-8e32-02163e009ce2
    ALL        server-version      := 4.4.17
    ALL        automounted         := 0
    ALL        max-inode-lock-ms   := 0.00
    # -----------------------------------------------------------------------------------------------------------


The first block contains global averages/sums for total IO time and IO
operations:

.. epigraph::

   =========== ================= ================= =================== ==============
   tag         description       avg/dev in ms     cumulative time     sum IOPS
   =========== ================= ================= =================== ==============
   ALL         Execution Time    4.80 +- 15.56     4.87s               (1267 ops)
   =========== ================= ================= =================== ==============

The second block contains counts for each filesystem operation, the
average rates in 5s, 1min, 5min and 1h windows, the average execution
time and standard deviation for a given filesystem operation, and
cumulative seconds spent in each operation.

.. epigraph::

    ====== ========================== ============= ========= ========= ========== ========== =========== =============== ===============
    who    filesystem counter name    sum of ops    5s avg    1m avg    5m avg   | 1h avg     avg(ms)     sigma(ms)       cumulative(s)
    ====== ========================== ============= ========= ========= ========== ========== =========== =============== ===============
    ALL    :sum                       1268          0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    access                     4             0.00      0.00      0.00       0.00       1.82825     +- 1.64279      0.01
    ALL    create                     0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    flush                      0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    forget                     0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    fsync                      0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    getattr                    16            0.00      0.00      0.00       0.00       2.01987     +- 7.13716      0.03
    ALL    getxattr                   56            0.00      0.00      0.00       0.00       0.02023     +- 0.00463      0.00
    ALL    link                       0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    listxattr                  0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    lookup                     342           0.00      0.00      0.00       0.00       0.78381     +- 3.70048      0.27
    ALL    mkdir                      0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    mknod                      0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    open                       0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    opendir                    215           0.00      0.00      0.00       0.00       20.5685     +- 26.64452     4.42
    ALL    read                       0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    readdir                    416           0.00      0.00      0.00       0.00       0.05781     +- 0.07550      0.02
    ALL    readlink                   1             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    release                    0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    releasedir                 215           0.00      0.00      0.00       0.00       0.00896     +- 0.00425      0.00
    ALL    removexattr                0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    rename                     0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    rm                         0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    rmdir                      0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    setattr                    0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    setattr:chmod              0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    setattr:chown              0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    setattr:truncate           0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    setattr:utimes             0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    setxattr                   1             0.00      0.00      0.00       0.00       0.08500     +- -NA-         0.00
    ALL    statfs                     2             0.00      0.00      0.00       0.00       57.7450     +- 48.80550     0.12
    ALL    symlink                    0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    unlink                     0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ALL    write                      0             0.00      0.00      0.00       0.00       -NA-        +- -NA-         0.00
    ====== ========================== ============= ========= ========= ========== ========== =========== =============== ===============

The third block displays inode related counts, which are explained
inline.

 .. epigraph::

    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    who   | counter name          | value            | description                                                                |
    +==========+=======================+==================+============================================================================+
    |    ALL   | inodes                | 375              | currently in-memory known-inodes                                           |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | inodes stack          | 0                | inodes which could be forgotten, but needed to be kept on the stack        |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | inodes-todelete       | 0                | inodes which still have to be deleted upstream                             |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | inodes-backlog        | 0                | inodes which still have to be updated upstream                             |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | inodes-ever           | 3051             | inodes ever seen by this mount                                             |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | inodes-ever-deleted   | 0                | inodes ever deleted by this mount                                          |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | inodes-open           | 0                | inodes associated with an open file descriptor                             |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | inodes-vmap           | 3051             | size of logical inode translation map                                      |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | inodes-caps           | 0                | inodes with a cache-callback subscription                                  |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | threads               | 32               | currently running threads                                                  |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | visze                 | 517.10 Mb        | virtual memory used by the running daemon                                  |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | rss                   | 35.13 Mb         | resident memory used by the runnig daemon                                  |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | pid                   | 1689             | process id of the running daemon                                           |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | log-size              | 367632           | size of the logfile of the running daemon                                  |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | wr-buf-inflight       | 0 b              | write buffer allocated with data in-flight in writing                      |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | wr-buf-queued         | 0 b              | write buffer allocated and kept on the queue for future reuse in writing   |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | wr-nobuff             | 0                | counter how often a \'no available buffer\' condition was hit in writing   |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | ra-buf-inflight       | 0 b              | read-ahead buffer allocated with data in-flight in read-ahead              |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | ra-buf-queued         | 0 b              | read-ahead buffer allocated and kept on the queue for future reuse in ra   |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | ra-xoff               | 0                | counter how often we needed to wait for an available read-ahead buffer     |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | ra-nobuff             | 0                | counter how often a \'no available buffer\' condition was hit in read-ahead|
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | rd-buf-inflight       | 0 b              | read buffer allocated with data in-flight for reading                      |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | rd-buf-queued         | 0 b              | read buffer allocated and kept on the queue for future reuse in reading    |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | version               | 4.4.17           | current version of the daemon                                              |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | fuseversion           | 28               | current version of the FUSE protocol                                       |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | starttime             | 1549548272       | starttime as unixtimestamp                                                 |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | uptime                | 64772            | run time of the daemon in seconds                                          |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | total-mem             | 8201658368       | total memory of the hosting machine                                        |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | free-mem              | 153280512        | free memory of the hosting machine                                         |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | load                  | 1313946976       | 1 minute load avg as returned by sysinfo                                   |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | total-rbytes          | 0                | total number of bytes read on this mount                                   |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | total-wbytes          | 0                | total number of bytes written on this mount                                |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | total-io-ops          | 1267             | total number of io operations done on this mount                           |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | read-mb/s             | 0.00             | 1 minute average read rate in MB/s                                         |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | write-mb/s            | 0.00             | 1 minute average write rate in MB/s                                        |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | iops                  | 0                | 1 minute average io ops rate                                               |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | xoffs                 | 0                | counter how often we needed to wait for an available write buffer          |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | instance-url          | myhost:1094      | hostname and port of the upstream EOS instance                             |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | client-uuid           | 4af8154c.....    | unique identifier of this client (UUID)                                    |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | server-version        | 4.4.17           | server version where this client is connected                              |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | automounted           | 0                | indicates if the mount is done via autofs                                  |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+
    |    ALL   | max-inode-lock-ms     | 0.00             | maximum time any thread in the thread pool is stuck in ms                  |
    +----------+-----------------------+------------------+----------------------------------------------------------------------------+

The statistics file can be printed by any user on request by running:

.. code-block:: bash

   eosxd get eos.stats <mount-point>


The statistics file counter can be reset by running this command as root:

.. code-block:: bash

   eosxd set system.eos.resetstat - /eos/


.. index::
   pair: FUSE; Virtual Attributes


Virtual attributes on EOS mounts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Virtual attributes allow getting information which is not exposed by POSIX APIs.


Virtual Attribute Getters
""""""""""""""""""""""""""

The following shows all the defined attributes and their meaning:

.. code-block:: bash

    eosxd get
    usage CLI   : eosxd get <key> [<path>]

                        eos.btime <path>                   : show inode birth time
                        eos.ttime <path>                   : show lastest mtime in tree
                        eos.tsize <path>                   : show size of directory tree
                        eos.dsize <path>                   : show total size of files inside a directory
                        eos.dcount <path>                  : show total number of directories inside a directory
                        eos.fcount <path>                  : show total number of files inside a directory
                        eos.name <path>                    : show EOS instance name for given path
                        eos.md_ino <path>                  : show inode number valid on MGM
                        eos.hostport <path>                : show MGM connection host + port for given path
                        eos.mgmurl <path>                  : show MGM URL for a given path
                        eos.stats <path>                   : show mount statistics
                        eos.stacktrace <path>              : test thread stack trace functionality
                        eos.quota <path>                   : show user quota information for a given path
                        eos.url.xroot                      : show the root:// protocol transport url for the given file
                        eos.reconnect <mount>              : reconnect and dump the connection credentials
                        eos.reconnectparent <mount>        : reconnect parent process and dump the connection credentials
                        eos.identity <mount>               : show credential assignment of the calling process
                        eos.identityparent <mount>         : show credential assignment of the executing shell

    as root             system.eos.md  <path>              : dump meta data for given path
                        system.eos.cap <path>              : dump cap for given path
                        system.eos.caps <mount>            : dump all caps
                        system.eos.vmap <mount>            : dump virtual inode translation table



Virtual Attribute Setters
""""""""""""""""""""""""""

The following shows all attributes, which can be set to trigger internal functions changing the state of a mount:

.. code-block:: bash

    eosxd set
    usage CLI   : eosxd set <key> <value> [<path>]

    as root             system.eos.debug <level> <mount>   : set debug level with <level>=crit|warn|err|notice|info|debug|trace
                        system.eos.dropcap - <mount>       : drop capability of the given path
                        system.eos.dropcaps - <mount>      : drop call capabilities for given mount
                        system.eos.resetstat - <mount>     : reset the statistic counters
                        system.eos.log <mode> <mount>      : make logfile public or private with <mode>=public|private
                        system.eos.fuzz all|config <mount> : enabling fuzzing in all modes with scaler 1 (all) or switch back to the initial configuration (config)



.. index::
   pair: FUSE; Serverce-side Configuration

Server Side Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

The **eosxd** network provides four configuration parameters, which can
be shown or modified using **eos fusex conf**

.. code-block:: bash

    [root@eos ]# eos fusex conf
    info: configured FUSEX broadcast max. client audience 256 listeners
    info: configured FUSEX broadcast audience to suppress match is '@b[67]'
    info: configured FUSEX heartbeat interval is 10 seconds
    info: configured FUSEX quota check interval is 10 seconds


The default heartbeat interval is 10 seconds, upon which each
**eosxd** process sends a heartbeat message to the MGM server. The quota
check interval is the interval after which the MGM FuseServer checks
again if a **eosxd** client went out of quota or back to quota. The
default is also 10 seconds.

When working with thousands of clients within a single directory the
amount of messages in the FuseServer broadcast network can overwhelm the
MGM messaging capacity. To reduce the amount of messages sent around
while files are open and written, a threshold can be defined after which
a certain audience of clients will not receive any more meta-data update
or forced refresh messages. If 1000 clients write 1000 files within a
single directory the message rate is 100kHz for file-size updates while
the clients are writing. In the example above if a message hits more
than 256 listeners and the client names start with b6 or b7 messages
will be suppressed. Messages emitted when files are created or
commmitted are not suppressed!

Limiting Server Side FUSEx access
""""""""""""""""""""""""""""""""""

**eosxd** client rates can be limited using the rate limiter interface
available via the **access** command in the CLI.

.. code-block:: bash

    # limit the access for listing to 100 Hz per user
    eos access set limit 100 rate:user:\*:Eosxd::prot::LS

    # limit the access for stats to 1000 Hz per user
    eos access set limit 1000 rate:user:\*:Eosxd::prot::STAT

    # limit the access for returning list entries to 10 kHz per user
    eos access set limit 10000 rate:user:\*:Eosxd::ext::LS-Entry

    # limit the access for meta-data updates to 1 kHz per user
    eos access set limit 1000 rate:user:\*:Eosxd::prot::SET


LS, STAT and SET limits are applied by the corresponding server side
protocol methods. LS-Entry is applied when another LS call is requested.
Please note the difference in the naming of the **prot** and **ext**
counter types.



Namespace Configuration
^^^^^^^^^^^^^^^^^^^^^^^

By default each client sends its desired leasetime for directory
subscriptions (300s default at time of writing). For certain directories
in the hierarchy which are essentially read-only it improves the overall
performance to define a longer leasetime. In a home directory hierarchy
like **/eos/user/f/foo** the first three directory levels could have a
longer lease time defined.

.. code-block:: bash

    [root@eos ]# eos attr set sys.forced.leasetime=86400 /eos/
    [root@eos ]# eos attr set sys.forced.leasetime=86400 /eos/user/
    [root@eos ]# eos attr set sys.forced.leasetime=86400 /eos/user/f


.. index::
   pair: FUSE; File State Tracking

File State Tracking for eosxd
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The namespace registers the state changes of a file inside the extended
attribute *sys.fusex.state*.

The extended attribute can track up to 127 operations, then gets
truncated to half. A truncation is indicated with a leading *||* in
the attribute.

Possible state flags are:

-   C := File has been created by the FuseServer

-   U := File has been updated in the FuseServer

-   T := File has been truncated in the FuseServer or opened with a
    TRUNCATE flag

-   ± := File size has been changed

-   R := File has been renamed in the FuseServer

-   M := File has been moved in the FuseServer

-   0 := an invalid operation has been seen in the FuseServer (should
    never happen)

-   Z := File recovery has been triggered by a FUSE client

-   +fs := File replica/stripe has been committed ( multiple entries
    possible, fs is the filesystem id in decimal)

-   c := File checksum has been committed

-   s := File size has been committed

-   v := Replica has been verified for the checksum

-   V := Replica has been verified for the size

-   \| := Terminates a commit sequence started with +fs

-   || := tracked operations exceeded 127 and the attribute has been
    truncated

Example:

.. code-block:: bash

    [root@eos ]# eos attr ls /eos/file
                ...
                sys.fusex.state="CU±+2sc|+1v|U±+2sc|+1v|U±+2sc|+1v|"
                ...


This examples show the creation \"C\", the file size update \"U±\", a
commit from filesystem 2 with checksum and size \"+2sc\", a commit from
filesystem 1 with checksum verification \"+1v\", then two
update sequences to the file resulting in filesize change.

Replica/Chunk Tracking
^^^^^^^^^^^^^^^^^^^^^^

The namespace registers all replica/stripe create,unlink and delete
operation inside the extended attribute *sys.fs.tracking*.

The extended attribute is truncated by half when it exceeds 127 characters. A
truncation is indicated with a leading *\|\|* in the attribute.

Possible indicators are:

.. epigraph::

   ========= ========================================================
   indicator description
   ========= ========================================================
   +fsid     a replica/stripe was attached on filesystem fsid
   -fsid     a replica/stripe has been unlinked for filesystem fsid
    fsid     a replica/stripe has been deleted on filesystem fsid
   ========= ========================================================

Example:

.. code-block:: bash

    [root@eos  ]# eos attr ls /eos/file
                  ...
                  sys.fs.tracking="+1+2+3+4-1-2/1/2"
                  ...


This examples shows the how replicas are attached on filesystems
1,2,3,4, then unlinked on 1,2 and finally deleted on 1,2.


.. index::
   pair: Using; EOS FUSE Ubuntu


EOS FUSE mount on Ubuntu/Debian
--------------------------------

The following releases of Ubuntu are currently supported:

* Ubuntu 22.04.5 LTS (Jammy Jellyfish)
* Ubuntu 24.04.3 LTS (Noble Numbat)
* Ubuntu 25.04 (Plucky Puffin) - starting with eos version 5.4.0


Follow these steps to configure the necessary APT repositories and install
the EOS client and FUSE packages:

.. code-blocK:: bash

   Setup the APT repositories holding the EOS packaage:
   # Import the EOS GPG key of the repository
   curl -sL http://storage-ci.web.cern.ch/storage-ci/storageci.key | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/storage-ci.gpg
   # Create the APT repository configuration
   echo "deb [arch=$(dpkg --print-architecture)] http://storage-ci.web.cern.ch/storage-ci/debian/eos/ $(lsb_release -cs) release" | sudo tee /etc/apt/sources.list.d/eos-client.list > /dev/null


Install the EOS packages and their dependency - requires root privileges and
create the local directory for the local mounts:

.. code-block:: bash

   sudo apt update
   sudo apt install -y eos-fusex
   # All mounts will be in this directory by default, but this can be chaged
   # by using the "localmountdir" in the configuration below.
   sudo mkdir /eos/


Create the configuration files for the EOS FUSE mountpoints. Depending on the EOS
instances that need to be accessed, one can create one configuration file per
instance, using the following convention:

* Configuration for accessing data stored on CERNBox can be placed in
  `/etc/eos/fuse.home-<initial>.conf` where <initial> should be replaced by a
  sigle letter eg. 'e', 'a', etc. The contents of this file should at least
  contain the following (note: this needs to be a valid JSON object):

  .. code-block:: bash

     {"name": "home-<initial>", "hostport":"eoshome-<initial>.cern.ch", "remotemountdir":"/eos/user/<initial>/"}


Example:

* Mount configuration for user account "userx" whose data is stored in CERNBox

  .. code-block:: bash

     {"name": "home-u", "hostport": "eoshome-u.cern.ch", "remotemountdir": "/eos/user/u/userx/"}


* Mount configuration for project "asdf" whose data is stored in the EOSPROJECT instance

  .. code-block:: bash

     {"name": "project-a", "hostport": "eosproject-a.cern.ch", "remotemountdir": "/eos/project/a/asdf/"}


* Mount configuration for accessing the EOSCMS instance

  .. code-block:: bash

     {"name": "cms", "hostport":"eoscms.cern.ch", "remotemountdir":"/eos/cms/"}


With the above configuration in place, one can setup automount to take care of managing the mountpoints.

.. code-block:: bash

   # Ensure the autofs package is installed:
   sudo apt install -y autofs

   # Check that the autofs service is up and running
   sudo systemctl status autofs

   # Create a file called "/etc/auto.eos" which containts the mountpoints to be managed by autofs.
   # Example contents of /etc/auto.eos
   home-a -fstype=eosx,fsname=home-a :eosxd
   # ... same for each user letter
   home-z -fstype=eosx,fsname=home-z :eosxd
   project-a -fstype=eosx,fsname=project-a :eosxd
   # ... some for each project letter
   project-z -fstype=eosx,fsname=project-z :eosxd
   cms -fstype=eosx,fsname=cms :eosxd

   #Create a file called "/etc/auto.master.d/eos.autofs" like this:
   echo "/eos /etc/auto.eos" > /etc/auto.master.d/eos.autofs


At this point the mountpoints are managed automatically by the autofs daemon.
Therefore, trying to access the local path `/eos/home-u/` given the above
configuration for user `userx` would display their CERNBox contents.
All mounts will be created inside the `/eos/` directory on the local file
system and can be accessed by concatenating the first column in the
`/etc/auto.eos` with the `/eos/` path.

For further configuration options when it comes to handling EOS FUSE mountpoints
please consult the following document:
https://gitlab.cern.ch/dss/eos/-/blob/master/fusex/README.md


SquashFS images for software distribution
-----------------------------------------

EOS provides support for SquashFS image files, which can be automatically mounted when the image path is traversed. This functionality requires an appropriate automount configuration.

To create SquashFS images a client needs the EOS shell and a local mount with a path prefix identical to that inside the client shell.
This means e.g. both commands as shown here point to the same directory:

.. code-block:: bash

   # access inside the shell
   eos ls -la /eos/foo/bar
   # access using the FUSE mount
   ls -la /eos/foo/bar


To really have read-only access to the  contents of SquashFS images, clients have to install the package **cern-eos-autofs-squashfs**.

All functionality of the SquashFS CLI is displayed using the help option:

.. code-block:: bash

   eos squash -h


The functionality can be grouped into two categories:

* Simple SquashFS packages
* Release SquashFS packages

.. index::
   pair: SquashFS; Simple Packages

Simple SquashFS Packages
^^^^^^^^^^^^^^^^^^^^^^^^

A simple SquashFS package consists of a symbolic link under the package path and a hidden package file in the same directory as the symbolic link.

The workflow to create a SquashFS package is shown here:

Create a new package
"""""""""""""""""""""
.. code-block:: bash

   [root@dev ]# eos mkdir -p /eos/dev/squash/
   [root@dev ]# eos squash new /eos/dev/squash/mypackage
   info: ready to install your software under '/eos/dev/squash/mypackage'
   info: when done run 'eos squash pack /eos/dev/squash/mypackage' to create an image file and a smart link in EOS!

   # see what happened - we have created a symbolic link in EOS with the package pathname and the link points to a local stage directory in /var/tmp/...
   [root@dev ]# eos ls -la /eos/dev/squash/
   drwxrwxrw+   1 root     root               59 May 27 13:32 .
   drwxrwxrw+   1 root     root       4751231651 May 27 13:32 ..
   lrwxrwxrwx   1 nobody   nobody             59 May 27 13:32 mypackage -> /var/tmp/root/eosxd/mksquash/..eos..dev..squash..mypackage/


Install software into a package
"""""""""""""""""""""""""""""""

.. code-block:: bash

   # install software into the package, de facto we work on the local disk under /var/tmp/...
   [root@dev ]# cd /eos/dev/squash/mypackage/
   [root@dev ]# touch HelloWorld

Pack a new package
""""""""""""""""""

.. code-block:: bash

   # pack the new package
   [root@dev ]# eos squash pack /eos/dev/squash/mypackage

   # see what happened - the symlink in EOS with the package pathname points to an encoded loction for the hidden package file .mypackage.sqsh
   [root@dev ]# eos ls -la /eos/ajp/squash/
   drwxrwxrw+   1 root     root             4161 May 27 13:38 .
   drwxrwxrw+   1 root     root       4751235753 May 27 13:32 ..
   -rw-r--r--   2 nobody   nobody           4096 May 27 13:38 .mypackage.sqsh
   lrwxrwxrwx   1 nobody   nobody             65 May 27 13:38 mypackage -> /eos/squashfs/ajp.cern.ch@---eos---ajp---squash---.mypackage.sqsh


If you try to use or access a package on a different client machine before you call **eos squash pack** you will get errors on clients, because the symbolic link points to a non-existing local directory as long as a package is not closed.

In general you have to treat SquashFS packages as write-once archives. There is the possiblity to unpack a packed archive, modify and re-pack, however this is problematic if a package is already accessed on other clients using the automount mechanism. They won't remount an updated package automatically unless the mount is removed by idle timeouts and re-mounted later.


Package information
"""""""""""""""""""


For completeness here are the commands to get information about a package:

.. code-block:: bash

   [root@dev ]# eos squash info /eos/dev/squash/mypackage
   info: '/eos/dev/squash/.mypackage.sqsh' has a squashfs image with size=4096 bytes
   info: squashfs image is currently packed - use 'eos squash unpack /eos/dev/squash/mypackage' to open image locally


Unpackaging
"""""""""""

As mentioned you can unpack an existing package:


.. code-block:: bash

   [root@dev ]# eos squash unpack /eos/ajp/squash/mypackage
   ...
   info: squashfs image is available unpacked under '/eos/dev/squash/mypackage'
   info: when done with modifications run 'eos squash pack /eos/dev/squash/mypackage' to create an image file and a smart link in EOS!


And pack it again:

.. code-block:: bash

   # pack the new package
   [root@dev ]# eos squash pack /eos/dev/squash/mypackage

Deleting a package
""""""""""""""""""

To delete a SquashFS package you run:

.. code-block:: bash

   # delete a package
   [root@dev ]# eos squash rm /eos/dev/squash/mypackage


Relabeling a package
""""""""""""""""""""

If a SquashFS package and/or package files has been moved around in the namespace e.g. by doing this:

.. code-block:: bash

   [root@dev ]# eos mv /eos/dev/squash/ /eos/dev/newsquash/

then the package links are broken. In this case one has to relabel the package:

.. code-block:: bash

   [root@dev ]# eos squash relabel /eos/dev/newsquash/mypackage


Remote web installation of packages
"""""""""""""""""""""""""""""""""""

The CLI provides a convenience function to install a .tar.gz package from a web URL:

.. code-block:: bash

   [roo@dev ]# eos squash install --curl=https://root.cern/download/root_v6.24.00.Linux-centos7-x86_64-gcc4.8.tar.gz /eos/dev/newsquash/root

After successful execution the software package is ready for use and no further packaging commands are required.

If you have the automounter RPM installed on your client you are ready to use the software:

.. code-block:: bash

   cd /eos/dev/newsquash/root/
   ...

.. index::
   pair: SquashFS; Release Packages

Release SquashFS Packages
^^^^^^^^^^^^^^^^^^^^^^^^^

The **simple** package functionality is sufficient, if properly used. Many times you want to deal with updates and new release/versions of software. In this case the **release** functionality is preferable.

Creating a new release package
""""""""""""""""""""""""""""""

Release package management is illustrated in the following:

.. code-block:: bash

   [root@dev ]# eos squash new-release /eos/dev/release/mypackage
   info: ready to install your software under '/eos/dev/release/mypackage/.archive/mypackage-20210527135506'
   info: when done run 'eos squash pack /eos/dev/release//mypackage/.archive/mypackage-20210527135506' to create an image file and a smart link in EOS!
   info: install the new release under '/eos/dev/release/mypackage/next'


This new release is now locally available under **/eos/dev/release/mypackage/next**. You can install your software to this location and then call

Packing a new release package
"""""""""""""""""""""""""""""

.. code-block:: bash

   [root@dev ]# eos squash pack-release /eos/dev/release/mypackage
   ...
   info: new release available under '/eos/ajp/squash/mypackage/current'

Now we have published the latest version of our release under **/eos/dev/release/mypcakge/current**. Our package name is in the release management mode a directory containing a **current** link, if there is an open new release a **next** link and a hidden **.archive** directory, where all versions of a release are stored.

By default a release is created with the unix timestamp during **new-release**. For most people it might be more convenient to specify a version number. In this case you call:

.. code-block:: bash

   [root@dev ]# eos squash new-release /eos/dev/release/mypackage v1.0.0
   ...
   [root@dev ]# eos squash pack-release /eos/dev/release/mypackage
   [root@dev ]# eos squash new-release /eos/dev/release/mypackage v1.1.0
   ...
   [root@dev ]# eos squash pack-release /eos/dev/release/mypackage

Release Package Information
"""""""""""""""""""""""""""

You can obtain information about all available versions/releases doing:

.. code-block:: bash

   [root@dev ]# eos squash info-release /eos/dev/release/mypackage
   ---------------------------------------------------------------------------
   - releases of '/eos/ajp/squash/mypackage'
   ---------------------------------------------------------------------------
   /eos/dev/squash/mypackage/.archive/mypackage-v1.0.0
   /eos/dev/squash/mypackage/.archive/mypackage-v1.1.0
   /eos/dev/squash/mypackage/current
   ---------------------------------------------------------------------------

The output shows two versions in the **archive** and the **current** link.

Trimming Release Packages
"""""""""""""""""""""""""

If you regularly build software releases, you want to limit the number of versions which are kept.

You can trim your software releases using:

.. code-block:: bash

   [root@dev ]# eos squash trim-release /eos/dev/release/mypackage 100

This commmand will keep only versions not older than 100 days.

Additionally you can specify the maximum number of versions to keep:

.. code-block:: bash

   [root@dev ]# eos squash trim-release /eos/dev/release/mypackage 100 10

In this case we don't want to keep more than the 10 most recent versions, not older than 100 days.

Deleting Release Packages
"""""""""""""""""""""""""

For completeness, there is a command to cleanup a release packge. Be aware that this will delete all your release versions!

.. code-block:: bash

   [root@dev ]# eos squash rm-release /eos/dev/release/mypackage
   ---------------------------------------------------------------------------
   - releases of '/eos/dev/release/mypackage'
   ---------------------------------------------------------------------------
   /eos/dev/release/mypackage/.archive/mypackage-v1.0.0
   /eos/dev/release/mypackage/.archive/mypackage-v1.1.0
   /eos/dev/release/mypackage/current
   ---------------------------------------------------------------------------
   info: wiping squashfs releases under '/eos/dev/release/mypackage'
   info: wiping links current,next ...
   info: wiping archive ...

The main difference between simple and release packages is that you can create a new release while the previous one is in use on any other client.

Shared filesystems as FST backends
----------------------------------

The EOS FST server can be configured to store data on any (shared) filesystem as storage device which supports extended attributes. To prevent filesystems sharing a device or a shared filesystem from being accounted multiple times in the space and node aggregation, one can label each filesystem with a shared filesystem name to indicated that all devices using this name share the same hardware resource.

The shared filesystem name can be configured when a filesystem is added e.g. a CephFS filesystem named cephfs1:

.. code-block:: bash

   fs add 7a41781f-62dc-4f18-8f64-375e57487578 foo.cern.ch /cephfs/ default rw cephfs1

If filesystems are already registered or the filesystem name has changed one can use the filesystem config command:

.. code-block:: bash

   fs config 1 sharedfs=cephfs1



Extended attribute locks
------------------------

An extended attribute lock is a simple mechanism to block file opens on locked files to foreigners. Foreigners are not owners. The owner is defined by the username and the application name.
So if any of these differ a client is considered a foreigner.

We define two types of locks:

-   exclusive : no foreigner can open a file with an exclusive lock for reading or writing

-   shared    : foreigner can open a file with an exclusive lock in case they are reading

Shared attribute locks are currently not exposed in the CLI.

To create an exclusive extended attribute lock:

.. code-block:: bash

   # create a lock
   eos -r 100 100 -a myapp file touch -l /eos/dev/lockedfile

   # the owner can read
   eos -r 100 100 -a myapp cp /eos/dev/lockedfile      - # will succeed

   # a foreigner can not read
   eos -r 101 101 -a myapp cp /eos/dev/lockedfile      - # will fail
   eos -r 100 100 -a anotherapp cp /eos/dev/lockedfile - # will fail

   # create a lock with a given liftime e.g. 1000s
   eos -r 100 100 -a myapp file touch -l /eos/dev/lockedfile 1000

   # create a lock which only requires the same user to be used
   eos -r 100 100 -a myapp file touch -l /eos/dev/lockedfile 1000 user

   # create a lock which only requires the same app to be used
   eos -r 100 100 -a myapp file touch -l /eos/dev/lockedfile 1000 app

By default locks are taken for 24h. The lifetime can be specified as seen before if needed. The audience can be relaxed to allow same app access or same user.

You can remove a lock if you are the owner by doing:

.. code-block:: bash

   # remove a lock
   eos -r 100 100 -a myapp file touch -u /eos/dev/lockedfile


The internal representation of an attribute lock is given here:

.. code-block:: bash

   attr ls /eos/dev/lockedfile | grep sys.app.locks
   # requiring a strict audience
   sys.app.lock="expires:1665042101,type:exclusive,owner:daemon:myapp"
   # requiring same user
   sys.app.lock="expires:1665042101,type:exclusive,owner:daemon:*"
   # requiring same app
   sys.app.lock="expires:1665042101,type:exclusive,owner:*:myapp"

The high-level functionality for creation/deletion of attribute locks can be circumvented by creating/deleting the *sys.app.locks* attribute using extended attribute interfaces.

.. index::
   pair: Using; Archiving

Archiving Directory Subtrees
----------------------------

The archive CLI supports the following commands:

.. code-block:: bash

   archive <subcmd>
           create <path>                      : create archive file
           put [--retry] <path>               : copy files from EOS to archive location
           get [--retry] <path>               : recall archive back to EOS
           purge[--retry] <path>              : purge files on disk
           transfers [all|put|get|purge|uuid] : show status of running jobs
           list [<path>]                      : show status of archived directories in the subtree
           kill <job_uuid>                    : kill transfer
           help [--help|-h]                   : display help message

In order to safely archive an EOS subtree to tape (CTA) the following steps detailed in this document must
be performed. Assume we want to archive the EOS subtree rooted at /eos/dir/archive/test. First of all,
the user needs to make sure that he/she has the necessary permissions to submit archiving commands.
The permissions check is enforced at directory level by using the **sys.acl** extended attribute
and it allows setting permissions at user, group or egroup level. The **ACL flag** for archiving
is **'a'**.

.. code-block:: bash

    sys.acl="u:tguy:a"  # user tguy has the right to archive for the current directory

The archive ACL entry is only required on the root of the archiving subtree.
Once the proper permissions are in place, we need to take a snapshot of all the metadata of the
files and directories under this subtree. For this we use the **archive create** command inside
the *EOS Console*:

.. code-block:: bash

   archive create /eos/dir/archive/test

There are some restrictions that apply to the contents for the archived hierarchy. These come
either from restrictions imposed by the tape backend or the type of files that can be created
on an EOS instance from an external client - as it's the case for the archive daemon. Therefore,
the archive creation will fail if any of the following types of files are present in the hierarchy.
Below you have different types of files that are **NOT** supported by the archive tool and
the corresponding eos command that you can run to determine if there are any such files in
your target hierarchy:

* 0 size files

.. code-bloc:: bash

   eos find --format fid,checksumtype,size /eos/<instance>/archive_dir/ | grep " size=0 "


* symlink/hardlink files

.. code-block:: bash

   eos find --format fid,checksumtype,size /eos/<instance>/archive_dir/ | grep " checksumtype=none "


* atomic, version or atomic-version files

.. code-block:: bash

   eos find --name ".sys.*" /eos/<instance>/archive_dir/


After creating an archive the EOS subtree is **immutable** and no updates are allowed either to the
data or the metadata. Transferring the data to tape (CTA) is done using the **archive put** command:

.. code-block:: bash

   archive put /eos/dir/archive/test

At any point during a transfer the user can retrieve the current status of the transfer by issuing an
**archive transfers** command. Once the transfer finishes there will be two additional files saved at
the root of the archived subtree: the **.archive.log** file which contains the logs of the last transfer
(note the 'dot' in the beginning of the filename, so to list it use **ls -la** in the *EOS Console*)
and another file called **.archive.<operation>.<outcome>** where operation is one of the following:
get/put/purge and the outcome can either be **done** or **err**.

While an archive operation is ongoing the file stored in EOS is marked with the **err** tag. For
example, an ongoing **put** operation, which can take several hours depending on the size of the
sub-tree being archived to tape, will appear in the **eos ls -la** output as **.archive.put.err**.
Once the put operation is successful, this file will be renamed to **.archive.put.done**. Therefore,
it's important to check the output of the **eos archive transfers** command which is listing the
status of the ongoing archive operations and not rely only on the status file in EOS.

If an error occurs the user has the possibility to resubmit the transfer by using the **--retry** option.

When the put operation is successful one should find a file called **.archive.put.done** at the root
of the subtree and the user can now issue the purge command which will delete all the data from EOS
thus freeing the space.

.. code-block:: bash

    archive purge /eos/dir/archive/test

To get the data back into EOS use the archive get command:

.. code-block:: bash

    archive get /eos/dir/archive/test

The same conventions as before apply when it comes to the progress and the final status of the transfer.
If the user would like to retrieve the status of previously archived directories he/she can use the
**archive list** command which will return the status of all archived directories rooted at the given
directory or if no directory is given then "/" is assumed. This command displays also the running
jobs but no detailed information about them is provided - for this you should use the **archive transfers**
command.

In case the user wants to permanently delete the data saved on **tape (CTA)**, then unless he has root
privileges on the EOS instance he will need to contact one of the administrators to perform this operation.
Permanently deleting the achive will not delete any data from EOS, but only the data saved in CTA.
Therefore, it is the **user's responsibility** to make sure he/she first gets the data back to EOS before
requesting the deletion of the archive.

Data Obufscation and Encryption
-------------------------------

We provide a generic EOS mechanism to obfuscate or encrypt data files stored on storage nodes, which does not require encrypted disk partitions. Each file is obfuscated/encrypted individually.
Encryption uses an obfuscation key (start vector) which is transformed into an encryption key using a client secret to compute an HMAC value of the obfuscation key. Data are then encrypted with a simple block cipher algorithm (ECB). This is the fastest way of encryption for random access without any read-write-amplification - but does not meet the highest security standards. Obfuscation keys are stored as an extended attribute of each file. It is not possible to view obfuscation keys using the EOS CLI. A low resolution fingerprint of the encryption key is also stored as an invisible extended attribute when encryption is done using a FUSE mount. This allows identifying incorrect client side keys and avoids returning unreadable content to clients on FUSE mounts. This mechanism is not used for remote access protocols.


.. index::
   pair: Encryption; Obfuscation

To enable obfuscation for individual files using remote protocols, one can use the CGI `&eos.obfuscate=1` when creating a new file.

To enable obfuscation for all new files created in in a directory use:

.. code-block:: bash

                [root@host~] eos attr set sys.file.obfuscate=1 /eos/obfuscate/


Obfuscated files are accessible with any protocol. For remote access protocols like xrdcp,eoscp,http files are unobfuscated by the FST gateway node. For FUSE mounts files are unobfuscated by the FUSE client.


.. NOTE:: Only new files are obfuscated when the obfuscation attribute was (re-)defined. Existing files will stay unobfuscated. You can use `eos convert --rewrite filename` to rewrite an existing file obfuscated.


Encryption requires obfuscation to be enabled! This is done by defining on the target directory:

.. code-block:: bash

                [root@host~] eos attr set sys.file.obfuscate=1 /eos/encryption/

Encryption is additionally enabled client-side by defining the environment variable `EOS_FUSE_SECRET`. It is used automatically by the `eoscp` command or the `eosxd` FUSE mounts, but not when using `xrdcp` or `http` access:

.. code-block:: bash

                [root@host~] eos attr set sys.file.obfuscate=1 /eos/encryption/
                [root@host~] export EOS_FUSE_SECRET=858aa9f8-545f-4b10-a823-3b7d822291a3
                [root@host~] eosxd get eos.reconnect /eos/ #after defining a new encryption key you have to reconnect the FUSE mount or create a new subshell
                [root@host~] eos cp /tmp/file root://localhost//eos/encryption/encrypted-file
                [root@host~] eos file info /eos/encryption/encrypted-file
                  File: '/eos/encryption/encrypted-file'  Flags: 0640
                  Size: 13
                 ...
                  #Rep: 1
                 Crypt: encrypted
                 ...

                [root@host~] cat /eos/encryption/encrypted-file
                Hello World!


When using `eoscp` files are decrypted by the FST and the encryption has to be forwared to the FST as part of the encrypted capability issued by the MGM node. When using FUSE mounts, encryption keys never leave the clients and decryption is done only on client side.


.. NOTE:: There is no way to recover contents of encrypted files if you lose the `EOS_FUSE_SECRET` key, which was used to encrypt a file!

.. NOTE:: To access encrypted files with remote protocols using `xrdcp` or `curl` you can define the encryption key using CGI e.g. `&eos.key=858aa9f8-545f-4b10-a823-3b7d822291a3`

Starting with EOS v5.2 it is possible to define a global encryption key in the fuse configuration file which is shared by all calling client applications. The configuration has to have mode 0400 and for shared mounts has to be owned by root:root, for private mounts it is has to be owned by the user/group id of the mounting user.

The syntax in the FUSE configuration file is as shown:

.. code-block:: bash

                cat /etc/eos/fuse.my.conf
                {"encryptionkey":"655361ab-5af9-4697-8a32-8069ade18a27"}

.. NOTE:: To create an unencrypted and encrypted area using single FUSE mounts, it is sufficient to define an encryption key in the FUSE configuration file, have a storage area where the `sys.eos.obfuscate` exteneded attribute is not defined (unencrypted) and one where the `sys.eos.obfuscate` attribute is defined (encrypted).


Running Authentication Front-ends
---------------------------------

The MGM supports servicing requests from a front-end XRootD authentication server. An authentication front-end server is an XRootD server running the EosAuthOfs plug-in. Using this plug-in the front-end server connects to a standard MGM service (back-end) over ZMQ protocol.
An authentication front-end allows one to configure a subset of authentication methods and to partition connections of certain use cases to this daemon, shielding the standard MGM service from direct conections.

To enable a standard MGM to allow connections from an authentication front-end use the following MGM configuration variables:

.. code-block:: bash

                #-------------------------------------------------------------------------------
                # Configuration for the authentication plugin EosAuth
                #-------------------------------------------------------------------------------
                # Set the number of authentication worker threads running on the MGM
                mgmofs.auththreads 64

                # Set the front end port number for incoming authentication requests
                mgmofs.authport 15555

                # Only listen on localhost connections
                mgmofs.authlocal 1


If you want to run your authentication front-end on a separate machine from the MGM service, you can use ```mgm.authlocal 0```.
Be aware that you have to protect the given port from 'unwanted' access. There is no authentication involved in the communication from
front-end to back-end MGM.

An example configuration file for a front-end server on the back-end MGM node looks like this:

.. code-block:: bash

                # ------------------------------------------------------------ #
                [mgm:xrootd:auth]
                # ------------------------------------------------------------ #
                xrd.port 2094
                all.export /

                # the back-end server - localhost in our case
                eosauth.mgm localhost:15555
                # number of socket connections - should match thread-pool size if only one front-end exists
                eosauth.numsockets 64
                # loglevel
                eosauth.loglevel info

                xrootd.fslib /usr/lib64/libEosAuthOfs.so
                xrootd.seclib libXrdSec.so
                xrootd.chksum adler

                # UNIX authentication + any other type of authentication wanted
                sec.protocol unix
                sec.protbind localhost.localdomain unix
                sec.protbind localhost unix
                sec.protbind * only unix


If an authentication front-end receives a redirection e.g. from a passive to an active MGM due to HA changes,
the front-end server uses redirect-collapse and redirects on the same port as the accessed front-end service - in case of this example this is port 2094!

One can overwrite the port used for a collapsing redirection using:


.. code-block:: bash

                eosauth.collapseport 3094


.. NOTE:: This feature is useful if you want to run several front-ends on the same back-end node.



QClient Configuration
---------------------

5.3.0 release of EOS introduces features to potentially speed up metadata using
increased parallelism. Currently we don't recommend setting these features on
your production clusters and only for your testing needs, especially if parallel file creation/deletion
is a bottleneck encountered in your clusters.

.. code-block:: bash

                #-------------------------------------------------------------------------------
                # Configuration for Qclient settings
                #-------------------------------------------------------------------------------
                # flusher type controls the temporary backend storage for qclient, which can
                # be utilized in case of crashes where the not acknowledged QuarkDB messages
                # are replayed. For purely developer clusters options like MEMORY &
                # MEMORY_MULTI provide faster interfaces which can help debug performance
                # problems and aid in future development; also TESTING_NULL_UNSAFE_IN_PROD
                # completely eliminates journalling for pure interface adherence tests
    # In multithreaded scenarios, we currently track the highest acknowledged message
    # this behaviour can be controlled by setting ROCKSDB_MULIT:LOW or HIGH respectively

                mgmofs.qclient_flusher_type ROCKSDB # choose between ROCKSDB (default) & ROCKSDB_MULTI

    # For tuning ROCKSDB itself, we provide the following option, please exercise caution
    # eg: "write_buffer_size=1073741824;max_write_buffer_number=5;min_write_buffer_number_to_merge=2"
    mgmofs.qclient_rocksdb_options  # NOT CONFIGURED by default

    # Path where the persistent storage lives; Only needed when you really need to drop and recreate rocksdb
    # which is almost never
    mgmofs.queue_path /var/eos/ns-queue


FlatScheduler configuration
---------------------------

EOS v5.2.0 release introduces a new scheduler where scheduling strategies can be
configured at runtime. These can be enabled on a per space level. The scheduler
is also weights aware, where a disk is alloted different weights according to
its capacity, so for groups with heterogenous disks one has a better filling of
disk capacities. Currently we fallback to the classical geoscheduler in case
valid placements using any of the scheduling strategies isn't found.


Scheduling Strategies
"""""""""""""""""""""

The following strategies are currently offered:

+------------------+----------------------------------------------------------------------------------------------+
| Strategy         | Description                                                                                  |
+==================+==============================================================================================+
| ``geo``          | The classical geotree engine, this is the default option                                     |
+------------------+----------------------------------------------------------------------------------------------+
| ``weightedrr``   | A strategy that fills weights for disks and distributes roundrobin according to weights      |
+------------------+----------------------------------------------------------------------------------------------+
| ``weightedrandom`` | Uses a weightedrandom engine, for randomly choosing according to weights                   |
+------------------+----------------------------------------------------------------------------------------------+
| ``random``       | Randomly chooses disks within groups, useful for homogeneous groups                          |
+------------------+----------------------------------------------------------------------------------------------+
| ``roundrobin``   | Goes in a roundrobin fashion choosing disks within groups, useful for homogeneous groups     |
+------------------+----------------------------------------------------------------------------------------------+
| ``tlrr``         | A more performant version of the roundrobin algorithm, where each MGM thread has its own     |
|                  | roundrobin. While not as coordinated as a global roundrobin, it should be more or less       |
|                  | amortized for large enough placements. Useful for homogeneous groups                         |
+------------------+----------------------------------------------------------------------------------------------+


.. code-block:: bash

   # configure scheduler type for a space
   eos space config <spacename> space.scheduler.type=weightedrandom



Disk Weight configuration
"""""""""""""""""""""""""
For weighted scheduling, by default disks are weighted as a short unsigned integer, where the number is equivalent to
the disk capacity in TB, ie. a disk with 4 TB capacity is allotted a weight of 4 and so forth. This can be
changed at runtime to for eg. do certain draining/maintenance operations where the weight can be lowered
to attract less writes.

.. code-block:: bash

   # configure weight as 0 for disk with fsid 10
   eos sched configure weight default 10 0


Alternative checksums
---------------------

The 5.4.0 release introduces support for **alternative checksums**. This feature allows for the computation and storage of multiple checksums for each file (e.g., MD5, SHA-256) in addition to the default one.

The desired checksums are configured on a **per-directory basis** using an extended attribute. The actual computation can be performed in one of two ways:

* Synchronously: the checksum is computed when the file is uploaded.
* Asynchronously: the checksum is computed later by a background process on the storage node.


Enabling Alternative Checksums on a Directory
"""""""""""""""""""""""""""""""""""""""""""""

To specify which alternative checksums should be computed for files within a directory, set the `sys.altxs`` extended attribute. The value should be a comma-separated list of the desired checksum algorithms.

For example, to compute MD5, SHA-1, and SHA-256 checksums for all new files in the `/eos/dev/altxs` directory, you would run:

.. code-block:: bash

   eos attr set sys.altxs="md5,sha1,sha256" /eos/dev/altxs


Administrator Configuration
"""""""""""""""""""""""""""

The following parameters control the behavior of the alternative checksums feature from the administrator's side.

Synchronous computation (during file upload) is controlled at the space level. To enable it for a specific space, set the `altxs` variable to on.

.. code-block:: bash

   eos space config <space_name> space.altxs=on

The following settings control the asynchronous computation and policy synchronization for the entire filesystem.

.. code-block:: bash

   # Enables or disables the periodic synchronization of alternative checksum
   # policies (from 'sys.altxs' attributes) from the namespace.
   # Set to 1 to enable, 0 to disable.
   eos fs config <fsid> altxs_sync=0 # 0 by default

   # Time interval in seconds after which checksum settings are refreshed from the
   # namespace. If set to 0, synchronization only happens once.
   # This parameter is only effective if 'altxs_sync' is enabled.
   eos fs config <fsid> altxs_sync_interval=0 # default is 0 (sync only once)

   # Time interval in seconds for the background thread that scans the namespace
   # for files needing asynchronous alternative checksum computation.
   eos fs config <fsid> scan_altxs_interval=3600

   # The maximum rate (in namespace entries per second) at which the scanner
   # checks for files that need alternative checksums computed.
   eos fs config <fsid> scan_altxs_rate=1000
