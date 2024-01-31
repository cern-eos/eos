eoscfsd
========

eoscfsd is a high-performance pass-through implementation for POSIX filesystems. It adds CERN.CH kerberos authentication and remote configuration and mount key obfuscation. 

eoscfsd fetches the mount instructions for a named mount from a configurable HTTPS server. 

Configuration File
------------------
The configuration file is located at `/etc/eos/cfsd/eoscfsd.conf`.

An example file looks like this:
```json
{
  "testmount" : {
     "server" : "testmount-server.cern.ch"
  },
  ...
}
````

The mount named `testmount` will fetch the mount instruction from HTTPS server `testmount-server.cern.ch`. The mount instruction has to be stored on this webserver on the top-level directory `.../html/testmount.key`. If you want to store the key in some subdirectory you can add the directory to the server name e.g. `testmount-server.cern.ch/cfsd-keys/`. 

Several named mounts can be configured pointing to identical or individual HTTPS server.

For each named mount (e.g. `testmount`) an unlock key for the mount instruction has to be installed on the client node under `/etc/eos/cfsd/[name].key` (e.g. `/etc/eos/cfsd/testmount.key).

If the HTTPS server is an empty string, eoscfsd will use a mount instruction defined at compile time. 

How to create a mount instruction?
----------------------------------

eoscfsd requires a mount instruction mounting any POSIX filesystem to /@eoscfsd.

First we create an unlock key:
`uuidgen > /etc/eos/cfsd/nfs.key ; chmod 400 /etc/eos/cfsd/nfs.key`
E.g. if we want to user kerberos identities on a non-kerberized NFS mount, we can create a mount instruction like this:

```
eos daemon seal "sudo mount -t nfs -o vers=4 nfsserver.cern.ch:/nfsshare/ /@eoscfsd/" `cat /etc/eos/cfsd/nfs.key` > /tmp/nfs.key
```

We upload `/tmp/nfs.key` to our key HTTPS server. 

Now we can run an eoscfsd mount using the syntax `eoscfsd <mount-point> <mount-name>`:
```
mkdir -p /cern/nfs
eoscfsd /cern/nfs nfs
```

When eoscfsd starts, it will have some verbose output about fetching the mount instruction from the configured HTTPS server. By default, eoscfsd always lists the top-level directory after running the mount instruction.

```
[root@node ] eoscfsd /test/ test
# cleanup: old mounts
# unsharing
# re-mounting
# mounting test
*   Trying xxx.xxx.xxx.xxx:443...
* Connected to xyz (xxx.xxx.xxx.xxx) port 443 (#0)
* ALPN, offering h2
* ALPN, offering http/1.1
*  CAfile: /etc/pki/tls/certs/ca-bundle.crt
* SSL connection using TLSv1.3 / TLS_AES_256_GCM_SHA384
* ALPN, server accepted to use h2
* Server certificate:
*  subject: CN=xyz
*  start date: Oct 21 14:31:18 2022 GMT
*  expire date: Nov 24 14:31:18 2024 GMT
*  subjectAltName: host "xyz" matched cert's "xyz"
*  issuer: DC=ch; DC=cern; CN=CERN Certification Authority
*  SSL certificate verify ok.
* Using HTTP2, server supports multi-use
* Connection state changed (HTTP/2 confirmed)
* Copying HTTP/2 data in stream buffer to connection buffer after upgrade: len=0
* Using Stream ID: 1 (easy handle 0x7f5e49c8d800)
> GET /test.key HTTP/2
Host: xyz
accept: */*

* old SSL session ID is stale, removing
* Connection state changed (MAX_CONCURRENT_STREAMS == 128)!
< HTTP/2 200 
< server: nginx/1.20.1
< date: Mon, 29 Jan 2024 08:58:34 GMT
< content-type: application/octet-stream
< content-length: 346
< last-modified: Tue, 23 Jan 2024 14:57:49 GMT
< etag: "65afd3ed-15a"
< accept-ranges: bytes
< 
* Connection #0 to host xyz left intact
info: ... mounting backends ...info: ... backends mounted ...total 10
drwxr-xr-x. 11 root       root   11 Jan 24 17:53 .
dr-xr-xr-x. 37 root       root 4096 Jan 23 15:58 ..
drwxr-xr-x.  4 root       root    2 Jan 24 18:07 .proc

```

Configuring Kerberos mapping in the backend filesystem
------------------------------------------------------

By default every access to the eoscfsd mount is mapping to uid:gid=99/99. Every user mapping has to be explicitely configured in the configuration subtree inside the backend filesystem, which is stored in the root of the mount under `.cfsd/`.

We are adding the kerberos name `testprod` and want to map it to uid:gid=100123:1100

```
mkdir -p <mount>/.cfsd/mapping/name
touch <mount>/.cfsd/mapping/name/testprod
chown 100123:1100 <mount>/.cfsd/mapping/name/testprod
```

Every access using kerberos name `testprod` will now be mapped to 100123:1100 on the backend filesystem. It is also possible to map a kerberos name to uid:gid=0:0 aka `root` by setting the owner on the mapping name to 0:0!

Be aware that mappings are cached for maximum 60 seconds and changes will be applied within a time window of maximum 60s.

Configuration Quota in the backend filesystem
----------------------------------------------
By default even if mapping is enabled and the mapped user should have quota and permissions in the backend filesystem, eoscfsd does not allow write access unless quota is enabled for the given uid or gid. There is a simple `yes` or `no` configuration for a given user and or group. 

```
mkdir -p <mount>/.cfsd/quota
mkdir -p <mount>/.cfsd/quota/user/
mkdir -p <mount>/.cfsd/quota/group/
# quota for user 100123
touch <mount>/.cfsd/quota/user/100123
# or quota for group 1100
touch <mount>/.cfsd/quota/group/1100
```

Be aware the quota settings are cached for a maximum of 60s and changes will be applied within a time window of maximum 60s. If you want to disallow writing for a given user/group, you remove the corresponding entry e.g.
```
unlink <mount>/.cfsd/quota/user/100123
```

Virtual attributes in eoscfsd mounts
------------------------------------

If you get the virtual attribte `cfs.id`, you will get as a response the current mapping and quota status of the calling process e.g.

```
getfattr -n cfs.id /test/
getfattr: Removing leading '/' from absolute path names
# file: test/
cfs.id="name: testprod uid:100123 gid:1100 quota:1"
```

This allows to debug quota/mapping problems.

eoscfsd recycle bin
-------------------

eoscfsd supports deletion trhough a recycle bin. It is still in beta state and will be documented when the feature has evolved for production.

autofs configuration
--------------------

By default eoscfsd mounts are mounted unde /cern/. This can be chaned in /etc/auto.master.d/cfsd.autofs
The mount configration has to be defined in the map file /etc/auto.cfsd

Entries look like e.g. mounting a cfsd filesystem named 'default':
default -fstype=eoscfs :default

The mount will appear under /cern/default in this case.

