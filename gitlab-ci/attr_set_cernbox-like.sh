#!/usr/bin/env bash

usr=${1:-"eos-user"} # if eos-user, uid=1000 and gid=1000
uid=$(id -u $usr)
gid=$(id -g $usr)

eos chmod 3711 /eos/dockertest/proc/recycle # drwx--s--+   1 root     root

eos chmod 2755 /eos # drwxr-sr-+   1 root     root
eos attr set sys.forced.blocksize="4k" /eos;
eos attr set sys.forced.checksum="adler" /eos;
eos attr set sys.forced.layout="replica" /eos;
eos attr set sys.forced.nstripes="2" /eos;
eos attr set sys.forced.space="default" /eos;

eos mkdir /eos/user
eos attr set sys.recycle="/eos/dockertest/proc/recycle/" /eos/user;

eos mkdir /eos/user/e
eos attr set sys.forced.maxsize="50000000000" /eos/user/e;
eos attr set sys.mask="755" /eos/user/e;
eos attr set sys.owner.auth="*" /eos/user/e;
eos attr set sys.versioning="10" /eos/user/e;

# usr home directory
eos mkdir /eos/user/e/$usr
eos chown $usr:$usr /eos/user/e/$usr
eos chmod 3711 /eos/user/e/$usr # drwx--s--+   1 eos-user     eos-user
eos attr set sys.acl="u:$uid:rwx" /eos/user/e/$usr;
eos attr set sys.allow.oc.sync="1" /eos/user/e/$usr;
eos attr set sys.forced.atomic="1" /eos/user/e/$usr;
eos attr set sys.mask="700" /eos/user/e/$usr;
eos attr set sys.mtime.propagation="1" /eos/user/e/$usr;
eos attr set user.acl="" /eos/user/e/$usr;
eos access allow user $usr
