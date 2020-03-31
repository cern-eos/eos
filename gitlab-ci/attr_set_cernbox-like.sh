#!/usr/bin/env bash

exec_cmd eos-mgm1 'eos attr set sys.acl="u:117703:rwx" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.allow.oc.sync="1" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.forced.atomic="1" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.forced.blocksize="4k" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.forced.checksum="adler" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.forced.layout="replica" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.forced.maxsize="50000000000" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.forced.nstripes="2" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.mask="700" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.mtime.propagation="1" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.owner.auth="*" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.recycle="/eos/home-i04/proc/recycle/" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set sys.versioning="10" eos/dockertest';
exec_cmd eos-mgm1 'eos attr set user.acl="" eos/dockertest';
