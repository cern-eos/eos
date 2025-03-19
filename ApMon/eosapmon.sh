#!/bin/sh

[ -f /etc/sysconfig/eos ] && . /etc/sysconfig/eos
[ -f /etc/sysconfig/eos_env ] && . /etc/sysconfig/eos_env

export PERL5LIB=$(perl -V:installsitearch | cut -d "'" -f 2)/ApMon
exec /usr/bin/setpriv --euid=daemon /usr/sbin/eos_apmond /usr/sbin/eos_apmonpl /var/log/eos/apmon/apmon.log ${MONALISAHOST:-"undef"} ${APMON_DEBUG_LEVEL:-"WARNING"} ${APMON_INSTANCE_NAME:-"unconfigured"} ${HOSTNAME}
