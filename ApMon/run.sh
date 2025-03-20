#!/bin/sh

[ -f /etc/sysconfig/eos ] && . /etc/sysconfig/eos
[ -f /etc/sysconfig/eos_env ] && . /etc/sysconfig/eos_env

cleanup() {
    # kill all subprocesses
    for pid in $(ps --ppid $$ --forest -o pid --no-headers); do
        kill $pid &> /dev/null
    done
    exit 0
}

trap cleanup SIGINT SIGTERM

if [ -z "${MONALISAHOST}" ]; then
    echo "error: please configure the MONALISAHOST variable in /etc/sysconfig/eos first!"
    exit 1
fi

eosuser=daemon
xrdpid=$(pgrep -u "${eosuser}" xrootd | head -1)

if [ -z "${xrdpid}" ]; then
    xrdpid=999999
fi


export PERL5LIB=$(perl -V:installsitearch | cut -d "'" -f 2)/ApMon
runuser -u ${eosuser} -- /opt/eos/apmon/eosapmond ${MONALISAHOST} /var/log/eos/apmon/apmon.log ${APMON_DEBUG_LEVEL:-"WARNING"} ${APMON_INSTANCE_NAME:-"unconfigured"} ${HOSTNAME} ${xrdpid} &
wait
