#!/bin/bash

# Sample shell script that provides host (and services) monitoring with ApMon.
#
# 2007-06-07
# Catalin.Cirstoiu@cern.ch

usage(){
	cat <<EOM
usage: servMon.sh [[-f|-k] -p pidfile] hostGroup [serv1Name pid1] ...
     -p pidfile   - put ApMon's PID in this file and run in background. If
                    pidfile already contains a running process, just exit.
	-f        - kill previous background-running ApMon with the same pidfile
	            and will start a new one with the new parameters
	-k        - just kill previous background-running ApMon
	hostGroup - base name for the host monitoring
	servXName - name of the X'th service
	pidX      - pid of the X'th service
EOM
	exit 1
}

# Get the machine's hostname
host=`hostname -f`

if [ $# -eq 0 ] ; then
  usage
fi

hostGroup=
newline="
"
while [ $# -gt 0 ] ; do
	case "$1" in
		-f)
			force=1   # Set the force flag
			;;
		-k)
			force=1	  
			justKill=1
			;;
		-p)
			pidfile=$2 # Set the pidfile
			shift
			;;
		-*)
			echo -e "Invalid parameter '$1'\n"
			usage
			;;
		*)
			if [ -z "$hostGroup" ] ; then
				hostGroup=$1 # First bareword is the host group, for host monitoring
			else
				if [ -n "$2" ] ; then
					srvMonCmds="${srvMonCmds}${newline}\$apm->addJobToMonitor($2, '', '${1}_Services', '$host');"
					shift
				else
					echo -e "Service '$1' needs pid number!\n"
  					usage
				fi
			fi
	esac
	shift
done

# If pidfile was given, check if there is a running process with that pid
if [ -n "$pidfile" ] ; then
	pid=`cat $pidfile 2>/dev/null`
	lines=`ps -p $pid 2>/dev/null | wc -l`
	if [ "$lines" -eq 2 ] ; then
		# there is a previous ApMon instance running
		if [ -n "$force" ] ; then
			echo "Killing previous ApMon instance with pid $pid ..."
			kill -s 1 $pid 2>/dev/null ; sleep 1
			lines=`ps -p $pid 2>/dev/null | wc -l`
			if [ "$lines" -eq 2 ] ; then
				echo "Failed killing ApMon instance with pid $pid! Trying with -9..."
				kill -s 9 $pid 2>/dev/null ; sleep 1
				lines=`ps -p $pid 2>/dev/null | wc -l`
				if [ "$lines" -eq 2 ] ; then
					echo "Failed killing -9 ApMon instance with pid $pid!!! Aborting."
					exit -1
				fi
			fi
		else
			# force flag is not set; just exit
			exit 1
		fi
	fi
fi

if [ -n "$justKill" ] ; then
	exit 0;
fi

#Set the destination for the monitoring information
#destination="\"http://monalisa2.cern.ch/~catac/apmon/destinations.conf\""
#destination="['pcardaab.cern.ch:8884']"
#destination="{'pcardaab.cern.ch' => {loglevel => 'NOTICE'}}"
MONALISA_HOST=${MONALISA_HOST:-"localhost"}
APMON_DEBUG_LEVEL=${APMON_DEBUG_LEVEL:-"WARNING"}
destination=${APMON_CONFIG:-"['$MONALISA_HOST']"}

#Finally, run the perl interpreter with a small program that sends all these parameters
exe="use strict;
use warnings;
use ApMon;
my \$apm = new ApMon(0);
\$apm->setLogLevel('$APMON_DEBUG_LEVEL');
\$apm->setDestinations($destination);
\$apm->setMonitorClusterNode('${hostGroup}_Nodes', '$host');$srvMonCmds
while(1){
  \$apm->sendBgMonitoring();
  sleep(120);
}
"

#echo "Exe = [$exe]"

export PERL5LIB=`dirname $0`

if [ -n "$pidfile" ] ; then
	# pid file given; run in background
	logfile="`dirname $pidfile`/`basename $pidfile .pid`.log"
	echo -e "`date` Starting ApMon in background mode...\nlogfile in: $logfile\npidfile in: $pidfile" | tee $logfile
	perl -e "$exe" </dev/null >> $logfile 2>&1 &
	pid=$!
	echo $pid > $pidfile
else
	# pid file not given; run in interactive mode
	echo -e "`date` Starting ApMon in interactive mode..."
	exec perl -e "$exe"
fi

