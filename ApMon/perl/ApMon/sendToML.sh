#!/bin/bash

# Sample shell script that sends the given parameters to the ML service 
# running on the same machine using ApMon.
#
# 2007-04-03
# Catalin.Cirstoiu@cern.ch

if [ $# -lt 2 ] ; then
	cat <<EOM
usage: send_params.sh cluster node [param1 value1] [param2 value2] .. [paramN valueN]
	cluster - cluster name as it will appear in the destination's farm configuraion
	node - node name as it will appear in the destination's farm configuration
	paramX - parameter X's name
	valueX - parameter X's value
EOM
	exit 1
fi

#First, concatenate all given parameters in a comma-sepparated list
params="\"$1\", \"$2\""
shift; shift
until [ -z "$1" -a -z "$2" ]
do
  params="$params, \"$1\", $2"
  shift; shift
done

#echo "Params = [$params]"

#Then, choose a destination
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
\$apm->sendParameters($params);"

#echo "Exe = [$exe]"

export PERL5LIB=`dirname $0`
echo $exe | perl

