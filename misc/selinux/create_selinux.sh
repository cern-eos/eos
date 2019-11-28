#!/bin/bash
set -e

####################################
# Create the EOS fuse SELinux policy
####################################

wdir=$(dirname $0)
checkmodule -M -m -o eosfuse.mod ${wdir}/eosfuse.te > /dev/null

# Package selinux policy
semodule_package -o eosfuse.pp -m eosfuse.mod

# Test the generated eosfuse.pp
semodule -i eosfuse.pp

