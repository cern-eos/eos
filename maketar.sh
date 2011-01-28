#!/bin/sh

# Extract package related information
specfile=`find . -maxdepth 1 -name '*.spec' -type f`
name=`awk '$1 == "Name:" { print $2 }' ${specfile}`
version=`awk '$1 == "Version:" { print $2 }' ${specfile}`

# Create the distribution tarball
rm -rf ${name}-${version}
rsync -aC --exclude '.__afs*' --exclude 'fuse' --exclude 'ApMon' . ${name}-${version}
tar -zcf ${name}-${version}.tar.gz ${name}-${version}
rm -rf ${name}-${version}
