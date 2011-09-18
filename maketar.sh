#!/bin/sh

# Extract package related information
specfile=`find . -maxdepth 1 -name '*.spec' -type f`
name=`awk '$1 == "Name:" { print $2 }' ${specfile}`
version=`awk '$1 == "Version:" { print $2 }' ${specfile}`
release=`awk '$1 == "Release:" { print $2 }' ${specfile}`

# Create the distribution tarball
rm -rf ${name}-${version}-${release}
rsync -aC --exclude '.__afs*' --exclude 'ApMon' --exclude '.git' . ${name}-${version}-${release}
tar -zcf ${name}-${version}-${release}.tar.gz ${name}-${version}-${release}
rm -rf ${name}-${version}-${release}
