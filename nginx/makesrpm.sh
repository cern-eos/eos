#!/bin/bash
#-------------------------------------------------------------------------------
# Build an RPM for nginx, customised for EOS
# Author: Justin Salmon <jsalmon@cern.ch> (25.07.2013)
#-------------------------------------------------------------------------------

SOURCEPATH="."
OUTPUTPATH="."

#-------------------------------------------------------------------------------
# Create a tempdir and copy the files there
#-------------------------------------------------------------------------------
# exit on any error
set -e

TEMPDIR=`mktemp -d /tmp/nginx.srpm.XXXXXXXXXX`
RPMSOURCES=$TEMPDIR/rpmbuild/SOURCES
mkdir -p $RPMSOURCES
mkdir -p $TEMPDIR/rpmbuild/SRPMS

echo "[i] Working in: $TEMPDIR" 1>&2

cp etc/init.d/*      $RPMSOURCES
cp etc/logrotate.d/* $RPMSOURCES
cp etc/nginx/*       $RPMSOURCES
cp etc/sysconfig/*   $RPMSOURCES
cp ./*.patch         $RPMSOURCES
cp ./nginx.spec      $TEMPDIR

#-------------------------------------------------------------------------------
# Download the source tarball
#-------------------------------------------------------------------------------
# no more exiting on error
set +e

spectool --get-files --directory $RPMSOURCES nginx.spec

#-------------------------------------------------------------------------------
# Build the source RPM
#-------------------------------------------------------------------------------
echo "[i] Creating the source RPM..."

# Dirty, dirty hack!
echo "%_sourcedir $RPMSOURCES" >> $TEMPDIR/rpmmacros
rpmbuild --define "_topdir $TEMPDIR/rpmbuild"    \
         --define "%_sourcedir $RPMSOURCES"      \
         --define "%_srcrpmdir %{_topdir}/SRPMS" \
         --define "_source_filedigest_algorithm md5" \
         --define "_binary_filedigest_algorithm md5" \
-bs $TEMPDIR/nginx.spec > $TEMPDIR/log
if test $? -ne 0; then
  echo "[!] RPM creation failed" 1>&2
  exit 1
fi

cp $TEMPDIR/rpmbuild/SRPMS/nginx*.src.rpm $OUTPUTPATH
rm -rf $TEMPDIR

echo "[i] Done."