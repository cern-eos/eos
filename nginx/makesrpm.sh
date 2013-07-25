#!/bin/bash
#-------------------------------------------------------------------------------
# Build an RPM for nginx, customised for EOS
# Author: Justin Salmon <jsalmon@cern.ch> (25.07.2013)
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# Print help
#-------------------------------------------------------------------------------
function printHelp()
{
  echo "Usage:"                                              1>&2
  echo "${0} [--help] [--source PATH] [--output PATH]"       1>&2
  echo "  --help        prints this message"                 1>&2
  echo "  --source PATH specify the root of the source tree" 1>&2
  echo "                defaults to ."                       1>&2
  echo "  --output PATH the directory where the source rpm"  1>&2
  echo "                should be stored, defaulting to ."   1>&2
}

#-------------------------------------------------------------------------------
# Parse the commandline
#-------------------------------------------------------------------------------
SOURCEPATH="."
OUTPUTPATH="."
PRINTHELP=0

while test ${#} -ne 0; do
  if test x${1} = x--help; then
    PRINTHELP=1
  elif test x${1} = x--source; then
    if test ${#} -lt 2; then
      echo "--source parameter needs an argument" 1>&2
      exit 1
    fi
    SOURCEPATH=${2}
    shift
  elif test x${1} = x--output; then
    if test ${#} -lt 2; then
      echo "--output parameter needs an argument" 1>&2
      exit 1
    fi
    OUTPUTPATH=${2}
    shift
  fi
  shift
done

if test $PRINTHELP -eq 1; then
  printHelp
  exit 0
fi

echo "[i] Working on: $SOURCEPATH"
echo "[i] Storing the output to: $OUTPUTPATH"

#-------------------------------------------------------------------------------
# Create a tempdir and copy the files there
#-------------------------------------------------------------------------------
# exit on any error
set -e

TEMPDIR=`mktemp -d /tmp/eos-nginx.srpm.XXXXXXXXXX`
RPMSOURCES=$TEMPDIR/rpmbuild/SOURCES
mkdir -p $RPMSOURCES
mkdir -p $TEMPDIR/rpmbuild/SRPMS

echo "[i] Working in: $TEMPDIR" 1>&2

cp $SOURCEPATH/etc/init.d/*      $RPMSOURCES
cp $SOURCEPATH/etc/logrotate.d/* $RPMSOURCES
cp $SOURCEPATH/etc/nginx/*       $RPMSOURCES
cp $SOURCEPATH/etc/sysconfig/*   $RPMSOURCES
cp $SOURCEPATH/*.patch           $RPMSOURCES
cp $SOURCEPATH/nginx.spec        $TEMPDIR

#-------------------------------------------------------------------------------
# Download the source tarball
#-------------------------------------------------------------------------------
# no more exiting on error
set +e

echo "[i] Downloading nginx source..."
VERSION=`cat nginx/nginx.spec | \
         egrep -e 'Version:\s+([0-9\.]*)' -o | \
         egrep -e '[0-9\.]*' -o`
curl -sS http://nginx.org/download/nginx-$VERSION.tar.gz -O
mv nginx-$VERSION.tar.gz $RPMSOURCES/

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

cp $TEMPDIR/rpmbuild/SRPMS/eos-nginx*.src.rpm $OUTPUTPATH
rm -rf $TEMPDIR

echo "[i] Done."