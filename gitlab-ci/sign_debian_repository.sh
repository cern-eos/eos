#!/bin/bash

#-------------------------------------------------------------------------------
# Publish debian artifacts on CERN Gitlab CI
# Author: Jozsef Makai <jmakai@cern.ch> (11.08.2017)
#-------------------------------------------------------------------------------

set -e

prefix=$1
dist=$2

components=$(find $prefix/dists/$dist/ -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | tr '\n' ' ')

# This thing can fail for the first time
set +e

rm $prefix/dists/$dist/Release $prefix/dists/$dist/InRelease $prefix/dists/$dist/Release.gpg

set -e

apt-ftparchive -o APT::FTPArchive::Release::Origin=CERN -o APT::FTPArchive::Release::Label=EOS -o APT::FTPArchive::Release::Codename=artful -o APT::FTPArchive::Release::Architectures=amd64 -o APT::FTPArchive::Release::Components="$components" release $prefix/dists/$dist/ > $prefix/dists/$dist/Release
gpg --homedir /home/stci/ --clearsign -o $prefix/dists/$dist/InRelease $prefix/dists/$dist/Release
gpg --homedir /home/stci/ -abs -o $prefix/dists/$dist/Release.gpg $prefix/dists/$dist/Release
