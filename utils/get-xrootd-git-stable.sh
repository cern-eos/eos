#!/bin/bash
if [ -e xrootd ]; then
    cd xrootd
    git pull
else
    git clone -b stable-3.3.x http://xrootd.cern.ch/repos/xrootd.git xrootd;
    cd xrootd/src
    mkdir include/
    ln -s ../../src/ include/xrootd
fi
