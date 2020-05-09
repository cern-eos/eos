#!/usr/bin/env sh

# Decide which xrootd binary to use
XROOTD_BINARY=/usr/bin/xrootd

if [ -x /opt/eos/xrootd/bin/xrootd ]; then
  XROOTD_BINARY=/opt/eos/xrootd/bin/xrootd
fi

echo "Using xrootd binary: ${XROOTD_BINARY}"
exec ${XROOTD_BINARY} "$@"

