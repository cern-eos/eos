#!/bin/bash
set -e

WORKDIR=$(dirname "$0")

cd ${WORKDIR}
make -f /usr/share/selinux/devel/Makefile eosfusex.pp
