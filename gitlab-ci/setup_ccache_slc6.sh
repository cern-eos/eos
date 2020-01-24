#!/bin/bash

set -x

export CCACHE_DIR="`pwd`/ccache"
export CCACHE_BASEDIR="`rpm --eval %_builddir`/`ls build/SRPMS/*.src.rpm | awk -F '.' 'BEGIN{OFS=".";} {NF-=3;}1' | awk -F '/' '{print $NF}'`"
export CCACHE_SLOPPINESS=pch_defines

ccache -z
ccache -p

set +x