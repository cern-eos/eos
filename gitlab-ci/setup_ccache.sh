#!/bin/bash

ccache --max-size=1.5G

export CCACHE_DIR="`pwd`/ccache"
export CCACHE_BASEDIR="`rpm --eval %_builddir`/`ls SRPMS/*.src.rpm | awk -F '.' 'BEGIN{OFS=".";} {NF-=4;}1' | awk -F '/' '{print $NF}'`"
export CCACHE_SLOPPINESS=pch_defines

ccache -s
ccache -p
