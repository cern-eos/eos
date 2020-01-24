#!/bin/bash

set -x

export CCACHE_DIR="`pwd`/ccache"
export CCACHE_BASEDIR="`pwd`"
export CCACHE_SLOPPINESS=pch_defines
export CCACHE_NOHASHDIR=true
export CCACHE_MAXSIZE=1.5G

ccache -z
ccache -p

set +x