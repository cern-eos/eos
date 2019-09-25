#!/usr/bin/env bash

#-------------------------------------------------------------------------
# File: eos-cdmi-setup.sh
# Author: Mihai Patrascoiu - CERN
#-------------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2019 CERN/Switzerland                                  *
# *                                                                      *
# * This program is free software: you can redistribute it and/or modify *
# * it under the terms of the GNU General Public License as published by *
# * the Free Software Foundation, either version 3 of the License, or    *
# * (at your option) any later version.                                  *
# *                                                                      *
# * This program is distributed in the hope that it will be useful,      *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
# * GNU General Public License for more details.                         *
# *                                                                      *
# * You should have received a copy of the GNU General Public License    *
# * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
# ************************************************************************

#------------------------------------------------------------------------------
# Description: The script produces the necessary file interface within /proc/
#              in order to facilitate CDMI metadata query of QoS classes.
#              Each file will be virtual, meaning upon an open request,
#              an associated QoS command will be executed.
#
# Context: A CDMI server, equiped with a remote-endpoint cdmi-qos-plugin
#          (in this case, the cdmi-dcache-qos plugin) would be able
#          to query EOS via the HTTP interface and gather necessary
#          metadata about the QoS classes provided.
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# Utility functions
#------------------------------------------------------------------------------

function usage() {
  filename=$(basename $0)
   echo "usage: $filename [<endpoint>] [-h|--help]"
   echo "                <endpoint> -- MGM endpoint"
   echo "                -h|--help  -- print this message"
   echo ""

   exit 1
}

function wants_help() {
  for arg in "$*"; do
    if [ "$arg" = "-h" ] || [ "$arg" = "--help" ]; then
      usage
    fi
  done
}

# Identify MGM endpoint
function identify_endpoint() {
  endpoint="root://localhost:1094/"

  if [ -n "$EOS_MGM_URL" ]; then
    endpoint=$EOS_MGM_URL
  elif [ -n "$1" ]; then
    endpoint=$1
  fi

  # Check if endpoint has URL format
  if [[ ! $endpoint =~ ^root:// ]]; then
    endpoint="root://$endpoint"

    if [[ ! $endpoint =~ .*:[0-9]+ ]]; then
      endpoint="$endpoint:1094"
    fi
  fi

  # Validate endpoint
  local regex="root://[a-zA-Z._-]+(:[0-9]+)?[/]?$"
  if [[ ! $endpoint =~ $regex ]]; then
    echo "error: invalid endpoint '$endpoint'. Must have following format: root://<endpoint>[:<port>]/"
    exit 1
  fi
}

# Identify instance name
function identify_instance() {
  if [ -n "$EOS_INSTANCE_NAME" ]; then
    instance=${EOS_INSTANCE_NAME#eos}
  else
    instance=$(eos $endpoint version | grep EOS_INSTANCE | cut -d= -f2)
    instance=${instance#eos}
  fi

  if [ -z "$instance" ]; then
    echo "error: could not identify instance name"
    exit 1
  fi
}

#------------------------------------------------------------------------------
# Global setup
#------------------------------------------------------------------------------

# Search for help request
wants_help $@

endpoint=""
identify_endpoint $*
identify_instance

export EOS_MGM_URL=$endpoint
echo "endpoint=$endpoint"
echo "instance=$instance"

set -e
eos rm -rf "/eos/$instance/proc/qos-management/" > /dev/null 2>&1
eos mkdir -p "/eos/$instance/proc/qos-management/qos/"

#-------------------------------------------------------------------------
# The virtual files need to have a 'sys.proc=<command>' extended attribute
# in order to execute a command on open.
#
# When the eos console is executed in debug mode, it prints the request
# sent to the server, which has the following format:
# root://<endpoint>//proc/user/?mgm.cmd.proto=<protob64>&<opaque>
#
# From the console printed request, the <protob64> string will be captured.
# Later on, the captured string is set as extended attribute:
# 'sys.proc="mgm.cmd.proto=<protob64>"'
#------------------------------------------------------------------------

# The pattern will start matching after it finds the "mgm.cmd.proto=" delimiter.
# It will lazily match everything until the end delimiter: & or end of line
grep_pattern="(?<=mgm.cmd.proto=).*?(?=&|$)"

for entry in "file" "directory"; do
  protob64=$(EOS_CONSOLE_DEBUG=1 eos -j $endpoint qos list | grep -Po $grep_pattern)

  eos touch "/eos/$instance/proc/qos-management/qos/$entry.api"
  eos attr set "sys.proc=mgm.cmd.proto=$protob64" "/eos/$instance/proc/qos-management/qos/$entry.api"

  eos mkdir "/eos/$instance/proc/qos-management/qos/$entry/"

  eos -j qos list | jq -r '.[]' | while read qos_class ; do
    protob64=$(EOS_CONSOLE_DEBUG=1 eos -j $endpoint qos list $qos_class | grep -Po $grep_pattern)

    eos touch "/eos/$instance/proc/qos-management/qos/$entry/$qos_class.api"
    eos attr set "sys.proc=mgm.cmd.proto=$protob64" "/eos/$instance/proc/qos-management/qos/$entry/$qos_class.api"
  done
done
