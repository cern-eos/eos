#!/bin/bash
# ----------------------------------------------------------------------
# File: eos_start_pre.sh
# Author: Ivan Arizanovic - ComTrade Solutions Engineering
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2016 CERN/Switzerland                                  *
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

. /etc/sysconfig/eos_env

# Start All EOS daemons (all required daemons from config file)
if [ "$1" = "eos-all" ]; then
  if [[ -z "$XRD_ROLES" ]]; then
    echo "<3>Error: No XRD_ROLES variable declared in \"/etc/sysconf/eos_env\""
    exit 1
  fi

  for i in ${XRD_ROLES}; do
    systemctl start eos@${i} &
  done

  # Wait for all the daemons to start
  FAIL=0
  for job in `jobs -p`;do
    echo "<5>Waiting for $job ..."
    wait $job || let "FAIL+=1"
  done

  if [ "$FAIL" == "0" ]; then
    exit 0
  else
    exit 1
  fi
fi

# StartPre EOS daemons
if [ "$1" = "eos-start-pre" ]; then
  if [[ "$XRD_ROLES" == *"$2"* ]]; then
    if [ -e /etc/eos.keytab ]; then
      chown daemon /etc/eos.keytab
      chmod 400 /etc/eos.keytab
    fi
    mkdir -p /var/eos/md /var/eos/config /var/eos/report
    chmod 700 /var/eos/md /var/eos/config
    chmod 755 /var/eos /var/eos/report
    mkdir -p /var/spool/eos/core/${2} /var/spool/eos/admin
    mkdir -p /var/log/eos /var/eos/config/${EOS_MGM_HOST}
    touch /var/eos/config/${EOS_MGM_HOST}/default.eoscf
    chown -R daemon /var/spool/eos
    find /var/log/eos -maxdepth 1 -type d -exec chown daemon {} \;
    find /var/eos/ -maxdepth 1 -mindepth 1 -not -path "/var/eos/fs" -not -path "/var/eos/fusex" -type d -exec chown -R daemon {} \;
    chmod -R 775 /var/spool/eos
    mkdir -p /var/eos/auth /var/eos/stage
    chown daemon /var/eos/auth /var/eos/stage
    setfacl -m default:u:daemon:r /var/eos/auth/

    # Require cmsd for fed daemon
    if [ "$2" = "fed" ]; then
      systemctl start cmsd@clustered
    fi
  else
    echo "<3>Error: Service $2 not in the XRD_ROLES in \"/etc/sysconf/eos_env\""
    exit 1
  fi
fi

# Stop EOS daemons
if [ "$1" = "eos-stop" ]; then
  if [ "$2" = "fed" ]; then
    systemctl stop cmsd@clustered
  fi
fi

# Start EOS Master
if [ "$1" = "eos-master" ]; then
  if [[ "$XRD_ROLES" == *"mq"* ]]; then
    touch /var/eos/eos.mq.master
  fi

  if [[ "$XRD_ROLES" == *"mgm"* ]]; then
    touch /var/eos/eos.mgm.rw
  fi
fi

# Start EOS Slave
if [ "$1" = "eos-slave" ]; then
  if [[ "$XRD_ROLES" == *"mq"* ]]; then
    unlink /var/eos/eos.mq.master
  fi

  if [[ "$XRD_ROLES" == *"mgm"* ]]; then
    unlink /var/eos/eos.mgm.rw
  fi
fi

# Start EOS fuse daemons
if [ "$1" = "eosd-start" ]; then
  mkdir -p /var/run/eosd/ /var/run/eosd/credentials/store ${EOS_FUSE_MOUNTDIR}
  chmod 1777 /var/run/eosd/credentials /var/run/eosd/credentials/store
  chmod 755 ${EOS_FUSE_MOUNTDIR}
fi

# Stop EOS fuse daemons
if [ "$1" = "eosd-stop" ]; then
  umount -f ${EOS_FUSE_MOUNTDIR}
fi

# Start EOS High Availability
if [ "$1" = "eosha-start" ]; then
  systemctl start eossync
  mkdir -p /var/log/eos/eosha
  chown daemon:daemon /var/log/eos/eosha
fi

# Stop EOS High Availability
if [ "$1" = "eosha-stop" ]; then
  systemctl stop eossync@*
fi
