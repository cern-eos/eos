#!/usr/bin/python
# ------------------------------------------------------------------------------
# File: eosarch_run.py
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
# ------------------------------------------------------------------------------
#
# ******************************************************************************
# EOS - the CERN Disk Storage System
# Copyright (C) 2014 CERN/Switzerland
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# ******************************************************************************

""" Script used for starting an archiving transfer in a subprocess which also
    closes the open file descriptors such that there is no interference between
    the processes using ZMQ.
"""

from __future__ import print_function
import ast
import sys
import os
import logging
from errno import EIO, EINVAL
from hashlib import sha256

# Note: this is to be enabled only when we want to get the logging from the
# XrdCl - notice that this can grow very big, very fast. We also have to do
# this here before the XrdCl module gets initialised.
#os.environ['XRD_LOGLEVEL'] = "Debug"
#os.environ['XRD_LOGFILE'] = "/tmp/eosarch_xrdcl.log"

from eosarch import Transfer, NoErrorException, Configuration

try:
    config = Configuration()
except Exception as err:
    print("Configuration failed, error:{0}".format(err), file=sys.stderr)
    raise

# Set location for local transfer files
for oper in [config.GET_OP, config.PUT_OP, config.PURGE_OP, config.DELETE_OP, config.BACKUP_OP]:
    path = config.EOS_ARCHIVE_DIR + oper + '/'
    config.DIR[oper] = path

req_dict = ast.literal_eval(sys.argv[1])
src = req_dict['src']
pos = src.find("//", src.find("//") + 1) + 1
root_dir = src[pos : src.rfind('/') + 1]
uuid = sha256(root_dir).hexdigest()
log_file = config.DIR[req_dict['cmd']] + uuid + ".log"
config.start_logging("transfer", log_file, False)

try:
    tx = Transfer(req_dict, config)
except Exception as err:
    config.logger.exception(err)
    raise

try:
    tx.run()
except IOError as err:
    print("{0}".format(err), file=sys.stderr)
    tx.logger.exception(err)
    tx.tx_clean(False)
    sys.exit(EIO)
except NoErrorException as err:
    tx.tx_clean(True)
except Exception as err:
    print("{0}".format(err), file=sys.stderr)
    tx.logger.exception(err)
    tx.tx_clean(False)
    sys.exit(EINVAL)
