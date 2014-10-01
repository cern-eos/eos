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

import os
import zmq
import ast
import sys
import json
import logging
import logging.handlers
from hashlib import sha256
from eosarch import Transfer, NoErrorException, Configuration

try:
    config = Configuration()
except Exception as err:
    print >> sys.stderr, "Configuration failed, error:{0}".format(err)
    raise

config.DIR = {}

# Set location for local transfer files
for oper in [config.GET_OP, config.PUT_OP, config.PURGE_OP, config.DELETE_OP]:
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
    print "{0}".format(err)
    tx.logger.exception(err)
    tx.clean_transfer(False)
    sys.exit(EIO)
except NoErrorException as err:
    tx.clean_transfer(True)
except Exception as err:
    print "{0}".format(err)
    tx.logger.exception(err)
    tx.clean_transfer(False)
    sys.exit(EINVAL)
