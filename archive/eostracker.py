#!/usr/bin/python

#-------------------------------------------------------------------------------
# File: eostracker.py
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
#-------------------------------------------------------------------------------
#
#*******************************************************************************
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
#*******************************************************************************

import json
import zmq
import const
import logging
import glob
import stat
import exceptions
import subprocess
from logging import handlers
from multiprocessing import Process
from sys import exit, getsizeof
from os import mkdir, makedirs, chmod
from os.path import join
from hashlib import sha256
from archivefile import ArchiveFile, NoErrorException
from errno import EIO, EINVAL
from veryprettytable import VeryPrettyTable


# Constants
const.BATCH_SIZE = 5       # max number of transfers to be performed in parallel
const.POLLTIMEOUT = 1000   # miliseconds
const.CREATE_OP = "create"
const.STAGE_OP = 'stage'   
const.MIGRATE_OP = 'migrate'
const.LIST_OP = 'list'
const.PURGE_OP = 'purge'
const.DELETE_OP = 'delete'
const.OPT_RETRY = 'retry'
const.ARCH_FN = "archive"
const.LOG_DIR = "/var/log/eos/archive/"
const.DIR = { const.STAGE_OP :   "/tmp/stage/",  
              const.MIGRATE_OP : "/tmp/mig/",
              const.PURGE_OP :   "/tmp/purge/"}
const.MAX_PENDING = 10 # max number of requests allowed in pending
const.IPC_FILE = "/tmp/archivebackend.ipc"
const.LOG_FORMAT = '%(asctime)-15s %(name)-10s %(levelname)s %(message)s'

# TODO: maybe parse the xrd.cf.mgm to get the configuration info

def do_transfer(eos_file, op, opt):
  """ Execute a transfer job.
  
  Args:
    eos_file (string): EOS location of the archive file 
    op (string): operation type: stage/migrate
    opt (string): option for the transfer: recover/purge
  """
  arch = ArchiveFile(eosf = eos_file, 
                     operation = op,
                     option = opt)

  try:
    arch.run()
  except IOError as e:
    logger = logging.getLogger(__name__)
    logger.error(e)
    arch.clean_transfer(False)
    exit(EIO)
  except NoErrorException as e:
    arch.clean_transfer(True)
  except Exception as e:
    logger = logging.getLogger(__name__)
    logger.error(e)
    arch.clean_transfer(False)
    exit(EINVAL)


class Dispatcher(object):
  """ Dispatcher daemon responsible for receiving requests from the clients
  and then spawning the proper executing process to stage, migrate or purge.
      
  Attributes:
    proc (dict): Dictionary containing the currently running processes for 
      both staging and migration.
    pending (dict): Dictionary containing the currently pending requests for
      both staging and migration.
    max_proc (int): Max number of concurrent proceeses of one type allowed.
  """
  def __init__(self):
    self.logger = logging.getLogger(type(self).__name__)
    log_file = const.LOG_DIR + "eostracker.log"
    formatter = logging.Formatter(const.LOG_FORMAT)
    rotate_handler = logging.handlers.TimedRotatingFileHandler(log_file, 'midnight')
    rotate_handler.setFormatter(formatter)
    self.logger.addHandler(rotate_handler)
    self.logger.propagate = False
        
    self.proc = { const.STAGE_OP :  {}, 
                  const.MIGRATE_OP: {},
                  const.PURGE_OP:   {} } 
    self.pending = { const.STAGE_OP:   {}, 
                     const.MIGRATE_OP: {},
                     const.PURGE_OP:   {} }


  def run(self):
    """ Server entry point which is responsible for spawning worker proceesses
    that do the actual transfers (staging/migration).
    """
    ctx = zmq.Context()
    self.logger.debug("Started dispatcher process")
    socket = ctx.socket(zmq.REP)
    socket.bind("ipc://" + const.IPC_FILE)
 
    try:
      chmod(const.IPC_FILE, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    except OSError as e:
      self.logger.error("Could not set permissions on IPC socket file:{0}".
                        format(const.IPC_FILE))
      raise
    
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    
    # TODO: attach orphaned processes and put them in a special list so that 
    # one can monitor them in case the tracker is restarted
    
    while True:
      events = dict(poller.poll(const.POLLTIMEOUT))
      
      # Update worker processes status
      for op, dict_jobs in self.proc.items():
        remove_elem = []
        for uuid, proc in dict_jobs.items():
          if not proc.is_alive():
            proc.join(1)
            self.logger.debug("Job:{0}, pid:{1}, exitcode:{2}".
                              format(uuid, proc.pid, proc.exitcode))
            remove_elem.append(uuid)
          
        for uuid in remove_elem:
          try:
            del self.proc[op][uuid]
          except ValueError as e:
            self.logger.error("Unable to remove job:{0} from list".format(uuid))
            
        del remove_elem[:]
      
      # Submit any pending jobs
      self.submit_pending()
      
      if events and events.get(socket) == zmq.POLLIN:
        try:
          req_json = socket.recv_json()
        except zmq.ZMQError as e:
          if e.errno == zmq.ETERM:
            break # shutting down, exit
          else:
            raise
        except ValueError as e:
          self.logger.error("Command in not in JSON format")
          socket.send("ERROR error:command not in JSON format")
          continue

        self.logger.debug("Received command: {0}".format(req_json))
        op = req_json['cmd']
        
        if (op == const.MIGRATE_OP or 
            op == const.STAGE_OP or 
            op == const.PURGE_OP):
          src, opt = req_json['src'], req_json['opt']
          root_src = src[:-(len(src) - src.rfind('/') - 1)]
          job_uuid = sha256(root_src).hexdigest()
           
          if job_uuid in self.proc[op]:
            self.logger.error("Transfer with same signature already exists");
            socket.send("ERROR error: transfer with same signature exists")
            continue
          
          if job_uuid not in self.pending[op]:
            if len(self.pending[op]) <= const.MAX_PENDING:
              self.pending[op][job_uuid] = (src, op, opt)
              self.submit_pending()
              socket.send("OK id=" + job_uuid)
            else:
              socket.send("ERROR too many pending requests, resubmit later")
          else:
            socket.send("ERROR error:transfer already queued")

        elif op == const.LIST_OP:
          reply = self.do_list(req_json)
          socket.send_string(reply)
        
        else:
          # Operation not supported reply to client with error
          self.logger.debug("ERROR operation not supported: {0}".format(msg))
          socket.send("ERROR error:operation not supported")


  def submit_pending(self):
    """ Submit as many pending requests as possible if there are enough available 
    workers to process them. 
    """
    for op, dict_jobs in self.pending.items():
      if dict_jobs:
        remove_elem = []
        num_proc = len([_ for _, proc in self.proc[op] if proc.is_alive()])
        self.logger.debug("Num. running processes: {0}".format(num_proc))
      
        for job_uuid, req_tuple in dict_jobs.items():
          if num_proc < const.BATCH_SIZE:
            self.logger.info("Pending request is submitted ...")
            proc = Process(target = do_transfer, args = (req_tuple))
            self.proc[op][job_uuid] = proc
            proc.start()
            remove_elem.append(job_uuid)
            num_proc += 1
          else:
            self.logger.debug("No more workers available")
            break 

        for key in remove_elem:
          del self.pending[op][key]
        
        del remove_elem[:]
        
  
  def do_list(self, req_json):
    """ List the transfers.

    Args:
      req_json (JSON command): Listing command in JSON format.

    Returns:
      String with the result of the listing to be returned to the client.
    """
    msg = "OK "
    ls_type = req_json['opt']
    self.logger.debug("Listing type: {0}".format(ls_type))
    table = VeryPrettyTable()
    table.field_names = ["Id", "Path", "Type", "State", "Message"]

    if (ls_type == const.MIGRATE_OP or 
        ls_type == const.STAGE_OP or 
        ls_type == const.PURGE_OP):

      for uuid in self.proc[ls_type]:
        path = self.proc[ls_type][uuid]._args[0]
        path = path[path.rfind("//") + 1 : path.rfind("/")]
        ps_file ="".join([const.DIR[ls_type], uuid, ".ps"])
        ps_proc = subprocess.Popen(['tail', '-1', ps_file], 
                                   stdout = subprocess.PIPE, 
                                   stderr = subprocess.PIPE)
        ps_out, ps_err = ps_proc.communicate()
        ps_out = ps_out.strip('\0')
        table.add_row([uuid, path, ls_type, "running", ps_out])

      for uuid in self.pending[ls_type]:
        path = self.pending[ls_type][uuid][0]
        path = path[path.rfind("//") + 1 : path.rfind("/")]
        table.add_row([uuid, path, ls_type, "pending", "none"])

    elif ls_type == "all":
      for ls_type in self.proc:
        for uuid in self.proc[ls_type]:
          path = self.proc[ls_type][uuid]._args[0]
          path = path[path.rfind("//") + 1 : path.rfind("/")]
          ps_file ="".join([const.DIR[ls_type], uuid, ".ps"])
          ps_proc = subprocess.Popen(['tail', '-1', ps_file], 
                                     stdout = subprocess.PIPE, 
                                     stderr = subprocess.PIPE)
          ps_out, ps_err = ps_proc.communicate()
          ps_out = ps_out.strip('\0')
          table.add_row([uuid, path, ls_type, "running", ps_out])
            
      for ls_type in self.pending:
        for uuid in self.pending[ls_type]:
          path = self.pending[ls_type][uuid][0]
          path = path[path.rfind("//") + 1 : path.rfind("/")]
          table.add_row([uuid, path, ls_type, "pending", "none"])
    else:
      # TODO ls_typ can be a job_uuid then only return the status 
      # of the requested job
      msg = "ERROR error:unsupported operation: {0}".format(ls_type)
      
    msg += table.get_string()
    return msg


def main():
  # Create log directory
  try:
    makedirs(const.LOG_DIR)
  except OSError as e:
    pass 

  logging.basicConfig(level=logging.DEBUG, format=const.LOG_FORMAT)
  logger = logging.getLogger(__name__)

  # Create the local directory structure
  for op in [const.MIGRATE_OP, const.STAGE_OP, const.PURGE_OP]:
    try:
      mkdir(const.DIR[op])
    except OSError as e:
      pass

  dispatcher = Dispatcher()
  dispatcher.run()

if __name__ == '__main__':
  main()


