#! /usr/bin/python
# ------------------------------------------------------------------------------
# File: eosarchiverd
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

"""Module running the eosarchiverd daemon which transfers files between EOS
   and CASTOR.
"""

import os
import sys
import json
import zmq
import glob
import stat
import subprocess
import daemon
import logging
import logging.handlers
from hashlib import sha256
from multiprocessing import Process
from eosarch import NoErrorException, Transfer, const
from errno import EIO, EINVAL


# Constants
const.BATCH_SIZE = 10  # max number of transfers to be performed by one thread
const.MAX_THREADS = 5  # max number of threads used per transfer process
const.POLL_TIMEOUT = 1000  # miliseconds
const.JOIN_TIMEOUT = 1  # join timeout for running threads (sec)
const.MAX_PENDING = 10  # max number of requests allowed in pending
const.CREATE_OP = 'create'
const.GET_OP = 'get'
const.PUT_OP = 'put'
const.LIST_OP = 'list'
const.PURGE_OP = 'purge'
const.DELETE_OP = 'delete'
const.KILL_OP = 'kill'
const.OPT_RETRY = 'retry'
const.ARCH_FN = ".archive"
const.ARCH_INIT = ".archive.init"
const.LOG_FORMAT = ('%(asctime)-15s %(name)s[%(process)d] %(filename)s:'
                    '%(lineno)d:LVL=%(levelname)s %(message)s')

# Map string log level to Python log level
const.LOG_DICT = {"debug": logging.DEBUG,
                  "notice": logging.INFO,
                  "info": logging.INFO,
                  "warning": logging.WARNING,
                  "error": logging.ERROR,
                  "crit": logging.CRITICAL,
                  "alert": logging.CRITICAL}

# Get environment variables and setup constants based on them
try:
    LOG_DIR = os.environ["LOG_DIR"]
except KeyError as err:
    print >> sys.stderr, "LOG_DIR env. not found"
    raise

try:
    EOS_ARCHIVE_DIR = os.environ["EOS_ARCHIVE_DIR"]
except KeyError as err:
    print >> sys.stderr, "EOS_ARCHIVE_DIR env. not found"
    raise

def do_transfer(eos_file, oper, opt):
    """ Execute a transfer job.

    Args:
        eos_file (string): EOS location of the archive file
        oper     (string): Operation type: get/put
        opt      (string): Option for the transfer: recover/purge
    """
    tx = Transfer(eosf=eos_file, operation=oper, option=opt)

    try:
        tx.run()
    except IOError as err:
        tx.logger.exception(err)
        tx.clean_transfer(False)
        sys.exit(EIO)
    except NoErrorException as err:
        tx.clean_transfer(True)
    except Exception as err:
        tx.logger.exception(err)
        tx.clean_transfer(False)
        sys.exit(EINVAL)


class Dispatcher(object):
    """ Dispatcher daemon responsible for receiving requests from the clients
    and then spawning the proper executing process to get, put or purge.

    Attributes:
        proc (dict): Dictionary containing the currently running processes for
            both put and get.
        pending (dict): Dictionary containing the currently pending requests for
            both put and get.
        max_proc (int): Max number of concurrent proceeses of one type allowed.
    """
    def __init__(self):
        self.logger = logging.getLogger(type(self).__name__)
        log_file = const.LOG_FILE
        formatter = logging.Formatter(const.LOG_FORMAT)
        rotate_handler = logging.handlers.TimedRotatingFileHandler(log_file, 'midnight')
        rotate_handler.setFormatter(formatter)
        self.logger.addHandler(rotate_handler)
        self.logger.propagate = False
        self.proc, self.pending, self.orphan = {}, {}, {}

        # Initialize the process dictionaries
        for op_type in [const.GET_OP, const.PUT_OP, const.PURGE_OP, const.DELETE_OP]:
            self.proc[op_type] = {}
            self.pending[op_type] = {}
            self.orphan[op_type] = {}

    def run(self):
        """ Server entry point which is responsible for spawning worker proceesses
        that do the actual transfers (put/get).
        """
        ctx = zmq.Context()
        self.logger.info("Started dispatcher process")
        socket = ctx.socket(zmq.REP)
        socket.bind("ipc://" + const.IPC_FILE)

        try:
            os.chmod(const.IPC_FILE, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        except OSError as err:
            self.logger.error("Could not set permissions on IPC socket file={0}".
                              format(const.IPC_FILE))
            raise

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        # Attach orphaned processes and put them in a special list so that
        # one can monitor them in case the tracker is restarted
        self.get_orphans()

        while True:
            events = dict(poller.poll(const.POLL_TIMEOUT))

            # Update worker processes status
            for oper, dict_jobs in self.proc.iteritems():
                remove_elem = []
                for uuid, proc in dict_jobs.iteritems():
                    if not proc.is_alive():
                        proc.join()
                        self.logger.info("Job={0}, pid={1}, exitcode={2}".
                                          format(uuid, proc.pid, proc.exitcode))
                        remove_elem.append(uuid)

                for uuid in remove_elem:
                    try:
                        del self.proc[oper][uuid]
                    except ValueError as err:
                        self.logger.error(("Unable to remove job={0} from list"
                                           "").format(uuid))

                del remove_elem[:]

            # Update orphan worker processes status
            for oper, dict_jobs in self.orphan.iteritems():
                remove_elem = []
                for uuid, info_pair in dict_jobs.iteritems():
                    try:
                        os.kill(int(info_pair[0]), 0)
                    except OSError as err:
                        self.logger.info("Job={0}, pid={1}, path={2} finshed, error={3}".
                                          format(uuid, info_pair[0], info_pair[1], err))
                        remove_elem.append(uuid)
                    except ValueError as err:
                        pass  # string is not an int value

                for uuid in remove_elem:
                    try:
                        del self.orphan[oper][uuid]
                    except ValueError as err:
                        self.logger.error(("Unable to remove job={0} from orphan"
                                          " list").format(uuid))
                del remove_elem[:]

            # Submit any pending jobs
            self.submit_pending()

            if events and events.get(socket) == zmq.POLLIN:
                try:
                    req_json = socket.recv_json()
                except zmq.ZMQError as err:
                    if err.errno == zmq.ETERM:
                        break  # shutting down, exit
                    else:
                        raise
                except ValueError as err:
                    self.logger.error("Command in not in JSON format")
                    socket.send("ERROR error:command not in JSON format")
                    continue

                self.logger.debug("Received command: {0}".format(req_json))
                oper = req_json['cmd']

                if oper in self.proc:  # Get, put, purge or delete operation
                    src, opt = req_json['src'], req_json['opt']
                    # Extract the archive root directory path
                    pos = src.find("//", src.find("//") + 1) + 1;
                    root_src = src[pos : src.rfind('/') + 1]
                    job_uuid = sha256(root_src).hexdigest()
                    self.logger.debug("Adding job={0}, path={1}".format(
                            job_uuid, root_src))

                    if (job_uuid in self.proc[oper] or
                        job_uuid in self.orphan[oper] or
                        job_uuid in self.pending[oper]):
                        self.logger.error("Transfer with same signature already exists")
                        socket.send("ERROR error: transfer with same signature exists")
                        continue

                    if len(self.pending[oper]) <= const.MAX_PENDING:
                        self.pending[oper][job_uuid] = (src, oper, opt)
                        self.submit_pending()
                        socket.send("OK Id=" + job_uuid)
                    else:
                        socket.send("ERROR too many pending requests, resubmit later")
                elif oper == const.LIST_OP:  # List
                    reply = self.do_list(req_json)
                    socket.send_string(reply)
                elif oper == const.KILL_OP:  # Kill transfer
                    reply = self.do_kill(req_json)
                    socket.send_string(reply)
                else:
                    # Operation not supported reply to client with error
                    self.logger.debug("ERROR operation not supported: {0}".format(oper))
                    socket.send("ERROR error:operation not supported")

    def submit_pending(self):
        """ Submit as many pending requests as possible if there are enough available
        workers to process them.
        """
        for oper, dict_jobs in self.pending.iteritems():
            if dict_jobs:
                remove_elem = []
                num_proc = len(self.proc[oper])
                self.logger.debug("Num. running processes={0}".format(num_proc))

                for job_uuid, req_tuple in dict_jobs.iteritems():
                    if num_proc < const.BATCH_SIZE:
                        self.logger.info(("Pending job={0} is submitted ..."
                                          "").format(job_uuid))
                        proc = Process(target=do_transfer, args=(req_tuple))
                        self.proc[oper][job_uuid] = proc
                        proc.start()
                        remove_elem.append(job_uuid)
                        num_proc += 1
                    else:
                        self.logger.warning("No more workers available")
                        break

                for key in remove_elem:
                    del self.pending[oper][key]

                del remove_elem[:]

    def get_orphans(self):
        """ Get the running proceesses so that we can monitor their evolution.
        We do this by listing all the status files in /var/eos/archive/*/*.ps
        """
        list_ps = glob.glob(EOS_ARCHIVE_DIR + "*/*.ps")

        if list_ps:
            for ps_file in list_ps:
                self.logger.debug("ps file={0}".format(ps_file))
                for key, val in const.DIR.iteritems():
                    if val in ps_file:
                        # Get the uuid of the job
                        uuid = ps_file[ps_file.rfind('/') + 1: ps_file.rfind('.')]
                        # Get the pid which is running the job
                        ps_proc = subprocess.Popen(['head', '-1', ps_file],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
                        ps_out, _ = ps_proc.communicate()
                        ps_out = ps_out.strip('\0\n')
                        tx_file = ps_file[:-3] + '.tx'
                        log_file = ps_file[:-3] + '.log'
                        pid = ps_out[ps_out.find("pid=") + 4:]
                        # Check if process is still alive
                        try:
                            os.kill(int(pid), 0)
                        except OSError as err:
                            err_msg = ("Uuid={0}, pid={1} is no longer alive "
                                       "msg={2}").format(uuid, pid, err)
                            self.logger.error(err_msg)
                            # Delete all files associated to this transfer
                            try:
                                os.remove(ps_file)
                            except OSError as __:
                                pass

                            try:
                                os.remove(tx_file)
                            except OSError as __:
                                pass

                            try:
                                os.remove(log_file)
                            except OSError as __:
                                pass
                        except ValueError as err:
                            # pid is not an int value
                            crit_msg = ("Could not read pid from file={0}, please "
                                        "check ongoing transfers before restarting "
                                        "- refuse to start because of risk of data "
                                        "corruption").format(ps_file)
                            self.logger.critical(crit_msg)
                            self.logger.critical("ps_out={0}".format(ps_out))
                            raise
                        else:
                            # Read the path from the transfer file
                            try:
                                with open(tx_file, 'r') as filed:
                                    header = json.loads(filed.readline())
                                    path = header['src']
                                    self.orphan[key][uuid] = (pid, path)
                                    self.logger.debug(("op={0}, uuid={1}, pid={2}"
                                                       "").format(key, uuid, pid))
                            except IOError as __:
                                self.orphan[key][uuid] = (pid, "...")
                                self.logger.debug(("op={0}, uuid={1}, pid={2}"
                                                   "").format(key, uuid, pid))

    def do_list(self, req_json):
        """ List the transfers.

        Args:
            req_json (JSON command): Listing command in JSON format.

        Returns:
            String with the result of the listing to be returned to the client.
        """
        msg = "OK "
        ls_type = req_json['opt']
        self.logger.debug("Listing type={0}".format(ls_type))
        row_data = []
        search_uuid = ''

        if ls_type == "all":
            op_list = self.proc.keys()
        elif ls_type in self.proc:
            op_list = [ls_type]
        else:
            search_uuid = ls_type
            op_list = self.proc.keys()

        for ls_type in op_list:
            for uuid in self.proc[ls_type]:
                if search_uuid and search_uuid != uuid:
                    continue
                path = self.proc[ls_type][uuid]._args[0]
                path = path[path.rfind("//") + 1: path.rfind("/")]
                ps_file = ''.join([const.DIR[ls_type], uuid, ".ps"])
                ps_proc = subprocess.Popen(['tail', '-1', ps_file],
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
                ps_out, _ = ps_proc.communicate()
                ps_out = ps_out.strip('\0\n')
                ps_msg = ps_out[ps_out.find("msg=") + 4:]
                row_data.append((uuid, path, ls_type, "running", ps_msg))

                if search_uuid:
                    break

            for uuid in self.orphan[ls_type]:
               if search_uuid and search_uuid != uuid:
                   continue
               _, path = self.orphan[ls_type][uuid]
               path = path[path.rfind("//") + 1: path.rfind("/")]
               ps_file = ''.join([const.DIR[ls_type], uuid, ".ps"])
               ps_proc = subprocess.Popen(['tail', '-1', ps_file],
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)
               ps_out, _ = ps_proc.communicate()
               ps_out = ps_out.strip('\0\n')
               ps_msg = ps_out[ps_out.find("msg=") + 4:]
               row_data.append((uuid, path, ls_type, "running (o)", ps_msg))

               if search_uuid:
                   break

            for uuid in self.pending[ls_type]:
                if search_uuid and search_uuid != uuid:
                    continue
                path = self.pending[ls_type][uuid][0]
                path = path[path.rfind("//") + 1: path.rfind("/")]
                row_data.append((uuid, path, ls_type, "pending", "none"))

                if search_uuid:
                    break

        # Prepare the table listing
        header = ("Id", "Path", "Type", "State", "Message")
        long_column = dict(zip((0, 1, 2, 3, 4), (len(str(x)) for x in header)))

        for info in row_data:
            long_column.update((i, max(long_column[i], len(str(elem)))) for i, elem in enumerate(info))

        line = "".join(("|-", "-|-".join(long_column[i] * "-" for i in xrange(5)), "-|"))
        row_format = "".join(("| ", " | ".join("%%-%ss" % long_column[i] for i in xrange(0, 5)), " |"))
        msg += "\n".join((line, row_format % header, line,
                          "\n".join(row_format % elem for elem in row_data), line))
        return msg

    def do_kill(self, req_json):
        """ Kill transfer.

        Args:
            req_json (JSON command): Arguments for kill command
        """
        pid = 0
        msg = "OK"
        job_uuid = req_json['opt']
        found = False

        # Get pid of the process
        for job_type in self.proc:
            if job_uuid in self.proc[job_type]:
                pid = self.proc[job_type][job_uuid].pid
                found = True
                break
            elif job_uuid in self.orphan[job_type]:
                pid = self.orphan[job_type][job_uuid][0]
                found = True
                break
            elif job_uuid in self.pending[job_type]:
                del self.pending[job_type][job_uuid]
                found = True
                break

        if found:
            if pid:
                self.logger.debug("Kill uuid={0} pid={1}".format(job_uuid, pid))
                kill_proc = subprocess.Popen(['kill', '-9', str(pid)],
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
                _, err = kill_proc.communicate()
                retc = kill_proc.returncode

            if retc:
                msg = "ERROR error:" + err
        else:
            msg = "ERROR error: job not found"

        self.logger.info("Kill pid={0}, msg={0}".format(pid, msg))
        return msg

def main():
    """ Main function """
    # Create the local directory structure in /var/eos/archive/
    # i.e /var/eos/archive/get/, /var/eos/archive/put/ etc.
    const.LOG_FILE = LOG_DIR + "eosarchiver.log"
    const.IPC_FILE = EOS_ARCHIVE_DIR + "archivebackend.ipc"
    tmp_dict = {}

    for oper in [const.GET_OP, const.PUT_OP, const.PURGE_OP, const.DELETE_OP]:
        path = EOS_ARCHIVE_DIR + oper + '/'
        tmp_dict[oper] = path

        try:
            os.mkdir(path)
        except OSError as __:
            pass  # directory already exists

    const.DIR = tmp_dict

    # Get the loglevel or set the default one
    try:
        sloglevel = os.environ["LOG_LEVEL"]
        sloglevel = sloglevel.lower()
    except KeyError as err:
        # Set default loglevel to INFO
        sloglevel = "info"

    LOG_LEVEL = const.LOG_DICT[sloglevel]
    os.environ["LOG_LEVEL"] = str(LOG_LEVEL)
    logging.basicConfig(level=LOG_LEVEL, format=const.LOG_FORMAT)

    # Create dispatcher object
    dispatcher = Dispatcher()

    try:
        dispatcher.run()
    except Exception as err:
        dispatcher.logger.exception(err)

with daemon.DaemonContext():
    main()

#if __name__ == '__main__':
#    main()
