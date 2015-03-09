#! /usr/bin/python
# ------------------------------------------------------------------------------
# File: eosarchived.py
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
from __future__ import unicode_literals
from __future__ import print_function
import os
import sys
import zmq
import stat
import subprocess
import daemon
import ast
import logging
import time
import logging.handlers
from eosarch import ProcessInfo, Configuration


class Dispatcher(object):
    """ Dispatcher daemon responsible for receiving requests from the clients
    and then spawning the proper executing process for archiving operations

    Attributes:
        procs (dict): Dictionary containing the currently running processes
    """
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("dispatcher")
        self.procs = {}
        self.backend_req, self.backend_pub, self.backend_poller = None, None, None

    def run(self):
        """ Server entry point which is responsible for spawning worker proceesses
        that do the actual transfers (put/get).
        """
        # Set the triggers for different types of commands
        trigger = {self.config.PUT_OP:    self.start_transfer,
                   self.config.GET_OP:    self.start_transfer,
                   self.config.DELETE_OP: self.start_transfer,
                   self.config.PURGE_OP:  self.start_transfer,
                   self.config.BACKUP_OP: self.start_transfer,
                   self.config.TX_OP:     self.do_show_transfers,
                   self.config.KILL_OP:   self.do_kill}
        ctx = zmq.Context.instance()
        self.logger.info("Started dispatcher process ...")
        # Socket used for communication with EOS MGM
        frontend = ctx.socket(zmq.REP)
        addr = "ipc://" + self.config.FRONTEND_IPC
        frontend.bind(addr.encode("utf-8"))
        # Socket used for communication with worker processes
        self.backend_req = ctx.socket(zmq.ROUTER)
        addr = "ipc://" + self.config.BACKEND_REQ_IPC
        self.backend_req.bind(addr.encode("utf-8"))
        self.backend_pub = ctx.socket(zmq.PUB)
        addr = "ipc://" + self.config.BACKEND_PUB_IPC
        self.backend_pub.bind(addr.encode("utf-8"))
        self.backend_poller = zmq.Poller()
        self.backend_poller.register(self.backend_req, zmq.POLLIN)
        mgm_poller = zmq.Poller()
        mgm_poller.register(frontend, zmq.POLLIN)
        time.sleep(1)

        # Attach orphan processes which may be running before starting the daemon
        self.get_orphans()

        while True:
            events = dict(mgm_poller.poll(self.config.POLL_TIMEOUT))
            self.update_status()

            if events and events.get(frontend) == zmq.POLLIN:
                try:
                    req_json = frontend.recv_json()
                except zmq.ZMQError as err:
                    if err.errno == zmq.ETERM:
                        break  # shutting down, exit
                    else:
                        raise
                except ValueError as err:
                    self.logger.error("Command in not in JSON format")
                    frontend.send("ERROR error:command not in JSON format")
                    continue

                self.logger.debug("Received command: {0}".format(req_json))

                try:
                    reply = trigger[req_json['cmd']](req_json)
                except KeyError as err:
                    self.logger.error("Unknown command type: {0}".format(req_json['cmd']))
                    reply = "ERROR error: operation not supported"
                    raise

                frontend.send_string(reply.encode("utf-8"))

    def get_orphans(self):
        """ Get orphan transfer processes from previous runs of the daemon
        """
        self.logger.info("Get orphans")
        tries = 0
        num = self.num_processes()

        # Get status for orphan processes
        while len(self.procs) != num and tries < 10:
            tries += 1
            self.procs.clear()
            num = self.num_processes()
            self.backend_pub.send_multipart([b"[MASTER]", b"{'cmd': 'orphan_status'}"])

            while True:
                events = dict(self.backend_poller.poll(1000))

                if events and events.get(self.backend_req) == zmq.POLLIN:
                    [__, resp] = self.backend_req.recv_multipart()
                    self.logger.info("Received response: {0}".format(resp))
                    # Convert response to python dictionary
                    dict_resp = ast.literal_eval(resp)

                    if not isinstance(dict_resp, dict):
                        err_msg = "Response={0} is not a dictionary".format(resp)
                        self.logger.error(err_msg)
                        continue

                    pinfo = ProcessInfo(None)
                    pinfo.update(dict_resp)

                    if pinfo.uuid not in self.procs:
                        self.procs[pinfo.uuid] = pinfo
                else: # TIMEOUT
                    self.logger.info("Get orphans status timeout")
                    break

            self.logger.debug(("Try={0}, got {1}/{2} orphan processe responses"
                              "").format(tries, len(self.procs), num))

    def num_processes(self):
        """ Get the number of running archive processes on the current system by
        executing the ps command

        Returns:
            Number of running processes

        Raises:
             ValueError in case the output of ps is not a valid pid number
        """
        pid = os.getpid()
        # TODO: make the resolution of the eosarch_run.py more elegant
        exec_fname = "eosarch_run.py"
        ps_proc = subprocess.Popen([("ps -eo pid,ppid,comm | egrep \"{0}\$\" | "
                                     "awk '{{print $1}}'").format(exec_fname)],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   shell=True)
        ps_out, __ = ps_proc.communicate()

        if len(ps_out) == 0:
            return 0

        ps_out = ps_out.strip('\0\n')
        proc_lst = ps_out.split('\n')

        try:
            num = len([x for x in proc_lst if pid != int(x)])
        except ValueError as __:
            self.logger.error("ps output x={0} is not a valid pid value".format(x))
            raise

        return num

    def update_status(self):
        """ Update the status of the processes
        """
        self.backend_pub.send_multipart([b"[MASTER]", b"{'cmd': 'status'}"])
        recv_uuid = []

        while len(recv_uuid) < len(self.procs):
            events = dict(self.backend_poller.poll(400))

            if events and events.get(self.backend_req) == zmq.POLLIN:
                [__, resp] = self.backend_req.recv_multipart()
                self.logger.debug("Received response: {0}".format(resp))
                # Convert response to python dictionary
                dict_resp = ast.literal_eval(resp)

                if not isinstance(dict_resp, dict):
                    self.logger.error("Response is not a dictionary")
                    continue

                # Update the local info about the process
                try:
                    self.procs[dict_resp['uuid']].update(dict_resp)
                except KeyError as __:
                    err_msg = ("Unkown process response:{0}").format(dict_resp)
                    self.logger.error(err_msg)

                recv_uuid.append(dict_resp['uuid'])
            else: # TIMEOUT
                self.logger.debug("Update status timeout")
                break

        # Check if processes that didn't respond are still alive
        unresp = [proc for (uuid, proc) in self.procs.iteritems()
                  if uuid not in recv_uuid]

        for pinfo in unresp:
            if not pinfo.is_alive():
                del self.procs[pinfo.uuid]

    def start_transfer(self, req_json):
        """ Start new transfer

        Args:
            req_json (json): New transfer information which must include:
            {
              cmd: get/put/delete/purge,
              src: full URL to archive file in EOS.
              opt: retry | ''
              uid: client uid
              gid: client gid
            }

        Returns:
            A message which is sent to the EOS MGM informing about the status
            of the request.
        """
        self.logger.debug("Start transfer {0}".format(req_json))

        if len(self.procs) >= self.config.MAX_TRANSFERS:
            self.logger.warning("Maximum number of concurrent transfers reached")
            return "ERROR error: max number of transfers reached"

        pinfo = ProcessInfo(req_json)
        self.logger.debug("Adding job={0}, path={1}".format(pinfo.uuid, pinfo.root_dir))

        if pinfo.uuid in self.procs:
            err_msg = "Job with same uuid={0} already exists".format(pinfo.uuid)
            self.logger.error(err_msg)
            return "ERROR error: job with same signature exists"

        # Don't pipe stdout and stderr as we log all the output
        pinfo.proc = subprocess.Popen(['/usr/bin/eosarch_run.py', "{0}".format(req_json)],
                                      close_fds=True)
        pinfo.pid = pinfo.proc.pid
        self.procs[pinfo.uuid] = pinfo
        return "OK Id={0}".format(pinfo.uuid)

    def do_show_transfers(self, req_json):
        """ Show onging transfers

        Args:
            req_json (JSON): Command in JSON format include:
            {
              cmd:    transfers,
              opt:    all/get/put/purge/delete/uuid,
              uid:    uid,
              gid:    gid,
              format: monitor|pretty
            }

        Returns:
            String with the result of the listing
         """
        msg = "OK "
        row_data, proc_list = [], []
        ls_type = req_json['opt']
        self.logger.debug("Show transfers type={0}".format(ls_type))

        if ls_type == "all":
            proc_list = self.procs.itervalues()
        elif ls_type in self.procs:
            # ls_type is a transfer uuid
            proc_list.append(self.procs[ls_type])
        else:
            proc_list = [elem for elem in self.procs.itervalues() if elem.op == ls_type]

        for proc in proc_list:
            row_data.append((time.asctime(time.localtime(proc.timestamp)), proc.uuid,
                             proc.root_dir, proc.op, proc.status))

        # Prepare the table listing
        if req_json['format'] == "monitor":
            for elem in row_data:
                kv_data = "path={0}".format(elem[2])
                msg += ''.join([kv_data, '|'])
        else:
            header = ("Start date", "Id", "Path", "Type", "Status")
            long_column = dict(zip((0, 1, 2, 3, 4), (len(str(x)) for x in header)))

            for info in row_data:
                long_column.update((i, max(long_column[i], len(str(elem))))
                                   for i, elem in enumerate(info))

            line = "".join(("|-", "-|-".join(long_column[i] * "-"
                                             for i in xrange(5)), "-|"))
            row_format = "".join(("| ", " | ".join("%%-%ss" % long_column[i]
                                                   for i in xrange(0, 5)), " |"))

            msg += "\n".join((line, row_format % header, line,
                              "\n".join((row_format % elem) for elem in row_data),
                              line))

        return msg

    def do_kill(self, req_json):
        """ Kill transfer.

        Args:
            req_json (JSON command): Arguments for kill command include:
            {
              cmd: kill,
              opt: uuid,
              uid: uid,
              gid: gid
            }
        """
        msg = "OK"
        job_uuid = req_json['opt']
        uid, gid = int(req_json['uid']), int(req_json['gid'])

        try:
            proc = self.procs[job_uuid]
        except KeyError as __:
            msg = "ERROR error: job not found"
            return msg

        if (uid == 0 or uid == proc.uid or
            (uid != proc.uid and gid == proc.gid)):

            self.logger.debug("Kill uuid={0} pid={1}".format(job_uuid, proc.pid))
            kill_proc = subprocess.Popen(['kill', '-SIGTERM', str(proc.pid)],
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE)
            _, err = kill_proc.communicate()

            if kill_proc.returncode:
                msg = "ERROR error:" + err
        else:
            self.logger.error(("User uid/gid={0}/{1} permission denied to kill job "
                               "with uid/gid={2}/{3}").format(uid, gid,
                                                              proc.uid, proc.gid))
            msg = "ERROR error: Permission denied - you are not owner of the job"

        self.logger.debug("Kill pid={0}, msg={0}".format(proc.pid, msg))
        return msg


def main():
    """ Main function """
    with daemon.DaemonContext():
        try:
            config = Configuration()
        except Exception as err:
            print("Configuration failed, error:{0}".format(err), file=sys.stderr)
            raise

        config.start_logging("dispatcher", config.LOG_FILE, True)
        logger = logging.getLogger("dispatcher")
        config.display()
        config.DIR = {}

        # Create the local directory structure in /var/eos/archive/
        # i.e /var/eos/archive/get/, /var/eos/archive/put/ etc.
        for oper in [config.GET_OP,
                     config.PUT_OP,
                     config.PURGE_OP,
                     config.DELETE_OP,
                     config.BACKUP_OP]:
            path = config.EOS_ARCHIVE_DIR + oper + '/'
            config.DIR[oper] = path

            try:
                os.mkdir(path)
            except OSError as __:
                pass  # directory exists

        # Prepare ZMQ IPC files
        for ipc_file in [config.FRONTEND_IPC,
                         config.BACKEND_REQ_IPC,
                         config.BACKEND_PUB_IPC]:
            if not os.path.exists(ipc_file):
                try:
                    open(ipc_file, 'w').close()
                    os.chmod(ipc_file, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
                except OSError as err:
                    err_msg = ("Failed setting permissioins on the IPC socket"
                               " file={0}").format(ipc_file)
                    logger.error(err_msg)
                    raise
                except IOError as err:
                    err_msg = ("Failed creating IPC socket file={0}").format(ipc_file)
                    logger.error(err_msg)
                    raise

        # Create dispatcher object
        dispatcher = Dispatcher(config)

        try:
            dispatcher.run()
        except Exception as err:
            logger.exception(err)

if __name__ == '__main__':
    try:
        main()
    except ValueError as __:
        # This is to deal the exception thrown when trying to close the log
        # file which is already deleted manualy by an exterior process
        pass
