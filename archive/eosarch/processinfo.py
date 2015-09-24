# ------------------------------------------------------------------------------
# File: processinfo.py
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
""" Class emulating the process information for an archive/backup transfer
    which is used by the eosarchived daemon to display the status of the
    ongoing transfers.
"""
import time
import logging
from os import kill
from hashlib import sha256


class ProcessInfo(object):
    """ Class containing information about a process. It can also hold information
    about an orphan process and in this case the self.proc is None.

    Attributes:
       proc    (Process): Process object
       uid         (int): UID of transfer owner
       gid         (int): GID of transfer owner
       uuid     (string): uuid of transfer
       pid         (int): PID of process executing the transfer
       root_dir (string): Root directory in EOS of the archive/backup
       op       (string): Operation type
       orig_req   (JSON): JSON object representing the original request
    """
    def __init__(self, req_json = None):
        """ Initializing the process info object

        Args:
            req_json (JSON): Json object containing the following information:
                cmd: type of operation
                src: EOS url to the archive/backup file
                uid: UID of the user triggering the archiving
                gid: GID of the user triggering the archiving
        """
        self.logger = logging.getLogger("dispatcher")
        self.proc = None
        self.orig_req = req_json;

        if req_json:
            # Normal, 'owned' process
            self.uid = int(req_json['uid'])
            self.gid = int(req_json['gid'])
            self.status = "pending"
            self.pid, self.op = 0, req_json['cmd']
            # Extract the archive/backup root directory path
            src = req_json['src']
            pos = src.find("//", src.find("//") + 1) + 1
            self.root_dir = src[pos : src.rfind('/') + 1]
            self.uuid = sha256(self.root_dir).hexdigest()
            self.timestamp = time.time()

    def update(self, dict_info):
        """ Update process information

        Args:
           dict_info (dict): Dictionary containing the following information
           about an orphan process: uuid, pid, root_dir, op, status, uid, gid.
           If this is not the orphan discovery step then we only have the
           status field.
        """
        self.status = dict_info['status']

        try:
            # Update for orphan processes if information present
            self.uuid = dict_info['uuid']
            self.root_dir = dict_info['root_dir']
            self.op = dict_info['op']
            self.status = dict_info['status']
            self.pid = int(dict_info['pid'])
            self.uid = int(dict_info['uid'])
            self.gid = int(dict_info['gid'])
            self.timestamp = float(dict_info['timestamp'])
        except KeyError as __:
            # This response is only a status update
            pass

    def is_alive(self):
        """ Check if the underlying process is alive. For processes started
        by the current dispatcher i.e. for which we hold a reference to the
        Process object we can use is_alive() method, for orphan processes
        we use the OS functionality and send it a signal to check if it is
        still running.

        Returns:
            True if process alive, false otherwise
        """
        if self.proc:
            ret = self.proc.poll()

            if ret != None:
                info_msg = ("Uuid={0}, pid={1}, op={2}, path={3} has terminated "
                            "returncode={4}").format(self.uuid, self.pid, self.op,
                                                     self.root_dir, ret)
                self.logger.info(info_msg)
                return False
        else:
            try:
                kill(self.pid, 0)
            except OSError as __:
                dbg_msg = ("Uuid={0}, pid={1}, op={2}, path={3} has terminated - "
                           "no returncode available").format(self.uuid, self.pid,
                                                           self.op, self.root_dir)
                self.logger.debug(dbg_msg)
                return False
        return True
