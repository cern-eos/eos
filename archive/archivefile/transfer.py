# ------------------------------------------------------------------------------
# File: tranfer.py
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
"""Module responsible for executing the archive transfer."""

import os
import time
import logging
import const
from os.path import join
from asynchandler import MetaHandler
from hashlib import sha256
from XRootD import client
from archivefile import ArchiveFile
from exceptions import NoErrorException
from utils import exec_cmd


class Transfer(object):
    """ Trasfer archive object.

    Attributes:
        eos_file (string): Location of archive file in EOS. It's a valid URL.
        log_file (string): Transfer log file. Path on the local disk.
        op (string): Operation type: put, get, purge, delete or list
        list_jobs (list): List of file copy jobs to be executed.
    """
    def __init__(self, eosf="", operation="", option=""):
        self.oper = operation
        self.do_retry = (option == const.OPT_RETRY)
        self.efile_full = eosf
        self.efile_root = self.efile_full[:-(len(self.efile_full) - self.efile_full.rfind('/') - 1)]
        local_file = join(const.DIR[self.oper], sha256(self.efile_root).hexdigest())
        self.tx_file = local_file + ".tx"
        self.log_file = local_file + ".log"
        self.ps_file = local_file + ".ps"
        self.list_jobs = []
        self.pid = os.getpid()
        self.archive = None

        # Set up the logging
        formatter = logging.Formatter(const.LOG_FORMAT)
        log_handler = logging.FileHandler(self.log_file)
        log_handler.setFormatter(formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(log_handler)
        self.logger.propagate = False

        try:
            self.fprogress = open(self.ps_file, 'w')
        except IOError as __:
            self.logger.error("Failed to open file={0}".format(self.ps_file))
            raise

    def __del__(self):
        try:
            self.fprogress.close()
        except ValueError as __:
            pass

    def run(self):
        """ Run requested operation - fist call prepare.
        """
        self.prepare()

        if self.oper in [const.PUT_OP, const.GET_OP]:
            self.do_transfer()
        elif self.oper == const.PURGE_OP:
            self.do_delete(False)
        elif self.oper == const.DELETE_OP:
            self.do_delete(True)

    def prepare(self):
        """ Prepare requested operation.
        """
        # Rename archive file in EOS
        efile_url = client.URL(self.efile_full)
        eosf_rename = ''.join([self.efile_root, const.ARCH_FN, ".", self.oper, ".err"])
        rename_url = client.URL(eosf_rename)
        frename = [rename_url.protocol, "://", rename_url.hostid,
                   "//proc/user/?mgm.cmd=file&mgm.subcmd=rename"
                   "&mgm.path=", efile_url.path,
                   "&mgm.file.source=", efile_url.path,
                   "&mgm.file.target=", rename_url.path]
        (status, __, stderr) = exec_cmd(''.join(frename))

        if not status:
            err_msg = ("Failed to rename archive file {0} to {1}, msg={2}"
                       "").format(self.efile_full, rename_url, stderr)
            self.logger.error(err_msg)
            raise IOError(err_msg)

        # Copy archive file from EOS to the local disk
        self.efile_full = eosf_rename
        eos_fs = client.FileSystem(self.efile_full)
        st, _ = eos_fs.copy(self.efile_full + "?eos.ruid=0&eos.rgid=0",
                            self.tx_file, True)

        if not st.ok:
            err_msg = ("Failed to copy archive file={0} to local disk at={1}"
                       "").format(self.efile_full, self.tx_file)
            self.logger.error(err_msg)
            raise IOError(err_msg)

        # Create the ArchiveFile object
        d2t = (self.oper == const.PUT_OP)
        self.archive = ArchiveFile(self.tx_file, d2t)

    def do_delete(self, tape_delete):
        """ Delete archive either from disk (purge) or from tape (delete)

        Args:
            tape_delete (boolean): If true delete data from tape, otherwise
            from disk.

        Raises:
            IOError: Failed to delete an entry.
        """
        del_dirs = []
        self.logger.info("Calling do_delete(tape_delete={0})".format(tape_delete))
        # First remove all the files and then the directories
        for fentry in self.archive.files():
            # d2t is false for both purge and deletion
            self.archive.del_entry(fentry[1], False, tape_delete)

        for dentry in self.archive.dirs():
            # Don't remove the root directory when purging
            if not tape_delete and dentry[1] == './':
                continue

            del_dirs.append(dentry[1])

        # Remove the directories from bottom up
        while len(del_dirs):
            dpath = del_dirs.pop()
            self.archive.del_entry(dpath, True, tape_delete)

        # Remove immutable flag from the EOS sub-tree
        if tape_delete:
            self.archive.make_mutable()

        self.clean_transfer(True)

    def do_transfer(self):
        """ Execute the put or get operation. What this method actually does is
        copy the JSON archive file from EOS to the local disk and read-in each
        entry, be it a file or a directory and creates it in the destination
        location. The archive file first contains the list of all the directories
        and then the files.

        Raises:
            IOError when an IO opperations fails.
        """
        self.logger.info("Calling do_transfer()")
        tstart = time.time()
        meta_handler = MetaHandler()
        indx_dir, indx_file = 0, 0

        # For retry get the first corrupted entry
        if self.do_retry:
            check_ok, err_entry = self.archive.verify()

            if check_ok:
                self.do_retry = False
                raise NoErrorException()

            # Delete the corrupted entry
            is_dir = True if err_entry[0] == 'd' else False
            self.archive.del_entry(err_entry[1], is_dir, None)

        found_checkpoint = False  # flag set when reaching recovery entry

        # Create directories
        for dentry in self.archive.dirs():
            # Search for the recovery checkpoint
            if self.do_retry and not found_checkpoint:
                if dentry != err_entry:
                    indx_dir += 1
                    continue
                else:
                    found_checkpoint = True

            # Do special checks for root directory
            if not self.do_retry and dentry[1] == "./":
                self.archive.check_root_dir()

            indx_dir += 1
            self.archive.mkdir(dentry)
            self.write_progress("create dir {0}/{1}".format(indx_dir,
                                self.archive.header['num_dirs']))

        # Copy files
        for fentry in self.archive.files():
            # Search for the recovery checkpoint
            if self.do_retry and not found_checkpoint:
                if fentry != err_entry:
                    indx_file += 1
                    continue
                else:
                    found_checkpoint = True

            indx_file += 1
            self.write_progress("copy file {0}/{1}".format(indx_file,
                                self.archive.header['num_files']))
            dict_finfo = dict(zip(self.archive.header['file_meta'], fentry[2:]))
            src, dst = self.archive.get_endpoints(fentry[1])

            # Copy file
            if self.archive.d2t:
                st = self.copy_file(src, dst, force=self.do_retry)
            else:
                st = self.copy_file(src, dst, dict_finfo, self.do_retry)

            if not st:
                self.logger.error("Failed to copy src={0}, dst={1}".format(src, dst))
                break

        # Flush all pending copies and set metadata info for GET operation
        st = self.flush_files(meta_handler)

        if not st:
            err_msg = "Failed to flush files"
            self.logger.error(err_msg)
            raise IOError(err_msg)

        self.write_progress("checking")
        check_ok, __ = self.archive.verify()
        self.write_progress("cleaning")
        self.logger.debug("Transfer wall time={0}".format(time.time() - tstart))
        self.clean_transfer(check_ok)

    def write_progress(self, msg):
        """ Write progress message to the local progress file.

        Args:
            msg (string): Progress status message.
        """
        self.fprogress.truncate(0)
        self.fprogress.write("pid={0} msg={1}".format(self.pid, msg))
        self.fprogress.flush()
        os.fsync(self.fprogress.fileno())

    def clean_transfer(self, check_ok):
        """ Clean the transfer by renaming the archive file in EOS adding the
        following extensions:
        .done - the transfer was successful
        .err  - there were errors during the transfer. These are logged in the
             file archive.log in the same directory.

        Args:
            check_ok (bool): True if no error occured during transfer,
            otherwise false.
        """
        # Rename arch file in EOS to reflect the status
        if not check_ok:
            eosf_rename = ''.join([self.efile_root, const.ARCH_FN, ".", self.oper, ".err"])
        else:
            eosf_rename = ''.join([self.efile_root, const.ARCH_FN, ".", self.oper, ".done"])

        old_url = client.URL(self.efile_full)
        new_url = client.URL(eosf_rename)
        frename = [old_url.protocol, "://", old_url.hostid, "//proc/user/?",
                   "mgm.cmd=file&mgm.subcmd=rename&mgm.path=", old_url.path,
                   "&mgm.file.source=", old_url.path,
                   "&mgm.file.target=", new_url.path]
        (status, __, stderr) = exec_cmd(''.join(frename))

        if not status:
            err_msg = "Failed to rename {0} to {1}, msg={2}".format(
                self.efile_full, eosf_rename, stderr)
            self.logger.error(err_msg)
            # TODO: raise IOError
        else:
            # For successful delete operations remove also the archive file
            if self.oper == const.DELETE_OP and check_ok:
                fs = client.FileSystem(self.efile_full)
                st_rm, _ = fs.rm(new_url.path + "?eos.ruid=0&eos.rgid=0")

                if not st_rm.ok:
                    warn_msg = "Failed to delete archive {0}".format(new_url.path)
                    self.logger.warning(warn_msg)

        # Copy local log file back to EOS directory and set the ownership to the
        # identity of the client who triggered the archive
        try:
            client_uid = self.archive.header["uid"]
            client_gid = self.archive.header["gid"]
        except KeyError as _:
            self.logger.warning("uid/gid not found in header, default to 99/99")
            client_uid = '99'
            client_gid = '99'

        dir_root = self.efile_root[self.efile_root.rfind('//') + 1:]
        eos_log = ''.join([old_url.protocol, "://", old_url.hostid, "/",
                           dir_root, const.ARCH_FN, ".log",
                           "?eos.ruid=", client_uid, "&eos.rgid=", client_gid])

        self.logger.debug("Copy log:{0} to {1}".format(self.log_file, eos_log))
        cp_client = client.FileSystem(self.efile_full)
        st, __ = cp_client.copy(self.log_file, eos_log, force=True)

        if not st.ok:
            self.logger.error("Failed to copy log file {0} to EOS at {1}".format(
                    self.log_file, eos_log))

        # Delete all files associated with this transfer
        try:
            os.remove(self.tx_file)
        except OSError as __:
            pass

        try:
            os.remove(self.log_file)
        except OSError as __:
            pass

        try:
            os.remove(self.ps_file)
        except OSError as __:
            pass

    def copy_file(self, src, dst, dfile=None, force=False, handler=None):
        """ Copy file from source to destination.

        Note that when doing put, the layout is not conserved. Therefore, a file
        with 3 replicas will end up as just a simple file in the new location.

        Args:
            src (string): Source of the copy (full URL).
            dst (string): Destiantion (full URL).
            dfile (dict): Dictionary containing metadata about the file. This
               is set only for get operations.
            force (boolean): Force copy only in recovery mode.
            handler (MetaHanlder): Metahandler obj used for async req.

        Returns:
            True if batch was executed successfully, otherwise false.
        """
        st = True

        # For GET we also have the dictionary with the metadata
        if dfile:
            dst = ''.join([dst, "?eos.ctime=", dfile['ctime'],
                           "&eos.mtime=", dfile['mtime'],
                           "&eos.ruid=", dfile['uid'],
                           "&eos.rgid=", dfile['gid'],
                           "&eos.bookingsize=", dfile['size'],
                           "&eos.targetsize=", dfile['size'],
                           "&eos.checksum=", dfile['xs']])

        self.logger.debug("Copying from {0} to {1}".format(src, dst))
        self.list_jobs.append((src, dst, force))

        if len(self.list_jobs) == const.BATCH_SIZE:
            st = self.flush_files(handler)

        return st

    def flush_files(self, __):
        """ Flush all pending transfers from the list of jobs.

        Args:
            handler (MetaHandler): Meta handler obj. used for async req.

        Returns:
            True if files flushed successfully, otherwise false.
        """
        proc = client.CopyProcess()

        for job in self.list_jobs:
            # TODO: do TPC when XRootD 3.3.6 does not crash anymore and use the
            # parallel mode starting with XRootD 4.1
            # TODO: use checksum verification if possible
            proc.add_job(job[0], job[1], force=job[2], thirdparty=False)

        proc.prepare()
        xrd_st = proc.run()

        if xrd_st.ok:
            self.logger.debug("Batch successful")
        else:
            self.logger.error("Batch error={0}".format(xrd_st))

        del self.list_jobs[:]
        return xrd_st.ok
