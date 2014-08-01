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
import threading
from os.path import join
from asynchandler import MetaHandler
from hashlib import sha256
from XRootD import client
from XRootD.client.flags import PrepareFlags
from archivefile import ArchiveFile
from exceptions import NoErrorException
from utils import exec_cmd
from asynchandler import MetaHandler


class ThreadJob(threading.Thread):
    """Job executing a client.CopyProcess in a separate thread. This makes sense
    since a third-party copy job is mostly waiting for the completion of the
    job not doing any other operations and therefore not using the GIL too much.

    Attributes:
        status (bool): Final status of the job.
        proc (client.CopyProcess): Copy process which is being executed.
    """
    def __init__(self, cpy_proc):
        """Constructor.

        Args:
            cpy_proc (client.CopyProcess): Copy process object.
        """
        threading.Thread.__init__(self)
        self.status = None
        self.proc = cpy_proc

    def run(self):
        """Run method.
        """
        self.proc.prepare()
        self.xrd_status = self.proc.run()


class Transfer(object):
    """ Trasfer archive object.

    Attributes:
        eos_file (string): Location of archive file in EOS. It's a valid URL.
        log_file (string): Transfer log file. Path on the local disk.
        op (string): Operation type: put, get, purge, delete or list.
        threads (list): List of threads doing parital transfers(CopyProcess jobs).
    """
    def __init__(self, eosf, operation, option):
        self.oper = operation
        self.do_retry = (option == const.OPT_RETRY)
        self.efile_full = eosf
        self.efile_root = self.efile_full[:-(len(self.efile_full) - self.efile_full.rfind('/') - 1)]
        dir_root = self.efile_root[self.efile_root.rfind('//') + 1:]
        local_file = join(const.DIR[self.oper], sha256(dir_root).hexdigest())
        self.tx_file = local_file + ".tx"
        self.log_file = local_file + ".log"
        self.ps_file = local_file + ".ps"
        self.list_jobs, self.threads = [], []
        self.archive = None
        # Special case for inital PUT as we need to copy also the archive file
        self.init_put = self.efile_full.endswith(const.ARCH_INIT)

        # Set up the logging
        formatter = logging.Formatter(const.LOG_FORMAT)
        log_handler = logging.FileHandler(self.log_file)
        log_handler.setFormatter(formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(log_handler)
        self.logger.propagate = False
        # Get the loglevel
        try:
            loglvl = os.environ["LOG_LEVEL"]
        except KeyError as __:
            loglvl = logging.LOG_INFO

        self.logger.setLevel(int(loglvl))

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
        """ Run requested operation - fist call prepare
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
        self.logger.info("Do delete with tape_delete={0}".format(tape_delete))
        # Delete also the archive file saved on tape
        if tape_delete:
            self.archive.del_entry(const.ARCH_INIT, False, tape_delete)

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
        t0 = time.time()
        indx_dir, indx_file = 0, 0
        err_entry = None

        # For retry get the first corrupted entry
        if self.do_retry:
            check_ok, err_entry = self.archive.verify()

            if check_ok:
                self.do_retry = False
                raise NoErrorException()

            # Delete the corrupted entry
            is_dir = True if err_entry[0] == 'd' else False
            self.logger.info("Delete corrupted entry={0}".format(err_entry))
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

        # For GET issue the Prepare2Get for all the files on tape
        self.write_progress("prepare2get")
        self.prepare2get(err_entry, found_checkpoint)

        # Copy files
        self.copy_files(err_entry, found_checkpoint)
        self.write_progress("checking")
        check_ok, __ = self.archive.verify()
        self.write_progress("cleaning")
        self.logger.info("TIMING_transfer={0} sec".format(time.time() - t0))
        self.clean_transfer(check_ok)

    def write_progress(self, msg):
        """ Write progress message to the local progress file.

        Args:
            msg (string): Progress status message.
        """
        self.fprogress.truncate(0)
        self.fprogress.seek(0)
        self.fprogress.write("pid={0} msg={1}".format(os.getpid(), msg))
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

        # Delete all local files associated with this transfer
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

    def copy_files(self, err_entry, found_checkpoint):
        """ Copy file from source to destination.

        Note that when doing put, the layout is not conserved. Therefore, a file
        with 3 replicas will end up as just a simple file in the new location.

        Args:
            err_entry (list): Entry record from the archive file corresponding
                 to the first file/dir that was corrupted.
            found_checkpoint (bool): If it's true, it means the checkpoint was
                 already found and we don't need to search for it.

        Raises:
            IOError: Copy request failed.
        """
        indx_file = 0
        meta_handler = MetaHandler()
        # For inital PUT copy also the archive file to tape
        if self.init_put:
            dst = self.archive.header['dst'] + const.ARCH_INIT
            self.list_jobs.append((self.efile_full, dst, self.do_retry))

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
            src, dst = self.archive.get_endpoints(fentry[1])

            # Copy file
            if not self.archive.d2t:
                # For GET we also have the dictionary with the metadata
                dfile = dict(zip(self.archive.header['file_meta'], fentry[2:]))
                dst = ''.join([dst, "?eos.ctime=", dfile['ctime'],
                               "&eos.mtime=", dfile['mtime'],
                               "&eos.ruid=", dfile['uid'],
                               "&eos.rgid=", dfile['gid'],
                               "&eos.bookingsize=", dfile['size'],
                               "&eos.targetsize=", dfile['size'],
                               "&eos.checksum=", dfile['xs']])

            self.logger.debug("Copying from {0} to {1}".format(src, dst))
            self.list_jobs.append((src, dst, self.do_retry))

            if len(self.list_jobs) == const.BATCH_SIZE:
                st = self.flush_files(False)

                if not st:
                    err_msg = "Failed to flush files"
                    self.logger.error(err_msg)
                    raise IOError(err_msg)

        # Flush all pending copies and set metadata info for GET operation
        st = self.flush_files(True)

        if not st:
            err_msg = "Failed to flush files"
            self.logger.error(err_msg)
            raise IOError(err_msg)

    def flush_files(self, wait_all):
        """ Flush all pending transfers from the list of jobs.

        Args:
            wait_all (bool): If true wait and collect the status from all
                executing threads.

        Returns:
            True if files flushed successfully, otherwise false.
        """
        status = True

        # Wait until a thread from the pool gets freed if we reached the maximum
        # allowed number of running threads
        while len(self.threads) >= const.MAX_THREADS:
            del_lst = []

            for indx, thread in enumerate(self.threads):
                thread.join(const.JOIN_TIMEOUT)

                # If thread finished get the status and mark it for removal
                if not thread.isAlive():
                    status = status and thread.xrd_status.ok
                    self.logger.debug("Thread={0} status={1}".format(
                           thread.ident, thread.xrd_status.ok))
                    del_lst.append(indx)

                    if not status:
                        self.logger.error("Thread={0} err_msg={2}".format(
                                thread.ident, stathread.xrd_status.message))
                        break

            for indx in del_lst:
                del self.threads[indx]

        # If previous transfers were successful and we still have jobs
        if status and self.list_jobs:
            proc = client.CopyProcess()

            for job in self.list_jobs:
                # TODO: do TPC when XRootD 3.3.6 does not crash anymore and use the
                # parallel mode starting with XRootD 4.1
                # TODO: use checksum verification if possible
                self.logger.info("Add job src={0}, dst={1}".format(job[0], job[1]))
                proc.add_job(job[0], job[1], force=job[2], thirdparty=False)

            del self.list_jobs[:]
            thread = ThreadJob(proc)
            thread.start()
            self.threads.append(thread)

        # If we already have failed transfers or we submitted all the jobs then
        # join the rest of the threads and collect also their status
        if not status or wait_all:
            for thread in self.threads:
                if thread.isAlive():
                    thread.join()
                    self.logger.debug("Thread={0} status={1}".format(
                            thread.ident, thread.xrd_status.ok))

                status = status and thread.xrd_status.ok

            del self.threads[:]

        return status

    def prepare2get(self, err_entry, found_checkpoint):
        """This method is only executed for GET operations and it's purpose is
        to issue the Prepapre2Get commands for the files in the archive which
        will later on be copied back to EOS.

        Args:
            err_entry (list): Entry record from the archive file corresponding
                 to the first file/dir that was corrupted.
            found_checkpoint (bool): If it's true, it means the checkpoint was
                 already found and we don't need to search for it.

        Raises:
            IOError: The Prepare2Get request failed.
        """
        limit = 20  # max files per prepare request
        oper = 'prepare'

        if not self.archive.d2t:
            t0 = time.time()
            lpaths = []
            metahandler = MetaHandler()

            for fentry in self.archive.files():
                # Find error checkpoint if not already found
                if err_entry and not found_checkpoint:
                    if fentry != err_entry:
                        continue
                    else:
                        found_checkpoint = True

                surl, __ = self.archive.get_endpoints(fentry[1])
                lpaths.append(surl[surl.rfind('//') + 1:])

                if len(lpaths) == limit:
                    xrd_st = self.archive.fs_dst.prepare(lpaths, PrepareFlags.STAGE,
                                callback=metahandler.register(oper, surl))

                    if not xrd_st.ok:
                        __ = metahandler.wait(oper)
                        self.logger.error(err_msg)
                        raise IOError(err_msg)

                    del lpaths[:]

            # Send the remaining requests
            if lpaths:
                xrd_st = self.archive.fs_dst.prepare(lpaths, PrepareFlags.STAGE,
                            callback=metahandler.register(oper, surl))

                if not xrd_st.ok:
                    __ = metahandler.wait(oper)
                    self.logger.error(err_msg)
                    raise IOError(err_msg)

                del lpaths[:]

            status  = metahandler.wait(oper)
            t1 = time.time()
            self.logger.info("TIMING_prepare2get={0} sec".format(t1 - t0))
