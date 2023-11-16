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
"""Module responsible for executing the transfers.
"""
from __future__ import unicode_literals
from __future__ import division
import os
import time
import logging
import threading
import zmq
import ast
from os.path import join
from time import sleep
from hashlib import sha256
from XRootD import client
from XRootD.client.flags import PrepareFlags, QueryCode, OpenFlags, StatInfoFlags
from eosarch.archivefile import ArchiveFile
from eosarch.utils import exec_cmd, is_version_file
from eosarch.asynchandler import MetaHandler
from eosarch.exceptions import NoErrorException


class ThreadJob(threading.Thread):
    """ Job executing a client.CopyProcess in a separate thread. This makes sense
    since a third-party copy job is mostly waiting for the completion of the
    job not doing any other operations and therefore not using the GIL too much.

    Attributes:
        status             (bool): Final status of the job
        lst_jobs           (list): List of jobs to be executed
        proc (client.CopyProcess): Copy process which is being executed
        retires             (int): Number of times this job was retried
    """
    def __init__(self, jobs, retry=0):
        """Constructor

        Args:
            jobs (list): List of transfers to be executed
            retry (int): Number of times this job was retried
        """
        threading.Thread.__init__(self)
        self.retries = retry
        self.xrd_status = None
        self.lst_jobs = list(jobs)

    def run(self):
        """ Run method
        """
        self.retries += 1
        proc = client.CopyProcess()

        for job in self.lst_jobs:
            # If file is 0-size then we do a normal copy, otherwise we enforce
            # a TPC transfer
            tpc_flag = "none"

            if (int(job[2]) != 0):
                tpc_flag = "only"

            # TODO: use the parallel mode starting with XRootD 4.1
            proc.add_job(job[0], job[1],
                         force=True, thirdparty=tpc_flag, tpctimeout=3600)

        self.xrd_status = proc.prepare()

        if self.xrd_status.ok:
            self.xrd_status, __ = proc.run()


class ThreadStatus(threading.Thread):
    """ Thread responsible for replying to any requests comming from the
    dispatcher process.
    """
    def __init__(self, transfer):
        """ Constructor

        Args:
            transfer (Transfer): Current transfer object
        """
        threading.Thread.__init__(self)
        # TODO: drop the logger as it may interfere with the main thread
        self.logger = logging.getLogger("transfer")
        self.transfer = transfer
        self.run_status = True
        self.lock = threading.Lock()

    def run(self):
        """ Run method
        """
        self.logger.info("Starting the status thread")
        ctx = zmq.Context()
        socket_rr = ctx.socket(zmq.DEALER)
        socket_rr.connect("ipc://" + self.transfer.config.BACKEND_REQ_IPC)
        socket_ps = ctx.socket(zmq.SUB)
        mgr_filter = b"[MASTER]"
        addr = "ipc://" + self.transfer.config.BACKEND_PUB_IPC
        socket_ps.connect(addr)
        socket_ps.setsockopt(zmq.SUBSCRIBE, mgr_filter)

        while self.keep_running():
            if socket_ps.poll(5000):
                try:
                    [__, msg] = socket_ps.recv_multipart()
                except zmq.ZMQError as err:
                    if err.errno == zmq.ETERM:
                        self.logger.error("ETERM error")
                        break # shutting down, exit
                    else:
                        self.logger.exception(err)
                        continue
                except Exception as err:
                    self.logger.exception(err)

                self.logger.debug("RECV_MSG: {0}".format(msg))
                dict_cmd = ast.literal_eval(msg.decode())

                if dict_cmd['cmd'] == 'orphan_status':
                    self.logger.info("Reconnect to master ... ")
                    resp = ("{{'uuid': '{0}', "
                            "'pid': '{1}', "
                            "'uid': '{2}',"
                            "'gid': '{3}',"
                            "'root_dir': '{4}', "
                            "'op': '{5}',"
                            "'status': '{6}', "
                            "'timestamp': '{7}'"
                            "}}").format(self.transfer.uuid,
                                         self.transfer.pid,
                                         self.transfer.uid,
                                         self.transfer.gid,
                                         self.transfer.root_dir,
                                         self.transfer.oper,
                                         self.transfer.get_status(),
                                         self.transfer.timestamp)
                elif dict_cmd['cmd'] == 'status':
                    resp = ("{{'uuid': '{0}', "
                            "'status': '{1}'"
                            "}}").format(self.transfer.uuid,
                                         self.transfer.get_status())
                else:
                    self.logger.error("Unknown command: {0}".format(dict_cmd))
                    continue

                self.logger.info("Sending response: {0}".format(resp))
                socket_rr.send_multipart([resp.encode()], zmq.NOBLOCK)

    def do_finish(self):
        """ Set the flag for the status thread to finish execution
        """
        self.lock.acquire()
        self.run_status = False
        self.lock.release()

    def keep_running(self):
        """ Check if we continue running - the transfer is ongoing

        Returns:
            True if status thread should keep running, otherwise False
        """
        self.lock.acquire()
        ret = self.run_status
        self.lock.release()
        return ret


class Transfer(object):
    """ Trasfer archive object

    Attributes:
        req_json (JSON): Command received from the EOS MGM. Needs to contains the
            following entries: cmd, src, opt, uid, gid
        threads (list): List of threads doing partial transfers(CopyProcess jobs)
    """
    def __init__(self, req_json, config):
        self.config = config
        self.oper = req_json['cmd']
        self.uid, self.gid = req_json['uid'], req_json['gid']
        self.do_retry = (req_json['opt'] == self.config.OPT_RETRY)
        self.force = (req_json['opt'] == self.config.OPT_FORCE)
        self.efile_full = req_json['src']
        self.efile_root = self.efile_full[:-(len(self.efile_full) - self.efile_full.rfind('/') - 1)]
        self.root_dir = self.efile_root[self.efile_root.rfind('//') + 1:]
        self.uuid = sha256(self.root_dir.encode()).hexdigest()
        local_file = join(self.config.DIR[self.oper], self.uuid)
        self.tx_file = local_file + ".tx"
        self.list_jobs, self.threads = [], []
        self.pid = os.getpid()
        self.archive = None
        # Special case for inital PUT as we need to copy also the archive file
        self.init_put = self.efile_full.endswith(self.config.ARCH_INIT)
        self.status = "initializing"
        self.lock_status = threading.Lock()
        self.timestamp = time.time()
        self.logger = logging.getLogger("transfer")
        self.thread_status = ThreadStatus(self)

    def get_status(self):
        """ Get current status

        Returns:
            String representing the status
        """
        self.lock_status.acquire()
        ret = self.status
        self.lock_status.release()
        return ret

    def set_status(self, msg):
        """ Set current status

        Args:
            msg (string): New status
        """
        self.lock_status.acquire()
        self.status = msg
        self.lock_status.release()

    def run(self):
        """ Run requested operation - fist call prepare

        Raises:
            IOError
        """
        self.thread_status.start()

        if self.oper in [self.config.PUT_OP, self.config.GET_OP]:
            self.archive_prepare()

            if self.do_retry:
                self.do_retry_transfer()
            else:
                self.do_transfer()
        elif self.oper in [self.config.PURGE_OP, self.config.DELETE_OP]:
            self.archive_prepare()
            self.do_delete((self.oper == self.config.DELETE_OP))
        elif self.oper == self.config.BACKUP_OP:
            self.backup_prepare()
            self.do_backup()

    def archive_prepare(self):
        """ Prepare requested archive operation.

        Raises:
            IOError: Failed to rename or transfer archive file.
        """
        # Rename archive file in EOS
        efile_url = client.URL(self.efile_full)
        eosf_rename = ''.join([self.efile_root, self.config.ARCH_FN, ".", self.oper, ".err"])
        rename_url = client.URL(eosf_rename)
        frename = ''.join([rename_url.protocol, "://", rename_url.hostid,
                           "//proc/user/?mgm.cmd=file&mgm.subcmd=rename"
                           "&mgm.path=", efile_url.path,
                           "&mgm.file.source=", efile_url.path,
                           "&mgm.file.target=", rename_url.path])
        (status, __, stderr) = exec_cmd(frename)

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
        d2t = (self.oper == self.config.PUT_OP)
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
            self.archive.del_entry(self.config.ARCH_INIT, False, tape_delete)

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

        self.archive_tx_clean(True)

    def do_transfer(self):
        """ Execute a put or get operation.

        Raises:
            IOError when an IO opperations fails.
        """
        t0 = time.time()
        indx_dir = 0

        # Create directories
        for dentry in self.archive.dirs():
            if dentry[1] == "./":
                self.archive.check_root_dir()

            indx_dir += 1
            self.archive.mkdir(dentry)
            msg = "create dir {0}/{1}".format(indx_dir, self.archive.header['num_dirs'])
            self.set_status(msg)

        # For GET issue the Prepare2Get for all the files on tape
        self.prepare2get()

        # Copy files
        self.copy_files()

        # For GET set file ownership and permissions
        self.update_file_access()

        # Verify the transferred entries
        self.set_status("verifying")
        check_ok, __ = self.archive.verify(False)

        # For PUT operations wait that all the files are on tape and for GET
        # send a "prepare evict" request to CTA to clear the disk cache
        if self.archive.d2t:
            self.set_status("wait_on_tape")
            self.wait_on_tape()
        else:
            self.set_status("evict_disk_cache")
            try:
                self.evict_disk_cache()
            except OverflowError as __:
                self.logger.warning("The XRootD Python bindings do not support "
                                    "the evict flag yet!")

        self.set_status("cleaning")
        self.logger.info("TIMING_transfer={0} sec".format(time.time() - t0))
        self.archive_tx_clean(check_ok)

    def do_retry_transfer(self):
        """ Execute a put or get retry operation.

        Raises:
            IOError when an IO opperations fails.
        """
        t0 = time.time()
        indx_dir = 0
        err_entry = None
        tx_ok, meta_ok = True, True
        found_checkpoint = False  # flag set when reaching recovery entry

        # Get the first corrupted entry and the type of corruption
        (tx_ok, meta_ok, lst_failed) = self.check_previous_tx()

        if not tx_ok or not meta_ok:
            err_entry = lst_failed[0]

        # Create directories
        for dentry in self.archive.dirs():
            # Search for the recovery checkpoint
            if not found_checkpoint:
                if dentry != err_entry:
                    indx_dir += 1
                    continue
                else:
                    found_checkpoint = True

            indx_dir += 1
            self.archive.mkdir(dentry)
            msg = "create dir {0}/{1}".format(indx_dir, self.archive.header['num_dirs'])
            self.set_status(msg)

        if not tx_ok:
            # For GET issue the Prepare2Get for all the files on tape
            self.prepare2get(err_entry, found_checkpoint)

            # Copy files
            self.copy_files(err_entry, found_checkpoint)

            # For GET set file ownership and permissions for all entries
            self.update_file_access(err_entry, found_checkpoint)

        else:
            # For GET metadata errors set file ownership and permissions only
            # for entries after the first corrupted one
            self.update_file_access()

        # Verify the transferred entries
        self.set_status("verifying")
        check_ok, __ = self.archive.verify(False)

        # For PUT operations what that all the files are on tape
        if self.archive.d2t:
           self.set_status("wait_on_tape")
           self.wait_on_tape()
        else:
            self.set_status("evict_disk_cache")
            try:
                self.evict_disk_cache()
            except OverflowError as __:
                self.logger.warning("The XRootD Python bindings do not support "
                                    "the evict flag yet!")

        self.set_status("cleaning")
        self.logger.info("TIMING_transfer={0} sec".format(time.time() - t0))
        self.archive_tx_clean(check_ok)

    def tx_clean(self, check_ok):
        """ Clean a backup/archive transfer depending on its type.
        """
        if self.oper == self.config.BACKUP_OP:
            self.backup_tx_clean()
        else:
            self.archive_tx_clean(check_ok)

    def backup_tx_clean(self):
        """ Clean after a backup transfer by copying the log file in the same
        directory as the destiantion of the backup.
        """
        # Copy local log file to EOS directory
        eos_log = ''.join([self.efile_root, ".sys.b#.backup.log?eos.ruid=0&eos.rgid=0"])
        self.logger.debug("Copy log:{0} to {1}".format(self.config.LOG_FILE, eos_log))
        self.config.handler.flush()
        cp_client = client.FileSystem(self.efile_full)
        st, __ = cp_client.copy(self.config.LOG_FILE, eos_log, force=True)

        if not st.ok:
            self.logger.error(("Failed to copy log file {0} to EOS at {1}"
                               "").format(self.config.LOG_FILE, eos_log))
        else:
            # Delete log file if it was successfully copied to EOS
            try:
                os.remove(self.config.LOG_FILE)
            except OSError as __:
                pass

        # Delete all local files associated with this transfer
        try:
            os.remove(self.tx_file)
        except OSError as __:
            pass

        # Join async status thread
        self.thread_status.do_finish()
        self.thread_status.join()

    def archive_tx_clean(self, check_ok):
        """ Clean the transfer by renaming the archive file in EOS adding the
        following extensions:
        .done - the transfer was successful
        .err  - there were errors during the transfer. These are logged in the
             file .archive.log in the same directory.

        Args:
            check_ok (bool): True if no error occured during transfer,
                otherwise false.
        """
        # Rename arch file in EOS to reflect the status
        if not check_ok:
            eosf_rename = ''.join([self.efile_root, self.config.ARCH_FN, ".", self.oper, ".err"])
        else:
            eosf_rename = ''.join([self.efile_root, self.config.ARCH_FN, ".", self.oper, ".done"])

        old_url = client.URL(self.efile_full)
        new_url = client.URL(eosf_rename)
        frename = ''.join([old_url.protocol, "://", old_url.hostid, "//proc/user/?",
                           "mgm.cmd=file&mgm.subcmd=rename&mgm.path=", old_url.path,
                           "&mgm.file.source=", old_url.path,
                           "&mgm.file.target=", new_url.path])
        (status, __, stderr) = exec_cmd(frename)

        if not status:
            err_msg = ("Failed to rename {0} to {1}, msg={2}"
                       "").format(self.efile_full, eosf_rename, stderr)
            self.logger.error(err_msg)
            # TODO: raise IOError
        else:
            # For successful delete operations remove also the archive file
            if self.oper == self.config.DELETE_OP and check_ok:
                fs = client.FileSystem(self.efile_full)
                st_rm, __ = fs.rm(new_url.path + "?eos.ruid=0&eos.rgid=0")

                if not st_rm.ok:
                    warn_msg = "Failed to delete archive {0}".format(new_url.path)
                    self.logger.warning(warn_msg)

        # Copy local log file back to EOS directory and set the ownership to the
        # identity of the client who triggered the archive
        dir_root = self.efile_root[self.efile_root.rfind('//') + 1:]
        eos_log = ''.join([old_url.protocol, "://", old_url.hostid, "/",
                           dir_root, self.config.ARCH_FN, ".log?eos.ruid=0&eos.rgid=0"])

        self.logger.debug("Copy log:{0} to {1}".format(self.config.LOG_FILE, eos_log))
        self.config.handler.flush()
        cp_client = client.FileSystem(self.efile_full)
        st, __ = cp_client.copy(self.config.LOG_FILE, eos_log, force=True)

        if not st.ok:
            self.logger.error(("Failed to copy log file {0} to EOS at {1}"
                               "").format(self.config.LOG_FILE, eos_log))
        else:
            # User triggering archive operation owns the log file
            eos_log_url = client.URL(eos_log)
            fs = client.FileSystem(eos_log)
            arg = ''.join([eos_log_url.path, "?eos.ruid=0&eos.rgid=0&mgm.pcmd=chown&uid=",
                           self.uid, "&gid=", self.gid])
            xrd_st, __ = fs.query(QueryCode.OPAQUEFILE, arg)

            if not xrd_st.ok:
                err_msg = ("Failed setting ownership of the log file in"
                           " EOS: {0}").format(eos_log)
                self.logger.error(err_msg)
                raise IOError(err_msg)
            else:
                # Delete log if successfully copied to EOS and changed ownership
                try:
                    os.remove(self.config.LOG_FILE)
                except OSError as __:
                    pass

        # Delete all local files associated with this transfer
        try:
            os.remove(self.tx_file)
        except OSError as __:
            pass

        # Join async status thread
        self.thread_status.do_finish()
        self.thread_status.join()

    def copy_files(self, err_entry=None, found_checkpoint=False):
        """ Copy files.

        Note that when doing PUT the layout is not conserved. Therefore, a file
        with 3 replicas will end up as just a simple file in the new location.

        Args:
            err_entry (list): Entry record from the archive file corresponding
                 to the first file/dir that was corrupted.
            found_checkpoint (boolean): If True it means the checkpoint was
                 already found and we don't need to search for it.

        Raises:
            IOError: Copy request failed.
        """
        indx_file = 0
        # For inital PUT copy also the archive file to tape
        if self.init_put:
            # The archive init is already renamed to archive.put.err at this
            # and we need to take this into consideration when trasferring it
            url = client.URL(self.efile_full)
            eos_fs = client.FileSystem(self.efile_full)
            st_stat, resp = eos_fs.stat(url.path)

            if st_stat.ok:
                __, dst = self.archive.get_endpoints(self.config.ARCH_INIT)
                self.list_jobs.append((self.efile_full + "?eos.ruid=0&eos.rgid=0" +
                                       "&eos.app=archive", dst, resp.size))
            else:
                err_msg = ''.join(["Failed to get init archive file info, msg=",
                                   st_stat.message])
                self.logger.error(err_msg)
                raise IOError(err_msg)

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
            msg = "copy file {0}/{1}".format(indx_file, self.archive.header['num_files'])
            self.set_status(msg)
            src, dst = self.archive.get_endpoints(fentry[1])
            dfile = dict(zip(self.archive.header['file_meta'], fentry[2:]))

            # Copy file
            if not self.archive.d2t:
                # For GET we also have the dictionary with the metadata

                dst = ''.join([dst, "?eos.ctime=", dfile['ctime'],
                               "&eos.mtime=", dfile['mtime'],
                               "&eos.bookingsize=", dfile['size'],
                               "&eos.targetsize=", dfile['size'],
                               "&eos.ruid=0&eos.rgid=0&eos.app=archive"])

                # If checksum 0 don't enforce it
                if dfile['xs'] != "0":
                    dst = ''.join([dst, "&eos.checksum=", dfile['xs']])

                # For backup we try to read as root from the source
                if self.oper == self.config.BACKUP_OP:
                    if '?' in src:
                        src = ''.join([src, "&eos.ruid=0&eos.rgid=0&eos.app=archive"])
                    else:
                        src = ''.join([src, "?eos.ruid=0&eos.rgid=0&eos.app=archive"])

                    # If this is a version file we save it as a 2-replica layout
                    if is_version_file(fentry[1]):
                        dst = ''.join([dst, "&eos.layout.checksum=", dfile['xstype'],
                                       "&eos.layout.type=replica&eos.layout.nstripes=2"])

                    # If time window specified then select only the matching entries
                    if (self.archive.header['twindow_type'] and
                            self.archive.header['twindow_val']):
                        twindow_sec = int(self.archive.header['twindow_val'])
                        tentry_sec = int(float(dfile[self.archive.header['twindow_type']]))

                        if tentry_sec < twindow_sec:
                            continue
            else:
                # For PUT read the files from EOS as root
                src = ''.join([src, "?eos.ruid=0&eos.rgid=0&eos.app=archive"])

            self.logger.info("Copying from {0} to {1}".format(src, dst))
            self.list_jobs.append((src, dst, dfile['size']))

            if len(self.list_jobs) >= self.config.BATCH_SIZE:
                st = self.flush_files(False)

                # For archives we fail immediately, for backups it's best-effort
                if not st and self.oper != self.config.BACKUP_OP:
                    err_msg = "Failed to flush files"
                    self.logger.error(err_msg)
                    raise IOError(err_msg)

        # Flush all pending copies and set metadata info for GET operation
        st = self.flush_files(True)

        if not st and self.oper != self.config.BACKUP_OP:
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
        while len(self.threads) >= self.config.MAX_THREADS:
            remove_indx, retry_threads = [], []

            for indx, thread in enumerate(self.threads):
                thread.join(self.config.JOIN_TIMEOUT)

                # If thread finished get the status and mark it for removal
                if not thread.isAlive():
                    # If failed then attempt a retry
                    if (not thread.xrd_status.ok and
                        thread.retries <= self.config.MAX_RETRIES):

                        self.logger.log(logging.INFO,
                                        ("Thread={0} failed, retries={1}").format
                                        (thread.ident, thread.retries))
                        rthread = ThreadJob(thread.lst_jobs, thread.retries)
                        rthread.start()
                        retry_threads.append(rthread)
                        remove_indx.append(indx)
                        self.logger.log(logging.INFO,("New thread={0} doing a retry").format
                                        (rthread.ident))
                        continue

                    status = status and thread.xrd_status.ok
                    log_level = logging.INFO if thread.xrd_status.ok else logging.ERROR
                    self.logger.log(log_level,("Thread={0} status={1} msg={2}").format
                                    (thread.ident, thread.xrd_status.ok,
                                     thread.xrd_status.message))
                    remove_indx.append(indx)
                    break

            # Remove old/finished threads and add retry ones. For removal we
            # need to start with big indexes first.
            remove_indx.reverse()

            for indx in remove_indx:
                del self.threads[indx]

            self.threads.extend(retry_threads)
            del retry_threads[:]
            del remove_indx[:]

        # If we still have jobs and previous archive jobs were successful or this
        # is a backup operartion (best-effort even if we have failed transfers)
        if (self.list_jobs and ((self.oper != self.config.BACKUP_OP and status) or
                                (self.oper == self.config.BACKUP_OP))):
            thread = ThreadJob(self.list_jobs)
            thread.start()
            self.threads.append(thread)
            del self.list_jobs[:]

        # If a previous archive job failed or we need to wait for all jobs to
        # finish then join the threads and collect their status
        if (self.oper != self.config.BACKUP_OP and not status) or wait_all:
            remove_indx, retry_threads = [], []

            while self.threads:
                for indx, thread in enumerate(self.threads):
                    thread.join()

                    # If failed then attempt a retry
                    if (not thread.xrd_status.ok and
                        thread.retries <= self.config.MAX_RETRIES):

                        self.logger.log(logging.INFO, ("Thread={0} failed, retries={1}").format
                                        (thread.ident, thread.retries))
                        rthread = ThreadJob(thread.lst_jobs, thread.retries)
                        rthread.start()
                        retry_threads.append(rthread)
                        remove_indx.append(indx)
                        self.logger.log(logging.INFO,("New thread={0} doing a retry").format
                                        (rthread.ident))
                        continue

                    status = status and thread.xrd_status.ok
                    log_level = logging.INFO if thread.xrd_status.ok else logging.ERROR
                    self.logger.log(log_level, ("Thread={0} status={1} msg={2}").format
                                    (thread.ident, thread.xrd_status.ok,
                                     thread.xrd_status.message))
                    remove_indx.append(indx)

                # Remove old/finished threads and add retry ones. For removal we
                # need to start with big indexes first.
                remove_indx.reverse()

                for indx in remove_indx:
                    del self.threads[indx]

                self.threads.extend(retry_threads)
                del retry_threads[:]
                del remove_indx[:]

        return status

    def update_file_access(self, err_entry=None, found_checkpoint=False):
        """ Set the ownership and the permissions for the files copied to EOS.
        This is done only for GET operation i.e. self.archive.d2t == False.

        Args:
           err_entry (list): Entry record from the archive file corresponding
               to the first file/dir that was corrupted.
           found_checkpoint (boolean): If True, it means the checkpoint was
                 already found and we don't need to search for it i.e. the
                 corrupted entry is a directory.

        Raises:
            IOError: chown or chmod operations failed
        """
        if self.archive.d2t:
            return

        self.set_status("updating file access")
        t0 = time.time()
        oper = 'query'
        metahandler = MetaHandler()
        fs = self.archive.fs_src

        for fentry in self.archive.files():
            # If backup operation and time window specified then update only matching ones
            if self.oper == self.config.BACKUP_OP:
                if self.archive.header['twindow_type'] and self.archive.header['twindow_val']:
                    dfile = dict(zip(self.archive.header['file_meta'], fentry[2:]))
                    twindow_sec = int(self.archive.header['twindow_val'])
                    tentry_sec = int(float(dfile[self.archive.header['twindow_type']]))

                    if tentry_sec < twindow_sec:
                        continue

            # Search for the recovery checkpoint
            if err_entry and not found_checkpoint:
                if fentry != err_entry:
                    continue
                else:
                    found_checkpoint = True

            __, surl = self.archive.get_endpoints(fentry[1])
            url = client.URL(surl)
            dict_meta = dict(zip(self.archive.header['file_meta'], fentry[2:]))

            # Send the chown async request
            arg = ''.join([url.path, "?eos.ruid=0&eos.rgid=0&mgm.pcmd=chown&uid=",
                           dict_meta['uid'], "&gid=", dict_meta['gid']])
            xrd_st = fs.query(QueryCode.OPAQUEFILE, arg,
                              callback=metahandler.register(oper, surl))

            if not xrd_st.ok:
                __ = metahandler.wait(oper)
                err_msg = "Failed query chown for path={0}".format(surl)
                self.logger.error(err_msg)
                raise IOError(err_msg)

            # Send the chmod async request
            mode = int(dict_meta['mode'], 8) # mode is saved in octal format
            arg = ''.join([url.path, "?eos.ruid=0&eos.rgid=0&mgm.pcmd=chmod&mode=",
                           str(mode)])
            xrd_st = fs.query(QueryCode.OPAQUEFILE, arg,
                              callback=metahandler.register(oper, surl))

            if not xrd_st.ok:
                __ = metahandler.wait(oper)
                err_msg = "Failed query chmod for path={0}".format(surl)
                self.logger.error(err_msg)
                raise IOError(err_msg)

            # Send the utime async request to set the mtime
            mtime = dict_meta['mtime']
            mtime_sec, mtime_nsec = mtime.split('.', 1)
            ctime = dict_meta['ctime']
            ctime_sec, ctime_nsec = ctime.split('.', 1)
            arg = ''.join([url.path, "?eos.ruid=0&eos.rgid=0&mgm.pcmd=utimes",
                           "&tv1_sec=", ctime_sec, "&tv1_nsec=", ctime_nsec,
                           "&tv2_sec=", mtime_sec, "&tv2_nsec=", mtime_nsec])
            xrd_st = fs.query(QueryCode.OPAQUEFILE, arg,
                              callback=metahandler.register(oper, surl))

            if not xrd_st.ok:
                __ = metahandler.wait(oper)
                err_msg = "Failed query utimes for path={0}".format(surl)
                self.logger.error(err_msg)
                raise IOError(err_msg)

        status = metahandler.wait(oper)

        if status:
            t1 = time.time()
            self.logger.info("TIMING_update_file_access={0} sec".format(t1 - t0))
        else:
            err_msg = "Failed update file access"
            self.logger.error(err_msg)
            raise IOError(err_msg)

    def check_previous_tx(self):
        """ Find checkpoint for a previous run. There are two types of checks
        being done:
        - transfer check = verify that the files exist and have the correct
                           size and checksum
        - metadata check = verify that all the entries have the correct meta-
                           data values set

        Returns:
           (tx_ok, meta_ok, lst_failed): Tuple holding the status of the
           different checks and the list of corrupted entries.
        """
        msg = "verify last run"
        self.set_status(msg)
        meta_ok = False
        # Check for existence, file size and checksum
        tx_ok, lst_failed = self.archive.verify(False, True)

        if tx_ok:
            meta_ok, lst_failed = self.archive.verify(False, False)

            if meta_ok:
                self.do_retry = False
                raise NoErrorException()

        # Delete the corrupted entry if this is a real transfer error
        if not tx_ok:
            err_entry = lst_failed[0]
            is_dir = (err_entry[0] == 'd')
            self.logger.info("Delete corrupted entry={0}".format(err_entry))

            if is_dir:
                self.archive.del_subtree(err_entry[1], None)
            else:
                self.archive.del_entry(err_entry[1], False, None)

        return (tx_ok, meta_ok, lst_failed)


    def prepare2get(self, err_entry=None, found_checkpoint=False):
        """This method is only executed for GET operations and its purpose is
        to issue the Prepapre2Get commands for the files in the archive which
        will later on be copied back to EOS.

        Args:
            err_entry (list): Entry record from the archive file corresponding
                 to the first file/dir that was corrupted.
            found_checkpoint (bool): If True it means the checkpoint was
                 already found and we don't need to search for it.

        Raises:
            IOError: The Prepare2Get request failed.
        """
        if self.archive.d2t:
            return

        count = 0
        limit = 50  # max files per prepare request
        oper = 'prepare'
        self.set_status("prepare2get")
        t0 = time.time()
        lpaths = []
        status = True
        metahandler = MetaHandler()

        for fentry in self.archive.files():
            # Find error checkpoint if not already found
            if err_entry and not found_checkpoint:
                if fentry != err_entry:
                    continue
                else:
                    found_checkpoint = True

            count += 1
            surl, __ = self.archive.get_endpoints(fentry[1])
            lpaths.append(surl[surl.rfind('//') + 1:].encode())

            if len(lpaths) == limit:
                xrd_st = self.archive.fs_dst.prepare(lpaths, PrepareFlags.STAGE,
                                                     callback=metahandler.register(oper, surl))

                if not xrd_st.ok:
                    __ = metahandler.wait(oper)
                    err_msg = "Failed prepare2get for path={0}".format(surl)
                    self.logger.error(err_msg)
                    raise IOError(err_msg)

                # Wait for batch to be executed
                del lpaths[:]
                status = status and metahandler.wait(oper)
                self.logger.debug(("Prepare2get done count={0}/{1}"
                                   "").format(count, self.archive.header['num_files']))

                if not status:
                    break

        # Send the remaining requests
        if lpaths and status:
            xrd_st = self.archive.fs_dst.prepare(lpaths, PrepareFlags.STAGE,
                                                 callback=metahandler.register(oper, surl))

            if not xrd_st.ok:
                __ = metahandler.wait(oper)
                err_msg = "Failed prepare2get"
                self.logger.error(err_msg)
                raise IOError(err_msg)

            # Wait for batch to be executed
            del lpaths[:]
            status = status and metahandler.wait(oper)

        if status:
            t1 = time.time()
            self.logger.info("TIMING_prepare2get={0} sec".format(t1 - t0))
        else:
            err_msg = "Failed prepare2get"
            self.logger.error(err_msg)
            raise IOError(err_msg)

        # Wait for all the files to be on disk
        for fentry in self.archive.files():
            surl, __ = self.archive.get_endpoints(fentry[1])
            url = client.URL(surl)

            while True:
                st_stat, resp_stat = self.archive.fs_dst.stat(url.path)

                if not st_stat.ok:
                    err_msg = "Error stat entry={0}".format(surl)
                    self.logger.error(err_msg)
                    raise IOError()

                # Check if file is on disk
                if resp_stat.flags & StatInfoFlags.OFFLINE:
                    self.logger.info("Sleep 5 seconds, file not on disk entry={0}".format(surl))
                    sleep(5)
                else:
                    break

        self.logger.info("Finished prepare2get, all files are on disk")


    def evict_disk_cache(self):
        """ Send a prepare eviect request to the CTA so that the files are
        removed from the disk cached of the tape system.
        """
        batch_size = 100
        timeout = 10
        batch = []
        # @todo(esindril) use the XRootD proived flag once this is
        # available in the Python interface
        xrd_prepare_evict_flag = 0x000100000000

        for fentry in self.archive.files():
            __, dst = self.archive.get_endpoints(fentry[1])
            url = client.URL(dst)
            batch.append(url.path)

            if len(batch) == batch_size:
                fs = self.archive.get_fs(dst)
                prep_stat, __ = fs.prepare(batch, xrd_prepare_evict_flag, 0, timeout)
                batch.clear()

                if not prep_stat.ok:
                    self.logger.warning("Failed prepare evit for batch")

        if len(batch) != 0:
            fs = self.archive.get_fs(dst)
            prep_stat, __ = fs.prepare(batch, xrd_prepare_evict_flag, 0, timeout)
            batch.clear()

            if not prep_stat.ok:
                self.logger.warning("Failed prepare evit for batch")

        self.logger.info("Finished sending all the prepare evict requests")


    def wait_on_tape(self):
        """ Check and wait that all the files are on tape, which in our case
        means checking the "m" bit. If file is not on tape then suspend the
        current thread for a period between 1 and 10 minutes depending on the
        index of the failed file.
        """
        min_timeout, max_timeout = 5, 1

        while True:
            indx = 0 # index of the first file not on tape
            all_on_tape = True

            for fentry in self.archive.files():
                indx += 1
                __, dst = self.archive.get_endpoints(fentry[1])
                url = client.URL(dst)
                st_stat, resp_stat = self.archive.fs_dst.stat(url.path)

                if not st_stat.ok:
                    err_msg = "Error stat entry={0}".format(dst)
                    self.logger.error(err_msg)
                    raise IOError()

                # Check file is on tape
                if resp_stat.size != 0 and not (resp_stat.flags & StatInfoFlags.BACKUP_EXISTS):
                    self.logger.debug("File {0} is not yet on tape".format(dst))
                    all_on_tape = False
                    break

            if all_on_tape:
                break
            else:
                # Set timeout value
                ratio = indx / int(self.archive.header['num_files'])
                timeout = int(max_timeout * (1 - ratio))

                if timeout < min_timeout:
                    timeout = min_timeout

                self.logger.info("Going to sleep for {0} seconds".format(timeout))
                sleep(timeout)

    def backup_prepare(self):
        """ Prepare requested backup operation.

        Raises:
            IOError: Failed to transfer backup file.
        """
        # Copy backup file from EOS to the local disk
        self.logger.info(("Prepare backup copy from {0} to {1}"
                          "").format(self.efile_full, self.tx_file))
        eos_fs = client.FileSystem(self.efile_full)
        st, _ = eos_fs.copy((self.efile_full + "?eos.ruid=0&eos.rgid=0"),
                            self.tx_file, True)

        if not st.ok:
            err_msg = ("Failed to copy backup file={0} to local disk at={1} err_msg={2}"
                       "").format(self.efile_full, self.tx_file, st.message)
            self.logger.error(err_msg)
            raise IOError(err_msg)

        # Create the ArchiveFile object for the backup which is similar to a
        # tape to disk transfer
        self.archive = ArchiveFile(self.tx_file, False)

        # Check that the destination directory exists and has mode 777, if
        # forced then skip checks
        if not self.force:
            surl = self.archive.header['dst']
            url = client.URL(surl)
            fs = self.archive.get_fs(surl)
            st_stat, resp_stat = fs.stat((url.path, + "?eos.ruid=0&eos.rgid=0"))

            if st_stat.ok:
                err_msg = ("Failed to stat backup destination url={0}"
                           "").format(surl)
                self.logger.error(err_msg)
                raise IOError(err_msg)

            if resp_stat.flags != (client.StatInfoFlags.IS_READABLE |
                                   client.StatInfoFlags.IS_WRITABLE):
                err_msg = ("Backup destination url={0} must have move 777").format(surl)
                self.logger.error(err_msg)
                raise IOError(err_msg)


    def do_backup(self):
        """ Perform a backup operation using the provided backup file.
        """
        t0 = time.time()
        indx_dir = 0

        # Root owns the .sys.b#.backup.file
        fs = client.FileSystem(self.efile_full)
        efile_url = client.URL(self.efile_full)
        arg = ''.join([efile_url.path, "?eos.ruid=0&eos.rgid=0&mgm.pcmd=chown&uid=0&gid=0"])
        xrd_st, __ = fs.query(QueryCode.OPAQUEFILE, arg)

        if not xrd_st.ok:
            err_msg = "Failed setting ownership of the backup file: {0}".format(self.efile_full)
            self.logger.error(err_msg)
            raise IOError(err_msg)

        # Create directories
        for dentry in self.archive.dirs():
            # Do special checks for root directory
            #if dentry[1] == "./":
            #    self.archive.check_root_dir()

            indx_dir += 1
            self.archive.mkdir(dentry)
            msg = "create dir {0}/{1}".format(indx_dir, self.archive.header['num_dirs'])
            self.set_status(msg)

        # Copy files and set metadata information
        self.copy_files()
        self.update_file_access()

        self.set_status("verifying")
        check_ok, lst_failed = self.archive.verify(True)
        self.backup_write_status(lst_failed, check_ok)

        self.set_status("cleaning")
        self.logger.info("TIMING_transfer={0} sec".format(time.time() - t0))
        self.backup_tx_clean()

    def backup_write_status(self, lst_failed, check_ok):
        """ Create backup status file which constains the list of failed files
            to transfer.

            Args:
               lst_filed (list): List of failed file transfers
               check_ok (boolean): True if verification successful, otherwise
                   false
        """
        if not check_ok:
            self.logger.error("Failed verification for {0} entries".format(len(lst_failed)))
            fn_status = ''.join([self.efile_root, ".sys.b#.backup.err.", str(len(lst_failed)),
                                 "?eos.ruid=0&eos.rgid=0"])
        else:
            self.logger.info("Backup successful - no errors detected")
            fn_status = ''.join([self.efile_root, ".sys.b#.backup.done?eos.ruid=0&eos.rgid=0"])

        with client.File() as f:
            f.open(fn_status, OpenFlags.UPDATE | OpenFlags.DELETE)
            offset = 0

            for entry in lst_failed:
                buff = "Failed entry={0}\n".format(entry)
                f.write(buff, offset, len(buff))
                offset += len(buff)
