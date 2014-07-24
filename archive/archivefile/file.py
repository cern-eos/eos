# ------------------------------------------------------------------------------
# File: file.py
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
import json
import time
import logging
import const
from os.path import normpath, relpath, join
from archivefile.asynchandler import MetaHandler
from hashlib import sha256
from XRootD import client
from XRootD.client.flags import MkDirFlags, OpenFlags


class NoErrorException(Exception):
  """ Exception raised in case we were requested to retry an operation but
  after the initial check there were no errors found.
  """
  pass


class ArchiveFile(object):
  """ Interact with a meta archive file.

  Attributes:
    eos_file (string): Location of archive file in EOS. It's a valid URL.
    log_file (string): Transfer log file. Path on the local disk.
    op (string): Operation type: put, get, purge, delete or list
    entries (list): List of entries (file + dirs) in the archive file. It also
      contains a metainfo dictionary as the first element.
    list_jobs (list): List of file copy jobs to be executed.
  """

  def __init__(self, eosf="", operation="", option=""):
    self.logger = logging.getLogger(type(self).__name__)
    self.entries, self.list_jobs = [], []
    self.header = {}
    self.oper = operation
    self.is_mig = (self.oper == const.PUT_OP)
    self.do_retry = (option == const.OPT_RETRY)
    self.force = self.do_retry
    self.efile_full = eosf
    self.efile_url = client.URL(self.efile_full)
    self.efile_root = self.efile_full[:-(len(self.efile_full) - self.efile_full.rfind('/') - 1)]
    local_file = join(const.DIR[self.oper], sha256(self.efile_root).hexdigest())
    self.tx_file = local_file + ".tx"
    self.log_file = local_file + ".log"
    self.ps_file = local_file + ".ps"
    formatter = logging.Formatter(const.LOG_FORMAT)
    log_handler = logging.FileHandler(self.log_file)
    log_handler.setFormatter(formatter)
    self.logger.addHandler(log_handler)
    self.logger.propagate = False
    self.err_entry = None
    self.pid = os.getpid()

  def run(self):
    """ Run requested operation - fist call prepare.
    """
    self.prepare()

    if self.oper == const.PUT_OP or self.oper == const.GET_OP:
      self.do_transfer()
    elif self.oper == const.PURGE_OP:
      self.do_delete(False)
    elif self.oper == const.DELETE_OP:
      self.do_delete(True)

  def prepare(self):
    """ Prepare requested operation.
    """
    # Rename archive file in EOS to reflect the fact that we are processing it
    eosf_rename = ''.join([self.efile_root, const.ARCH_FN, ".", self.oper, ".err"])
    rename_url = client.URL(eosf_rename)
    frename = [rename_url.protocol, "://", rename_url.hostid, "//proc/user/?",
               "mgm.cmd=file&mgm.subcmd=rename"
               "&mgm.path=", self.efile_url.path,
               "&mgm.file.source=", self.efile_url.path,
               "&mgm.file.target=", rename_url.path]
    
    (status, __, stderr) = self.execute_command(''.join(frename))

    if not status:
      err_msg = ("Failed to rename archive file {0} to {1}, msg={2}"
                 "").format(self.efile_full, rename_url, stderr)
      self.logger.error(err_msg)
      raise IOError(err_msg)
    else:
      self.efile_full = eosf_rename

    # Copy archive file from EOS to the local disk
    eos_fs = client.FileSystem(self.efile_full)
    st, _ = eos_fs.copy(self.efile_full + "?eos.ruid=0&eos.rgid=0", self.tx_file, True)

    if not st.ok:
      err_msg = ("Failed to copy archive file={0} to local disk at={1}"
                 "").format(self.efile_full, self.tx_file)
      self.logger.error(err_msg)
      raise IOError(err_msg)

    # For recovery get the first corrupted entry
    if self.do_retry and (self.oper in [const.PUT_OP, const.GET_OP]):
      check_ok, self.err_entry = self.check_transfer()

      if not check_ok:
        # Delete the corrupted entry first
        __, entry_str = self.get_endpoints(self.err_entry[1], self.src, self.dst)
        entry_url = client.URL(entry_str)
        self.logger.debug("Remove entry={0}".format(entry_url.path))
        fs = client.FileSystem(entry_str)
        status_stat, __ = fs.stat(entry_url.path)

        if status_stat.ok:
          if self.err_entry[0] == 'd':
            status_rm, __ = fs.rmdir(entry_url.path)
          elif self.err_entry[0] == 'f':
            status_rm, __ = fs.rm(entry_url.path)
          else:
            err_msg = "Unknown entry type={0}".format(self.err_entry)
            self.logger.error(err_msg)
            raise IOError(err_msg)

          if not status_rm.ok:
            err_msg = "Error removing corrupted entry={0}".format(entry_str)
            self.logger.error(err_msg)
            raise IOError(err_msg)
        else:
          self.logger.debug("Stat for {0} failed - nothing else to do".format(entry_str))
      else:
        self.do_retry = False
        raise NoErrorException()

  def do_delete(self, tape_delete):
    """ Delete archive either from disk (purge) or from tape (delete

    Args:
      tape_delete (boolean): If true delete data from tape, otherwise from disk.

    Raises:
      IOError
    """
    with open(self.tx_file, 'r') as ftrans:
      self.header = json.loads(ftrans.readline())
      self.src = client.URL(self.header['src'])
      self.dst = client.URL(self.header['dst'])
      url = self.dst if tape_delete else self.src

      self.logger.info("Deleting from={0}".format(str(url)))
      fs = client.FileSystem(str(url))
      del_dirs = []
      mutable_dirs = []

      # First remove all the files and then the directories
      for line in ftrans:
        entry = json.loads(line)
        # is_mig is false for both purge and deletion
        src_entry, dst_entry = self.get_endpoints(entry[1], self.src, self.dst)

        if tape_delete:
          entry_url = client.URL(src_entry)
        else:
          entry_url = client.URL(dst_entry)

        if entry[0] == 'd':
          # Don't remove the root directory when purging
          if not tape_delete and str(entry_url) == self.header['src']:
            continue
          del_dirs.append(entry_url.path)
          # Save EOS dirs to be made mutable
          if tape_delete:
            mutable_dirs.append(dst_entry[dst_entry.rfind("//") + 1:])
        elif entry[0] == 'f':
          status_rm, __ = fs.rm(entry_url.path + "?eos.ruid=0&eos.rgid=0")
          if not status_rm.ok:
            status_stat, __ = fs.stat(entry_url.path)
            if status_stat.ok:
              err_msg = "Error removing file={0}".format(str(entry_url))
              self.logger.error(err_msg)
              raise IOError(err_msg)
            else:
              self.logger.warning("already deleted entr={0}".format(str(entry_url)))
        else:
          err_msg = "Unknown entry type={0}".format(entry)
          self.logger.error(err_msg)
          raise IOError(err_msg)

      # Remove the directories from bottom up
      while len(del_dirs):
        entry_path = del_dirs.pop()
        status_rm, __ = fs.rmdir(entry_path + "?eos.ruid=0&eos.rgid=0")

        if not status_rm.ok:
          err_msg = "Error removing dir={0}".format(entry_path)
          self.logger.warning(err_msg)
          # raise IOError(err_msg)

      # Remove immutable flag from the EOS sub-tree
      if tape_delete:
        self.make_mutable(mutable_dirs)

    self.clean_transfer(True)

  def make_mutable(self, list_dirs):
    """ Make the EOS sub-tree mutable

    Args:
      list_dirs (list): List of directories in the EOS sub-tree.

    Raises:
      IOError when operation fails.
    """
    url = self.src  # Always src as this is in EOS

    for dir_path in list_dirs:
      fgetattr = ''.join([url.protocol, "://", url.hostid, "//proc/user/?",
                          "mgm.cmd=attr&mgm.subcmd=get&mgm.attr.key=sys.acl",
                          "&mgm.path=", dir_path])
      (status, stdout, __) = self.execute_command(''.join(fgetattr))

      if not status:
        warn_msg = "Expecting xattr sys.acl for dir={0}, but not found".format(dir_path)
        self.logger.warning(warn_msg)
      else:
        # Remove the 'z:i' rule from the acl list
        acl_val = stdout[stdout.find('=') + 1:]
        pos = acl_val.find('z:i')

        if pos != -1:
          if acl_val[pos - 1] == ',':
            acl_val = acl_val[:pos - 1] + acl_val[pos + 3:]
          elif acl_val[pos + 3] == ',':
            acl_val = acl_val[pos + 3:]
          else:
            acl_val = ''

        if acl_val:
          # Set the new sys.acl xattr
          fmutable = ''.join([url.protocol, "://", url.hostid, "//proc/user/?",
                              "mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl",
                              "&mgm.attr.value=", acl_val, "&mgm.path=", dir_path])
          (status, __, stderr) = self.execute_command(''.join(fmutable))

          if not status:
            err_msg = "Error making dir={0} mutable, msg={1}".format(
              dir_path, stderr)
            self.logger.error(err_msg)
            raise IOError(err_msg)
        else:
          # sys.acl empty, remove it from the xattrs
          frmattr = [url.protocol, "://", url.hostid, "//proc/user/?",
                     "mgm.cmd=attr&mgm.subcmd=rm&mgm.attr.key=sys.acl",
                     "&mgm.path=", dir_path]
          (status, __, stderr) = self.execute_command(''.join(frmattr))

          if not status:
            err_msg = "Error removing xattr=sys.acl for dir={0}, msg={1}".format(
              dir_path, stderr)
            self.logger.error(err_msg)
            raise IOError(err_msg)

  def do_transfer(self):
    """ Execute the put or get operation. What this method actually does is copy
    the JSON archive file from EOS to the local disk and read-in each entry, be
    it a file or a directory and creates it in the destination location.
    The archive file first contains the list of all the directories and then the
    files.

    Raises:
      IOError when an IO opperations fails.
    """
    tstart = time.time()
    done_dirs = False        # set flag after creating dir hierarchy
    found_checkpoint = False  # flag set when reaching recovery entry
    meta_handler = MetaHandler()
    indx_dir = 0
    indx_file = 0

    with open(self.tx_file, 'r') as ftrans:
      with open(self.ps_file, 'w') as fprogress:
        # TODO: maybe review this if there are too many directories since it would take up
        # some memory. The alternative is to rewind the file and parse again the meta data
        # of the directories and apply it.
        ldirs = []   # list of directory full URL and meta data dictionary
        self.header = json.loads(ftrans.readline())
        self.src = client.URL(self.header['src'])
        self.dst = client.URL(self.header['dst'])

        if self.is_mig:
          fs = client.FileSystem(str(self.dst))
        else:
          fs = client.FileSystem(str(self.src))

        for line in ftrans:
          entry = json.loads(line)

          # Search for the recovery checkpoint
          if self.do_retry and not found_checkpoint:
            if entry != self.err_entry:
              if entry[0] == 'd':
                indx_dir += 1
              else:
                indx_file += 1
              continue
            else:
              found_checkpoint = True

          if entry[0] == 'd':
            indx_dir += 1
            self.write_progress(fprogress, "creating dir {0}/{1}".format(
                indx_dir, self.header['num_dirs']))
            dict_dinfo = dict(zip(self.header['dir_meta'], entry[2:]))
            __, dst = self.get_endpoints(entry[1], self.src, self.dst)
            ldirs.append((dst, dict_dinfo))
            dst_url = client.URL(dst)

            if not self.do_retry and (dst_url.path in [self.src.path, self.dst.path]):
              self.check_root_dir(dst_url)

            # Create directory
            st = fs.mkdir(dst_url.path + "?eos.ruid=0&eos.rgid=0", MkDirFlags.MAKEPATH,
                          callback=meta_handler.register_mkdir(dst_url.path))

            if not st.ok:
              err_msg = "Dir={0}, failed to create hierarchy".format(dst)
              self.logger.error(err_msg)
              raise IOError(err_msg)

          elif entry[0] == 'f':
            # We are dealing with a file, first check that all dirs were created
            if not done_dirs:
              done_dirs = True
              st_async = meta_handler.wait_mkdir()

              if not st_async:
                err_msg = "Dir={0}, failed to create hierarchy".format(
                  self.header['dst'])
                self.logger.error(err_msg)
                raise IOError(err_msg)
              else:
                # For GET set directory metadata information: ownership, xattrs
                if not self.is_mig:
                  for dentry in ldirs:
                    # Skip the root directory from EOS as it's already set
                    if dentry[0] is not self.src.path:
                      st = self.dir_set_metadata(dentry)

                      if not st:
                        err_msg = ("Dir={0}, failed to set metadata on directory"
                                   "").format(dentry[0])
                        self.logger.error(err_msg)
                        raise IOError(err_msg)

            indx_file += 1
            self.write_progress(fprogress, "copying file {0}/{1}".format(
                indx_file, self.header['num_files']))
            path = entry[1]
            dict_finfo = dict(zip(self.header['file_meta'], entry[2:]))
            src, dst = self.get_endpoints(path, self.src, self.dst)

            # Copy file from source to destination
            if self.is_mig:
              st = self.copy_file(src, dst, force=self.force)
            else:
              st = self.copy_file(src, dst, dict_finfo, self.force)

            if st is not None and not st:
              self.logger.error("Failed to copy src={0}, dst={1}".format(src, dst))
              break

          else:
            self.logger.error("Unkown type of entry={0} in archive file".format(entry))
            break

        # Flush all pending copies and set metadata info for GET operation
        st = self.flush_files(meta_handler)

        if st is not None and not st.ok:
          err_msg = "Failed to flush files"
          self.logger.error(err_msg)
          raise IOError(err_msg)

        self.write_progress(fprogress, "checking")
        check_ok, __ = self.check_transfer()
        self.write_progress(fprogress, "cleaning")
        self.logger.debug("Transfer wall time={0}".format(time.time() - tstart))
        self.clean_transfer(check_ok)

  def check_root_dir(self, url):
    """ Do the necessary checks for the destination directory depending on the
    type of the transfer.

    Args:
      url (client.URL): Destination directory URL.

    Return:
      True if check successful, otherwise false.
    """
    str_url = str(url)
    fs = client.FileSystem(str_url)
    st, __ = fs.stat(url.path + "?eos.ruid=0&eos.rgid=0")

    if self.is_mig and st.ok:
      # For put destination must NOT exist
      err_msg = "Root put destination={0} exists".format(str_url)
      self.logger.error(err_msg)
      raise IOError(err_msg)
    elif not self.is_mig:
      # For get destination must exist and contain just the archive file
      if not st.ok:
        err_msg = "Root get destination={0} does NOT exist".format(str_url)
        self.logger.error(err_msg)
        raise IOError(err_msg)
      else:
        ffindcount = [url.protocol, "://", url.hostid, "//proc/user/?"
                      "mgm.cmd=find&mgm.path=", url.path, "&mgm.option=Z"]
        (status, stdout, stderr) = self.execute_command(''.join(ffindcount))

        if status:
          for entry in stdout.split():
            tag, num = entry.split('=')
            self.logger.debug("Tag={0}, val={1}".format(tag, num))

            if ((tag == 'nfiles' and int(num) != 1 and int(num) != 2) or
                (tag == 'ndirectories' and int(num) != 1)):
              err_msg = ("Root GET destination={0} should contain at least "
                         "one file and at most two - clean up and try again"
                         "").format(str_url)
              self.logger.error(err_msg)
              raise IOError(err_msg)
        else:
          err_msg = ("Error doing find count on root GET destination={0} "
                     "msg={1}").format(str_url, stderr)
          self.logger.error(err_msg)
          raise IOError(err_msg)

  def write_progress(self, f, msg):
    """ Write progress message to the local progress file.

    Args:
      f (file): Python file object.
      msg (string): Progress status message.
    """
    f.truncate(0)
    f.write("pid={0} msg={1}".format(self.pid, msg))
    f.flush()
    os.fsync(f.fileno())

  def check_transfer(self):
    """ Check the integrity of a transfer.

    Returns:
      (status, entry) - Status is true if transfer is correct, otherwise false.
      In case the transfer failed return also the entry from the archive file
      where the first error occured, otherwise return an empty list.
    """
    ret_entry = []
    status = True

    with open(self.tx_file, 'r') as ftrans:
      self.header = json.loads(ftrans.readline())
      self.src = client.URL(self.header['src'])
      self.dst = client.URL(self.header['dst'])

      for line in ftrans:
        entry = json.loads(line)
        st = self.check_entry(entry)

        if not st:
          self.logger.error("Failed check for: {0}".format(entry[1]))
          status = False
          ret_entry = entry
          break

    return (status, ret_entry)

  def check_entry(self, entry):
    """ Check that the entry (file/dir) has the proper meta data.

    Args:
      entry (string): Entry from the arhive file containing all info
        about that particular file/directory.

    Returns:
      True if the entry is correct, otherwise false.
    """
    status = True
    is_dir = (entry[0] == 'd')
    path = entry[1]
    __, dst = self.get_endpoints(path, self.src, self.dst)
    url = client.URL(dst)

    if self.is_mig:
      # Just check that the entry exists and its size
      fs = client.FileSystem(dst)
      st, stat_info = fs.stat(url.path)
      status = st.ok

      if not is_dir and status:
        indx = self.header["file_meta"].index("size") + 2
        orig_size = int(entry[indx])

        if stat_info.size != orig_size:
          self.logger.error("File={0}, size={1}, expected size={2}".format(
              dst, orig_size, stat_info.size))
          status = False

    else:
      # For GET check all metadata
      try:
        if is_dir:
          tags = ['uid', 'gid', 'attr']
        else:
          tags = ['size', 'mtime', 'ctime', 'uid', 'gid', 'xstype', 'xs']

        meta_info = self.entry_info(url, tags, is_dir)
        if not is_dir:
          # TODO: review this - fix EOS to honour the mtime argument and don't
          # remove it below and check also the file checksum if posssible
          indx = self.header["file_meta"].index("mtime") + 2  # TODO: don't trust me :P
          del meta_info[indx]
          del entry[indx]

      except (AttributeError, IOError) as e:
        self.logger.error(e)
        status = False

      if status and not (meta_info == entry):
        self.logger.error("Check failed for entry={0}, expect={1}, got={2}".
                          format(dst, entry, meta_info))
        status = False

    self.logger.debug("Check={0}, status={1}".format(dst, status))
    return status

  def clean_transfer(self, check_ok):
    """ Clean the transfer by renaming the archive file in EOS adding the
    following extensions:
    .done - the transfer was successful
    .err  - there were errors during the transfer. These are logged in the
            file archive.log in the same directory.

    Args:
      check_ok (bool): True if no error occured during transfer, otherwise false.
    """
    if not check_ok:
      eosf_rename = ''.join([self.efile_root, const.ARCH_FN, ".", self.oper, ".err"])
    else:
      eosf_rename = ''.join([self.efile_root, const.ARCH_FN, ".", self.oper, ".done"])

    # Rename arch file in EOS to reflect the status
    old_url = client.URL(self.efile_full)
    new_url = client.URL(eosf_rename)

    frename = [old_url.protocol, "://", old_url.hostid, "//proc/user/?",
               "mgm.cmd=file&mgm.subcmd=rename&mgm.path=", old_url.path,
               "&mgm.file.source=", old_url.path,
               "&mgm.file.target=", new_url.path]

    (status, __, stderr) = self.execute_command(''.join(frename))

    if not status:
      err_msg = "Failed to rename {0} to {1}, msg={2}".format(
        self.efile_full, eosf_rename, stderr)
      self.logger.error(err_msg)
      # TODO: raise IOError
    else:
      # For successful delete operations remove also the archive file
      if self.oper == const.DELETE_OP and check_ok:
        fs = client.FileSystem(self.efile_full)
        status_rm, _ = fs.rm(new_url.path + "?eos.ruid=0&eos.rgid=0")

        if not status_rm.ok:
          warn_msg = "Failed to delete archive {0}".format(new_url.path)
          self.logger.warning(warn_msg)

    # Copy local log file back to EOS directory and set the ownership to the
    # identity of the client who triggered the archive
    try:
      client_uid = self.header["uid"]
      client_gid = self.header["gid"]
    except KeyError as _:
      self.logger.warning("uid/gid not found in header, default to 0/0")
      client_uid = '0'
      client_gid = '0'

    dir_root = self.efile_root[self.efile_root.rfind('//') + 1:]
    eos_log = ''.join([old_url.protocol, "://", old_url.hostid, "/", dir_root,
                       const.ARCH_FN, ".log"])

    self.logger.debug("Copy log:{0} to {1}".format(self.log_file, eos_log))
    cp_client = client.FileSystem(self.efile_full)
    st, __ = cp_client.copy(self.log_file, eos_log + "?eos.ruid=0&eos.rgid=0", force=True)

    if not st.ok:
      self.logger.error("Failed to copy log file {0} to EOS at {1}".format(
          self.log_file, eos_log))

    fsetowner = [old_url.protocol, "://", old_url.hostid, "//proc/user/?",
                 "mgm.cmd=chown&mgm.path=", dir_root, const.ARCH_FN, ".log",
                 "&mgm.chown.owner=", client_uid, ":", client_gid]
    (status, __, stderr) = self.execute_command(''.join(fsetowner))
    
    if not status:
      self.logger.error(("Failed chown EOS log file in dir={0}, msg=%{2}"
                         "").format(dir_root, stderr))

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

  def dir_set_metadata(self, entry):
    """ Set directory meta data information such as ownership and xattrs.

    Args:
      entry (tuple): Tuple of two elements: full URL of directory and 
        dictionary containing meta data information.

    Returns:
      True if directory metadata set successfully, otherwise false.
    """
    path, dict_dinfo = entry
    url = client.URL(path)

    # Change ownership of the directory
    fsetowner = [url.protocol, "://", url.hostid, "//proc/user/?",
                 "mgm.cmd=chown&mgm.path=", url.path,
                 "&mgm.chown.owner=", dict_dinfo['uid'], ":", dict_dinfo['gid']]
    (status, stdout, __) = self.execute_command(''.join(fsetowner))

    if status:
      # Remove any existing attributes
      flsattr = [url.protocol, "://", url.hostid, "//proc/user/?",
                 "mgm.cmd=attr&mgm.subcmd=ls&mgm.path=", url.path]

      (status, stdout, stderr) = self.execute_command(''.join(flsattr))

      if status:
        lattrs = [s.split('=', 1)[0] for s in stdout.splitlines()]

        for attr in lattrs:
          frmattr = [url.protocol, "://", url.hostid, "//proc/user/?",
                     "mgm.cmd=attr&mgm.subcmd=rm&mgm.attr.key=", attr,
                     "&mgm.path=", url.path]
          (status, stdout, stderr) = self.execute_command(''.join(frmattr))

          if not status:
            self.logger.error(("Dir={0} error while removing attr={1}, msg={2}"
                               "").format(url.path, attr, stderr))
            break

      if status:
        # Set the expected extended attributes
        dict_dattr = dict_dinfo['attr']

        for key, val in dict_dattr.iteritems():
          fsetattr = [url.protocol, "://", url.hostid, "//proc/user/?",
                      "mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=", key,
                      "&mgm.attr.value=", val, "&mgm.path=", url.path]
          (status, stdout, stderr) = self.execute_command(''.join(fsetattr))

          if not status:
            self.logger.error(("Dir={0}, error while setting attr={1}, msg={2}"
                               "").format(url.path, key, stderr))
            break

    return status

  def execute_command(self, command):
    """ Execute an EOS /proc/user/ command.

    Args:
      command (string): Command to be executed.

    Returns:
      (status, stdout, stderr) which are of type (bool, string) - the status is
         true if command was successful, otherwise false. If data need to be
         returned then it's set in stdout and in case of an error the error
         message is in stderr.
    """
    status = False
    retc, stdout, stderr = "0", "", ""
    # Execute the command as root if role not already set
    if command.find("eos.ruid") == -1:
      if command.find('?') == -1:
        command += "?eos.ruid=0&eos.rgid=0"
      else:
        command += "&eos.ruid=0&eos.rgid=0"

    self.logger.debug("Execute: {0}".format(command))

    with client.File() as f:
      st, __ = f.open(command, OpenFlags.READ)

      if st.ok:
        st, data = f.read(0, 4096)

        if st.ok:
          status = True
          lpairs = data.split('&')

          for elem in lpairs:
            if "mgm.proc.retc=" in elem:
              retc = elem[(elem.index('=') + 1):].strip()

              if retc != "0":
                status = False

            elif "mgm.proc.stdout=" in elem:
              stdout = elem[(elem.index('=') + 1):].strip()
            elif "mgm.proc.stderr=" in elem:
              stderr = elem[(elem.index('=') + 1):].strip()
        else:
          stderr = "error while reading response for command: {0}".format(command)
      else:
        stderr = "error while sending command: {0}".format(command)

    # self.logger.debug("Return command: {0}".format((status, stdout, stderr)))
    return (status, stdout, stderr)

  def copy_file(self, src, dst, dfile=None, force=False, handler=None):
    """ Copy file from source to destination.

    Note that when doing put, the layout is not conserved. Therefore, a file
    with 3 replicas will end up as just a simple file in the new location.

    Args:
      src (string): Source of the copy (full URL).
      dst (string): Destiantion (full URL).
      dfile (dict): Dictionary containing metadata about the file. This is
        set only for get operations.
      force (boolean): Force copy only in recovery mode.
      handler (MetaHanlder): Metahandler obj used for async req.

    Returns:
      True if batch was executed successfully, otherwise false.
    """
    st = None

    # For get we also have the dictionary with the metadata
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

  def flush_files(self, handler):
    """ Flush all pending transfers from the list of jobs.

    Args:
      handler (MetaHandler): Meta handler obj. used for async req.

    Returns:
      The status of the copy process.
    """
    proc = client.CopyProcess()

    for job in self.list_jobs:
      # TODO: do TPC when XRootD 3.3.6 does not crash anymore and use the
      # parallel mode starting with XRootD 4.1

      # TODO: use checksum verification if possible
      proc.add_job(job[0], job[1], force=job[2], thirdparty=False)

    proc.prepare()
    st = proc.run()

    if st.ok:
      self.logger.debug("Batch successful")
    else:
      self.logger.error("Batch error={0}".format(st))

    del self.list_jobs[:]
    return st

  def get_endpoints(self, path, src_url, dst_url):
    """ Get final location of the dir/file depending on the source and the
    destination of the archiving operation but also on its type: put or get

    Args:
      path (string): Path relative to the root_src.
      src_url (XRootD.URL): URL path of the root directory used for get.
      dst_url (XRootD.URL): URL path of the root directory of the archive.

    Returns:
      A pair of strings which represent the source and the destination URL of
      the transfer.
    """
    if path == "./":
      src = str(src_url)
      dst = str(dst_url)
    else:
      src = ''.join([src_url.protocol, "://",
                     src_url.hostid, "/",
                     normpath(src_url.path), "/", path])
      dst = ''.join([dst_url.protocol, "://",
                     dst_url.hostid, "/",
                     normpath(dst_url.path), "/", path])

    if self.is_mig:
      # self.logger.debug("Source: {0}, destination: {1}".format(src, dst))
      return (src, dst)
    else:
      # self.logger.debug("Source: {0}, destination: {1}".format(dst, src))
      return (dst, src)

  def entry_info(self, url, tags, is_dir):
    """ Get file/directory metadata information from source.

    Args:
      url  (XRootD.URL): Full URL to EOS location.
      tags       (list): List of tags to look for in the fileinfo result.
      is_dir  (boolean): If True entry is a directory, otherwise a file.

    Returns:
      A list containing the info corresponding to the tags supplied in the args.

    Raises:
      IOError: Fileinfo request can not be submitted.
      AttributeError: Not all expected tags are provided.
      KeyError: Extended attribute value is not present.
    """
    finfo = [url.protocol, "://", url.hostid, "//proc/user/?",
             "mgm.cmd=fileinfo&mgm.path=", url.path,
             "&mgm.file.info.option=-m"]
    (status, stdout, stderr) = self.execute_command(''.join(finfo))

    if status:
      # Keep only the interesting info for an entry
      lpairs = stdout.split(' ')
      dict_info = {}
      dict_attr = {}
      it_list = iter(lpairs)

      for elem in it_list:
        key, value = elem.split('=', 1)

        if key in tags:
          dict_info[key] = value
        elif key == "xattrn" and is_dir:
          xkey, xval = next(it_list).split('=', 1)

          if xkey != "xattrv":
            raise KeyError("Dir={0} no value for xattribute={1}".format(url.path, value))
          else:
            dict_attr[value] = xval

      # For directories add also the xattr dictionary
      if is_dir and 'attr' in tags:
        dict_info["attr"] = dict_attr

      if len(dict_info) == len(tags):
        # Dir path must end with slash just as the output of EOS fileinfo -d
        tentry = 'd' if is_dir else 'f'
        path = relpath(url.path, self.src.path)
        path += '/' if is_dir else ''
        dinfo = [tentry, path]

        for tag in tags:
          dinfo.append(dict_info[tag])

        return dinfo
      else:
        raise AttributeError("Path={0}, not all expected tags found".format(url.path))
    else:
      raise IOError("Path={0}, failed fileinfo request, msg={1}".format(url.path, stderr))
