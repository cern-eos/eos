# ------------------------------------------------------------------------------
# File: archivefile.py
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
""" Class modelling an EOS archive file.
"""
from __future__ import unicode_literals
import logging
import json
from XRootD import client
from XRootD.client.flags import QueryCode
from eosarch.utils import is_atomic_version_file, seal_path
from eosarch.utils import exec_cmd, get_entry_info, set_dir_info
from eosarch.exceptions import CheckEntryException


class ArchiveFile(object):
    """ Class modelling an EOS archive file.

    Attributes:
        file: File object pointing to local archive file.
        d2t: True if operation from disk to tape, otherwise False. For backup
             operations we consider it as a transfer from tape to disk thus it
             is False.
        header: Archive header dictionary.
    """

    def __init__(self, path, d2t):
        """Initialize ArchiveFile object.

        Args:
            path (str): Local path to archive file.
            d2t (bool): True if transfer is to be disk to tape.

        Raises:
            IOError: Failed to open local transfer file.
        """
        self.logger = logging.getLogger("transfer")
        self.d2t = d2t

        try:
            self.file = open(path, 'r')
        except IOError as __:
            self.logger.error("Failed to open file={0}".format(path))
            raise

        line = self.file.readline().decode("utf-8")
        self.header = json.loads(line)
        self.fseek_dir = self.file.tell()  # save start position for dirs
        pos = self.fseek_dir

        while line:
            line = self.file.readline().decode("utf-8")
            entry = json.loads(line)

            if entry[0] == 'f':
                self.fseek_file = pos  # save start position for files
                break

            pos = self.file.tell()

        # Create two XRootD.FileSystem object for source and destination
        # which are to be reused throughout the transfer.
        self.fs_src = client.FileSystem(self.header['src'].encode("utf-8"))
        self.fs_dst = client.FileSystem(self.header['dst'].encode("utf-8"))
        self.logger.debug("fseek_dir={0}, fseek_file={1}".format(self.fseek_dir,
                                                                 self.fseek_file))

    def __del__(self):
        """Destructor needs to close the file.
        """
        try:
            self.file.close()
        except ValueError as __:
            self.logger.warning("File={0} already closed".format(self.file.name))

    def dirs(self):
        """Generator to read directory entries from the archive file.

        Returns:
            Return a directory entry from the archive file which looks like
            this: ['d', "./rel/path/dir", "val1", ,"val2" ... ]
        """
        self.file.seek(self.fseek_dir)
        line = self.file.readline().decode("utf-8")

        while line:
            dentry = json.loads(line)

            if dentry[0] == 'd':
                yield dentry
                line = self.file.readline().decode("utf-8")
            else:
                break

    def files(self):
        """Generator to read file entries from the archive file.

        Returns:
            Return a file entry from the archive file which looks like this:
            ['f', "./rel/path/file", "val1", ,"val2" ... ]
        """
        self.file.seek(self.fseek_file)
        line = self.file.readline().decode("utf-8")

        while line:
            fentry = json.loads(line)

            if fentry[0] == 'f':
                yield fentry
                line = self.file.readline().decode("utf-8")
            else:
                break

    def entries(self):
        """ Generator to read all entries from the archive file.

        Return:
            A list representing a file or directory entry. See above for the
            actual format.
        """
        for dentry in self.dirs():
            yield dentry

        for fentry in self.files():
            yield fentry

    def get_fs(self, url):
        """ Get XRootD.FileSystem object matching the host in the url.

        Args:
            url (string): XRootD endpoint URL.

        Returns:
            FileSystem object to be used or None.
        """
        if url.startswith(self.header['src']):
            return self.fs_src
        elif url.startswith(self.header['dst']):
            return self.fs_dst
        else:
            return None

    def get_endpoints(self, rel_path):
        """Get full source and destination URLs for the given relative path.

        For this use the information from the header. Take into account whether
        it is a disk to tape transfer or not. The src in header is always the
        disk and dst is the tape.

        Args:
            rel_path (str): Entry relative path.

        Returns:
            Return a tuple of string representing the source and the destination
            of the transfer.
        """
        if rel_path == "./":
            rel_path = ""

        src = self.header['src'] + rel_path
        dst = self.header['dst'] + rel_path

        if self.header['svc_class']:
            dst = ''.join([dst, "?svcClass=", self.header['svc_class']])

        return (src, dst) if self.d2t else (dst, src)

    def del_entry(self, rel_path, is_dir, tape_delete):
        """ Delete file/dir. For directories it is successful only if the dir
        is empty. For deleting the subtree rooted in a directory one needs to
        use the del_subtree method.

        Args:
            rel_path (str): Entry relative path as stored in the archive file.
            is_dir (bool): True is entry is dir, otherwise False.
            tape_delete(bool): If tape_delete is None the delete comes from a
                PUT or GET operations so we only use the value of self.d2t to
                decide which entry we will delete. If tape_delete is True we
                delete tape data, otherwise we purge (delete from disk only).

        Raises:
            IOError: Deletion could not be performed.
        """
        src, dst = self.get_endpoints(rel_path)

        if tape_delete is None:
            surl = dst  # self.d2t is already used inside get_endpoints
        else:
            surl = src if tape_delete else dst

        url = client.URL(surl.encode("utf-8"))
        fs = self.get_fs(surl)
        self.logger.debug("Delete entry={0}".format(surl))

        if is_dir:
            st_rm, __ = fs.rmdir((url.path + "?eos.ruid=0&eos.rgid=0").encode("utf-8"))
        else:
            st_rm, __ = fs.rm((url.path + "?eos.ruid=0&eos.rgid=0").encode("utf-8"))

        if not st_rm.ok:
            # Check if entry exists
            st_stat, __ = fs.stat(url.path.encode("utf-8"))

            if st_stat.ok:
                err_msg = "Error removing entry={0}".format(surl)
                self.logger.error(err_msg)
                raise IOError()

            self.logger.warning("Entry={0} already removed".format(surl))

    def del_subtree(self, rel_path, tape_delete):
        """ Delete the subtree rooted at the provided path. Walk through all
        the files and delete them one by one then proceding with the directories
        from the deepest one to the root.

        Args:
            rel_path (string): Relative path to the subtree
            tape_delete (boolean or None): If present and true this is a
                deletion otherwise is a purge operation

        Raises:
            IOError: Deletion could not be performed
        """
        self.logger.debug("Del subtree for path={0}".format(rel_path))
        lst_dirs = []

        for fentry in self.files():
            path = fentry[1]
            # Delete only files rooted in current subtree
            if path.startswith(rel_path):
                self.del_entry(path, False, tape_delete)

        for dentry in self.dirs():
            path = dentry[1]

            if path.startswith(rel_path):
                lst_dirs.append(path)

        # Reverse the list so that we start deleting deepest (empty) dirs first
        lst_dirs.reverse()

        for path in lst_dirs:
            self.del_entry(path, True, tape_delete)

    def make_mutable(self):
        """ Make the EOS sub-tree pointed by header['src'] mutable.

        Raises:
            IOError when operation fails.
        """
        url = client.URL(self.header['src'].encode("utf-8"))

        for dentry in self.dirs():
            dir_path = url.path + dentry[1]
            fgetattr = ''.join([url.protocol, "://", url.hostid, "//proc/user/",
                                "?mgm.cmd=attr&mgm.subcmd=get&mgm.attr.key=sys.acl",
                                "&mgm.path=", seal_path(dir_path)])
            (status, stdout, __) = exec_cmd(fgetattr)

            if not status:
                warn_msg = "No xattr sys.acl found for dir={0}".format(dir_path)
                self.logger.warning(warn_msg)
            else:
                # Remove the 'z:i' rule from the acl list
                stdout = stdout.replace('"', '')
                acl_val = stdout[stdout.find('=') + 1:]
                rules = acl_val.split(',')
                new_rules = []

                for rule in rules:
                    if rule.startswith("z:"):
                        tag, definition = rule.split(':')
                        pos = definition.find('i')

                        if pos != -1:
                            definition = definition[:pos] + definition[pos + 1:]

                            if definition:
                                new_rules.append(':'.join([tag, definition]))

                            continue

                    new_rules.append(rule)

                acl_val = ','.join(new_rules)
                self.logger.error("new acl: {0}".format(acl_val))

                if acl_val:
                    # Set the new sys.acl xattr
                    fmutable = ''.join([url.protocol, "://", url.hostid, "//proc/user/?",
                                        "mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=sys.acl",
                                        "&mgm.attr.value=", acl_val, "&mgm.path=", dir_path])
                    (status, __, stderr) = exec_cmd(fmutable)

                    if not status:
                        err_msg = "Error making dir={0} mutable, msg={1}".format(
                            dir_path, stderr)
                        self.logger.error(err_msg)
                        raise IOError(err_msg)
                else:
                    # sys.acl empty, remove it from the xattrs
                    frmattr = ''.join([url.protocol, "://", url.hostid, "//proc/user/?",
                                       "mgm.cmd=attr&mgm.subcmd=rm&mgm.attr.key=sys.acl",
                                       "&mgm.path=", dir_path])
                    (status, __, stderr) = exec_cmd(frmattr)

                    if not status:
                        err_msg = ("Error removing xattr=sys.acl for dir={0}, msg={1}"
                                   "").format(dir_path, stderr)
                        self.logger.error(err_msg)
                        raise IOError(err_msg)

    def check_root_dir(self):
        """ Do the necessary checks for the destination directory depending on
        the type of the transfer.

        Raises:
             IOError: Root dir state inconsistent.
        """
        root_str = self.header['dst' if self.d2t else 'src']
        fs = self.get_fs(root_str)
        url = client.URL(root_str.encode("utf-8"))
        arg = url.path + "?eos.ruid=0&eos.rgid=0"
        st, __ = fs.stat(arg.encode("utf-8"))

        if self.d2t:
            if st.ok:
                # For PUT destination dir must NOT exist
                err_msg = "Root PUT dir={0} exists".format(root_str)
                self.logger.error(err_msg)
                raise IOError(err_msg)
            else:
                # Make sure the rest of the path exists as for the moment CASTOR
                # mkdir -p /path/to/file does not work properly
                pos = url.path.find('/', 1)

                while pos != -1:
                    dpath = url.path[: pos]
                    pos = url.path.find('/', pos + 1)
                    st, __ = fs.stat(dpath.encode("utf-8"))

                    if not st.ok:
                        st, __ = fs.mkdir(dpath.encode("utf-8"))

                        if not st.ok:
                            err_msg = ("Dir={0} failed mkdir errmsg={1}"
                                       "").format(dpath, st.message.decode("utf-8"))
                            self.logger.error(err_msg)
                            raise IOError(err_msg)

        elif not self.d2t:
            # For GET destination must exist and contain just the archive file
            if not st.ok:
                err_msg = "Root GET dir={0} does NOT exist".format(root_str)
                self.logger.error(err_msg)
                raise IOError(err_msg)
            else:
                ffindcount = ''.join([url.protocol, "://", url.hostid,
                                      "//proc/user/?mgm.cmd=find&mgm.path=",
                                      seal_path(url.path), "&mgm.option=Z"])
                (status, stdout, stderr) = exec_cmd(ffindcount)

                if status:
                    for entry in stdout.split():
                        tag, num = entry.split('=')

                        if ((tag == 'nfiles' and num not in ['1', '2']) or
                                (tag == 'ndirectories' and num != '1')):
                            err_msg = ("Root GET dir={0} should contain at least "
                                       "one file and at most two - clean up and "
                                       "try again").format(root_str)
                            self.logger.error(err_msg)
                            raise IOError(err_msg)
                else:
                    err_msg = ("Error doing find count on GET destination={0}"
                               ", msg={1}").format(root_str, stderr)
                    self.logger.error(err_msg)
                    raise IOError(err_msg)

    def verify(self, best_effort, tx_check_only=False):
        """ Check the integrity of the archive either on disk or on tape.

        Args:
            best_effort (boolean): If True then try to verify all entries even if
                we get an error during the check. This is used for the backup while
                for the archive, we return as soon as we find the first error.
            tx_check_only (boolean): If True then only check the existence of the
                entry, the size and checksum value. This is done only for archive
                GET operations.

        Returns:
            (status, lst_failed) - Status is True if archive is valid, otherwise
            false. In case the archive has errors return also the first corrupted
            entry from the archive file, otherwise return an empty list.
            For BACKUP operations return the status and the list of entries for
            which the verfication failed in order to provide a summary to the user.
        """
        self.logger.info("Do transfer verification")
        status = True
        lst_failed = []

        for entry in self.entries():
            try:
                self._verify_entry(entry, tx_check_only)
            except CheckEntryException as __:
                lst_failed.append(entry)
                status = False

                if best_effort:
                    continue
                else:
                    break

        return (status, lst_failed)

    def _verify_entry(self, entry, tx_check_only):
        """ Check that the entry (file/dir) has the proper meta data.

        Args:
            entry (list): Entry from the arhive file containing all info about
               this particular file/directory.
            tx_check_only (boolean): If True then for files only check their
                existence, size and checksum values.

        Raises:
            CheckEntryException: if entry verification fails.
        """
        self.logger.debug("Verify entry={0}".format(entry))
        is_dir, path = (entry[0] == 'd'), entry[1]
        __, dst = self.get_endpoints(path)
        url = client.URL(dst.encode("utf-8"))

        if self.d2t:  # for PUT check entry size and checksum if possible
            fs = self.get_fs(dst)
            st, stat_info = fs.stat(url.path.encode("utf-8"))

            if not st.ok:
                err_msg = "Entry={0} failed stat".format(dst)
                self.logger.error(err_msg)
                raise CheckEntryException("failed stat")

            if not is_dir:  # check file size match
                indx = self.header["file_meta"].index("size") + 2
                orig_size = int(entry[indx])

                if stat_info.size != orig_size:
                    err_msg = ("Verify entry={0}, expect_size={1}, size={2}"
                               "").format(dst, orig_size, stat_info.size)
                    self.logger.error(err_msg)
                    raise CheckEntryException("failed file size match")

                # Check checksum only if it is adler32 - only one supported by CASTOR
                indx = self.header["file_meta"].index("xstype") + 2

                # !!!HACK!!! Check the checksum only if file size is not 0 since
                # CASTOR does not store any checksum for 0 size files
                if stat_info.size != 0 and entry[indx] == "adler":
                    indx = self.header["file_meta"].index("xs") + 2
                    xs = entry[indx]
                    st, xs_resp = fs.query(QueryCode.CHECKSUM, url.path)

                    if not st.ok:
                        err_msg = "Entry={0} failed xs query".format(dst)
                        self.logger.error(err_msg)
                        raise CheckEntryException("failed xs query")

                    # Result has an annoying \x00 character at the end and it
                    # contains the xs type (adler32) and the xs value
                    resp = xs_resp.split('\x00')[0].split()

                    # If checksum value is not 8 char long then we need padding
                    if len(resp[1]) != 8:
                        resp[1] = "{0:0>8}".format(resp[1])

                    if resp[0] == "adler32" and resp[1] != xs:
                        err_msg = ("Entry={0} xs value missmatch xs_expected={1} "
                                   "xs_got={2}").format(dst, xs, resp[1])
                        self.logger.error(err_msg)
                        raise CheckEntryException("xs value missmatch")

        else:  # for GET check all metadata
            if is_dir:
                tags = self.header['dir_meta']
            else:
                tags = self.header['file_meta']
                try:
                    if self.header['twindow_type'] and self.header['twindow_val']:
                        dfile = dict(zip(tags, entry[2:]))
                        twindow_sec = int(self.header['twindow_val'])
                        tentry_sec = int(float(dfile[self.header['twindow_type']]))

                        if tentry_sec < twindow_sec:
                            # No check for this entry
                            return

                    # This is a backup so don't check atomic version files
                    if is_atomic_version_file(entry[1]):
                        return
                except KeyError as __:
                    # This is not a backup transfer but an archive one, carry on
                    pass

            try:
                meta_info = get_entry_info(url, path, tags, is_dir)
            except (AttributeError, IOError, KeyError) as __:
                self.logger.error("Failed getting metainfo entry={0}".format(dst))
                raise CheckEntryException("failed getting metainfo")

            # Check if we have any excluded xattrs
            try:
                excl_xattr = self.header['excl_xattr']
            except KeyError as __:
                excl_xattr = list()

            if is_dir and excl_xattr:
                # For directories and configurations containing excluded xattrs
                # we refine the checks. If "*" in excl_xattr then no check is done.
                if "*" not in excl_xattr:
                    ref_dict = dict(zip(tags, entry[2:]))
                    new_dict = dict(zip(tags, meta_info[2:]))

                    for key, val in ref_dict.iteritems():
                        if not isinstance(val, dict):
                            if new_dict[key] != val:
                                err_msg = ("Verify failed for entry={0} expect={1} got={2}"
                                           " at key={3}").format(dst, entry, meta_info, key)
                                self.logger.error(err_msg)
                                raise CheckEntryException("failed metainfo match")
                        else:
                            for kxattr, vxattr in val.iteritems():
                                if kxattr not in excl_xattr:
                                    if vxattr != new_dict[key][kxattr]:
                                        err_msg = ("Verify failed for entry={0} expect={1} got={2}"
                                                   " at xattr key={3}").format(dst, entry, meta_info, kxattr)
                                        self.logger.error(err_msg)
                                        raise CheckEntryException("failed metainfo match")
            else:
                # For files with tx_check_only verification, we refine the checks
                if tx_check_only and not is_dir:
                    idx_size = self.header["file_meta"].index("size") + 2
                    idx_xstype = self.header["file_meta"].index("xstype") + 2
                    idx_xsval = self.header["file_meta"].index("xs") + 2

                    if (meta_info[idx_size] != entry[idx_size] or
                        meta_info[idx_xstype] != entry[idx_xstype] or
                        meta_info[idx_xsval] != entry[idx_xsval]):
                        err_msg = ("Partial verify failed for entry={0} expect={1} got={2}"
                                   "").format(dst, entry, meta_info)
                        self.logger.error(err_msg)
                        raise CheckEntryException("failed metainfo partial match")
                else:
                    if is_dir:
                        # Compensate for the removal fo the S_ISGID bit
                        mask_mode = int("02000", base=8)
                        val_mode = int(entry[4], base=8)
                        val_mode |= mask_mode
                        compat_entry = list(entry)
                        compat_entry[4] = "{0:o}".format(val_mode)

                    if not meta_info == entry and not compat_entry == entry:
                        err_msg = ("Verify failed for entry={0} expect={1} got={2}"
                                   "").format(dst, entry, meta_info)
                        self.logger.error(err_msg)
                        raise CheckEntryException("failed metainfo match")

        self.logger.info("Entry={0}, status={1}".format(dst, True))

    def mkdir(self, dentry):
        """ Create directory and optionally for GET operations set the
        metadata information.

        Args:
            dentry (list): Directory entry as read from the archive file.

        Raises:
            IOError: Directory creation failed.
        """
        __, surl = self.get_endpoints(dentry[1])
        fs = self.get_fs(surl)
        url = client.URL(surl.encode("utf-8"))

        # Create directory if not already existing
        st, __ = fs.stat((url.path + "?eos.ruid=0&eos.rgid=0").encode("utf-8"))

        if not st.ok:
            if not self.d2t:
                st, __ = fs.mkdir((url.path + "?eos.ruid=0&eos.rgid=0").encode("utf-8"))
            else:
                st, __ = fs.mkdir((url.path).encode("utf-8"))

            if not st.ok:
                err_msg = ("Dir={0} failed mkdir errmsg={1}, errno={2}, code={3}"
                           "").format(surl, st.message.decode("utf-8"), st.errno, st.code)
                self.logger.error(err_msg)
                raise IOError(err_msg)

        # For GET operations set also the metadata
        if not self.d2t:
            dict_dinfo = dict(zip(self.header['dir_meta'], dentry[2:]))

            # Get the list of excluded extended attributes if it exists
            try:
                excl_xattr = self.header['excl_xattr']
            except KeyError as __:
                excl_xattr = list()

            try:
                set_dir_info(surl, dict_dinfo, excl_xattr)
            except IOError as __:
                err_msg = "Dir={0} failed setting metadata".format(surl)
                self.logger.error(err_msg)
                raise IOError(err_msg)
