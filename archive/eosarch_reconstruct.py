#! /usr/bin/python
# ------------------------------------------------------------------------------
# File: eosarch_reconstruct.py
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
""" This tool can be used to reconstruct an archive file starting from the data
which is actually saved on tape (CASTOR). The tape systems needs to have an
XRootD interface. The way it works it that the archive file is constructed
locally and then is uploaded to the specified EOS root directory which must not
exist previously. The archive file is copied to EOS using the filename
.archive.purge.done so that the user is then able to get the data from the
tape system back into EOS. The UID which is provided when launching the command
is given permission to execute archive operations on the corresponding EOS
directory.
"""

from __future__ import print_function
import sys
import os
import ast
import errno
import stat
import time
import argparse
import tempfile
from eosarch.utils import exec_cmd, set_dir_info

try:
    from XRootD import client
    from XRootD.client.flags import DirListFlags, StatInfoFlags, QueryCode
except ImportError as ierr:
    print("Missing xrootd-python package", file=sys.stderr)


class EosAccessException(Exception):
    """ Exception raised when the current user does not have full sudo rights
    EOS to perform the necessary operation for the archiving reconstruct.
    """
    pass

class TapeAccessException(Exception):
    """ Exception raised when the current user can not access information from
    the tape system.
    """
    pass


class ArchReconstruct(object):
    """ Class responsible for reconstructing the archive file from an already
    directory subtree from tape.
    """
    def __init__(self, surl, durl, args):
        """ Initialize the ArchReconstruct object

        Args:
            surl (XRootD.URL): URL to tape backend (CASTOR)
            durl (XRootD.URL): URL to disk destination (EOS)
            args  (Namespace): Namespace object containing at least the following
            attributes: uid (string): UID of archive owner in numeric format
                        gid (string): GID of archive owner in numeric format
                        svc_class (string): Service class used for retrieving the
                            archived data
        """
        self.src_url = surl
        self.dst_url = durl
        self.uid, self.gid = args.uid, args.gid
        self.svc_class = args.svc_class
        self.ffiles = tempfile.TemporaryFile(mode='w+')
        self.fdirs = tempfile.TemporaryFile(mode='w+')
        self.farchive = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        print("Temp. archive file saved in: {0}".format(self.farchive.name),
              file=sys.stdout)

    def __del__(self):
        """ Destructor - make sure we close the temporary files
        """
        self.ffiles.close()
        self.fdirs.close()
        self.farchive.close()

    def breadth_first(self):
        """ Traverse the filesystem subtree using breadth-first search and
        collect the directory information and file information into separate
        files which will be merged in the end.
        """
        # Dir format: type, rel_path, uid, gid, mode, attr
        dir_meta = "[\"uid\", \"gid\", \"mode\", \"attr\"]"
        dir_format = "[\"d\", \"{0}\", \"{1}\", \"{2}\", \"{3}\", {4}]"
        # File format: type, rel_path, size, mtime, ctime, uid, gid, mode, xstype, xs
        # Fake mtime and ctime subsecond precision
        file_meta = ("[\"size\", \"mtime\", \"ctime\", \"uid\", \"gid\", \"mode\", "
                     "\"xstype\", \"xs\"]")
        file_format = ("[\"f\", \"{0}\", \"{1}\", \"{2}.0\", \"{3}.0\", \"{4}\", "
                       "\"{5}\", \"{6}\", \"{7}\", \"{8}\"]")
        # Attrs for 2 replica layout in EOS with current user the only one
        # allowed to trigger archiving operations
        replica_attr = ("{{\"sys.acl\": \"u:{0}:a,z:i\", "
                        "\"sys.forced.blockchecksum\": \"crc32c\", "
                        "\"sys.forced.blocksize\": \"4k\", "
                        "\"sys.forced.checksum\": \"adler\", "
                        "\"sys.forced.layout\": \"replica\", "
                        "\"sys.forced.nstripes\": \"2\", "
                        "\"sys.forced.space\": \"default\"}}").format(self.uid)
        num_files, num_dirs = 0, 0
        fs = client.FileSystem(str(self.src_url))

        # Add root directory which is a bit special and set its metadata
        # Dir mode is 42755 and file mode is 0644
        dir_mode = oct(stat.S_IFDIR | stat.S_ISGID | stat.S_IRWXU
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        dir_mode = dir_mode[1:] # remove leading 0 used for octal format
        file_mode = oct(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        print(dir_format.format("./", self.uid, self.gid, dir_mode,
                                replica_attr), file=self.fdirs)
        dict_attr = ast.literal_eval(replica_attr)
        dict_dinfo = dict(zip(["uid", "gid", "mode", "attr"],
                              [self.uid, self.gid, dir_mode, dict_attr]))
        set_dir_info((str(self.dst_url), dict_dinfo))
        root_path = self.src_url.path
        lst_dirs = [root_path]

        while lst_dirs:
            path = lst_dirs.pop(0)
            st, listing = fs.dirlist(path, DirListFlags.STAT)

            if not st.ok:
                msg = "Failed list dir= {0}".format(self.src_url.path)
                raise TapeAccessException(msg)

            for elem in listing:
                if elem.name == ".archive.init":
                    msg = ("Trying to reconstruct an already existing archive "
                           "in directory: {0}").format(path)
                    raise TapeAccessException(msg)

                if elem.statinfo.flags & StatInfoFlags.IS_DIR:
                    num_dirs += 1
                    full_path = ''.join([path, elem.name, '/'])
                    rel_path = full_path.replace(root_path, "")
                    lst_dirs.append(full_path)
                    print(dir_format.format(rel_path, self.uid, self.gid, dir_mode,
                                            replica_attr), file=self.fdirs)
                else:
                    num_files += 1
                    full_path = ''.join([path, elem.name])
                    rel_path = full_path.replace(root_path, "")
                    st, xs_resp = fs.query(QueryCode.CHECKSUM, full_path)

                    if not st.ok:
                        msg = "File={0} failed xs query".format(full_path)
                        raise TapeAccessException(msg)

                    # Result has an annoying \x00 character at the end and it
                    # contains the xs type (adler32) and the xs value
                    resp = xs_resp.strip('\x00\0\n ').split()

                    # If checksum value is not 8 char long then we need padding
                    if len(resp[1]) != 8 :
                        resp[1] = "{0:0>8}".format(resp[1])

                    if resp[0] != "adler32":
                        msg = ("Unknown checksum type={0} from tape system"
                               "".format(resp[0]))
                        raise TapeAccessException(msg)

                    print(file_format.format(rel_path, elem.statinfo.size,
                                             elem.statinfo.modtime,
                                             elem.statinfo.modtime,
                                             self.uid, self.gid, file_mode,
                                             "adler", resp[1]),
                          file=self.ffiles)

        # Write archive file header
        header_format = ("{{\"src\": \"{0}\", "
                         "\"dst\": \"{1}\", "
                         "\"svc_class\": \"{2}\", "
                         "\"dir_meta\": {3}, "
                         "\"file_meta\": {4}, "
                         "\"num_dirs\": {5}, "
                         "\"num_files\": {6}, "
                         "\"uid\": \"{7}\", "
                         "\"gid\": \"{8}\", "
                         "\"timestamp\": \"{9}\"}}")
        print(header_format.format(str(self.dst_url), str(self.src_url),
                                   self.svc_class, dir_meta, file_meta,
                                   num_dirs, num_files, self.uid,
                                   self.gid, time.time()),
              file=self.farchive, end="\n")
        # Rewind to the begining of the tmp files
        self.fdirs.seek(0)
        self.ffiles.seek(0)

        # Write directories
        for line in self.fdirs:
            print(line, file=self.farchive, end="")

        # Write files
        for line in self.ffiles:
            print(line, file=self.farchive, end="")

        self.farchive.close()

    def upload_archive(self):
        """ Upload archive file to EOS directory. Note that we save it the the
        name .archive.purge since this is the only possible operation when we
        do such a reconstruct.
        """
        cp = client.CopyProcess()
        dst = ''.join([str(self.dst_url), ".archive.purge.done?eos.ruid=0&eos.rgid=0"])
        cp.add_job(self.farchive.name, dst, force=True)
        status = cp.prepare()

        if not status.ok:
            msg = "Failed while preparing to upload archive file to EOS"
            raise EosAccessException(msg)

        status = cp.run()

        if not status.ok:
            msg = "Failed while copying the archive file to EOS"
            raise EosAccessException(msg)
        else:
            # Delete local archive file
            try:
                os.remove(self.farchive.name)
            except OSError as __:
                pass

def check_eos_access(url):
    """ Check that the current user executing the programm is mapped as root in
    EOS otherwise he will not be able to set all the necessary attributes for
    the newly built archive. Make sure also that the root destination does not
    exist already.

    Args:
       url (XRootD.URL): EOS URL to the destination path

    Raises:
       EosAccessException
    """
    fwhoami = ''.join([url.protocol, "://", url.hostid, "//proc/user/?mgm.cmd=whoami"])
    (status, out, __) = exec_cmd(fwhoami)

    if not status:
        msg = "Failed to execute EOS whoami command"
        raise EosAccessException(msg)

    # Extrach the uid and gid from the response
    out.strip("\0\n ")
    lst = out.split(' ')

    try:
        for token in lst:
            if token.startswith("uid="):
                uid = int(token[4:])
            elif token.startswith("gid="):
                gid = int(token[4:])
    except ValueError as __:
        msg = "Failed while parsing uid/gid response to EOS whoami command"
        raise EosAccessException(msg)

    if uid != 0 or gid != 0:
        msg = "User {0} does not have full rights in EOS - aborting".format(os.getuid())
        raise EosAccessException(msg)

    # Check that root directory does not exist already
    fs = client.FileSystem(str(url))
    st, __ = fs.stat(url.path)

    if st.ok:
        msg = "EOS root directory already exists"
        raise EosAccessException(msg)

    fmkdir = ''.join([url.protocol, "://", url.hostid, "//proc/user/?mgm.cmd=mkdir&"
                      "mgm.path=", url.path])
    (status, __, __) = exec_cmd(fmkdir)

    if not status:
        msg = "Failed to create EOS directory: {0}".format(url.path)
        raise EosAccessException(msg)


def main():
    """ Main function """
    parser = argparse.ArgumentParser(description="Tool used to create an archive "
                                     "file from an already existing archvie such "
                                     "that the recall of the files can be done "
                                     "using the EOS archiving tool. The files are "
                                     "copied back to EOS using the 2replica layout.")
    parser.add_argument("-s", "--src", required=True,
                        help="XRootD URL to archive tape source (CASTOR location)")
    parser.add_argument("-d", "--dst", required=True,
                        help="XRootD URL to archive disk destination (EOS location)")
    parser.add_argument("-c", "--svc_class", default="default",
                        help="Service class used for getting the files from tape")
    parser.add_argument("-u", "--uid", default="0", help="User UID (numeric)")
    parser.add_argument("-g", "--gid", default="0", help="User GID (numeric)")
    args = parser.parse_args()

    try:
        int(args.uid)
        int(args.gid)
    except ValueError as __:
        print("Error: UID/GID must be in numeric format", file=sys.stderr)
        exit(errno.EINVAL)

    # Make sure the source and destination are directories
    if args.src[-1] != '/':
        args.src += '/'
    if args.dst[-1] != '/':
        args.dst += '/'

    # Check the source and destiantion are valid XRootD URLs
    url_dst = client.URL(args.dst)
    url_src = client.URL(args.src)

    if not url_dst.is_valid() or not url_src.is_valid():
        print("Error: Destination/Source URL is not valid", file=sys.stderr)
        exit(errno.EINVAL)

    avoid_local = ["localhost", "localhost4", "localhost6",
                   "localhost.localdomain", "localhost4.localdomain4",
                   "localhost6.localdomain6"]

    if url_dst.hostname in avoid_local or url_src.hostname in avoid_local:
        print("Please use FQDNs in the XRootD URLs", file=sys.stderr)
        exit(errno.EINVAL)

    try:
        check_eos_access(url_dst)
    except EosAccessException as err:
        print("Error: {0}".format(str(err)), file=sys.stderr)
        exit(errno.EPERM)

    archr = ArchReconstruct(url_src, url_dst, args)

    try:
        archr.breadth_first()
        archr.upload_archive()
    except (TapeAccessException, IOError) as err:
        print("Error: {0}".format(str(err)), file=sys.stderr)
        exit(errno.EIO)

if __name__ == '__main__':
    main()
