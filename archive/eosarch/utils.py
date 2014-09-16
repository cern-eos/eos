# ------------------------------------------------------------------------------
# File: utils.py
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
"""Module containing helper function for the EOS archiver daemon."""

import logging
from os.path import relpath
from XRootD import client
from XRootD.client.flags import OpenFlags

logger = logging.getLogger("archivefile.transfer")

def exec_cmd(cmd):
    """ Execute an EOS /proc/user/ command.

    Args:
      cmd (str): Command to execute.

    Returns:
      Tuple containing the following elements: (status, stdout, stderr). Status
      is a boolean value while the rest are string. If data needs to be returned
      then it's put in stdout and any error messages are in stderr.
    """
    logger.debug("Execute: {0}".format(cmd))
    status, retc, stdout, stderr = False, "0", "", ""

    # Execute the command as root if role not already set
    if cmd.find("eos.ruid=") == -1:
        if cmd.find('?') == -1:
            cmd += "?eos.ruid=0&eos.rgid=0"
        else:
            cmd += "&eos.ruid=0&eos.rgid=0"

    with client.File() as f:
        st, __ = f.open(cmd, OpenFlags.READ)

        if st.ok:
            # Read the whole response
            data = ""
            off, sz = 0, 4096
            st, chunk = f.read(off, sz)

            if st.ok:
                while st.ok and len(chunk):
                    off += len(chunk)
                    data += chunk
                    st, chunk = f.read(off, sz)

                lpairs = data.split('&')
                for elem in lpairs:
                    if "mgm.proc.retc=" in elem:
                        retc = elem[(elem.index('=') + 1):].strip()
                        status = True if (retc == "0") else False
                    elif "mgm.proc.stdout=" in elem:
                        stdout = elem[(elem.index('=') + 1):].strip()
                    elif "mgm.proc.stderr=" in elem:
                        stderr = elem[(elem.index('=') + 1):].strip()
            else:
                stderr = "error reading response for command: {0}".format(cmd)
        else:
            stderr = "error sending command: {0}".format(cmd)

    # logger.debug("Return command: {0}".format((status, stdout, stderr)))
    return (status, stdout, stderr)


def get_entry_info(url, rel_path, tags, is_dir):
    """ Get file/directory metadata information from EOS.

    Args:
        url  (XRootD.URL): Full URL to EOS location.
        rel_path (str): Entry's relative path as saved in the archive file.
        tags (list): List of tags to look for in the fileinfo result.
        is_dir (bool): If True entry is a directory, otherwise a file.

    Returns:
        A list containing the info corresponding to the tags supplied in
        the args.

    Raises:
        IOError: Fileinfo request can not be submitted.
        AttributeError: Not all expected tags are provided.
        KeyError: Extended attribute value is not present.
    """
    dinfo = []
    finfo = [url.protocol, "://", url.hostid, "//proc/user/?",
             "mgm.cmd=fileinfo&mgm.path=", url.path,
             "&mgm.file.info.option=-m"]
    (status, stdout, stderr) = exec_cmd(''.join(finfo))

    if not status:
        err_msg = ("Path={0}, failed fileinfo request, msg={1}").format(
            url.path, stderr)
        logger.error(err_msg)
        raise IOError(err_msg)

    # Keep only the interesting info for an entry
    lpairs = stdout.split(' ')
    dict_info, dict_attr = {}, {}
    it_list = iter(lpairs)

    # Parse output of fileinfo -m
    for elem in it_list:
        key, value = elem.split('=', 1)

        if key in tags:
            dict_info[key] = value
        elif key == "xattrn" and is_dir:
            xkey, xval = next(it_list).split('=', 1)

            if xkey != "xattrv":
                err_msg = ("Dir={0} no value for xattribute={1}").format(
                    url.path, value)
                logger.error(err_msg)
                raise KeyError(err_msg)
            else:
                dict_attr[value] = xval

        # For directories add also the xattr dictionary
        if is_dir and "attr" in tags:
            dict_info["attr"] = dict_attr

    if len(dict_info) == len(tags):
        # Dirs  must end with '/' just as the output of EOS fileinfo -d
        tentry = 'd' if is_dir else 'f'
        dinfo.extend([tentry, rel_path])

        for tag in tags:
            dinfo.append(dict_info[tag])
    else:
        err_msg = ("Path={0}, not all expected tags found").format(url.path)
        logger.error(err_msg)
        raise AttributeError(err_msg)

    return dinfo

def set_dir_info(entry):
    """ Set directory metadata information in EOS.

    Args:
        entry (tuple): Tuple of two elements: full URL of directory and
        dictionary containing meta data information.

    Raises:
        IOError: Metadata operation failed.
    """
    path, dict_dinfo = entry
    url = client.URL(path)

    # Change ownership of the directory
    fsetowner = [url.protocol, "://", url.hostid, "//proc/user/?",
                 "mgm.cmd=chown&mgm.path=", url.path,
                 "&mgm.chown.owner=", dict_dinfo['uid'], ":", dict_dinfo['gid']]
    (status, stdout, stderr) = exec_cmd(''.join(fsetowner))

    if not status:
        err_msg = "Dir={0}, error doing chown, msg={1}".format(url.path, stderr)
        logger.error(err_msg)
        raise IOError(err_msg)

    # Remove any existing attributes
    flsattr = [url.protocol, "://", url.hostid, "//proc/user/?",
               "mgm.cmd=attr&mgm.subcmd=ls&mgm.path=", url.path]

    (status, stdout, stderr) = exec_cmd(''.join(flsattr))

    if not status:
        err_msg = "Dir={0}, error listing xattrs, msg ={1}",format(url.path, stderr)
        logger.error(err_msg)
        raise IOError(err_msg)

    lattrs = [s.split('=', 1)[0] for s in stdout.splitlines()]

    for attr in lattrs:
        frmattr = [url.protocol, "://", url.hostid, "//proc/user/?",
                   "mgm.cmd=attr&mgm.subcmd=rm&mgm.attr.key=", attr,
                   "&mgm.path=", url.path]
        (status, __, stderr) = exec_cmd(''.join(frmattr))

        if not status:
            err_msg = ("Dir={0} error while removing attr={1}, msg={2}"
                       "").format(url.path, attr, stderr)
            logger.error(err_msg)
            raise IOError(err_msg)

    # Set the expected extended attributes
    dict_dattr = dict_dinfo['attr']

    for key, val in dict_dattr.iteritems():
        fsetattr = [url.protocol, "://", url.hostid, "//proc/user/?",
                    "mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=", key,
                    "&mgm.attr.value=", val, "&mgm.path=", url.path]
        (status, __, stderr) = exec_cmd(''.join(fsetattr))

        if not status:
            err_msg = "Dir={0}, error setting attr={1}, msg={2}".format(
                url.path, key, stderr)
            logger.error(err_msg)
            raise IOError(err_msg)
