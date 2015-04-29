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

from __future__ import unicode_literals
import logging
from XRootD import client
from XRootD.client.flags import OpenFlags

logger = logging.getLogger("transfer")

def seal_path(path, seal_dict={'&': "#AND#"}):
    """ Seal a path by replacing the key characters in the dictionary with their
    values so that EOS is happy.

    Args:
        path (str): Path to be sealed
        seal (dict): Seal dictionary

    Returns:
        The path transformed using the dictionary mapping.
    """
    for key, val in seal_dict.iteritems():
        path = path.replace(key, val)

    return path


def unseal_path(path, seal_dict={"#and#": '&'}):
    """ Unseal a path by replacing the key characters in the dictionary with their
    values so that we are happy.

    Args:
        path (str): Path to be unsealed
        seal (dict): Unseal dictionary

    Returns:
        The path transformed using the dictionary mapping.
    """
    for key, val in seal_dict.iteritems():
        path = path.replace(key, val)

    return path


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
        st, __ = f.open(cmd.encode("utf-8"), OpenFlags.READ)

        if st.ok:
            # Read the whole response
            data = ""
            off, sz = 0, 4096
            st, chunk = f.read(off, sz)

            if st.ok:
                while st.ok and len(chunk):
                    off += len(chunk)
                    data += chunk.decode("utf-8")
                    st, chunk = f.read(off, sz)

                lpairs = data.split('&')
                for elem in lpairs:
                    if "mgm.proc.retc=" in elem:
                        retc = elem[(elem.index('=') + 1):].strip()
                        status = True if (retc == "0") else False
                    elif "mgm.proc.stdout=" in elem:
                        stdout = elem[(elem.index('=') + 1):].strip()
                        stdout = unseal_path(stdout)
                    elif "mgm.proc.stderr=" in elem:
                        stderr = elem[(elem.index('=') + 1):].strip()
                        stderr = unseal_path(stdout)
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
    finfo = ''.join([url.protocol, "://", url.hostid, "//proc/user/?",
                     "mgm.cmd=fileinfo&mgm.path=", seal_path(url.path),
                     "&mgm.file.info.option=-m"])
    (status, stdout, stderr) = exec_cmd(finfo)

    if not status:
        err_msg = ("Path={0} failed fileinfo request, msg={1}").format(
            url.path, stderr)
        logger.error(err_msg)
        raise IOError(err_msg)

    # Extract the path by using the keylength.file value which represents the
    # size of the path. This is because the path can contain spaces.
    size_pair, file_pair, tail = stdout.split(' ', 2)
    sz_key, sz_val = size_pair.split('=', 1)
    file_key, file_val = file_pair.split('=', 1)

    if sz_key == "keylength.file" and file_key == "file" :
        path = file_val
        path_size = int(sz_val)

        while path_size > len(path.encode("utf-8")):
            path_token, tail = tail.split(' ', 1)
            path += ' '
            path += path_token
    else:
        err_msg = ("Fileinfo response does not start with keylength.file "
                   "for path").format(url.path)
        logger.error(err_msg)
        raise IOError(err_msg)

    # For the rest we don't expect any surprizes, they shoud be key=val pairs
    lpairs = tail.split(' ')
    it_list = iter(lpairs)
    dict_info, dict_attr = {}, {}

    # Parse output of fileinfo -m keeping only the required keys
    for elem in it_list:
        key, value = elem.split('=', 1)

        if key in tags:
            dict_info[key] = value
        elif key == "xattrn" and is_dir:
            xkey, xval = next(it_list).split('=', 1)

            if xkey != "xattrv":
                err_msg = ("Dir={0} no value for xattrn={1}").format(
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

def set_dir_info(surl, dict_dinfo, excl_xattr):
    """ Set directory metadata information in EOS.

    Args:
        surl (string): Full URL of directory
        dict_dinfo (dict): Dictionary containsing meta-data information
        excl_xattr (list): List of excluded extended attributes

    Raises:
        IOError: Metadata operation failed.
    """
    url = client.URL(surl.encode("utf-8"))

    # Change ownership of the directory
    fsetowner = ''.join([url.protocol, "://", url.hostid, "//proc/user/?",
                         "mgm.cmd=chown&mgm.path=", seal_path(url.path),
                         "&mgm.chown.owner=", dict_dinfo['uid'], ":",
                         dict_dinfo['gid']])
    (status, stdout, stderr) = exec_cmd(fsetowner)

    if not status:
        err_msg = "Dir={0}, error doing chown, msg={1}".format(url.path, stderr)
        logger.error(err_msg)
        raise IOError(err_msg)

    # Set permission on the directory
    fchmod = ''.join([url.protocol, "://", url.hostid, "//proc/user/?",
                      "mgm.cmd=chmod&mgm.path=", seal_path(url.path),
                      "&mgm.chmod.mode=", dict_dinfo['mode']])
    (status, stdout, stderr) = exec_cmd(fchmod)

    if not status:
        err_msg = "Dir={0}, error doing chmod, msg={1}".format(url.path, stderr)
        logger.error(err_msg)
        raise IOError(err_msg)

    # Remove any existing attributes
    flsattr = ''.join([url.protocol, "://", url.hostid, "//proc/user/?",
                       "mgm.cmd=attr&mgm.subcmd=ls&mgm.path=", seal_path(url.path)])

    (status, stdout, stderr) = exec_cmd(flsattr)

    if not status:
        err_msg = "Dir={0}, error listing xattrs, msg ={1}".format(
            url.path, stderr)
        logger.error(err_msg)
        raise IOError(err_msg)

    lattrs = [s.split('=', 1)[0] for s in stdout.splitlines()]

    for attr in lattrs:
        # Don't remove the excluded xattrs
        if attr in excl_xattr:
            continue

        frmattr = ''.join([url.protocol, "://", url.hostid, "//proc/user/?",
                           "mgm.cmd=attr&mgm.subcmd=rm&mgm.attr.key=", attr,
                           "&mgm.path=", seal_path(url.path)])
        (status, __, stderr) = exec_cmd(frmattr)

        if not status:
            err_msg = ("Dir={0} error while removing attr={1}, msg={2}"
                       "").format(url.path, attr, stderr)
            logger.error(err_msg)
            raise IOError(err_msg)

    # Set the expected extended attributes
    dict_dattr = dict_dinfo['attr']

    for key, val in dict_dattr.iteritems():
        # Don't set the excluded xattrs
        if key in excl_xattr:
            continue

        fsetattr = ''.join([url.protocol, "://", url.hostid, "//proc/user/?",
                            "mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=", key,
                            "&mgm.attr.value=", val, "&mgm.path=", seal_path(url.path)])
        (status, __, stderr) = exec_cmd(fsetattr)

        if not status:
            err_msg = "Dir={0}, error setting attr={1}, msg={2}".format(
                url.path, key, stderr)
            logger.error(err_msg)
            raise IOError(err_msg)
