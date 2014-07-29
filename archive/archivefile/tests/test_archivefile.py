# ------------------------------------------------------------------------------
# File: test_archivefile.py
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
import unittest
import json
from archivefile.utils import exec_cmd
from archivefile.archivefile import ArchiveFile
from XRootD import client
from env import *

def test_exec_cmd():
    """Check the exec command.

    List directory extended attributes from EOS local instance.
    """
    url = client.URL(''.join([SERVER_URL, EOS_DIR]))
    flsattr = ''.join([url.protocol, "://", url.hostid, "//proc/user/",
                        "?mgm.cmd=attr&mgm.subcmd=ls&mgm.path=", EOS_DIR])
    (status, stdout, __) = exec_cmd(''.join(flsattr))
    assert(status)


class TestArchiveFile(unittest.TestCase):
    """ Unittest class for ArchiveFile."""

    def setUp(self):
        """ SetUp function."""
        self.local_path = os.getcwd() + '/' + LOCAL_FILE
        self.d2t = True
        self.arch = ArchiveFile(self.local_path, self.d2t)

    def tearDown(self):
        """TearDown function."""
        pass

    def test_list_dirs(self):
        """Check generator dir listing.
        """
        with open(self.local_path, 'r') as farch:
            _ = farch.readline()  # skip the header
            for dentry in self.arch.dirs():
                for line in farch:
                    entry = json.loads(line)
                    if entry[0] == 'd':
                        self.assertEqual(entry, dentry)
                        break

    def test_list_files(self):
        """Check generator file listing.
        """
        with open(self.local_path, 'r') as farch:
            _ = farch.readline()  # skip the header
            for fentry in self.arch.files():
                for line in farch:
                    entry = json.loads(line)
                    if entry[0] == 'f':
                        self.assertEqual(entry, fentry)
                        break

    def test_list_entries(self):
        """Check generator of all entries.
        """
        with open(self.local_path, 'r') as farch:
            _ = farch.readline()  # skip the header
            for aentry in self.arch.entries():
                for line in farch:
                    entry = json.loads(line)
                    self.assertEqual(entry, aentry)
                    break

    def test_get_endpoints(self):
        """Check endpoints based on transfer type.
        """
        for aentry in self.arch.entries():
            src, dst = self.arch.get_endpoints(aentry[1])
            self.assertTrue(src.find(self.arch.header['src']) == 0)
            self.assertTrue(dst.find(self.arch.header['dst']) == 0)

        self.d2t = False  # test tape to disk
        self.arch = ArchiveFile(self.local_path, self.d2t)

        for aentry in self.arch.entries():
            src, dst = self.arch.get_endpoints(aentry[1])
            self.assertTrue(src.find(self.arch.header['dst']) == 0)
            self.assertTrue(dst.find(self.arch.header['src']) == 0)
