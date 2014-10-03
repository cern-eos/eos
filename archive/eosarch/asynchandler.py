# ------------------------------------------------------------------------------
# File: asynchandler.py
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
""" Objects used for handling asynchronous XRootD requests.
"""

import logging
from threading import Condition


class _MkDirHandler(object):
    """ Async mkdir handler which reports to MetaHandler.

    Attributes:
        path (string): Directory path for which the handler is created.
        meta_handler (MetaHandler): Meta handler object.
    """
    def __init__(self, path, meta_handler):
        self.type = 'mkdir'
        self.path = path
        self.meta_handler = meta_handler

    def __call__(self, status, response, hostlist):
        self.meta_handler.handle(self.type, status, self.path)


class _PrepareHandler(object):
    """ Async prepare handler which reports to MetaHandler.

    Attributes:
        path (string): Directory path for which the handler is created.
        meta_handler (MetaHandler): Meta handler object.
    """
    def __init__(self, path, meta_handler):
        self.type = 'prepare'
        self.path = path
        self.meta_handler = meta_handler

    def __call__(self, status, response, hostlist):
        self.meta_handler.handle(self.type, status, self.path)


class MetaHandler(object):
    """ Meta handler for different types of async requests.

    Attributes:
        cond: Condition variable used for synchronization.
        logger: Logger object.
        mkdir_failed: List of directories failed to create.
        mkdir_status: Status of mkdir requests, logical and between individual
            mkdir commands.
        mkdir_num: Number of mkdir commands waiting for reply.
    """
    def __init__(self):
        list_op = ['mkdir', 'prepare']
        self.num, self.status, self.failed = {}, {}, {}
        self.handlers = {'mkdir': _MkDirHandler,
                         'prepare': _PrepareHandler}

        for op in list_op:
            self.num[op] = 0
            self.status[op] = True
            self.failed[op] = []

        self.cond = Condition()
        self.logger = logging.getLogger("transfer")

    def register(self, op, path):
        """ Register handler for operation.
        """
        self.cond.acquire()
        self.num[op] += 1
        self.cond.release()
        return self.handlers[op](path, self)

    def handle(self, op, status, path):
        """Handle incoming response.
        """
        self.cond.acquire()
        self.status[op] = self.status[op] and status.ok
        self.num[op] -= 1

        if not status.ok:
            self.failed[op].append(path)

        if self.num[op] == 0:
            self.cond.notifyAll()

        self.cond.release()

    def wait(self, op):
        """Wait for all responses to arrive.
        """
        self.cond.acquire()

        while self.num[op] != 0:
            self.cond.wait()

        if self.failed[op]:
            self.logger.error("List of failed {0} paths is: {0}".format(op, self.failed[op]))
        else:
            self.logger.debug("All {0} requests were successful".format(op))

        self.cond.release()
        return self.status[op]
