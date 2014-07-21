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
import logging
import json
from threading import Condition


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
    self.mkdir_num = 0
    self.mkdir_status = True
    self.mkdir_failed = []
    self.cond = Condition()
    self.logger = logging.getLogger("ArchiveFile." + type(self).__name__)

  def register_mkdir(self, path):
    self.cond.acquire()
    self.mkdir_num += 1
    self.cond.release()
    return self._MkDirHandler(path, self)

  def handle_mkdir(self, status, path):
    self.cond.acquire()
    self.mkdir_status = self.mkdir_status and status.ok
    self.mkdir_num -= 1

    if not status.ok:
      self.mkdir_failed.append(path)

    if self.mkdir_num == 0:
      self.cond.notifyAll()

    self.cond.release()

  def wait_mkdir(self):
    self.cond.acquire()

    while self.mkdir_num != 0:
      self.cond.wait()

    if self.mkdir_failed:
      self.logger.error("List of failed mkdir paths is: {0}".format(self.mkdir_failed))
    else:
      self.logger.debug("All mkdir requests were successful")

    self.cond.release()
    return self.mkdir_status

  class _MkDirHandler(object):
    """ Async mkdir handler which reports to MetaHandler.

    Attributes:
      path (string): Directory path for which the handler is created.
      meta_handler (MetaHandler): Meta handler object.
    """
    def __init__(self, path, meta_handler):
      self.path = path
      self.meta_handler = meta_handler

    def __call__(self, status, response, hostlist):
      self.meta_handler.handle_mkdir(status, self.path)
