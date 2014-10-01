# ------------------------------------------------------------------------------
# File: configuration.py
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
import sys
import logging
import logging.handlers


class Configuration(object):
    """ Configuration class for the archiving daemon and the transfer processes.
    """
    def __init__(self):
	""" Initialize the configuration by reading in all the parameters from
	the configuration file supplied.

	Args:
	    fn_conf (string): Path to the configuration file, which in normal
	    conditions should be/etc/eosarchived.conf
	"""
	# Get environment variables and setup constants based on them
	try:
	    LOG_DIR = os.environ["LOG_DIR"]
	except KeyError as err:
	    print >> sys.stderr, "LOG_DIR env. not found"
	    raise

	try:
	    self.__dict__['EOS_ARCHIVE_DIR'] = os.environ["EOS_ARCHIVE_DIR"]
	except KeyError as err:
	    print >> sys.stderr, "EOS_ARCHIVE_DIR env. not found"
	    raise

	try:
	    archive_conf = os.environ["EOS_ARCHIVE_CONF"]
	except KeyError as err:
	    print >> sys.stderr, "EOS_ARCHIVE_CONF env. not found using /etc/eosarchived.conf"
	    archive_conf= "/etc/eosarchived.conf"

	log_dict = {"debug":   logging.DEBUG,
		    "notice":  logging.INFO,
		    "info":    logging.INFO,
		    "warning": logging.WARNING,
		    "error":   logging.ERROR,
		    "crit":    logging.CRITICAL,
		    "alert":   logging.CRITICAL}

	self.__dict__['LOG_FILE'] = LOG_DIR + "eosarchive.log"
	self.__dict__['FRONTEND_IPC'] = self.EOS_ARCHIVE_DIR + "archive_frontend.ipc"
	self.__dict__['BACKEND_REQ_IPC'] = self.EOS_ARCHIVE_DIR + "archive_backend_req.ipc"
	self.__dict__['BACKEND_PUB_IPC'] = self.EOS_ARCHIVE_DIR + "archive_backend_pub.ipc"
	self.__dict__['CREATE_OP'] = 'create'
	self.__dict__['GET_OP'] = 'get'
	self.__dict__['PUT_OP'] = 'put'
	self.__dict__['LIST_OP'] = 'list'
	self.__dict__['PURGE_OP'] = 'purge'
	self.__dict__['DELETE_OP'] = 'delete'
	self.__dict__['KILL_OP'] = 'kill'
	self.__dict__['OPT_RETRY'] = 'retry'
	self.__dict__['ARCH_FN'] = ".archive"
	self.__dict__['ARCH_INIT'] = ".archive.init"

	with open(archive_conf, 'r') as f:
	    for line in f:
		line = line.strip('\0\n ')

		if len(line) and line[0] != '#':
		    tokens = line.split('=', 1)
		    # Try to convert to int by default
		    try:
			self.__dict__[tokens[0]] = int(tokens[1])
		    except ValueError as __:
			if tokens[0] == 'LOG_LEVEL':
			    self.__dict__[tokens[0]] = log_dict[tokens[1]]
			else:
			    self.__dict__[tokens[0]] = tokens[1]

	# If no loglevel is set use INFO
	try:
	    self.__dict__['LOG_LEVEL']
	except KeyError as __:
	    self.__dict__['LOG_LEVEL'] = logging.INFO

    def start_logging(self, logger_name, log_file, timed_rotating = False):
	""" Configure the logging

	Args:
	    logger_name (string): Name of the logger
	    timed_rotating (boolean): If True is a TimedRotatingFileHandler
	"""
	log_format = ('%(asctime)-15s %(name)s[%(process)d] %(filename)s:'
		      '%(lineno)d LVL=%(levelname)s %(message)s')
	logging.basicConfig(level=self.LOG_LEVEL, format=log_format)
	self.__dict__['LOGGER_NAME'] = logger_name
	self.__dict__['LOG_FILE'] = log_file
	self.logger = logging.getLogger(self.LOGGER_NAME)
	formatter = logging.Formatter(log_format)

	if timed_rotating:
	    self.handler = logging.handlers.TimedRotatingFileHandler(self.LOG_FILE, 'midnight')
	else:
	    self.handler = logging.FileHandler(self.LOG_FILE)

	self.handler.setFormatter(formatter)
	self.logger.addHandler(self.handler)
	self.logger.propagate = False

    def display(self):
	""" Print configuration either to the log file or stderr
	"""
	try:
	    self.logger.info("Configuration parameters:")

	    for key, val in self.__dict__.iteritems():
		self.logger.info("conf.{0} = {1}".format(key, val))
	except AttributeError as __:
	    print >> sys.stderr, "Configuration parameters:"

	    for key, val in self.__dict__.iteritems():
		print >> sys.stderr, "conf.{0} = {1}".format(key, val)

    def __setattr__(self, name, value):
	""" Set object attribute

	Args:
	    name (string): Attribute name
	    value (string): Attribute value
	"""
	self.__dict__[name] = value
