#!/usr/bin/python2
#-------------------------------------------------------------------------------
# @file eos-star-accounting.py
# @author Elvin Sindrilaru - CERN 2021
#------------------------------------------------------------------------------

# ******************************************************************************
# EOS - the CERN Disk Storage System
# Copyright (C) 2021 CERN/Switzerland
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

# Module to generate an accounting record following the EMI StAR specs
# in the version 1.2, for details see
# * http://cds.cern.ch/record/1452920/files/GFD.201.pdf
# * https://wiki.egi.eu/wiki/APEL/Storage
#
# Syntax:
#
# eos-star-accounting [-h] [--help]
# .. to get the help screen
#
# Dependencies:
#  yum install python-lxml python-uuid
#

from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

import os
import sys
import socket
import base64
import lxml.builder as lb
from lxml import etree
import uuid
import datetime
import logging
import subprocess
import optparse
from io import BytesIO

try:
    from urllib.parse import urlparse, urljoin
    import http.client as http_client
except ImportError:
    from urlparse import urlparse, urljoin
    import httplib as http_client

__version__ = '0.0.1'
__author__ = 'Elvin Sindrilaru'

_log = logging.getLogger('eos-star')

SR_NAMESPACE = "http://eu-emi.eu/namespaces/2011/02/storagerecord"
SR = "{%s}" % SR_NAMESPACE
NSMAP = {"sr": SR_NAMESPACE}


def addrecord(xmlroot, hostname, group, user, site, filecount, resourcecapacityused, logicalcapacityused, validduration, recordid=None):
    # Update XML
    rec = etree.SubElement(xmlroot, SR+'StorageUsageRecord')
    rid = etree.SubElement(rec, SR+'RecordIdentity')
    rid.set(SR+"createTime",
            datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))

    if hostname:
        ssys = etree.SubElement(rec, SR+"StorageSystem")
        ssys.text = hostname

    recid = recordid

    if not recid:
        recid = hostname+"-"+str(uuid.uuid1())

    rid.set(SR+"recordId", recid)
    subjid = etree.SubElement(rec, SR+'SubjectIdentity')

    if group:
        grp = etree.SubElement(subjid, SR+"Group")
        grp.text = group

    if user:
        usr = etree.SubElement(subjid, SR+"User")
        usr.text = user

    if site:
        st = etree.SubElement(subjid, SR+"Site")
        st.text = site

    e = etree.SubElement(rec, SR+"StorageMedia")
    e.text = "disk"

    if validduration:
        e = etree.SubElement(rec, SR+"StartTime")
        d = datetime.datetime.utcnow() - datetime.timedelta(seconds=validduration)
        e.text = d.strftime("%Y-%m-%dT%H:%M:%SZ")

    e = etree.SubElement(rec, SR+"EndTime")
    e.text = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    if filecount:
        e = etree.SubElement(rec, SR+"FileCount")
        e.text = str(filecount)

    if not resourcecapacityused:
        resourcecapacityused = 0

    e1 = etree.SubElement(rec, SR+"ResourceCapacityUsed")
    e1.text = str(resourcecapacityused)

    e3 = etree.SubElement(rec, SR+"ResourceCapacityAllocated")
    e3.text = str(resourcecapacityused)

    if not logicalcapacityused:
        logicalcapacityused = 0

    e2 = etree.SubElement(rec, SR+"LogicalCapacityUsed")
    e2.text = str(logicalcapacityused)


def get_site_statistics():
    """
       Get site statistics concerning EOS instance"
    """
    info_dict = {}
    # Get information about used and free bytes
    space_proc = subprocess.Popen(["sudo eos space ls -m"],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, shell=True)
    space_out, __ = space_proc.communicate()

    if space_proc.returncode:
        _log.error("failed eos space ls command")
        raise RuntimeException("failed space ls command")

    tags = ["sum.stat.statfs.usedbytes=", "sum.stat.statfs.freebytes"]

    for token in space_out.split():
        for tag in tags:
            if token.startswith(tag):
                if "usedbytes" in token:
                    info_dict["usedbytes"] = token.split('=')[1]
                elif "freebytes" in token:
                    info_dict["freebytes"] = token.split('=')[1]

    # Get information about number of files
    ns_proc = subprocess.Popen(["sudo eos ns stat -m | grep \"ns.total.files=\""],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, shell=True)
    ns_out, __ = ns_proc.communicate()

    if ns_proc.returncode:
        _log.error("failed eos ns stat command")
        raise RuntimeException("failed eos ns stat command")

    for token in ns_out.split():
        if token.startswith("ns.total.files="):
            info_dict["filecount"] = token.split('=')[1]
            break

    return info_dict


def star(reportgroups, reportusers, record_id, site, hostname, validduration):
    # Init the xml generator
    xmlroot = etree.Element(SR+"StorageUsageRecords", nsmap=NSMAP)

    if not site:
        # Grab the site name from the EOS_INSTANCE_NAME env variable
        site_proc = subprocess.Popen(["cat /etc/sysconfig/eos_env | grep EOS_INSTANCE_NAME | awk -F '=' '{print $2;}'"],
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     shell=True)
        site_out, __ = site_proc.communicate()

        if len(site_out) == 0:
            _log.error("could not determine site name")
            sys.exit(EINVAL)
        else:
            site = site_out.strip('\0\n')
            _log.debug("using \"{0}\" as site name".format(site))

    if site:
        # Report about site
        _log.debug("Site reporting: starting")
        info_dict = get_site_statistics()
        _log.debug("Site stats: {0}".format(info_dict))
        addrecord(xmlroot, hostname, None, None, site, info_dict["filecount"],
                  info_dict["usedbytes"], info_dict["usedbytes"],
                  validduration)

    if reportgroups:
        # Report about groups
        _log.debug("Groups reporting: starting")
        pass

    if reportusers:
        #
        # Report about users
        #
        _log.debug("Users reporting: starting")
        pass

    out = BytesIO()
    et = etree.ElementTree(xmlroot)
    et.write(out, pretty_print=True, encoding="utf-8")
    return out.getvalue().decode('utf-8')


def main():
    # basic logging configuration
    streamHandler = logging.StreamHandler(sys.stderr)
    streamHandler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s](%(module)s:%(lineno)d) %(message)s", "%d %b %H:%M:%S"))
    _log.addHandler(streamHandler)
    _log.setLevel(logging.WARN)

    parser = optparse.OptionParser()
    parser.add_option('--reportgroups', dest='reportgroups', action='store_true', default=False, help="Report about groups")
    parser.add_option('--reportusers', dest='reportusers', action='store_true', default=False, help="Report about users")
    parser.add_option('-v', '--debug', dest='verbose', action='count', default=0, help='Increase verbosity level for debugging (on stderr)')
    parser.add_option('--hostname', dest='hostname', default=socket.getfqdn(), help="The hostname string to use in the record. Default: this host.")
    parser.add_option('--site', dest='site', default="", help="The site string to use in the record. Default: none.")
    parser.add_option('--recordid', dest='recordid', default=None, help="The recordid string to use in the record. Default: a newly computed unique string.")
    parser.add_option('--validduration', dest='validduration', default=86400, help="Valid duration of this record, in seconds (default: 1 day)")

    options, args = parser.parse_args(sys.argv[1:])
    if options.verbose == 0: _log.setLevel(logging.ERROR)
    elif options.verbose == 1: _log.setLevel(logging.WARN)
    elif options.verbose == 2: _log.setLevel(logging.INFO)
    else: _log.setLevel(logging.DEBUG)

    data = star(options.reportgroups, options.reportusers, options.recordid, options.site, options.hostname, options.validduration)
    sys.stdout.write(data)
    _log.debug('done')

    return os.EX_OK

if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        print("Got an exception: {0}".format(err), file=sys.stderr)
