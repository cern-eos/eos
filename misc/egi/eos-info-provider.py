#!/usr/bin/python2
#-------------------------------------------------------------------------------
# @file eos-info-provider.py
# @author: Elvin-Alin Sindrilaru - CERN 2021
#-------------------------------------------------------------------------------
#
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

import argparse
import socket
import json
import subprocess
import os
import sys
import ldap3
import traceback
from datetime import datetime


class SystemInfo(object):
    """All necessary info on EOS and the system"""

    def __init__(self, host, cert, key):
        self.host = host
        self.cert = cert
        self.key = key
        self.capath = '/etc/grid-security/certificates/'
        self.ports = set([])
        self.getlisteningports()

    def getlisteningports(self):
        """Check which ports are being listened on"""
        try:
            devnull = os.open(os.devnull, os.O_RDWR) # subprocess.DEVNULL only since python 3.3
            pipe_out, _ = subprocess.Popen("ss -tln", shell=True, stdout=subprocess.PIPE, stderr=devnull).communicate()
            for listen in pipe_out.split('\n'):
                m = re.search(r":([0-9]+)\s", listen)
                if m != None:
                    self.ports.add(int(m.group(1)))
        except:
            pass

    def rpmversion(self, package):
        """Get version number from rpm package"""
        rpm_proc = subprocess.Popen(["rpm -qa | grep {0} | awk -F '-' '{{print $3;}}'".format(package)],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, shell=True)
        rpm_out, __ = rpm_proc.communicate()

        if rpm_proc.returncode:
            return str("none")

        return str(rpm_out)

    def getprotinfo(self, prot):
        """Return relevant info for each protocol"""
        if prot == "https":
            if 9000 in self.ports:
                interfacename = "https"
                interfaceversion = self.rpmversion("eos-xrootd")
                return interfacename, interfaceversion, interfacename, interfaceversion
            else:
                return None
        if prot == "root":
            if 1094 in self.ports:
                interfacename = "xroot"
                interfaceversion = self.rpmversion("eos-xrootd")
                return interfacename, interfaceversion, interfacename, interfaceversion
            else:
                return None
        return None


    def get_site_statistics(self):
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



class Entry(object):
    """Base class for all GLUE2 entries"""

    def __init__(self):
        # An array of child objects in the ldap hierarchy
        self.Children = []
        # This will be the attribute taken for the DN
        self.name = ""
        self.Attributes = {}
        self.Attributes["GLUE2EntityCreationTime"] = [datetime.utcnow().replace(microsecond=0).isoformat() + 'Z']
        if sys.version_info[:2] < (2, 7):
            self.ldif_writer = ldif.LDIFWriter(sys.stdout, cols=1000000)
        else:
            self.ldif_writer = ldap3.Connection(None, client_strategy='LDIF')

    def add_child(self, entry):
        self.Children.append(entry)

    def convert(self, data):
        ret = {}
        for k, v in data.items():
            ret[k] = list(map(lambda x: x.encode('utf-8'), v))

    def print_out(self, parent=None):
        """ Recursively print out with all children"""
        if parent == None:
            dn = self.name + "=" + self.Attributes[self.name][0]
        else:
            dn = self.name + "=" + self.Attributes[self.name][0] + "," + parent
        bAttributes = {}
        if sys.version_info[:2] < (2, 7):
            # python3 LDIFWriter works only with string dictionary keys
            # and list of bytes as dictionary attribute values
            for k, v in self.Attributes.items():
                bAttributes[k] = list(map(lambda x: x.encode('utf-8'), v))
            self.ldif_writer.unparse(dn, bAttributes)
        else:
            bClasses = None
            for k, v in self.Attributes.items():
                if k.lower() == 'objectclass':
                    bClasses = list(map(lambda x: x.encode('utf-8'), v))
                else:
                    bAttributes[k] = list(map(lambda x: x.encode('utf-8'), v))
            sys.stdout.write(self.ldif_writer.add(dn, bClasses, bAttributes))
            sys.stdout.write('\n')

        for e in self.Children:
            e.print_out(dn)

    def set_foreign_key(self):
        """To be implemented by subclasses"""
        pass

    def getname(self):
        return self.Attributes[self.name][0]


class Service(Entry):
    """A GLUE2Service"""

    def __init__(self, hostname, sitename):
        Entry.__init__(self)
        self.name = "GLUE2ServiceID"
        self.Attributes["GLUE2ServiceID"] = [hostname + "/Service"]
        self.Attributes["GLUE2ServiceType"] = ["eos"]
        self.Attributes["GLUE2ServiceQualityLevel"] = ["production"]
        self.Attributes["GLUE2ServiceCapability"] = [
            'data.access.flatfiles',
            'data.transfer',
            'data.management.replica',
            'data.management.storage',
            'data.management.transfer',
            'security.authentication',
            'security.authorization',
        ]
        self.Attributes["GLUE2ServiceAdminDomainForeignKey"] = [sitename]
        self.Attributes["ObjectClass"] = ["GLUE2Service", "GLUE2StorageService"]


class Endpoint(Entry):
    """A GLUE2Endpoint"""

    def __init__(self, epprot, ephost, epport, eppath, epcert, interfacename, interfaceversion, implname, implversion):
        Entry.__init__(self)
        self.name = "GLUE2EndpointID"
        self.Attributes["GLUE2EndpointID"] = [ephost + "/Endpoint/" + epprot]
        self.Attributes["GLUE2EndpointURL"] = [epprot + "://" + ephost + ":" + str(epport) + eppath]
        if epcert != None: self.Attributes["GLUE2EndpointIssuerCA"] = [epcert]
        self.Attributes["GLUE2EndpointInterfaceName"] = [interfacename]
        self.Attributes["GLUE2EndpointInterfaceVersion"] = [interfaceversion]
        self.Attributes["GLUE2EndpointImplementationName"] = [implname]
        self.Attributes["GLUE2EndpointImplementationVersion"] = [implversion]
        self.Attributes["GLUE2EndpointQualityLevel"] = ["production"]
        self.Attributes["GLUE2EndpointHealthState"] = ["ok"]
        self.Attributes["GLUE2EndpointServingState"] = ["production"]
        self.Attributes["GLUE2EndpointCapability"] = [
            'data.access.flatfiles',
            'data.transfer',
            'data.management.replica',
            'data.management.storage',
            'data.management.transfer',
            'security.authentication',
            'security.authorization',
        ]
        self.Attributes["GLUE2EndpointServiceForeignKey"] = ["Undefined"]
        self.Attributes["ObjectClass"] = ["GLUE2StorageEndpoint", "GLUE2Endpoint"]

    def set_foreign_key(self, value):
        self.Attributes["GLUE2EndpointServiceForeignKey"] = [value]


class AccessProtocol(Entry):
    """A Glue2AccessProtocol"""

    def __init__(self, ephost, interfacename, interfaceversion, implname, implversion):
        Entry.__init__(self)
        self.name = "GLUE2StorageAccessProtocolID"
        self.Attributes["GLUE2StorageAccessProtocolID"] = [ephost + "/AccessProtocol/" + interfacename]
        self.Attributes["GLUE2StorageAccessProtocolVersion"] = [interfaceversion]
        self.Attributes["GLUE2StorageAccessProtocolType"] = [interfacename]
        # self.Attributes["GLUE2StorageAccessProtocolEndpoint"]=[endpointurl]
        self.Attributes["GLUE2StorageAccessProtocolStorageServiceForeignKey"] = ["Undefined"]
        self.Attributes["ObjectClass"] = ["GLUE2StorageAccessProtocol"]

    def set_foreign_key(self, value):
        self.Attributes["GLUE2StorageAccessProtocolStorageServiceForeignKey"] = [value]


class AccessPolicy(Entry):
    """A GLUE2AccessPolicy"""

    def __init__(self, groups, hostname):
        Entry.__init__(self)
        self.name = "GLUE2PolicyID"
        self.Attributes["GLUE2PolicyID"] = [hostname + "/AccessPolicy"]
        self.Attributes["GLUE2PolicyRule"] = groups
        self.Attributes["GLUE2PolicyScheme"] = ["org.glite.standard"]
        self.Attributes["GLUE2AccessPolicyEndpointForeignKey"] = ["Undefined"]
        self.Attributes["ObjectClass"] = ["GLUE2AccessPolicy"]

    def set_foreign_key(self, value):
        self.Attributes["GLUE2AccessPolicyEndpointForeignKey"] = [value]


class StorageServiceCapacity(Entry):
    """A GLUE2StorageServiceCapacity"""

    def __init__(self, hostname, tot, used):
        Entry.__init__(self)
        self.name = "GLUE2StorageServiceCapacityID"
        self.Attributes["GLUE2StorageServiceCapacityID"] = [hostname + "/StorageServiceCapacity"]
        self.Attributes["GLUE2StorageServiceCapacityType"] = ["online"]
        self.Attributes["GLUE2StorageServiceCapacityFreeSize"] = [str((tot - used) // 1024**3)]
        self.Attributes["GLUE2StorageServiceCapacityTotalSize"] = [str(tot // 1024**3)]
        self.Attributes["GLUE2StorageServiceCapacityUsedSize"] = [str(used // 1024**3)]
        self.Attributes["GLUE2StorageServiceCapacityStorageServiceForeignKey"] = ["Undefined"]
        self.Attributes["ObjectClass"] = ["GLUE2StorageServiceCapacity"]

    def set_foreign_key(self, value):
        self.Attributes["GLUE2StorageServiceCapacityStorageServiceForeignKey"] = [value]


class DataStore(Entry):
    """A GLUE2DataStore"""

    def __init__(self, hostname, tot, used):
        Entry.__init__(self)
        self.name = "GLUE2ResourceID"
        self.Attributes["GLUE2ResourceID"] = [hostname + "/DataStore"]
        self.Attributes["GLUE2DataStoreTotalSize"] = [str(tot // 1024**3)]
        self.Attributes["GLUE2DataStoreUsedSize"] = [str(used // 1024**3)]
        self.Attributes["GLUE2DataStoreFreeSize"] = [str((tot - used) // 1024**3)]
        self.Attributes["GLUE2DataStoreType"] = ["disk"]
        self.Attributes["GLUE2DataStoreLatency"] = ["online"]
        self.Attributes["GLUE2DataStoreStorageManagerForeignKey"] = ["Undefined"]
        self.Attributes["GLUE2ResourceManagerForeignKey"] = ["Undefined"]
        self.Attributes["ObjectClass"] = ["GLUE2DataStore"]

    def set_foreign_key(self, value):
        self.Attributes["GLUE2DataStoreStorageManagerForeignKey"] = [value]
        self.Attributes["GLUE2ResourceManagerForeignKey"] = [value]


class MappingPolicy(Entry):
    """A GLUE2MappingPolicy"""

    def __init__(self, qtname, groups, hostname):
        Entry.__init__(self)
        self.name = "GLUE2PolicyID"
        self.Attributes["GLUE2PolicyID"] = [hostname + "/MappingPolicy/" + qtname]
        self.Attributes["GLUE2PolicyRule"] = groups
        self.Attributes["GLUE2PolicyScheme"] = ["org.glite.standard"]
        self.Attributes["GLUE2MappingPolicyShareForeignKey"] = ["Undefined"]
        self.Attributes["ObjectClass"] = ["GLUE2MappingPolicy"]

    def set_foreign_key(self, value):
        self.Attributes["GLUE2MappingPolicyShareForeignKey"] = [value]


class Manager(Entry):
    """A GLUE2Policy"""

    def __init__(self, eosversion, hostname):
        Entry.__init__(self)
        self.name = "GLUE2ManagerID"
        self.Attributes["GLUE2ManagerID"] = [hostname + "/Manager"]
        self.Attributes["GLUE2ManagerProductName"] = ["EOS"]
        self.Attributes["GLUE2ManagerProductVersion"] = [eosversion]
        self.Attributes["GLUE2StorageManagerStorageServiceForeignKey"] = ["Undefined"]
        self.Attributes["GLUE2ManagerServiceForeignKey"] = ["Undefined"]
        self.Attributes["ObjectClass"] = ["GLUE2StorageManager", "GLUE2Manager"]

    def set_foreign_key(self, value):
        self.Attributes["GLUE2StorageManagerStorageServiceForeignKey"] = [value]
        self.Attributes["GLUE2ManagerServiceForeignKey"] = [value]


def create_endpoints(sysinfo):
    """Find all the StorageEndpoints on this DPM
    and return an array of them"""
    cert_subject = None
    try:
        from M2Crypto import X509
        x509 = X509.load_cert(sysinfo.cert, X509.FORMAT_PEM)
        cert_subject = "/%s" % '/'.join(x509.get_issuer().as_text().split(', '))
    except:
        pass

    ret = []
    if 9000 in sysinfo.ports:
        ret.append(Endpoint("https", sysinfo.host, 9000, "", cert_subject, *sysinfo.getprotinfo("https")))
    if 1094 in sysinfo.ports:
        ret.append(Endpoint("xroot", sysinfo.host, 1094, "/", cert_subject, *sysinfo.getprotinfo("root")))

    return ret


def create_accessprotocols(sysinfo):
    """Find all the StorageEndpoints and return an array of them"""
    ret = []
    if 9000 in sysinfo.ports:
        ret.append(AccessProtocol(sysinfo.host, *sysinfo.getprotinfo("https")))
    if 1094 in sysinfo.ports:
        ret.append(AccessProtocol(sysinfo.host, *sysinfo.getprotinfo("root")))
    return ret


def create_manager(sysinfo):
    """Create a StorageManager"""
    info_dict = sysinfo.get_site_statistics()
    mgr = Manager(sysinfo.rpmversion("eos-server"), sysinfo.host)
    totalcapacity = int(info_dict["usedbytes"]) + int(info_dict["freebytes"])
    ds = DataStore(sysinfo.host, totalcapacity, int(info_dict["usedbytes"]))
    ds.set_foreign_key(mgr.getname())
    mgr.add_child(ds)
    return mgr


def main():
    # Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert", help="Path to host certificate", default="/etc/grid-security/hostcert.pem")
    parser.add_argument("--key", help="Path to host key", default="/etc/grid-security/hostkey.pem")
    parser.add_argument("--host", help="fqdn", default=socket.getfqdn())
    parser.add_argument("--sitename", help="site name", required=True)
    args = parser.parse_args()

    # Create the top of the tree
    top = Service(args.host, args.sitename)
    sysinfo = SystemInfo(args.host, args.cert, args.key)
    info_dict = sysinfo.get_site_statistics()
    totalcapacity = int(info_dict["usedbytes"]) + int(info_dict["freebytes"])
    ssc = StorageServiceCapacity(args.host, totalcapacity, int(info_dict["usedbytes"]))
    ssc.set_foreign_key(top.getname())
    top.add_child(ssc)

    for endpoint in create_endpoints(sysinfo):
        endpoint.set_foreign_key(top.getname())
        ap = AccessPolicy(totalgroups, args.host)
        ap.set_foreign_key(endpoint.getname())
        endpoint.add_child(ap)
        top.add_child(endpoint)

    for accessprotocol in create_accessprotocols(sysinfo):
        accessprotocol.set_foreign_key(top.getname())
        top.add_child(accessprotocol)

    manager = create_manager(sysinfo)
    manager.set_foreign_key(top.getname())
    top.add_child(manager)

    # Print everything out
    prefix = "GLUE2GroupID=resource,o=glue"
    top.print_out(prefix)


# Excecute
if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        print("Got an exception: {0} in {1}".format(err, traceback.print_exc()))
