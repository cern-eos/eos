#!/usr/bin/python3
# ----------------------------------------------------------------------
# File: gridmap-iam.py
# Author: Manuel Reis - CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2021 CERN/Switzerland                                  *
# *                                                                      *
# * This program is free software: you can redistribute it and/or modify *
# * it under the terms of the GNU General Public License as published by *
# * the Free Software Foundation, either version 3 of the License, or    *
# * (at your option) any later version.                                  *
# *                                                                      *
# * This program is distributed in the hope that it will be useful,      *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
# * GNU General Public License for more details.                         *
# *                                                                      *
# * You should have received a copy of the GNU General Public License    *
# * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
# ************************************************************************

import re
import json
import logging
import argparse
from sys import exit
from os import getenv
from urllib import request, parse
from configparser import ConfigParser
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

class IAM_Server:
    TOKEN_ENDPOINT = '/token'
    USER_ENDPOINT ='/scim/Users'

    def __init__(self, server, client_id, client_secret, token_server = None):
        self.server = server
        self.client_id = client_id
        self.client_secret = client_secret
        # Assuming token server is the same as IAM's
        self.token_server = token_server or server
        self._token = None

    def __hash__(self):
        return hash(self.server)
    
    def __eq__(self, other):
        return self.server == other.server

    def __get_token(self):
        """
        Authenticates with the iam server and returns the access token.
        """
        request_data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "scim:read"
        }
        now = datetime.now()
        
        response = request.urlopen(f'https://{self.token_server}{self.TOKEN_ENDPOINT}', data=parse.urlencode(request_data).encode('utf-8'))
        response = json.loads(response.read())

        if 'access_token' not in response:
            raise BaseException("Authentication Failed")
        response['request_time'] = now
        self._token = response
    
    @property
    def token(self):
        """
        Property that return and renews the bearer token if expired
        """
        if self._token is None or self._token['request_time'] + timedelta(seconds=self._token['expires_in']-10) < datetime.now():
            self.__get_token()
        return self._token['access_token']

    def get_users(self, start_index = 0,count = 1, filter_function=None, **kwargs):
        """
        Queries the server to get all users belonging to the VO.
        Each batch can be up to 100 records so the requests are parallelized
        """
        # Get's a new token if expired
        header = {"Authorization": f"Bearer {self.token}"} 

        users_so_far = 0
        startIndex = 0
        params = {"startIndex": startIndex, "count": count}
        params["startIndex"] = startIndex
        # Get's a new token if expired
        header["Authorization"] = f"Bearer {self.token}"
        req = request.Request(f"https://{self.server}{self.USER_ENDPOINT}?{parse.urlencode(params)}", headers=header)
        response = request.urlopen(req)
        response = json.loads(response.read())
        
        users_dn = set() 
        # We can use a with statement to ensure threads are cleaned up promptly
        with ThreadPoolExecutor(max_workers=int(response['totalResults']/100)) as executor:
            # Start the load operations and mark each future with its URL
            reqs = []
            for start_index in range(0,response['totalResults'],100):
                params["startIndex"] = start_index
                # Get's a new token if expired
                header["Authorization"] = f"Bearer {self.token}"
                req = request.Request(f"https://{self.server}{self.USER_ENDPOINT}?{parse.urlencode(params)}", headers=header)
                reqs.append(executor.submit(request.urlopen, req))

            for req in as_completed(reqs):
                try:
                    response=req.result()
                except Exception as e:
                    print(f'{req} generated an exception: {e}')
                else:
                    response = json.loads(response.read())
                    users_dn.update(filter_function(*response['Resources'], **kwargs))

        return users_dn

    def dn_filter(self, *users, pattern=None, sensitive=0, prefer_cern=False):
        """
        Collect users with DN certificates matching regex
        """
        logging.debug(f"This request has {len(users)}")
        matching_dn = set()
        for user in users:
            try:
                certs = user['urn:indigo-dc:scim:schemas:IndigoUser']['certificates']
                # Is there a CERN certificate if prefered?
                if prefer_cern:
                    certs = [*filter(lambda x: x.get('subjectDn',x.get('issuerDn')).endswith('DC=cern,DC=ch'), certs)] or certs
        
                for cert in certs:
                    # Revert subjectDn and replace , with /
                    grid_dn = '/'.join(cert["subjectDn"].split(',')[::-1])
                    if pattern is None or pattern.search(grid_dn): 
                        matching_dn.add(f'/{grid_dn}')
            except KeyError:
                logging.warning(f"User {user['id']} doesn't have certificate to extract info (skipping it)")

        logging.info(f"{len(matching_dn)} matching certificates")
        return matching_dn


def buildgridmap(users_dn, account, ifile, ofile):
    grid_map = {}
    if ifile:
        # As some entries may be encoded in latin let's escape it as unicode
        with open(ifile, "r", encoding='unicode_escape') as igridmap_file:
            for dn,acc in (l.rsplit(' ',1) for l in igridmap_file.readlines()):
                grid_map[dn] = acc.strip()
    
    # Overwrite / append results
    for dn in users_dn:
        if dn in grid_map:
            logging.debug(f'Overwritting {dn}')
        grid_map[f'"{dn}"'] = account

    content = '\n'.join(f'{dn} {acc}' for dn, acc in grid_map.items())
    if ofile:
        try: 
            with open(ofile, "w", encoding='utf-8') as ogridmap_file:
                ogridmap_file.write(content)
        except Exception as e:
            logging.error(f'Unable to write to {ofile}, raised exception {e}')
            exit(4)
    
    print(content)


def configure(credentials, servers, targets):

    # First level of configuration - file
    iam_servers = set()

    # Second configuration stage is to load environment variable configuration
    envconf = zip(getenv('EOS_IAM_SERVER','').splitlines(), getenv('EOS_IAM_CLIENT_ID','').splitlines(), getenv('EOS_IAM_CLIENT_SECRET','').splitlines())
    # First configuration stage is to use command args 
    for server, client_id, client_secret in servers or envconf:
        iam_servers.add(IAM_Server(server, client_id, client_secret))

    # Third option is to rely on configuration file
    #[<iam server hostname>]
    #client-id = <id>
    #client-secret = <key>
    if credentials and len(iam_servers) == 0 :
        config = ConfigParser()
        files_read = config.read(credentials)
        if len(files_read) > 0: 
            # Credentials file should have IAM server on the section
            if targets is not None:
                it = filter(lambda x: True if targets in x else False, config.sections())
            else:
                it = config.sections()

            for section in it:
                server = section
                client_id = config.get(section,'client-id')
                client_secret = config.get(section,'client-secret')
                # Assuming IAM server is token server if not defined
                token_server = config.get(section,'token-server', fallback=server)
                iam_servers.add(IAM_Server(server, client_id, client_secret, token_server))
        else:
            logging.warning("Credentials couldn't be loaded from configuration file")

    if len(iam_servers):
        return iam_servers
    else:
        logging.error('Configuration problem! Configuration file not loaded (correctly?), environment not set, arguments not passed')
        exit(3)
    
def main(server = None, credentials=None, targets = None, account=None, ifile=None, ofile=None, pattern=None, sensitive=0, debug_level=logging.WARNING, prefer_cern=False):
    """
    Configure IAM servers to be queried, update/write gridmap file format
    """

    logging.basicConfig(level=debug_level)
    try:
        pattern = re.compile(pattern, flags=sensitive)
    except:
        if pattern is not None:
            logging.critical(f'Pattern provided cannot be compiled: {pattern}')
            exit(1)
    
    iam_servers = configure(credentials, server, targets)
    
    # Query IAM server
    users_dn = set()
    for iam in iam_servers:
        users_dn.update(iam.get_users(filter_function=iam.dn_filter, pattern=pattern, sensitive=sensitive, count=100, prefer_cern=prefer_cern))

    # Update/Write gridmap file
    buildgridmap(users_dn, account, ifile, ofile)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GRID Map file generation from IAM Server')
    parser.add_argument('-v', '--verbose', type = str.upper, nargs='?', const="DEBUG", default="WARNING", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"), dest = 'debug', help = 'Control log verbosity')
    parser.add_argument('-s', '--server', dest = 'server', nargs=3,action='append', help = 'IAM server to query with respective client key and secret (space separated)', metavar=('SERVER', 'CLIENT_ID','CLIENT_KEY'))
    parser.add_argument('-c', '--credentidals', dest = 'credentials', help = r'Client credentials file (for API access) in the following format: `[<iam server hostname>]\nclient-id = <id>\nclient-secret = <key>`')
    parser.add_argument('-t', '--targets', dest = 'targets', help = 'Target specific IAM servers defined in the configuration file (must be used together with -c)')
    parser.add_argument('-i', '--inputfile', dest = 'ifile', default=None, help = "Path to existing gridmapfile to be updated (matching DN's will be overwritten)")
    parser.add_argument('-o', '--outfile', dest = 'ofile', default=None, help = 'Path to dump gridmapfile')
    parser.add_argument('-a', '--account', dest = 'account', required=True, help = 'Account to which the result from the match should be mapped to')
    
    parser.add_argument('-C','--case-sensitive', dest='sensitive',action='store_const', const=0, default=re.IGNORECASE, help = 'Pattern to search on user certificates `subject DN` field')
    parser.add_argument('-p', '--pattern', type=str, dest = 'pattern', default=None, help = 'Pattern to search on user certificates `subject DN` field')
    parser.add_argument('-u', '--prefer-cern-certs', dest = 'prefer_cern',action='store_true', help = 'Prefers CERN.CH certificates (if any) to map user (uniquely)')

    args = parser.parse_args()

    logging.basicConfig(level=eval(f"logging.{args.debug}"))
    logging.debug(args)
    main(server=args.server, credentials=args.credentials, targets=args.targets, ifile=args.ifile, ofile=args.ofile, account=args.account, pattern=args.pattern, sensitive=args.sensitive, debug_level=logging.DEBUG, prefer_cern=args.prefer_cern )
