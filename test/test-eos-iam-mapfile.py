#!/usr/bin/python3
# ----------------------------------------------------------------------
# File: test-eos-iam-mapfile.py
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

from unittest import TestCase
from importlib.machinery import SourceFileLoader
import types
loader = SourceFileLoader('eos-iam-mapfile', '/usr/sbin/eos-iam-mapfile')
iam = types.ModuleType(loader.name)
loader.exec_module(iam)

# Disable iam logging
iam.logging.disable(iam.logging.WARNING)

def make_user(id='123',fname='dwight',lname='shrutte', role='sales', cert=None):
    ret = {
        'id': f'{id}',
        'meta': {
            'created': '2022-01-01T01:01:01.000+01:00',
            'lastModified': '2022-01-22T01:01:47.000+01:00',
            'location': f'https://example/scim/Users/{id}',
            'resourceType': 'User'
        },
        'schemas': [
            'urn:ietf:params:scim:schemas:core:2.0:User',
            'urn:indigo-dc:scim:schemas:IndigoUser'
        ],
        'userName': f'{fname.lower()[0]}{lname.lower()}',
        'name': {
            'familyName': lname.upper(),
            'formatted': f'{fname.upper()} {lname.upper()}',
            'givenName': lname.upper()
        },
        'displayName': f'{fname.upper()[0]}{fname.lower()[1:]} {lname.upper()[0]}{lname.lower()[1:]}',
        'active': True,
        'emails': [
            {
                'type': 'work',
                'value': f'{fname.lower()[0]}{lname.lower()}@example.com',
                'primary': True
                }
        ],
        'groups': [
            {
                'display': role,
                'value': f'{id}',
                '$ref': f'https://example.com/scim/Groups/{id}'
            }
        ]
    }
    if cert is not None:
        dn = "DC=dunder,DC=us"
        issuer = "CN=Dunder Auth,C=us"
        if cert.lower() == 'cern':
            dn = "DC=cern,DC=ch"
            issuer="CN=CERN Grid Certification Authority,DC=cern,DC=ch"

        if cert.lower() == 'upper':
            dn = "DC=cern,DC=ch".upper()
            issuer="CN=CERN Grid Certification Authority,DC=cern,DC=ch"
        elif cert.lower() == 'lower':
            dn = dn.lower()
            issuer="CN=CERN Grid Certification Authority,DC=cern,DC=ch"

        ret["urn:indigo-dc:scim:schemas:IndigoUser"] = {
            "oidcIds": [
                {
                    "issuer": "https:/example.com/auth/realms/cern",
                    "subject": ret["userName"]
                }
            ],
            "certificates": [
                {
                    "primary": True,
                    "subjectDn": f"CN={ret['displayName']},OU={role[0].upper()}{role[1:]},{dn}",
                    "issuerDn": issuer,
                    "pemEncodedCertificate": "-----BEGIN CERTIFICATE-----\n THIS IS A VALID VERTIFICATE!?==\n-----END CERTIFICATE-----",
                    "display": "cert-0",
                    "created": "2022-01-21T17:32:31.000+01:00",
                    "lastModified": "2022-01-21T17:32:31.000+01:00",
                    "hasProxyCertificate": False
                }
            ]
        }

    return ret


class FiltersTestCase(TestCase):

    def test_dn_filter(self):
        dwight  = make_user()
        jim     = make_user(id='23123123-2312-2312-2312-231231231231',fname='jim',lname='Halpert', role='sales',cert='dunder')
        pam     = make_user(id='31231231-3123-3123-3123-312312312312',fname='Pam',lname='Dawber',role="reception",cert='cern')
        kevin   = make_user(id='41231231-3123-3123-3123-312312312312', fname='Kevin', lname='Malone', role='accounting', cert='upper')
        creed   = make_user(id='51231231-3123-3123-3123-312312312312', fname='Creed', lname='Bratton', role='accounting', cert='lower')
        michael = make_user(id='61231231-3123-3123-3123-312312312312',fname='Michael',lname='Scott',role="management",cert='cern')

        pamSubDN='/DC=ch/DC=cern/OU=Reception/CN=Pam Dawber'
        jimSubDN='/DC=us/DC=dunder/OU=Sales/CN=Jim Halpert'
        kevinSubDN='/DC=CH/DC=CERN/OU=Accounting/CN=Kevin Malone'
        creedSubDN='/dc=us/dc=dunder/OU=Accounting/CN=Creed Bratton'
        michaelSubDN='/DC=ch/DC=cern/OU=Management/CN=Michael Scott'

        # Only Dwight doesn't have a certificate
        assert iam.dn_filter(dwight,pam,jim,kevin,creed,michael) == {pamSubDN,jimSubDN,kevinSubDN,creedSubDN,michaelSubDN}

        # Pam , Michael and Kevin certificates were issued by CERN but Kevin's DN is lowcase
        assert iam.dn_filter(dwight,pam,jim,kevin,creed,michael, prefer_cern=True) == {pamSubDN,michaelSubDN}

        import re
        p = re.compile(r'Pam')
        assert iam.dn_filter(dwight,pam,jim,kevin,creed,michael, prefer_cern=True, pattern=p) == {pamSubDN}

        import re
        p = re.compile(r'michael', flags=re.IGNORECASE)
        assert iam.dn_filter(dwight,pam,jim,kevin,creed,michael, prefer_cern=True, pattern=p) == {michaelSubDN}

    def test_name_map_filter(self):
        user1 = make_user(id='123')
        user2 = make_user(id=1234)
        user3 = make_user(id=12345)
        user4 = make_user(id=123456)
        user5 = {'ID':321} # not valid

        assert iam.name_map_filter(user1, user2, user3, user4, user5) == {'123','1234','12345','123456'}

        assert iam.name_map_filter(user5) == set()
