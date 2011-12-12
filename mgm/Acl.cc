// ----------------------------------------------------------------------
// File: Acl.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/Acl.hh"
#include "mgm/Egroup.hh"
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
Acl::Acl(std::string sysacl, std::string useracl, eos::common::Mapping::VirtualIdentity &vid)
{
  Set(sysacl, useracl, vid);
}

/*----------------------------------------------------------------------------*/
void
Acl::Set(std::string sysacl, std::string useracl, eos::common::Mapping::VirtualIdentity &vid)
{
  std::string acl="";
  if (sysacl.length()) {
    acl += sysacl;
  }

  if (useracl.length()) {
    if (acl.length())
      acl += ",";
    acl += useracl;
  }
    
  hasAcl = false;
  canRead = false;
  canWrite = false;
  canWriteOnce = false;
  canBrowse = false;
  canChmod = false;
  canNotDelete = false;
  hasEgroup = false;

  if (!acl.length()) {
    // no acl definition
    return ;
  }
  int errc = 0;
  std::vector<std::string> rules;
  std::string delimiter = ",";
  eos::common::StringConversion::Tokenize(acl,rules,delimiter);
  std::vector<std::string>::const_iterator it;

  XrdOucString sizestring1;
  XrdOucString sizestring2;

  std::string userid  = eos::common::StringConversion::GetSizeString(sizestring1, (unsigned long long)vid.uid);
  std::string groupid = eos::common::StringConversion::GetSizeString(sizestring2, (unsigned long long)vid.gid);

  std::string usertag =  "u:"; usertag  += userid; usertag += ":";
  std::string grouptag = "g:"; grouptag += groupid; grouptag += ":";

  std::string username  = eos::common::Mapping::UidToUserName(vid.uid,errc);
  if (errc) {
    username = "_INVAL_";
  }
  std::string groupname = eos::common::Mapping::GidToGroupName(vid.gid,errc);
  if (errc) {
    groupname = "_INVAL_";
  }

  std::string usertagfn =  "u:"; usertagfn  += username;  usertagfn += ":";
  std::string grouptagfn = "g:"; grouptagfn += groupname; grouptagfn += ":";

  for (it = rules.begin(); it != rules.end(); it++) {
    bool egroupmatch = false;
    if (!it->compare(0, strlen("egroup:"), "egroup:")) {
      std::vector<std::string> entry;
      std::string delimiter = ":";
      eos::common::StringConversion::Tokenize(*it, entry, delimiter);
      if (entry.size() <3 ) {
        continue; 
      }

      egroupmatch = Egroup::Member(username, entry[1]);
      hasEgroup = egroupmatch;
    }
    if ((!it->compare(0, usertag.length(), usertag)) || (!it->compare(0,grouptag.length(), grouptag)) || (egroupmatch) ||
        (!it->compare(0, usertagfn.length(), usertagfn)) || (!it->compare(0,grouptagfn.length(), grouptagfn)) ) {
      // that is our rule
      std::vector<std::string> entry;
      std::string delimiter = ":";
      eos::common::StringConversion::Tokenize(*it, entry, delimiter);

      if (entry.size() <3 ) {
        continue; 
      }

      if ((entry[2].find("r"))!= std::string::npos) {
        canRead = true;
        hasAcl = true;
      }

      if ((entry[2].find("x"))!= std::string::npos) {
        canBrowse = true;
        hasAcl = true;
      }

      if ((entry[2].find("m"))!= std::string::npos) {
        canChmod = true;
        hasAcl = true;
      }

      if ((entry[2].find("!d"))!= std::string::npos) {
        canNotDelete = true;
        hasAcl = true;
      }

      if (((entry[2].find("wo"))!= std::string::npos)) {
        canWriteOnce = true;
        hasAcl = true;
      }

      if ((!canWriteOnce) && (entry[2].find("w"))!= std::string::npos) {
        canWrite = true;
        hasAcl = true;
      }
    }
  }
}
 
EOSMGMNAMESPACE_END
