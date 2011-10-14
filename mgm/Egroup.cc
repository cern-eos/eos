// ----------------------------------------------------------------------
// File: Egroup.cc
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
#include "mgm/Egroup.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

XrdSysMutex Egroup::Mutex;
std::map < std::string, std::map < std::string, bool > > Egroup::Map;
std::map < std::string, std::map < std::string, time_t > > Egroup::LifeTime;
/*----------------------------------------------------------------------------*/

bool
Egroup::Member(std::string &username, std::string &egroupname)
{
  Mutex.Lock();
  time_t now = time(NULL);

  if (Map.count(egroupname)) {
    if (Map[egroupname].count(username)) {
      // we now that user
      if (LifeTime[egroupname][username] > now) {
        // that is ok, we can return member or not member from the cache
        Mutex.UnLock();
        return Map[egroupname][username];
      }
    }
  }

  std::string cmd = "ldapsearch -LLL -h xldap -x -b 'OU=Users,Ou=Organic Units,DC=cern,DC=ch' 'sAMAccountName=";
  cmd += username;
  cmd += "' memberOf | grep CN=";
  cmd += egroupname;
  cmd += ",";
  cmd += ">& /dev/null";
  int rc = system(cmd.c_str());

  if (!WEXITSTATUS(rc)) {
    Map[egroupname][username] = true;
    LifeTime[egroupname][username] = now + EOSEGROUPCACHETIME;

    Mutex.UnLock();
    return true;
  } else {
    Map[egroupname][username] = false;
    LifeTime[egroupname][username] = now + EOSEGROUPCACHETIME;

    Mutex.UnLock();
    return false;
  }
}
 
EOSMGMNAMESPACE_END
