// ----------------------------------------------------------------------
// File: Egroup.hh
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

#ifndef __EOSMGM_EGROUP__HH__
#define __EOSMGM_EGROUP__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <string>
#include <ldap.h>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

#define EOSEGROUPCACHETIME 1800

class Egroup {
public:
  static XrdSysMutex Mutex;
  static std::map < std::string, std::map <std::string, bool > > Map;
  static std::map < std::string, std::map <std::string, time_t > > LifeTime;

  Egroup(){};
  ~Egroup(){};
  
  static bool Member(std::string &username, std::string &egroupname);
  
};

EOSMGMNAMESPACE_END

#endif
