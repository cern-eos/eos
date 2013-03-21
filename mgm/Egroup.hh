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
#include <deque>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

#define EOSEGROUPCACHETIME 300

class Egroup
{
  // -------------------------------------------------------------
  // ! Provides static functions to check egroup membership's by 
  // ! username/egroup name.
  // ! The Egroup object mantains a thread which is serving async
  // ! Egroup membership update requests. The application has to 
  // ! generate a single Egroup object and should use the 
  // ! static Egroup::Member function to check Egroup membership.
  // ! The problem here is that the calling function in the MGM
  // ! has a read lock durign the Egroup::Member call and the
  // ! refreshing of Egroup permissions should be done if possible
  // ! asynchronous to avoid mutex starvation.
  // -------------------------------------------------------------
private:
  pthread_t mThread; //< thread id of the async refresh thread

  void DoRefresh (std::string &egroupname, std::string &username);

public:
  static std::deque <std::pair<std::string, std::string > > LdapQueue; //< queue with pairs of egroup/username 
  static XrdSysMutex Mutex; //< protecting static Egroup objects
  static std::map < std::string, std::map <std::string, bool > > Map;
  static std::map < std::string, std::map <std::string, time_t > > LifeTime;
  static XrdSysCondVar mCond; ///< cond. variable used for synchronisation

  Egroup ();
  virtual ~Egroup ();
  
  bool Start();
  
  static bool Member (std::string &username, std::string &egroupname);

  static void AsyncRefresh (std::string &egroupname, std::string &username);
  void* Refresh (); // async thread loop doing egroup fetching
  static void* StaticRefresh (void* arg);
};

EOSMGMNAMESPACE_END

#endif
