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
#include <ldap.h>

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/**
 * @file Egroup.hh
 * 
 * @brief Class implementing egroup support via LDAP queries
 * 
 */

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

#define EOSEGROUPCACHETIME 1800

/*----------------------------------------------------------------------------*/
/**
 * @brief Class implementing EGROUP support
 * 
 * Provides static functions to check egroup membership's by 
 * username/egroup name.\n
 * The Egroup object mantains a thread which is serving async
 * Egroup membership update requests. \n
 * The application has to generate a single Egroup object and should use the 
 * static Egroup::Member function to check Egroup membership.\n\n
 * The problem here is that the calling function in the MGM
 * has a read lock durign the Egroup::Member call and the
 * refreshing of Egroup permissions should be done if possible asynchronous to 
 * avoid mutex starvation.
 */
/*----------------------------------------------------------------------------*/
class Egroup
{
private:
  /// thread id of the async refresh thread
  pthread_t mThread;

  // ---------------------------------------------------------------------------
  // Synchronous refresh function doing an LDAP query for a given Egroup/user
  // ---------------------------------------------------------------------------
  void DoRefresh (std::string &egroupname, std::string &username);

public:
  /// static queue with pairs of egroup/username 
  static std::deque <std::pair<std::string, std::string > > LdapQueue;

  /// static mutex protecting static Egroup objects
  static XrdSysMutex Mutex;

  /// static map indicating egroup memebership for egroup/username pairs
  static std::map < std::string, std::map <std::string, bool > > Map;

  /// static map storing the validity of egroup/username pairs in Map
  static std::map < std::string, std::map <std::string, time_t > > LifeTime;

  /// static condition variable to notify the asynchronous update thread about
  /// a new egroup request
  static XrdSysCondVar mCond;

  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------
  Egroup ();

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  virtual ~Egroup ();

  // ---------------------------------------------------------------------------
  // Start function to execute the asynchronous Egroup fetch thread
  // ---------------------------------------------------------------------------
  bool Start ();

  // ---------------------------------------------------------------------------
  // Stop function to terminate the asynchronous Egroup fetch thread
  // ---------------------------------------------------------------------------
  void Stop ();

  // ---------------------------------------------------------------------------
  // static function to check if username is member in egroupname
  // ---------------------------------------------------------------------------
  static bool Member (std::string &username, std::string &egroupname);


  // ---------------------------------------------------------------------------
  // static function to schedule an asynchronous refresh for egroup/username
  // ---------------------------------------------------------------------------
  static void AsyncRefresh (std::string &egroupname, std::string &username);

  // ---------------------------------------------------------------------------
  // asynchronous thread loop doing egroup/username fetching
  // ---------------------------------------------------------------------------
  void* Refresh ();

  // ---------------------------------------------------------------------------
  // static thread startup function
  // ---------------------------------------------------------------------------
  static void* StaticRefresh (void* arg);
};

EOSMGMNAMESPACE_END

#endif
