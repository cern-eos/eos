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

#include "mgm/Namespace.hh"
#include "common/AssistedThread.hh"
#include <qclient/WaitableQueue.hh>
#include "XrdSys/XrdSysPthread.hh"
#include <sys/types.h>
#include <string>
#include <map>
#include <chrono>

/*----------------------------------------------------------------------------*/
/**
 * @file Egroup.hh
 *
 * @brief Class implementing egroup support via LDAP queries
 *
 */
/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

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
public:
  enum class Status {
    kMember,
    kNotMember,
    kError
  };

  struct CachedEntry {
    bool isMember;
    std::chrono::steady_clock::time_point timestamp;
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Egroup();

  //----------------------------------------------------------------------------
  //! Destructor - join asynchronous refresh thread
  //----------------------------------------------------------------------------
  virtual ~Egroup();

  //----------------------------------------------------------------------------
  //! Reset all stored information
  //----------------------------------------------------------------------------
  void Reset();

  //----------------------------------------------------------------------------
  // Method to check if username is member in egroupname
  //----------------------------------------------------------------------------
  bool Member(const std::string& username, const std::string& egroupname);

  //----------------------------------------------------------------------------
  // Display information about this specific username / groupname pair
  //----------------------------------------------------------------------------
  std::string DumpMember(const std::string& username,
    const std::string& egroupname);

  //----------------------------------------------------------------------------
  // Display all cached information
  //----------------------------------------------------------------------------
  std::string DumpMembers();

  //----------------------------------------------------------------------------
  // static function to schedule an asynchronous refresh for egroup/username
  //----------------------------------------------------------------------------
  void AsyncRefresh(const std::string& egroupname, const std::string& username);

  //----------------------------------------------------------------------------
  // asynchronous thread loop doing egroup/username fetching
  //----------------------------------------------------------------------------
  void Refresh(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  // Fetch cached value, and fills variable out if a result is found.
  // Return false if item does not exist in cache.
  //----------------------------------------------------------------------------
  bool fetchCached(const std::string& username,
    const std::string& egroupname, CachedEntry &out);

private:
  const std::chrono::seconds kCacheDuration { 1800 };

  /// async refresh thread
  AssistedThread mThread;

  //----------------------------------------------------------------------------
  // Synchronous refresh function doing an LDAP query for a given Egroup/user
  //----------------------------------------------------------------------------
  void DoRefresh(const std::string& egroupname, const std::string& username);

  /// mutex protecting static Egroup objects
  XrdSysMutex Mutex;

  /// map indicating egroup memebership for egroup/username pairs
  std::map < std::string, std::map <std::string, bool > > Map;

  /// map storing the validity of egroup/username pairs in Map
  std::map < std::string, std::map <std::string, std::chrono::steady_clock::time_point > > LifeTime;

  /// thred-safe queue keeping track of pending refresh requests
  qclient::WaitableQueue<std::pair<std::string, std::string>, 500> PendingQueue;

  //----------------------------------------------------------------------------
  //! Main LDAP lookup function - bypasses the cache, hits the LDAP server.
  //----------------------------------------------------------------------------
  Status isMemberUncached(const std::string &username,
    const std::string &egroupname);

public:

};

EOSMGMNAMESPACE_END

#endif
