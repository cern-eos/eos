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
#include "common/SteadyClock.hh"
#include "common/RWMutex.hh"
#include <qclient/queueing/WaitableQueue.hh>
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
 */
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
//! Class implementing EGROUP support.
//!
//! Provides functionality for checking egroup membership by username / egroup
//! name.
//!
//! The Egroup object maintains a thread which is serving async Egroup
//! membership update requests.
//!
//! The problem here is that the calling function in the MGM has a read lock
//! during the Egroup::Member call and the refreshing of Egroup permissions
//! should be done if possible asynchronously to avoid mutex starvation.
//------------------------------------------------------------------------------
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

    //--------------------------------------------------------------------------
    //! Constructors
    //--------------------------------------------------------------------------
    CachedEntry(bool member, std::chrono::steady_clock::time_point ts)
      : isMember(member), timestamp(ts) {}

    CachedEntry() : isMember(false) {}
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Egroup(eos::common::SteadyClock* clock = nullptr);

  //----------------------------------------------------------------------------
  //! Destructor - join asynchronous refresh thread
  //----------------------------------------------------------------------------
  virtual ~Egroup();

  //----------------------------------------------------------------------------
  //! Reset all stored information
  //----------------------------------------------------------------------------
  void Reset();

  //----------------------------------------------------------------------------
  // Major query method - uses cache
  //----------------------------------------------------------------------------
  CachedEntry query(const std::string& username, const std::string& egroupname);

  //----------------------------------------------------------------------------
  // Convenience function, method to check if username is member in egroupname
  //----------------------------------------------------------------------------
  bool Member(const std::string& username, const std::string& egroupname)
  {
    return query(username, egroupname).isMember;
  }

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
  void scheduleRefresh(const std::string& username,
                       const std::string& egroupname);

  //----------------------------------------------------------------------------
  // Fetch cached value, and fills variable out if a result is found.
  // Return false if item does not exist in cache.
  //----------------------------------------------------------------------------
  bool fetchCached(const std::string& username,
                   const std::string& egroupname, CachedEntry& out);

  //----------------------------------------------------------------------------
  // Inject item into the fake LDAP server. If injections are active, any time
  // this class tries to contact the LDAP server, it will serve injected data
  // instead.
  //
  // Simulates response of "isMemberUncached" function.
  //----------------------------------------------------------------------------
  void inject(const std::string& username, const std::string& egroupname,
              Status status);

  //----------------------------------------------------------------------------
  // Return number of asynchronous refresh requests currently pending
  //----------------------------------------------------------------------------
  size_t getPendingQueueSize() const;

  //----------------------------------------------------------------------------
  //! Synchronous refresh function doing an LDAP query for a given Egroup/user.
  //! If the pair exists in the cache, it is ignored and replaced.
  //----------------------------------------------------------------------------
  CachedEntry refresh(const std::string& username,
                      const std::string& egroupname);

private:
  const std::chrono::seconds kCacheDuration
  {
    1800
  };
  eos::common::SteadyClock* clock = nullptr;

  //----------------------------------------------------------------------------
  //! Store entry into the cache
  //----------------------------------------------------------------------------
  void storeIntoCache(const std::string& username,
                      const std::string& egroupname, bool isMember,
                      std::chrono::steady_clock::time_point timestamp);

  /// async refresh thread
  AssistedThread mThread;

  //----------------------------------------------------------------------------
  //! Check if cache entry is stale
  //----------------------------------------------------------------------------
  bool isStale(const CachedEntry& entry) const;

  //----------------------------------------------------------------------------
  //! Asynchronous thread loop doing egroup/username fetching
  //----------------------------------------------------------------------------
  void Refresh(ThreadAssistant& assistant) noexcept;

  /// mutex protecting static Egroup objects
  eos::common::RWMutex mMutex;

  /// map indicating egroup memebership for egroup/username pairs
  std::map<std::string, std::map<std::string, CachedEntry>> cache;

  /// thred-safe queue keeping track of pending refresh requests
  qclient::WaitableQueue<std::pair<std::string, std::string>, 500> PendingQueue;

  /// injections to simulate LDAP server responses - different than the cache
  std::map<std::string, std::map<std::string, Status>> injections;

  //----------------------------------------------------------------------------
  //! Main LDAP lookup function - bypasses the cache, hits the LDAP server.
  //----------------------------------------------------------------------------
  Status isMemberUncached(const std::string& username,
                          const std::string& egroupname);
};

EOSMGMNAMESPACE_END

#endif
