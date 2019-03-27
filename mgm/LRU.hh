// ----------------------------------------------------------------------
// File: LRU.hh
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

#ifndef __EOSMGM_LRU__HH__
#define __EOSMGM_LRU__HH__

#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
#include "common/AssistedThread.hh"
#include "namespace/interface/IContainerMD.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include <sys/types.h>

EOSMGMNAMESPACE_BEGIN

/**
 * @file   LRU.hh
 *
 * @brief  This class implements an LRU engine to apply policies based on atime
 *
 */

class LRU
{
private:
  AssistedThread mThread; ///< thread id of the LRU thread
  time_t mMs; //< forced sleep time used for find / scans
  eos::common::VirtualIdentity mRootVid;//< we operate with the root vid
  XrdOucErrInfo mError; //< XRootD error object

public:

  //----------------------------------------------------------------------------
  //! Simple struct describing LRU options
  //----------------------------------------------------------------------------
  struct Options {
    bool enabled;                  //< Is LRU even enabled?
    std::chrono::seconds interval; //< Run LRU every this many seconds.
  };

  //----------------------------------------------------------------------------
  //! Retrieve current LRU configuration options
  //----------------------------------------------------------------------------
  Options getOptions();

  //----------------------------------------------------------------------------
  //! Retrieve "lru.interval" configuration option as string, or empty if
  //! cannot be found. Assumes gFsView.ViewMutex is at-least readlocked.
  //----------------------------------------------------------------------------
  std::string getLRUIntervalConfig() const;

  /* Default Constructor - use it to run the LRU thread by calling Start
   */
  LRU()
  {
    mMs = 0;
    mRootVid = eos::common::VirtualIdentity::Root();
  }

  /**
   * @brief get the millisecond sleep time for find
   * @return configured sleep time
   */
  time_t GetMs()
  {
    return mMs;
  }

  /**
   * @brief set the millisecond sleep time for find
   * @param ms sleep time in milliseconds to enforce
   */
  void SetMs(time_t ms)
  {
    mMs = ms;
  }

  /* Start the LRU thread engine
   */
  bool Start();

  /* Stop the LRU thread engine
   */
  void Stop();

  /* LRU method doing the actual policy scrubbing
   */
  void LRUr(ThreadAssistant& assistant) noexcept;

  /**
   * @brief Destructor
   *
   */
  ~LRU()
  {
    Stop();
    std::cerr << __FUNCTION__ << ":: end of destructor" << std::endl;
  }

  /* expire by age if empty
   */
  void AgeExpireEmpty(const char* dir, std::string& policy);

  /* expire by age
   */
  void AgeExpire(const char* dir, std::string& policy);

  /* expire by volume
   */
  void CacheExpire(const char* dir, std::string& low, std::string& high);

  /* convert by match
   */
  void ConvertMatch(const char* dir,  eos::IContainerMD::XAttrMap& map);

  static const char* gLRUPolicyPrefix;

  struct lru_entry {
    // compare operator to use struct in a map
    bool operator< (lru_entry const& lhs) const
    {
      if (lhs.getCTime() == getCTime()) {
        return (getPath() < lhs.getPath());
      }

      return getCTime() < lhs.getCTime();
    }
    std::string path;
    time_t ctime;
    unsigned long long size;

    // ctime getter
    time_t getCTime() const
    {
      return ctime;
    }

    // path getter
    std::string getPath() const
    {
      return path;
    }
  } ;

  // entry in an lru queue having path name,mtime,size
  typedef struct lru_entry lru_entry_t;
};

EOSMGMNAMESPACE_END

#endif
