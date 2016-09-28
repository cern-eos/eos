//------------------------------------------------------------------------------
//! @file Health.hh
//! @author Paul Hermann Lensing <paul.lensing@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef __EOSFST_HEALTH_HH__
#define __EOSFST_HEALTH_HH__

#include "fst/Namespace.hh"
#include "fst/storage/FileSystem.hh"
#include <mutex>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class retrieving disk health information
//------------------------------------------------------------------------------
class DiskHealth
{
public:
  //----------------------------------------------------------------------------
  //! Get health information about a certain device
  //!
  //! @param devpath path of the targeted device
  //!
  //! @return map of health parameters and values
  //----------------------------------------------------------------------------
  std::map<std::string, std::string> getHealth(const char* devpath);

  //----------------------------------------------------------------------------
  //! Update health information for all the registered devices
  //----------------------------------------------------------------------------
  void Measure();

private:
  //----------------------------------------------------------------------------
  //! Parse /proc/mdstat to obtain raid health. Existing indicator shows
  //! rebuild in progress.
  //!
  //! @param device targeted device
  //!
  //! @return map of health parameters and values
  //----------------------------------------------------------------------------
  std::map<std::string, std::string> parse_mdstat(const char* device);

  //----------------------------------------------------------------------------
  //! Obtain health of a single locally attached storage device by evaluating
  //! S.M.A.R.T values.
  //!
  //! @param device targeted device
  //!
  //! @return string representin the result of smartctl run on the targeted
  //!         device. Can be one of the following: OK, no smartclt, N/A, Check,
  //!         invalid, FAILING.
  //----------------------------------------------------------------------------
  std::string smartctl(const char* device);

  //! Map holding the smartclt results
  std::map<std::string, std::map<std::string, std::string>> smartctl_results;
  std::mutex mMutex; ///< Protect acces to the smartctl_results map
};

//------------------------------------------------------------------------------
//! Class Health
//------------------------------------------------------------------------------
class Health
{
public:
  //----------------------------------------------------------------------------
  //! Static helper function for starting the health monitoring thread
  //!
  //! @param pp pointer to a Health object
  //----------------------------------------------------------------------------
  static void* StartHealthThread(void* pp);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Health(unsigned int ival_minutes = 15);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Health();

  //----------------------------------------------------------------------------
  //! Method starting the health monitoring thread
  //!
  //! @return true if thread started succesfully, otherwise false
  //----------------------------------------------------------------------------
  bool Monitor();

  //----------------------------------------------------------------------------
  //! Loop run by the monitoring thread to keep updated the disk health info.
  //----------------------------------------------------------------------------
  void Measure();

  //----------------------------------------------------------------------------
  //! Get disk health information for a specific device. If no measurements
  //! are available then trigger the monitoring thread to do an update.
  //!
  //! @param devpath targeted device
  //!
  //! @return map of health parameters and values
  //----------------------------------------------------------------------------
  std::map<std::string, std::string> getDiskHealth(const char* devpath);

private:
  ///< Trigger update thread without waiting for the whole interval to elapse
  bool mSkip;
  pthread_t mTid; ///< Monitoring thread id
  unsigned int mIntervalMin; ///< Minutes interval when monitoring thread runs
  DiskHealth mDiskHealth; ///< Objecting collecting disk health information
};

EOSFSTNAMESPACE_END

#endif
