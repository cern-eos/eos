//------------------------------------------------------------------------------
//! @file MonitorVarPartition.hh
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
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

#ifndef __EOSFST_STORAGE_MONITORVARPARTITION__HH__
#define __EOSFST_STORAGE_MONITORVARPARTITION__HH__

#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "common/RWMutex.hh"
#include "fst/Namespace.hh"
#include "fst/storage/FileSystem.hh"
#include <unistd.h>
#include <sys/statvfs.h>
#include <string>
#include <errno.h>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class MonitorVarPartition
//!
//! @description The var partition monitoring thread is responsible with
//!  switching the FST on a particular machine in read-only mode if the space
//!  available on the /var/ partition drops under a specified threshold value.
//------------------------------------------------------------------------------
template<class FSs>
class MonitorVarPartition : public eos::common::LogId
{
  double mSpaceThreshold; ///< Free space threshold when FST state is changed
  int mIntervalMicroSec; ///< Check time interval
  std::string mPath; ///< Monitored path
  bool mRunning; ///< State of monitoring thread

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param threshold threshold value when FSTs are switched to RO mode
  //! @param time thread interval check in seconds
  //! @param path path being monitored
  //----------------------------------------------------------------------------
  MonitorVarPartition(double threshold, int time, std::string path) :
    mSpaceThreshold(threshold), mIntervalMicroSec(time * 1000 * 1000),
    mPath(path), mRunning(true)
  {
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~MonitorVarPartition() = default;

  //----------------------------------------------------------------------------
  //! Actual monitoring implementation
  //!
  //! @param fss list of FSTs that needs to be updated
  //! @param mtx FST status mutex
  //----------------------------------------------------------------------------
  void Monitor(FSs& fss, eos::common::RWMutex& mtx)
  {
    eos_info("FST Partition Monitor activated ...");
    struct statvfs buf;
    char buffer[256];

    while (mRunning) {
      // Get info about filesystem where mPath is located
      if (statvfs(mPath.c_str(), &buf) == -1) {
        char* errorMessage = strerror_r(errno, buffer, 256);
        eos_err("statvfs failed, error=\"%s\" ", errorMessage);
        continue;
      }

      // Calculating precentage of free space left while ignoring fragment
      // size as doesn't matter in calculating percentage
      double free_percentage = ((buf.f_bfree * 1.) / buf.f_blocks) * 100.;

      if (free_percentage < mSpaceThreshold) {
        eos_crit("partition holding %s is almost full, FSTs set to read-only "
                 "mode - please take action", mPath.c_str());
        eos::common::RWMutexReadLock lock(mtx);

        for (auto fs = fss.begin(); fs != fss.end(); ++fs) {
          if ((*fs)->GetConfigStatus() != eos::common::FileSystem::eConfigStatus::kRO) {
            (*fs)->SetConfigStatus(eos::common::FileSystem::eConfigStatus::kRO);
          }
        }
      }

      usleep(mIntervalMicroSec);
    }
  }

  //----------------------------------------------------------------------------
  //! Switch off monitoring
  //----------------------------------------------------------------------------
  void StopMonitoring()
  {
    mRunning = false;
  }
};

EOSFSTNAMESPACE_END
#endif // __EOSFST_STORAGE_MONITORVARPARTITION__HH__
