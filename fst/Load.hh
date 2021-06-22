//------------------------------------------------------------------------------
// File: Load.hh
// Author: Andreas-Joachim Peters <apeters@cern.ch>
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

#ifndef __EOSFST_LOAD_HH__
#define __EOSFST_LOAD_HH__

#include "fst/Namespace.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <vector>
#include <map>
#include <string>
#include <sys/time.h>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class collecting disk statistics
//------------------------------------------------------------------------------
class DiskStat
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  DiskStat();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~DiskStat() {}

  //----------------------------------------------------------------------------
  //! Get disk measurement values
  //!
  //! @param fn_path abs path to file to be read
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Measure(const std::string& fn_path = "/proc/diskstats");

  //----------------------------------------------------------------------------
  //! Get rate type for device
  //!
  //! @param dev device identifier
  //! @param key rate type
  //!
  //! @return rate value
  //----------------------------------------------------------------------------
  double GetRate(const char* dev, const char* key);

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  //! Map from device name to map of key/values
  std::map<std::string, std::map<std::string, std::string > > values_t2;
  std::map<std::string, std::map<std::string, std::string > > values_t1;
  std::map<std::string, std::map<std::string, double > > mRates;
  struct timespec t1; ///< Timestamp 1
  struct timespec t2; ///< Timestamp 2 for calculating the rates
  std::vector<std::string> mTags; ///< Disk statistics tags
  XrdSysRWLock mMutexRW; ///< RW mutex for protecting accces to the rates map
};

//------------------------------------------------------------------------------
//! Class collecting network statistics
//------------------------------------------------------------------------------
class NetStat
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  NetStat();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~NetStat() {};

  //----------------------------------------------------------------------------
  //! Get disk measurement values
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Measure();

  //----------------------------------------------------------------------------
  //! Get rate type for device
  //!
  //! @param dev device identifier
  //! @param key rate type
  //!
  //! @return rate value
  //----------------------------------------------------------------------------
  double GetRate(const char* dev, const char* key);

private:
  //! Map from device name to map of key/values
  std::map<std::string, std::map<std::string, std::string > > values_t2;
  std::map<std::string, std::map<std::string, std::string > > values_t1;
  std::map<std::string, std::map<std::string, double > > mRates;
  struct timespec t1; ///< Timestamp 1
  struct timespec t2; ///< Timestamp 2 for calculating the rates
  std::vector<std::string> mTags; ///< Disk statistics tags
  XrdSysRWLock mMutexRW; ///< RW mutex for protecting accces to the rates map
};

//------------------------------------------------------------------------------
//! Class Load
//------------------------------------------------------------------------------
class Load
{
public:
  //----------------------------------------------------------------------------
  //! Get device name mounted at the given path
  //!
  //! @param dev_path device mount path
  //!
  //! @return name of the devices which is mounted at the given path or empty
  //!         string if no device found
  //----------------------------------------------------------------------------
  static const std::string DevMap(const std::string& dev_path);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Load(unsigned int ival = 15);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Load();

  //----------------------------------------------------------------------------
  //! Start scrubber thread
  //!
  //! @return true if thread started successfully, otherwise false
  //----------------------------------------------------------------------------
  bool Monitor();

  //----------------------------------------------------------------------------
  //! Method run by scurbber thread to  measurement both disk and network values
  //! on regular intervals.
  //----------------------------------------------------------------------------
  void Measure();

  //----------------------------------------------------------------------------
  //! Get disk rate type for a particular device
  //!
  //! @param dev_path device path
  //! @param tag type of disk rate retrieved
  //!
  //! @return disk rate value
  //----------------------------------------------------------------------------
  virtual double GetDiskRate(const char* dev_path, const char* tag);

  //----------------------------------------------------------------------------
  //! Get net rate type for a particular device
  //!
  //! @param dev device
  //! @param tag type of net rate retrieved
  //!
  //! @return net rate value
  //----------------------------------------------------------------------------
  double GetNetRate(const char* dev, const char* tag);

  //----------------------------------------------------------------------------
  //! Static method used to start the scrubber thread
  //----------------------------------------------------------------------------
  static void* StartLoadThread(void* pp);

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  pthread_t mTid; ///< Monitor thread id
  unsigned int mInterval; ///< Sampling interval for the monitor thread
  DiskStat fDiskStat; ///< Disk statistics
  NetStat fNetStat; ///< Network statistics
};

EOSFSTNAMESPACE_END

#endif // __EOSFST_LOAD_HH__
