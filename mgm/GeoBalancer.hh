// ----------------------------------------------------------------------
// File: GeoBalancer.hh
// Author: Joaquim Rocha - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @file GeoBalancer.hh
//! @brief Balancing among geo locations
//------------------------------------------------------------------------------

#ifndef __EOSMGM_GEOBALANCER__
#define __EOSMGM_GEOBALANCER__

/* -------------------------------------------------------------------------- */
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
/* -------------------------------------------------------------------------- */
#include "XrdSys/XrdSysPthread.hh"
/* -------------------------------------------------------------------------- */
#include <vector>
#include <string>
#include <deque>
#include <cstring>
#include <ctime>

//! Forward declaration
namespace eos
{
  class IFileMD;
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief Class representing a geotag's size
//! It holds the capacity and the current used space of a geotag.
//------------------------------------------------------------------------------
class GeotagSize {
public:
  GeotagSize (uint64_t usedBytes, uint64_t capacity);

  uint64_t
  usedBytes () const {
    return mSize;
  };

  void
  setUsedBytes (uint64_t usedBytes) {
    mSize = usedBytes;
  };

  void
  setCapacity (uint64_t capacity) {
    mCapacity = capacity;
  };

  uint64_t
  capacity () const {
    return mCapacity;
  };

  double
  filled () const {
    return (double) mSize / (double) mCapacity;
  };

private:
  uint64_t mSize;
  uint64_t mCapacity;
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class running the balancing among geotags
 *
 * For it to work, the Converter also needs to be enabled.
 */

/*----------------------------------------------------------------------------*/
class GeoBalancer {
private:
  /// thread id
  pthread_t mThread;

  /// name of the space this geo balancer serves
  std::string mSpaceName;
  /// the threshold with which to compare the geotags
  double mThreshold;

  /// geotags and respective filesystems
  std::map<std::string, std::vector<eos::common::FileSystem::fsid_t >> mGeotagFs;
  /// fs->geotag cache
  std::map<eos::common::FileSystem::fsid_t, std::string> mFsGeotag;
  /// geotags' sizes cache
  std::map<std::string, GeotagSize *> mGeotagSizes;
  /// cache with geotags over the current average
  std::vector<std::string> mGeotagsOverAvg;

  /// average filled percentage in geotags
  double mAvgUsedSize;

  /// last time the geotags' real used space was checked
  time_t mLastCheck;

  /// transfers scheduled (maps files' ids with their path in proc)
  std::map<eos::common::FileId::fileid_t, std::string> mTransfers;

  std::string getFileProcTransferNameAndSize (eos::common::FileId::fileid_t fid,
                                              uint64_t *size);

  eos::common::FileId::fileid_t chooseFidFromGeotag (const std::string &geotag);

  void populateGeotagsInfo (void);

  void clearCachedSizes (void);

  void fillGeotagsByAvg (void);

  void prepareTransfers (int nrTransfers);

  void prepareTransfer (void);

  bool scheduleTransfer (eos::common::FileId::fileid_t fid,
                         const std::string &sourceGeotag);

  int getRandom (int max);

  bool cacheExpired (void);

  void updateTransferList (void);

  bool fileIsInDifferentLocations (const eos::IFileMD *fmd);

public:

  // ---------------------------------------------------------------------------
  // Constructor (per space)
  // ---------------------------------------------------------------------------
  GeoBalancer (const char* spacename);

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  ~GeoBalancer ();

  // ---------------------------------------------------------------------------
  // thread stop function
  // ---------------------------------------------------------------------------
  void Stop ();

  // ---------------------------------------------------------------------------
  // Service thread static startup function
  // ---------------------------------------------------------------------------
  static void* StaticGeoBalancer (void*);

  // ---------------------------------------------------------------------------
  // Service implementation e.g. eternal conversion loop running third-party 
  // conversion
  // ---------------------------------------------------------------------------
  void* GeoBalance (void);

};

EOSMGMNAMESPACE_END
#endif

