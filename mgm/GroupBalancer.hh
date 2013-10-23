// ----------------------------------------------------------------------
// File: GroupBalancer.hh
// Author: Joaquim Rocha - CERN
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

#ifndef __EOSMGM_GROUPBALANCER__
#define __EOSMGM_GROUPBALANCER__

/* -------------------------------------------------------------------------- */
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
/* -------------------------------------------------------------------------- */
#include "XrdSys/XrdSysPthread.hh"
/* -------------------------------------------------------------------------- */
#include <vector>
#include <string>
#include <deque>
#include <cstring>
#include <ctime>
/* -------------------------------------------------------------------------- */
/**
 * @file GroupBalancer.hh
 *
 * @brief Balancing among groups
 *
 */
/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

class FsGroup;

/*----------------------------------------------------------------------------*/
/**
 * @brief Class representing a group's size
 *
 * It holds the capacity and the current used space of a group.
 */

/*----------------------------------------------------------------------------*/
class GroupSize {
public:
  GroupSize (uint64_t usedBytes, uint64_t capacity);
  void swapFile (GroupSize *toGroup, uint64_t size);

  uint64_t
  usedBytes () const {
    return mSize;
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
 * @brief Class running the balancing among groups
 *
 * For it to work, the Converter also needs to be enabled.
 */

/*----------------------------------------------------------------------------*/
class GroupBalancer {
private:
  /// thread id
  pthread_t mThread;

  /// name of the space this group balancer serves
  std::string mSpaceName;
  /// the threshold with which to compare the groups
  double mThreshold;

  /// groups whose size is over the average size of the groups
  std::map<std::string, FsGroup*> mGroupsOverAvg;
  /// groups whose size is under the average size of the groups
  std::map<std::string, FsGroup*> mGroupsUnderAvg;
  /// groups' sizes cache
  std::map<std::string, GroupSize *> mGroupSizes;
  /// average filled percentage in groups
  double mAvgUsedSize;

  /// last time the groups' real used space was checked
  time_t mLastCheck;

  /// transfers scheduled (maps files' ids with their path in proc)
  std::map<eos::common::FileId::fileid_t, std::string> mTransfers;

  char* getFileProcTransferNameAndSize (eos::common::FileId::fileid_t fid,
                                        FsGroup *group,
                                        uint64_t *size);

  eos::common::FileId::fileid_t chooseFidFromGroup (FsGroup *group);

  void populateGroupsInfo (void);

  void clearCachedSizes (void);

  void updateGroupAvgCache (FsGroup *group);

  void fillGroupsByAvg (void);

  void recalculateAvg (void);

  void prepareTransfers (int nrTransfers);

  void prepareTransfer (void);

  void scheduleTransfer (eos::common::FileId::fileid_t fid,
                         FsGroup *sourceGroup,
                         FsGroup *targetGroup);

  int getRandom (int max);

  bool cacheExpired (void);

  void updateTransferList (void);

public:

  // ---------------------------------------------------------------------------
  // Constructor (per space)
  // ---------------------------------------------------------------------------
  GroupBalancer (const char* spacename);

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  ~GroupBalancer ();

  // ---------------------------------------------------------------------------
  // thread stop function
  // ---------------------------------------------------------------------------
  void Stop ();

  // ---------------------------------------------------------------------------
  // Service thread static startup function
  // ---------------------------------------------------------------------------
  static void* StaticGroupBalancer (void*);

  // ---------------------------------------------------------------------------
  // Service implementation e.g. eternal conversion loop running third-party 
  // conversion
  // ---------------------------------------------------------------------------
  void* GroupBalance (void);

};

EOSMGMNAMESPACE_END
#endif

