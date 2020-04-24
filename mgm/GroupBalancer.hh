//------------------------------------------------------------------------------
// File: GroupBalancer.hh
// Author: Joaquim Rocha - CERN
//------------------------------------------------------------------------------

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

#pragma once
#include "mgm/Namespace.hh"
#include "common/FileId.hh"
#include "common/AssistedThread.hh"
#include <vector>
#include <string>
#include <cstring>
#include <ctime>
#include <map>

EOSMGMNAMESPACE_BEGIN

class FsGroup;

//------------------------------------------------------------------------------
//! @brief Class representing a group's size
//! It holds the capacity and the current used space of a group.
//------------------------------------------------------------------------------
class GroupSize
{
public:
  //------------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------------
  GroupSize(uint64_t usedBytes, uint64_t capacity);

  //------------------------------------------------------------------------------
  //! Subtracts the given size from this group and adds it to the given toGroup
  //!
  //! @param toGroup the group where to add the size
  //! @param size the file size that should be swapped
  //------------------------------------------------------------------------------
  void swapFile(GroupSize* toGroup, uint64_t size);

  uint64_t
  usedBytes() const
  {
    return mSize;
  }

  uint64_t
  capacity() const
  {
    return mCapacity;
  }

  double
  filled() const
  {
    return (double) mSize / (double) mCapacity;
  }

private:
  uint64_t mSize;
  uint64_t mCapacity;
};

//------------------------------------------------------------------------------
//! @brief Class running the balancing among groups
//! For it to work, the Converter also needs to be enabled.
//------------------------------------------------------------------------------
class GroupBalancer
{
public:

  //----------------------------------------------------------------------------
  //! Constructor (per space)
  //
  //! @param spacename name of the associated space
  //----------------------------------------------------------------------------
  GroupBalancer(const char* spacename);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~GroupBalancer();

  //----------------------------------------------------------------------------
  //! Stop group balancing thread
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  // Service implementation e.g. eternal conversion loop running third-party
  // conversion
  //----------------------------------------------------------------------------
  void GroupBalance(ThreadAssistant& assistant) noexcept;

private:
  AssistedThread mThread; ///< Thread scheduling jobs
  std::string mSpaceName; ///< Attached space name
  double mThreshold; ///< Threshold for group balancing

  /// groups whose size is over the average size of the groups
  std::map<std::string, FsGroup*> mGroupsOverAvg;
  /// groups whose size is under the average size of the groups
  std::map<std::string, FsGroup*> mGroupsUnderAvg;
  /// groups' sizes cache
  std::map<std::string, GroupSize*> mGroupSizes;
  /// average filled percentage in groups
  double mAvgUsedSize;
  /// last time the groups' real used space was checked
  time_t mLastCheck;
  /// transfers scheduled (maps files' ids with their path in proc)
  std::map<eos::common::FileId::fileid_t, std::string> mTransfers;

  //----------------------------------------------------------------------------
  //! Produces a file conversion path to be placed in the proc directory taking
  //! into account the given group and also returns its size
  //!
  //! @param fid the file ID
  //! @param group the group to which the file will be transferred
  //! @param size return address for the size of the file
  //!
  //! @return name of the proc transfer file
  //----------------------------------------------------------------------------
  std::string getFileProcTransferNameAndSize(eos::common::FileId::fileid_t fid,
      FsGroup* group, uint64_t* size);

  //----------------------------------------------------------------------------
  //! Chooses a random file ID from a random filesystem in the given group
  //!
  //! @param group the group from which the file id will be chosen
  //!
  //! @return the chosen file ID
  //----------------------------------------------------------------------------
  eos::common::FileId::fileid_t chooseFidFromGroup(FsGroup* group);

  //----------------------------------------------------------------------------
  // Fills mGroupSizes, calculates the mAvgUsedSize and fills mGroupsUnderAvg
  // and mGroupsOverAvg
  //----------------------------------------------------------------------------
  void populateGroupsInfo(void);

  //----------------------------------------------------------------------------
  //! Deletes all the GrouSize objects stored in mGroupSizes and empties it
  //----------------------------------------------------------------------------
  void clearCachedSizes(void);

  //----------------------------------------------------------------------------
  //! Places group in mGroupsOverAvg or mGroupsUnderAvg in case they're greater
  //! than or less than the current mAvgUsedSize, respectively.
  //----------------------------------------------------------------------------
  void updateGroupAvgCache(FsGroup* group);

  //----------------------------------------------------------------------------
  //! Fills mGroupsOverAvg and mGroupsUnderAvg with the objects in mGroupSizes,
  //! in case they're greater than or less than the current mAvgUsedSize,
  //! respectively
  //----------------------------------------------------------------------------
  void fillGroupsByAvg(void);

  //----------------------------------------------------------------------------
  //! Recalculates the sizes average from the mGroupSizes
  //----------------------------------------------------------------------------
  void recalculateAvg(void);

  void prepareTransfers(int nrTransfers);

  //----------------------------------------------------------------------------
  //! Picks two groups (source and target) randomly and schedule a file ID
  //! to be transferred
  //----------------------------------------------------------------------------
  void prepareTransfer(void);

  //----------------------------------------------------------------------------
  //! Creates the conversion file in proc for the file ID, from the given
  //! sourceGroup, to the targetGroup (and updates the cache structures)
  //!
  //! @param fid the id of the file to be transferred
  //! @param sourceGroup the group where the file is currently located
  //! @param targetGroup the group to which the file is will be transferred
  //----------------------------------------------------------------------------
  void scheduleTransfer(eos::common::FileId::fileid_t fid,
                        FsGroup* sourceGroup, FsGroup* targetGroup);

  //----------------------------------------------------------------------------
  //! Gets a random int between 0 and a given maximum
  //!
  //! @param max the upper bound of the range within which the int will be
  //!        generated
  //----------------------------------------------------------------------------
  int getRandom(int max);

  //----------------------------------------------------------------------------
  //! Check if the sizes cache should be updated (based on the time passed since
  //! they were last updated)
  //!
  //! @return whether the cache expired or not
  //----------------------------------------------------------------------------
  bool cacheExpired(void);

  //----------------------------------------------------------------------------
  //! For each entry in mTransfers, checks if the files' paths exist, if they
  //! don't, they are deleted from the mTransfers
  //----------------------------------------------------------------------------
  void updateTransferList(void);
};

EOSMGMNAMESPACE_END
