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

//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   User quota accounting
//------------------------------------------------------------------------------

#ifndef EOS_NS_QUOTA_STATS_HH
#define EOS_NS_QUOTA_STATS_HH

#include "namespace/Namespace.hh"
#include "namespace/interface/IQuota.hh"
#include <map>

EOSNSNAMESPACE_BEGIN

//! Forward declration
class QuotaStats;

//------------------------------------------------------------------------------
//! Placeholder for space occupancy statistics of an accounting node
//------------------------------------------------------------------------------
class QuotaNode: public IQuotaNode
{
 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuotaNode(IQuotaStats* quotaStats):
      IQuotaNode(quotaStats)
  {}

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getUsedSpaceByUser(uid_t uid) throw(MDException)
  {
    return pUserUsage[uid].space;
  }

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getUsedSpaceByGroup(gid_t gid) throw(MDException)
  {
    return pGroupUsage[gid].space;
  }

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getPhysicalSpaceByUser(uid_t uid) throw(MDException)
  {
    return pUserUsage[uid].physicalSpace;
  }

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getPhysicalSpaceByGroup(gid_t gid) throw(MDException)
  {
    return pGroupUsage[gid].physicalSpace;
  }

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getNumFilesByUser(uid_t uid) throw(MDException)
  {
    return pUserUsage[uid].files;
  }

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getNumFilesByGroup(gid_t gid) throw(MDException)
  {
    return pGroupUsage[gid].files;
  }

  //----------------------------------------------------------------------------
  //! Change the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  void changeSpaceUser(uid_t uid, int64_t delta) throw(MDException)
  {
    pUserUsage[uid].space += delta;
  }

  //----------------------------------------------------------------------------
  //! Change the amount of space occpied by the given group
  //----------------------------------------------------------------------------
  void changeSpaceGroup(gid_t gid, int64_t delta) throw(MDException)
  {
    pGroupUsage[gid].space += delta;
  }

  //----------------------------------------------------------------------------
  //! Change the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  void changePhysicalSpaceUser(uid_t uid, int64_t delta) throw(MDException)
  {
    pUserUsage[uid].physicalSpace += delta;
  }

  //----------------------------------------------------------------------------
  //! Change the amount of space occpied by the given group
  //----------------------------------------------------------------------------
  void changePhysicalSpaceGroup(gid_t gid, int64_t delta) throw(MDException)
  {
    pGroupUsage[gid].physicalSpace += delta;
  }

  //----------------------------------------------------------------------------
  //! Change the number of files owned by the given user
  //----------------------------------------------------------------------------
  uint64_t changeNumFilesUser(uid_t uid, uint64_t delta) throw(MDException)
  {
    return pUserUsage[uid].files += delta;
  }

  //----------------------------------------------------------------------------
  //! Change the number of files owned by the given group
  //----------------------------------------------------------------------------
  uint64_t changeNumFilesGroup(gid_t gid, uint64_t delta) throw(MDException)
  {
    return pGroupUsage[gid].files += delta;
  }

  //----------------------------------------------------------------------------
  //! Account a new file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  void addFile(const IFileMD* file);

  //----------------------------------------------------------------------------
  //! Remove a file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  void removeFile(const IFileMD* file);

  //----------------------------------------------------------------------------
  //! Meld in another quota node
  //----------------------------------------------------------------------------
  void meld(const IQuotaNode* node);

};

//----------------------------------------------------------------------------
//! Manager of the quota nodes
//----------------------------------------------------------------------------
class QuotaStats: public IQuotaStats
{
 public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuotaStats() {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuotaStats();

  //----------------------------------------------------------------------------
  //! Get a quota node associated to the container id
  //----------------------------------------------------------------------------
  IQuotaNode* getQuotaNode(IContainerMD::id_t nodeId);

  //----------------------------------------------------------------------------
  //! Register a new quota node
  //----------------------------------------------------------------------------
  IQuotaNode* registerNewNode(IContainerMD::id_t nodeId) throw(MDException);

  //----------------------------------------------------------------------------
  //! Remove quota node
  //----------------------------------------------------------------------------
  void removeNode(IContainerMD::id_t nodeId) throw(MDException);
};

EOSNSNAMESPACE_END

#endif // EOS_NS_QUOTA_STATS_HH
