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

#pragma once
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
  QuotaNode(IQuotaStats* quotaStats, IContainerMD::id_t id):
    IQuotaNode(quotaStats, id)
  {}

  //----------------------------------------------------------------------------
  //! Change the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  void changeSpaceUser(uid_t uid, int64_t delta)
  {
    pCore.mUserInfo[uid].space += delta;
  }

  //----------------------------------------------------------------------------
  //! Change the amount of space occpied by the given group
  //----------------------------------------------------------------------------
  void changeSpaceGroup(gid_t gid, int64_t delta)
  {
    pCore.mGroupInfo[gid].space += delta;
  }

  //----------------------------------------------------------------------------
  //! Change the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  void changePhysicalSpaceUser(uid_t uid, int64_t delta)
  {
    pCore.mUserInfo[uid].physicalSpace += delta;
  }

  //----------------------------------------------------------------------------
  //! Change the amount of space occpied by the given group
  //----------------------------------------------------------------------------
  void changePhysicalSpaceGroup(gid_t gid, int64_t delta)
  {
    pCore.mGroupInfo[gid].physicalSpace += delta;
  }

  //----------------------------------------------------------------------------
  //! Change the number of files owned by the given user
  //----------------------------------------------------------------------------
  uint64_t changeNumFilesUser(uid_t uid, uint64_t delta)
  {
    return pCore.mUserInfo[uid].files += delta;
  }

  //----------------------------------------------------------------------------
  //! Change the number of files owned by the given group
  //----------------------------------------------------------------------------
  uint64_t changeNumFilesGroup(gid_t gid, uint64_t delta)
  {
    return pCore.mGroupInfo[gid].files += delta;
  }

  //----------------------------------------------------------------------------
  //! Account a new file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  void addFile(const IFileMD* file) override;

  //----------------------------------------------------------------------------
  //! Remove a file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  void removeFile(const IFileMD* file) override;

  //----------------------------------------------------------------------------
  //! Meld in another quota node
  //----------------------------------------------------------------------------
  void meld(const IQuotaNode* node) override;

  //----------------------------------------------------------------------------
  //! Replace underlying QuotaNodeCore object.
  //----------------------------------------------------------------------------
  void replaceCore(const QuotaNodeCore &updated) override;

  //----------------------------------------------------------------------------
  //! Partial update of underlying QuotaNodeCore object.
  //----------------------------------------------------------------------------
  void updateCore(const QuotaNodeCore &updated) override;
};

//------------------------------------------------------------------------------
//! Manager of the quota nodes
//------------------------------------------------------------------------------
class QuotaStats: public IQuotaStats
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuotaStats() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuotaStats();

  //----------------------------------------------------------------------------
  //! Configure the quota service
  //----------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config)
  override
  {
    // noting to do for this implementation
    return;
  }

  //----------------------------------------------------------------------------
  //! Get the set of all quota node ids. The quota node id corresponds to the
  //! container id.
  //!
  //! @return set of quota node ids
  //----------------------------------------------------------------------------
  std::unordered_set<IContainerMD::id_t> getAllIds() override;

  //----------------------------------------------------------------------------
  //! Get a quota node associated to the container id
  //----------------------------------------------------------------------------
  IQuotaNode* getQuotaNode(IContainerMD::id_t nodeId) override;

  //----------------------------------------------------------------------------
  //! Register a new quota node
  //----------------------------------------------------------------------------
  IQuotaNode* registerNewNode(IContainerMD::id_t nodeId) override;

  //----------------------------------------------------------------------------
  //! Remove quota node
  //----------------------------------------------------------------------------
  void removeNode(IContainerMD::id_t nodeId) override;

private:
  std::map<IContainerMD::id_t, IQuotaNode*> pNodeMap; ///< Map of quota nodes
};

EOSNSNAMESPACE_END
