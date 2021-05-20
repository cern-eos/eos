/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief  Quota node core logic, shared between the two namespaces
//------------------------------------------------------------------------------

#ifndef EOS_NS_COMMON_QUOTA_NODE_CORE_HH
#define EOS_NS_COMMON_QUOTA_NODE_CORE_HH

#include "common/SharedMutexWrapper.hh"
#include "namespace/Namespace.hh"
#include "namespace/interface/Identifiers.hh"
#include <map>
#include <unordered_set>

EOSNSNAMESPACE_BEGIN

class IQuotaNode;
class QuotaNode;

//------------------------------------------------------------------------------
//! QuotaNode core logic, which keeps track of user/group volume/inode use for
//! a single quotanode.
//------------------------------------------------------------------------------
class QuotaNodeCore
{
public:

  struct UsageInfo {
    UsageInfo(): space(0), physicalSpace(0), files(0) {}
    UsageInfo& operator += (const UsageInfo& other)
    {
      space         += other.space;
      physicalSpace += other.physicalSpace;
      files         += other.files;
      return *this;
    }

    bool operator==(const UsageInfo& other) const
    {
      return (space == other.space) &&
             (physicalSpace == other.physicalSpace) &&
             (files == other.files);
    }

    uint64_t space;
    uint64_t physicalSpace;
    uint64_t files;
  };

  //----------------------------------------------------------------------------
  //! Constructor. The object is initially empty, no files whatsoever are being
  //! accounted.
  //----------------------------------------------------------------------------
  QuotaNodeCore() {}

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getUsedSpaceByUser(uid_t uid) const;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getUsedSpaceByGroup(gid_t gid) const;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getPhysicalSpaceByUser(uid_t uid) const;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getPhysicalSpaceByGroup(gid_t gid) const;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getNumFilesByUser(uid_t uid) const;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getNumFilesByGroup(gid_t gid) const;

  //----------------------------------------------------------------------------
  //! Account a new file.
  //----------------------------------------------------------------------------
  void addFile(uid_t uid, gid_t gid, uint64_t size, uint64_t physicalSize);

  //----------------------------------------------------------------------------
  //! Remove a file.
  //----------------------------------------------------------------------------
  void removeFile(uid_t uid, gid_t gid, uint64_t size, uint64_t physicalSize);

  //----------------------------------------------------------------------------
  //! Meld in another quota node core
  //----------------------------------------------------------------------------
  void meld(const QuotaNodeCore& other);

  //----------------------------------------------------------------------------
  //! Get the set of uids for which information is stored in the current quota
  //! node.
  //!
  //! @return set of uids
  //----------------------------------------------------------------------------
  std::unordered_set<uint64_t> getUids() const;

  //----------------------------------------------------------------------------
  //! Get the set of gids for which information is stored in the current quota
  //! node.
  //!
  //! @return set of gids
  //----------------------------------------------------------------------------
  std::unordered_set<uint64_t> getGids() const;

  //----------------------------------------------------------------------------
  //! operator=
  //----------------------------------------------------------------------------
  QuotaNodeCore& operator=(const QuotaNodeCore&);

  //----------------------------------------------------------------------------
  //! operator<< (replacing all entries from update in core)
  //----------------------------------------------------------------------------
  QuotaNodeCore& operator<< (const QuotaNodeCore &update);

  //----------------------------------------------------------------------------
  //! equality operator==
  //----------------------------------------------------------------------------
  bool operator==(const QuotaNodeCore& other) const;

  //----------------------------------------------------------------------------
  //! set usage info by uid
  //----------------------------------------------------------------------------
  void setByUid(uid_t uid, const UsageInfo& info);

  //----------------------------------------------------------------------------
  //! set usage info by gid
  //----------------------------------------------------------------------------
  void setByGid(gid_t gid, const UsageInfo& info);

  //----------------------------------------------------------------------------
  //! filter usage info by uid
  //----------------------------------------------------------------------------
  void filterByUid(uid_t uid);

  //----------------------------------------------------------------------------
  //! filter usage info by gid
  //----------------------------------------------------------------------------
  void filterByGid(gid_t gid);

private:
  friend class IQuotaNode;
  friend class QuotaNode;
  friend class QuarkQuotaNode;

  mutable std::shared_timed_mutex mtx;

  std::map<uid_t, UsageInfo> mUserInfo;
  std::map<gid_t, UsageInfo> mGroupInfo;
};

EOSNSNAMESPACE_END

#endif
