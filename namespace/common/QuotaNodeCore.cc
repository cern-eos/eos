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

#include "namespace/common/QuotaNodeCore.hh"
#include <mutex>
#include <set>

EOSNSNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Get the amount of space occupied by the given user
//----------------------------------------------------------------------------
uint64_t QuotaNodeCore::getUsedSpaceByUser(uid_t uid) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mtx);
  auto it = mUserInfo.find(uid);

  if (it == mUserInfo.end()) {
    return 0;
  }

  return it->second.space;
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given group
//------------------------------------------------------------------------------
uint64_t QuotaNodeCore::getUsedSpaceByGroup(gid_t gid) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mtx);
  auto it = mGroupInfo.find(gid);

  if (it == mGroupInfo.end()) {
    return 0;
  }

  return it->second.space;
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given user
//------------------------------------------------------------------------------
uint64_t QuotaNodeCore::getPhysicalSpaceByUser(uid_t uid) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mtx);
  auto it = mUserInfo.find(uid);

  if (it == mUserInfo.end()) {
    return 0;
  }

  return it->second.physicalSpace;
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given group
//------------------------------------------------------------------------------
uint64_t QuotaNodeCore::getPhysicalSpaceByGroup(gid_t gid) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mtx);
  auto it = mGroupInfo.find(gid);

  if (it == mGroupInfo.end()) {
    return 0;
  }

  return it->second.physicalSpace;
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given user
//------------------------------------------------------------------------------
uint64_t QuotaNodeCore::getNumFilesByUser(uid_t uid) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mtx);
  auto it = mUserInfo.find(uid);

  if (it == mUserInfo.end()) {
    return 0;
  }

  return it->second.files;
}

//------------------------------------------------------------------------------
// Get the amount of space occupied by the given group
//------------------------------------------------------------------------------
uint64_t QuotaNodeCore::getNumFilesByGroup(gid_t gid) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mtx);
  auto it = mGroupInfo.find(gid);

  if (it == mGroupInfo.end()) {
    return 0;
  }

  return it->second.files;
}

//------------------------------------------------------------------------------
// Account a new file.
//------------------------------------------------------------------------------
void QuotaNodeCore::addFile(uid_t uid, gid_t gid, uint64_t size,
                            uint64_t physicalSize)
{
  std::unique_lock<std::shared_timed_mutex> lock(mtx);
  UsageInfo& user  = mUserInfo[uid];
  UsageInfo& group = mGroupInfo[gid];
  user.physicalSpace  += physicalSize;
  group.physicalSpace += physicalSize;
  user.space  += size;
  group.space += size;
  user.files++;
  group.files++;
}

//------------------------------------------------------------------------------
// Remove a file.
//------------------------------------------------------------------------------
void QuotaNodeCore::removeFile(uid_t uid, gid_t gid, uint64_t size,
                               uint64_t physicalSize)
{
  std::unique_lock<std::shared_timed_mutex> lock(mtx);
  UsageInfo& user  = mUserInfo[uid];
  UsageInfo& group = mGroupInfo[gid];
  user.physicalSpace  -= physicalSize;
  group.physicalSpace -= physicalSize;
  user.space  -= size;
  group.space -= size;
  user.files--;
  group.files--;
}

//------------------------------------------------------------------------------
// Meld in another quota node core
//------------------------------------------------------------------------------
void QuotaNodeCore::meld(const QuotaNodeCore& other)
{
  std::lock(mtx, other.mtx);

  for (auto it = other.mUserInfo.begin(); it != other.mUserInfo.end(); it++) {
    mUserInfo[it->first] += it->second;
  }

  for (auto it = other.mGroupInfo.begin(); it != other.mGroupInfo.end(); it++) {
    mGroupInfo[it->first] += it->second;
  }

  mtx.unlock();
  other.mtx.unlock();
}

//----------------------------------------------------------------------------
// Get the set of uids for which information is stored in the current quota
// node.
//----------------------------------------------------------------------------
std::unordered_set<uint64_t> QuotaNodeCore::getUids() const
{
  std::shared_lock<std::shared_timed_mutex> lock(mtx);
  std::unordered_set<uint64_t> uids;

  for (auto it = mUserInfo.begin(); it != mUserInfo.end(); ++it) {
    uids.insert(it->first);
  }

  return uids;
}

//----------------------------------------------------------------------------
// Get the set of gids for which information is stored in the current quota
// node.
//----------------------------------------------------------------------------
std::unordered_set<uint64_t> QuotaNodeCore::getGids() const
{
  std::shared_lock<std::shared_timed_mutex> lock(mtx);
  std::unordered_set<uint64_t> gids;

  for (auto it = mGroupInfo.begin(); it != mGroupInfo.end(); ++it) {
    gids.insert(it->first);
  }

  return gids;
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
QuotaNodeCore& QuotaNodeCore::operator=(const QuotaNodeCore& other)
{
  std::lock(mtx, other.mtx);
  mUserInfo = other.mUserInfo;
  mGroupInfo = other.mGroupInfo;
  mtx.unlock();
  other.mtx.unlock();
  return *this;
}


//------------------------------------------------------------------------------
// operator<< (replacing all entries from update in core)
//------------------------------------------------------------------------------
QuotaNodeCore& QuotaNodeCore::operator<< (const QuotaNodeCore &other)
{
  std::lock(mtx, other.mtx);

  for (auto it = other.mUserInfo.begin(); it != other.mUserInfo.end(); it++) {
    mUserInfo[it->first] = it->second;
  }

  for (auto it = other.mGroupInfo.begin(); it != other.mGroupInfo.end(); it++) {
    mGroupInfo[it->first] = it->second;
  }

  mtx.unlock();
  other.mtx.unlock();
  return *this;
}

//------------------------------------------------------------------------------
// equality operator==
//------------------------------------------------------------------------------
bool QuotaNodeCore::operator==(const QuotaNodeCore& other) const
{
  std::lock(mtx, other.mtx);

  bool result = mUserInfo == other.mUserInfo && mGroupInfo == other.mGroupInfo;

  mtx.unlock();
  other.mtx.unlock();

  return result;
}

//----------------------------------------------------------------------------
//! set usage info by uid
//----------------------------------------------------------------------------
void QuotaNodeCore::setByUid(uid_t uid, const UsageInfo& info)
{
  std::unique_lock<std::shared_timed_mutex> lock(mtx);
  mUserInfo[uid] = info;
}

//----------------------------------------------------------------------------
//! set usage info by gid
//----------------------------------------------------------------------------
void QuotaNodeCore::setByGid(gid_t gid, const UsageInfo& info)\
{
  std::unique_lock<std::shared_timed_mutex> lock(mtx);
  mGroupInfo[gid] = info;
}

//----------------------------------------------------------------------------
//! filter usage info by uid
//----------------------------------------------------------------------------
void QuotaNodeCore::filterByUid(uid_t uid)
{
  std::unique_lock<std::shared_timed_mutex> lock(mtx);
  std::set<uid_t> uidv;
  for ( auto it : mUserInfo ) {
    uidv.insert(it.first);
  }
  for ( auto it : uidv ) {
    if ( it != uid ) {
      mUserInfo.erase(it);
    }
  }
}

//----------------------------------------------------------------------------
//! filter usage info by gid
//----------------------------------------------------------------------------
void QuotaNodeCore::filterByGid(gid_t gid)
{
  std::unique_lock<std::shared_timed_mutex> lock(mtx);
  std::set<gid_t> gidv;
  for ( auto it : mGroupInfo ) {
    gidv.insert(it.first);
  }
  for ( auto it : gidv ) {
    if ( it != gid ) {
      mGroupInfo.erase(it);
    }
  }
}


EOSNSNAMESPACE_END
