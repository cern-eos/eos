//------------------------------------------------------------------------------
// File: RecycleEntry.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/****************************(********************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "mgm/recycle/RecycleEntry.hh"
#include "mgm/recycle/Recycle.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/VirtualIdentity.hh"
#include <sys/stat.h>

//! Static members
uint32_t RecycleEntry::sMaxEntriesPerDir = 100000;
eos::common::VirtualIdentity RecycleEntry::mRootVid =
  eos::common::VirtualIdentity::Root();

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RecycleEntry::RecycleEntry(std::string_view path, std::string_view recycle_dir,
                           std::string_view rid,
                           eos::common::VirtualIdentity* vid, uid_t uid,
                           gid_t gid, unsigned long long id):
  mPath(path), mRecycleDir(recycle_dir),
  mOwnerUid(uid), mOwnerGid(gid), mId(id)
{
  if (rid.empty()) {
    mRecycleId = SSTR("uid:" << mOwnerUid);
  } else {
    mRecycleId = SSTR("rid:" << rid.data());
  }

  // Make sure the recycle dir path does not have ending '/'
  if (!mRecycleDir.empty() && (*mRecycleDir.rbegin() == '/')) {
    mRecycleDir.erase(mRecycleDir.length() - 1);
  }
}

//------------------------------------------------------------------------------
// Compute recycle path directory for given user and timestamp
//------------------------------------------------------------------------------
int
RecycleEntry::GetRecyclePrefix(const char* epname, XrdOucErrInfo& error,
                               std::string& recycle_prefix)
{
  char prefix[4096];
  time_t now = time(NULL);
  struct tm nowtm;
  localtime_r(&now, &nowtm);
  size_t index = 0;

  do {
    snprintf(prefix, sizeof(prefix) - 1, "%s/%s/%04u/%02u/%02u/%lu",
             mRecycleDir.c_str(), mRecycleId.c_str(), 1900 + nowtm.tm_year,
             nowtm.tm_mon + 1, nowtm.tm_mday, index);
    struct stat buf;

    // If index directory exists and it has more than 100k enties then
    // move on to the next index.
    if (!gOFS->_stat(prefix, &buf, error, mRootVid, "")) {
      if (buf.st_blksize > sMaxEntriesPerDir) {
        ++index;
        continue;
      }
    } else {
      // Create recycle directory
      if (gOFS->_mkdir(prefix, S_IRUSR | S_IXUSR | SFS_O_MKPTH,
                       error, mRootVid, "")) {
        return gOFS->Emsg(epname, error, EIO, "remove existing file - the "
                          "recycle space user directory couldn't be created");
      }

      // Check recycle directory ownership
      if (gOFS->_stat(prefix, &buf, error, mRootVid, "")) {
        return gOFS->Emsg(epname, error, EIO, "remove existing file - could "
                          "not determine ownership of the recycle space "
                          "user directory", prefix);
      }

      // Update the ownership of the user directory
      if ((buf.st_uid != mOwnerUid) || (buf.st_gid != mOwnerGid)) {
        if (gOFS->_chown(prefix, mOwnerUid, mOwnerGid, error, mRootVid, "")) {
          return gOFS->Emsg(epname, error, EIO, "remove existing file - could "
                            "not change ownership of the recycle space user "
                            "directory", prefix);
        }
      }
    }

    recycle_prefix = prefix;
    return SFS_OK;
  } while (true);
}


//------------------------------------------------------------------------------
// Recycle the given object (file or subtree)
//------------------------------------------------------------------------------
int
RecycleEntry::ToGarbage(const char* epname, XrdOucErrInfo& error)
{
  char recycle_path[4096];
  // If path ends with '/' we recycle a full directory tree aka directory
  bool is_dir = false;
  // rewrite the file name /a/b/c as #:#a#:#b#:#c
  XrdOucString contractedpath = mPath.c_str();

  if (contractedpath.endswith("/")) {
    is_dir = true;
    mPath.erase(mPath.length() - 1);
    // remove the '/' indicating a recursive directory recycling
    contractedpath.erase(contractedpath.length() - 1);
  }

  while (contractedpath.replace("/", "#:#")) {
  }

  // For dir's we add a '.d' in the end of the recycle path
  std::string lPostFix = "";

  if (is_dir) {
    lPostFix = Recycle::gRecyclingPostFix;
  }

  std::string rpath;
  int rc = 0;

  // retrieve the current valid index directory
  if ((rc = GetRecyclePrefix(epname, error, rpath))) {
    return rc;
  }

  snprintf(recycle_path, sizeof(recycle_path) - 1, "%s/%s.%016llx%s",
           rpath.c_str(), contractedpath.c_str(), mId, lPostFix.c_str());

  // Finally do the rename
  if (gOFS->_rename(mPath.c_str(), recycle_path, error, mRootVid,
                    "", "", true, true, false, true)) {
    return gOFS->Emsg(epname, error, EIO, "rename file/directory",
                      recycle_path);
  }

  // Store the recycle path in the error object
  error.setErrInfo(0, recycle_path);
  return SFS_OK;
}

EOSMGMNAMESPACE_END
