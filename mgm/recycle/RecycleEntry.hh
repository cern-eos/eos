//------------------------------------------------------------------------------
// File: RecycleEntry.hh
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

#pragma once
#include "mgm/Namespace.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include <string>
#include <string_view>

//! Forward declarations
namespace eos
{
namespace common
{
class VirtualIdentity;
}
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class RecycleEntry - modelling an entry that is to be handled by the
//! recycling functionality
//------------------------------------------------------------------------------
class RecycleEntry
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param path path to recycle
  //! @param recycle_dir recycle bin directory
  //! @param owner_uid user id
  //! @param owner_gid group id
  //! @param id of the container or file
  //----------------------------------------------------------------------------
  RecycleEntry(std::string_view path, std::string_view recycle_dir,
               eos::common::VirtualIdentity* vid, uid_t owner_uid,
               gid_t owner_gid, unsigned long long id);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RecycleEntry() = default;

  //----------------------------------------------------------------------------
  //! Recycle the given object (file or subtree)
  //!
  //! @param epname error tag
  //! @param error object
  //!
  //! @return SFS_OK if ok, otherwise SFS_ERR + errno + error object set
  //----------------------------------------------------------------------------
  int ToGarbage(const char* epname, XrdOucErrInfo& error);

private:
  std::string mPath; ///< Path for the entry to recycle
  std::string mRecycleDir; ///< Path for the top recycle directory
  uid_t mOwnerUid; ///< Original uid owner of the entry
  gid_t mOwnerGid; ///< Origianl gid owner of the entry
  unsigned long long mId; ///< File or container identifier
  //! Root virtual identity used internally for unrestricted operations
  static eos::common::VirtualIdentity mRootVid;

  //----------------------------------------------------------------------------
  //! Compute recycle path directory for given user and timestamp
  //!
  //! epname error printing name
  //! error error object
  //! recyclepath computed by this function
  //!
  //! SFS_OK if ok, otherwise SFS_ERR + errno + error object set
  //----------------------------------------------------------------------------
  int GetRecyclePrefix(const char* epname, XrdOucErrInfo& error,
                       std::string& recyclepath);

};

EOSMGMNAMESPACE_END
