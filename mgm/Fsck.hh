//------------------------------------------------------------------------------
// File: Fsck.hh
// Author: Andreas-Joachim Peters - CERN
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

#ifndef __EOSMGM_FSCK__HH__
#define __EOSMGM_FSCK__HH__

#include "mgm/Namespace.hh"
#include "common/FileSystem.hh"
#include "common/FileId.hh"
#include "common/ExpiryCache.hh"
#include <string>
#include <map>

//------------------------------------------------------------------------------
//! @file Fsck.hh
//! @brief Class aggregating FSCK statistics and repair functionality
//------------------------------------------------------------------------------
EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief Class implementing the EOS filesystem check.
//!
//! When the FSCK thread is enabled it collects in a regular interval the
//! FSCK results broadcasted by all FST nodes into a central view.
//!
//! The FSCK interface offers a 'report' and a 'repair' utility allowing to
//! inspect and to actively try to run repair commands to fix inconsistencies.
//------------------------------------------------------------------------------
class Fsck
{
public:

  //----------------------------------------------------------------------------
  //! Method ot issue a repair action
  //!
  //! @param out return of the action output
  //! @param err return of STDERR
  //! @param option selection of repair action (see code or command help)
  //----------------------------------------------------------------------------
  bool Repair(XrdOucString& out, XrdOucString& err, XrdOucString option = "");

  void Stat(XrdOucString& out);

  void Report(XrdOucString& out, XrdOucString option, const XrdOucString& selection);

private:
  using InconsistencyMap = std::map<std::string, std::map<eos::common::FileSystem::fsid_t, std::pair<std::list<eos::common::FileId::fileid_t>, std::time_t>>>;
  eos::common::ExpiryCache<InconsistencyMap> cache{std::chrono::seconds(20)};

  static InconsistencyMap* RetrieveInconsistencies();

  std::string GenerateJsonReport(const std::set<std::string>& inconsistencyTypes, bool perfsid, bool printlfn);

  std::string GenerateTextReport(const std::set<std::string>& inconsistencyTypes, bool perfsid, bool printlfn);

  void RemoveFsckFile(const string& inconsistency, eos::common::FileSystem::fsid_t fsid,
                      eos::common::FileId::fileid_t fid);
};

EOSMGMNAMESPACE_END

#endif
