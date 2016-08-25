//------------------------------------------------------------------------------
// File: Backup.hh
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef __EOSMGM_BACKUP_HH__
#define __EOSMGM_BACKUP_HH__

#include "mgm/ProcInterface.hh"
#include <set>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class TwindowFilter to exclude older entries
//------------------------------------------------------------------------------
class TwindowFilter: public IFilter, public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param twindow_type time window type
  //! @param twindow_val time window value
  //----------------------------------------------------------------------------
  TwindowFilter(const std::string& twindow_type, const std::string& twindow_val);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~TwindowFilter() {};

  //----------------------------------------------------------------------------
  //! Filter file entry if it is a version file i.e. contains ".sys.v#." or if
  //! it is ouside the timewindow
  //!
  //! @param entry_info entry information on which the filter is applied
  //!
  //! @return true if entry should be filtered out, otherwise false
  //----------------------------------------------------------------------------
  bool FilterOutFile(const std::map<std::string, std::string>& entry_info);

  //----------------------------------------------------------------------------
  //! Filter the directory entry
  //!
  //! @param path current directory path
  //!
  //! @return true if entry should be filtered out, otherwise false
  //----------------------------------------------------------------------------
  bool FilterOutDir(const std::string& path);

private:
  std::string mTwindowType; ///< Time window type
  std::string mTwindowVal; ///< Time window value
  std::set<std::string> mSetDirs; ///< Set of directories to keep
};

EOSMGMNAMESPACE_END

#endif // __EOS_MGM_BACKUP_HH__
