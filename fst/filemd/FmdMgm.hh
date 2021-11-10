//------------------------------------------------------------------------------
//! @file FmdMgm.hh
//! @author Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include "fst/Namespace.hh"
#include "common/Fmd.hh"

namespace eos::ns
{
  class FileMdProto;
}

EOSFSTNAMESPACE_BEGIN

class FmdMgmHandler
{
public:
  //----------------------------------------------------------------------------
  //! Convert an FST env representation to an Fmd struct
  //!
  //! @param env env representation
  //! @param fmd reference to Fmd struct
  //!
  //! @return true if successful otherwise false
  //----------------------------------------------------------------------------
  static bool EnvMgmToFmd(XrdOucEnv& env, eos::common::FmdHelper& fmd);

  //----------------------------------------------------------------------------
  //! Convert namespace file proto md to an Fmd struct
  //!
  //! @param file namespace file proto object
  //! @param fmd reference to Fmd struct
  //!
  //! @return true if successful otherwise false
  //----------------------------------------------------------------------------
  static bool NsFileProtoToFmd(eos::ns::FileMdProto&& filemd,
                               eos::common::FmdHelper& fmd);

  //----------------------------------------------------------------------------
  //! Return Fmd from MGM doing getfmd command
  //!
  //! @parm manager manager hostname:port
  //! @param fid file id
  //! @param fmd reference to the Fmd struct to store Fmd
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  static int GetMgmFmd(const std::string& manager,
                       eos::common::FileId::fileid_t fid,
                       eos::common::FmdHelper& fmd);

  //----------------------------------------------------------------------------
  //! Execute "fs dumpmd" on the MGM node
  //!
  //! @param mgm_host MGM hostname
  //! @param fsid filesystem id
  //! @param fn_output file name where output is written
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  static bool ExecuteDumpmd(const std::string& mgm_hosst,
                            eos::common::FileSystem::fsid_t fsid,
                            std::string& fn_output);


  //----------------------------------------------------------------------------
  //! Exclude unlinked locations from the given string representation
  //!
  //! @param slocations string of locations separated by commad with unlinked
  //!        locations having an ! in front
  //!
  //! @return string with the linked locations excluded
  //----------------------------------------------------------------------------
  static std::string ExcludeUnlinkedLoc(const std::string& slocations);

};

EOSFSTNAMESPACE_END
