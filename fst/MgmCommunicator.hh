//------------------------------------------------------------------------------
// File: MgmCommunicator.hh
// Author: Jozsef Makai - CERN
//------------------------------------------------------------------------------

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


#pragma once

#include "fst/Fmd.hh"
#include "common/FileId.hh"
#include "fst/Namespace.hh"
#include "XrdOuc/XrdOucEnv.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class handling communication with the MGM
//------------------------------------------------------------------------------
class MgmCommunicator {
public:
  //----------------------------------------------------------------------------
  //! Return Fmd from MGM doing getfmd command
  //!
  //! @param manager host:port of the mgm to contact
  //! @param fid file id
  //! @param fmd reference to the Fmd struct to store Fmd
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  virtual int GetMgmFmd(const char* manager, eos::common::FileId::fileid_t fid,
                        struct Fmd& fmd);

  //----------------------------------------------------------------------------
  //! Convert an FST env representation to an Fmd struct
  //!
  //! @param env env representation
  //! @param fmd reference to Fmd struct
  //!
  //! @return true if successful otherwise false
  //----------------------------------------------------------------------------
  static bool EnvMgmToFmd(XrdOucEnv& env, struct Fmd& fmd);

  //----------------------------------------------------------------------------
  //! Call the 'auto repair' function e.g. 'file convert --rewrite'
  //!
  //! @param manager host:port of the server to contact
  //! @param fid file id to auto-repair
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  virtual int CallAutoRepair(const char* manager, eos::common::FileId::fileid_t fid);
};

extern MgmCommunicator gMgmCommunicator;

EOSFSTNAMESPACE_END