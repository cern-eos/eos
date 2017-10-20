//------------------------------------------------------------------------------
//! @file Drainer.hh
//! @@author Andrea Manzi - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "mgm/TableFormatter/TableFormatterBase.hh"

EOSMGMNAMESPACE_BEGIN

//! Forward declaration
class DrainFS;
class DrainTransferJob;

//------------------------------------------------------------------------------
//! @brief Class running the centralized draining
//------------------------------------------------------------------------------
class Drainer: public eos::common::LogId
{
public:
  //! DrainFS thread map pair (maps DrainFS threads with their fs )
  // @todo (amanzi) the DrainFS already knows the fsid, this seems redundant
  typedef std::pair<eos::common::FileSystem::fsid_t,
          std::shared_ptr<eos::mgm::DrainFS>> DrainMapPair;

  //! Map node to vector of draining file systems
  typedef std::map<std::string, std::vector<DrainMapPair>> DrainMap;

  //----------------------------------------------------------------------------
  // Service thread static startup function
  //----------------------------------------------------------------------------
  static void* StaticDrainer(void*);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Drainer();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Drainer();

  //----------------------------------------------------------------------------
  //! Stop running thread
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  //! Start draining of a given file system
  //! @todo (amanzi): this interface in not very friendly, maybe passsing
  //!                 directly the fsid seems more natural as this class should
  //!                 not know how the fsid is encoded in the env. The same
  //!                 for the next ones.
  //----------------------------------------------------------------------------
  bool StartFSDrain(XrdOucEnv&, XrdOucString&);

  //----------------------------------------------------------------------------
  //! Stop draining of a given file system
  //----------------------------------------------------------------------------
  bool StopFSDrain(XrdOucEnv&, XrdOucString&);

  //----------------------------------------------------------------------------
  //!  Clear the Draining info for the given FS
  //---------------------------------------------------------------------------
  bool ClearFSDrain(XrdOucEnv&, XrdOucString&);

  //----------------------------------------------------------------------------
  //! Get draining status (global or specific to a fsid)
  //!
  //! @param env
  //! @param out
  //! @param err
  //----------------------------------------------------------------------------
  bool GetDrainStatus(XrdOucEnv&, XrdOucString&, XrdOucString&);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  unsigned int GetSpaceConf(const std::string& space);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void PrintTable(TableFormatterBase&, std::string, DrainMapPair&);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void PrintJobsTable(TableFormatterBase&, DrainTransferJob*);

private:

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void* Drain(void);

  pthread_t mThread;
  //contains per space the max allowed fs draining per node
  std::map<std::string, int> maxFSperNodeConfMap;
  DrainMap  mDrainFS;
  XrdSysMutex mDrainMutex, drainConfMutex;
};

EOSMGMNAMESPACE_END
