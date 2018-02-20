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

EOSMGMNAMESPACE_BEGIN

//! Forward declaration
class DrainFS;
class DrainTransferJob;
class TableFormatterBase;

//------------------------------------------------------------------------------
//! @brief Class running the centralized draining
//------------------------------------------------------------------------------
class Drainer: public eos::common::LogId
{
public:

  //! Map node to vector of draining file systems
  typedef std::map<std::string, std::set<std::shared_ptr<eos::mgm::DrainFS>>> DrainMap;

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
  //! Start  of a given file system
  //----------------------------------------------------------------------------
  bool StartFSDrain(unsigned int sourceFsId, unsigned int targetFsId, XrdOucString&);

  //----------------------------------------------------------------------------
  //! Stop draining of a given file system
  //----------------------------------------------------------------------------
  bool StopFSDrain(unsigned int fsId, XrdOucString&);

  //----------------------------------------------------------------------------
  //!  Clear the Draining info for the given FS
  //---------------------------------------------------------------------------
  bool ClearFSDrain(unsigned int fsId, XrdOucString&);

  //----------------------------------------------------------------------------
  //! Get draining status (global or specific to a fsid)
  //!
  //! @param env
  //! @param out
  //! @param err
  //----------------------------------------------------------------------------
  bool GetDrainStatus(unsigned int fsId, XrdOucString&, XrdOucString&);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  unsigned int GetSpaceConf(const std::string& space);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void PrintTable(TableFormatterBase&, std::string, DrainFS* fs);

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
