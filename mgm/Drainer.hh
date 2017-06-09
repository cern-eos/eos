// ----------------------------------------------------------------------
// File: Drainer.hh
// Author: Andrea Manzi - CERN
// ----------------------------------------------------------------------

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

#ifndef __EOSMGM_DRAINER__
#define __EOSMGM_DRAINER__

/* -------------------------------------------------------------------------- */
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "mgm/drain/DrainFS.hh"
#include "mgm/drain/DrainTransferJob.hh"
/* -------------------------------------------------------------------------- */
#include "XrdSys/XrdSysPthread.hh"
/* -------------------------------------------------------------------------- */
#include <vector>
#include <string>
#include <deque>
#include <cstring>
#include <ctime>
#include <map>
#include <memory>
/* -------------------------------------------------------------------------- */
/**
 * @file Drainer.hh
 *
 * @brief Drain filesystems using GeoTreeEngine
 *
 */
/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/**
 * @brief Class running the drain
 *
 *
*/

/*----------------------------------------------------------------------------*/
class Drainer: public eos::common::LogId
{
private:
  /// thread id
  pthread_t mThread;

  /// DrainFS thread map (maps DrainFS threads  with their fs )
  typedef std::pair<eos::common::FileSystem::fsid_t, shared_ptr<eos::mgm::DrainFS>>
      DrainMapPair;
  typedef std::map<DrainMapPair::first_type, DrainMapPair::second_type> DrainMap;

  //contains per space the number of draining file systems and the max allowed
  std::map<std::string, std::pair<int, int>> drainingFSMap;

  DrainMap  mDrainFS;

  XrdSysMutex mDrainMutex, mDrainingFSMutex;

public:

  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------
  Drainer();

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  ~Drainer();

  // ---------------------------------------------------------------------------
  // thread stop function
  // ---------------------------------------------------------------------------
  void Stop();

  // ---------------------------------------------------------------------------
  //   Start Draining the given FS
  // ---------------------------------------------------------------------------
  bool StartFSDrain(XrdOucEnv&, XrdOucString&);

  // ---------------------------------------------------------------------------
  //   Stop Draining the given FS
  // ---------------------------------------------------------------------------
  bool StopFSDrain(XrdOucEnv&, XrdOucString&);

  // ---------------------------------------------------------------------------
  //  Clear the Draining info for the given FS
  //-- ------------------------------------------------------------------------
  bool ClearFSDrain(XrdOucEnv&, XrdOucString&);

  // ---------------------------------------------------------------------------
  //   Get Draining status ( global or specific to a fsid)
  // ---------------------------------------------------------------------------
  bool GetDrainStatus(XrdOucEnv&, XrdOucString&, XrdOucString&);
  // --------------------------------------------
  // Service thread static startup function
  // ---------------------------------------------------------------------------
  static void* StaticDrainer(void*);
  //

  void* Drain(void);
};

EOSMGMNAMESPACE_END
#endif

