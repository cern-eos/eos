// ----------------------------------------------------------------------
// File: MasterSignal.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#include "common/Logging.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"
#include "mgm/IMaster.hh"
#include "mgm/Master.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Received signal to bounce everything to the remote master
//----------------------------------------------------------------------------
int
XrdMgmOfs::MasterSignalBounce(const char* path,
                              const char* ininfo,
                              XrdOucEnv& env,
                              XrdOucErrInfo& error,
                              eos::common::LogId& ThreadLogId,
                              eos::common::Mapping::VirtualIdentity& vid,
                              const XrdSecEntity* client)
{
  static const char* epname = "MasterSignalBounce";

  REQUIRE_SSS_OR_LOCAL_AUTH;

  eos::mgm::Master* master =
      dynamic_cast<eos::mgm::Master*>(gOFS->mMaster.get());

  if (master) {
    master->TagNamespaceInodes();
    master->RedirectToRemoteMaster();
  }

  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  return SFS_DATA;
}

//----------------------------------------------------------------------------
// Received signal from remote master to reload namespace
//----------------------------------------------------------------------------
int
XrdMgmOfs::MasterSignalReload(const char* path,
                              const char* ininfo,
                              XrdOucEnv& env,
                              XrdOucErrInfo& error,
                              eos::common::LogId& ThreadLogId,
                              eos::common::Mapping::VirtualIdentity& vid,
                              const XrdSecEntity* client)
{
  static const char* epname = "MasterSignalReload";

  REQUIRE_SSS_OR_LOCAL_AUTH;

  bool compact_files       = env.Get("compact_files") != nullptr;
  bool compact_directories = env.Get("compact_dirs")  != nullptr;

  eos::mgm::Master* master =
      dynamic_cast<eos::mgm::Master*>(gOFS->mMaster.get());

  if (master) {
    master->WaitNamespaceFilesInSync(compact_files, compact_directories);
    master->RebootSlaveNamespace();
  }

  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  return SFS_DATA;
}

//----------------------------------------------------------------------------
// Query to determine if current node is acting as master
//----------------------------------------------------------------------------
int
XrdMgmOfs::IsMaster(const char* path,
                    const char* ininfo,
                    XrdOucEnv& env,
                    XrdOucErrInfo& error,
                    eos::common::LogId& ThreadLogId,
                    eos::common::Mapping::VirtualIdentity& vid,
                    const XrdSecEntity* client)
{
  static const char* epname = "IsMaster";

  // TODO (esindril): maybe enable SSS at some point
  // REQUIRE_SSS_OR_LOCAL_AUTH;

  if (!gOFS->mMaster->IsMaster()) {
    return Emsg(epname, error, ENOENT, "find master file [ENOENT]", "");
  }

  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  return SFS_DATA;
}
