// ----------------------------------------------------------------------
// File: GetFusex.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Get MD for a fusex client
//----------------------------------------------------------------------------
int
XrdMgmOfs::GetFusex(const char* path,
                    const char* ininfo,
                    XrdOucEnv& env,
                    XrdOucErrInfo& error,
                    eos::common::VirtualIdentity& vid,
                    const XrdSecEntity* client)
{
  static const char* epname = "GetFusex";
  ACCESSMODE_R;
  FUNCTIONMAYSTALL("Eosxd::prot::STAT", vid, error);
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("Eosxd::prot::STAT");
  gOFS->MgmStats.Add("Eosxd::prot::STAT", vid.uid, vid.gid, 1);
  ProcCommand procCommand;
  std::string spath = path;

  if (spath != "/proc/user/") {
    return Emsg(epname, error, EINVAL,
                "call GetFusex - no proc path given [EINVAL]", path);
  }

  int rc = 0;

  if ((rc = procCommand.open("/proc/user/", ininfo, vid, &error))) {
    return rc;
  } else {
    size_t len;
    const char* result = procCommand.GetResult(len);
    char* dup_result = (char*) malloc(len);

    if (result && dup_result) {
      memcpy(dup_result, result, len);
      XrdOucBuffer* buff = new XrdOucBuffer(dup_result, len);
      error.setErrInfo(len, buff);
      EXEC_TIMING_END("Eosxd::prot::STAT");
      return SFS_DATA;
    } else {
      return Emsg(epname, error, ENOMEM, "call GetFusex - out of memory", path);
    }
  }
}
