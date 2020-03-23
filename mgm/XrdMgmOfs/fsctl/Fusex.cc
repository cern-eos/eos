// ----------------------------------------------------------------------
// File: Fusex.cc
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
#include "mgm/FsView.hh"
#include "mgm/ZMQ.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Fuse extension.
// Will redirect to the RW master.
//----------------------------------------------------------------------------
int
XrdMgmOfs::Fusex(const char* path,
                 const char* ininfo,
                 std::string protobuf,
                 XrdOucEnv& env,
                 XrdOucErrInfo& error,
                 eos::common::VirtualIdentity& vid,
                 const XrdSecEntity* client)
{
  static const char* epname = "Fusex";
  ACCESSMODE_W;
  FUNCTIONMAYSTALL("Eosxd::prot::SET", vid, error);
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("Eosxd::prot::SET");
  gOFS->MgmStats.Add("Eosxd::prot::SET", vid.uid, vid.gid, 1);
  eos_static_debug("protobuf-len=%d", protobuf.length());
  eos::fusex::md md;

  if (!md.ParseFromString(protobuf)) {
    return Emsg(epname, error, EINVAL, "parse protocol buffer [EINVAL]", "");
  }

  std::string resultstream;
  std::string id = std::string("Fusex::sync:") + vid.tident.c_str();
  int rc = gOFS->zMQ->gFuseServer.HandleMD(id, md, vid, &resultstream, 0);

  if (rc) {
    return Emsg(epname, error, rc, "handle request", "");
  }

  if (resultstream.empty()) {
    return Emsg(epname, error, EINVAL,
                "illegal request - no response [EINVAL]", "");
  }

  std::string b64response;
  eos::common::SymKey::Base64(resultstream, b64response);
  XrdOucString response = "Fusex:";
  response += b64response.c_str();
  error.setErrInfo(response.length(), response.c_str());
  EXEC_TIMING_END("Eosxd::prot::SET");
  return SFS_DATA;
}
