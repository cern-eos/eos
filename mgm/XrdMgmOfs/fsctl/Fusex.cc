// ----------------------------------------------------------------------
// File: Fusex.cc
// Author: Andreas-Joachim Peters - CERN
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

{
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Fuse-Fusex", vid.uid, vid.gid, 1);

  // receive a protocol buffer and apply to the namespace

  std::string id = std::string("Fusex::sync:") + vid.tident.c_str();

  eos_static_debug("protobuf-len=%d", protobuf.length());
  
  eos::fusex::md md;
  if (!md.ParseFromString(protobuf))
  {
    return Emsg(epname, error, EINVAL, "parse protocol buffer", "");
  }
  
  std::string resultstream="";
  
  int rc = gOFS->zMQ->gFuseServer.HandleMD(id, md, &resultstream, 0);

  if (rc)
  {
    return Emsg(epname, error, rc, "handle request", "");
  }
  
  if (!resultstream.length())
  {
    return Emsg(epname, error, EINVAL, "illegal request - no response", "");
  }
  
  std::string b64response;
  eos::common::SymKey::Base64(resultstream,b64response);
      
  std::string response = "Fusex:";
  response+= b64response;
  

  error.setErrInfo(response.length(), response.c_str());
  return SFS_DATA;
}
