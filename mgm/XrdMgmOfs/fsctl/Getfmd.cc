// ----------------------------------------------------------------------
// File: Getfmd.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("GetMd", 0, 0, 1);

  char* afid = env.Get("mgm.getfmd.fid"); // decimal fid

  eos::common::FileId::fileid_t fid = afid ? strtoull(afid, 0, 10) : 0;

  if (!fid)
  {
    // illegal request
    XrdOucString response = "getfmd: retc=";
    response += EINVAL;
    error.setErrInfo(response.length() + 1, response.c_str());
    return SFS_DATA;
  }

  eos::IFileMD* fmd = 0;
  std::string fullpath;
  eos::common::RWMutexReadLock(gOFS->eosViewRWMutex);

  try
  {
    fmd = gOFS->eosFileService->getFileMD(fid);
    fullpath = gOFS->eosView->getUri(fmd);

  }
  catch (eos::MDException &e)
  {
    XrdOucString response = "getfmd: retc=";
    response += e.getErrno();
    error.setErrInfo(response.length() + 1, response.c_str());
    return SFS_DATA;
  }

  eos::common::Path cPath(fullpath.c_str());
  std::string fmdenv = "";
  fmd->getEnv(fmdenv, true);
  fmdenv += "&container=";
  XrdOucString safepath = cPath.GetParentPath();
  ;
  while (safepath.replace("&", "#AND#"))
  {
  }
  fmdenv += safepath.c_str();


  XrdOucString response = "getfmd: retc=0 ";
  response += fmdenv.c_str();
  if ( (response.find("checksum=&")) != STR_NPOS )
    response.replace("checksum=&", "checksum=none&"); // XrdOucEnv does not deal with empty values ... sigh ...

  {
    // patch the name of the file
    safepath = cPath.GetName();
    if ( safepath.find("&") != STR_NPOS )
    {
      XrdOucString orig_name="name=";
      orig_name += safepath;
      
      while (safepath.replace("&", "#AND#"))
      {
      } 
      
      XrdOucString safe_name="name=";
      safe_name += safepath;
      response.replace(orig_name,safe_name);
    }
  }

  error.setErrInfo(response.length() + 1, response.c_str());
  return SFS_DATA;
}
