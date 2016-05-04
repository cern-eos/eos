// ----------------------------------------------------------------------
// File: Redirect.cc
// Author: Geoffray Adde - CERN
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
  gOFS->MgmStats.Add("OpenRedirect", vid.uid, vid.gid, 1);
  XrdMgmOfsFile* file = new XrdMgmOfsFile(const_cast<char*>(client->tident));

  if (file)
  {
    XrdSfsFileOpenMode oflags = SFS_O_RDONLY;
    mode_t omode = 0;
    if(env.Get("eos.client.openflags"))
    {
      std::string openflags=env.Get("eos.client.openflags");
      oflags = SFS_O_RDONLY;
      if(openflags.find("wo")!=std::string::npos) oflags |= SFS_O_WRONLY;
      if(openflags.find("rw")!=std::string::npos) oflags |= SFS_O_RDWR;
      if(openflags.find("cr")!=std::string::npos) oflags |= SFS_O_CREAT;
      if(openflags.find("tr")!=std::string::npos) oflags |= SFS_O_TRUNC;
      std::string openmode=env.Get("eos.client.openmode");
      omode = (mode_t) strtol(openmode.c_str(),NULL,8);
    }

    if ( (oflags & SFS_O_CREAT) ||
	 (oflags & SFS_O_RDWR) ||
	 (oflags & SFS_O_TRUNC) )
    {
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;
    }
    else 
    {
      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;      
    }


    int rc = file->open(spath.c_str(), oflags, omode, client, opaque.c_str());
    std::string ei = file->error.getErrText();
    if (rc == SFS_REDIRECT)
    {
      char buf[1024];
      snprintf(buf,1024,":%d/%s?",file->error.getErrInfo(),spath.c_str());
      ei.replace(ei.find("?"),1,buf);
      error.setErrInfo(ei.size() + 1, ei.c_str());
      delete file;
      eos_static_debug("sucess redirect=%s",error.getErrText());
      return SFS_DATA;
    }
    else
    {
      error.setErrInfo(ei.size() + 1, ei.c_str());
      eos_static_debug("fail redirect=%s",error.getErrText());

      error.setErrCode(file->error.getErrInfo());
      delete file;
      return SFS_ERROR;
    }
  }
  else
  {
    error.setErrInfo(ENOMEM, "allocate file object");
    return SFS_ERROR;
  }
}
