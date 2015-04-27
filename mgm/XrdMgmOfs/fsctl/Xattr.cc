// ----------------------------------------------------------------------
// File: Xattr.cc
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
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Fuse-XAttr", vid.uid, vid.gid, 1);

  eos_thread_debug("cmd=xattr subcmd=%s path=%s", env.Get("mgm.subcmd"), spath.c_str());

  const char* sub_cmd;
  struct stat buf;

  // check if it is a file or directory ....
  int retc = lstat(spath.c_str(),
                   &buf,
                   error,
                   client,
                   0);

  if (!retc && S_ISDIR(buf.st_mode))
  { //extended attributes for directories
    if ((sub_cmd = env.Get("mgm.subcmd")))
    {
      XrdOucString subcmd = sub_cmd;
      if (subcmd == "ls")
      { //listxattr
        eos::IContainerMD::XAttrMap map;
        int rc = gOFS->attr_ls(spath.c_str(), error, client, (const char *) 0, map);

        XrdOucString response = "lsxattr: retc=";
        response += rc;
        response += " ";
        if (rc == SFS_OK)
        {
          for (std::map<std::string,
                  std::string>::iterator iter = map.begin();
                  iter != map.end(); iter++)
          {
            response += iter->first.c_str();
            response += "&";
          }
          response += "\0";
          while (response.replace("user.", "tmp."))
          {
          }
          while (response.replace("tmp.", "user.eos."))
          {
          }
          while (response.replace("sys.", "user.admin."))
          {
          }
        }
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
      else if (subcmd == "get")
      { //getxattr
        XrdOucString value;
        XrdOucString key = env.Get("mgm.xattrname");
        key.replace("user.admin.", "sys.");
        key.replace("user.eos.", "user.");
        int rc = gOFS->attr_get(spath.c_str(), error, client,
                                (const char*) 0, key.c_str(), value);

        XrdOucString response = "getxattr: retc=";
        response += rc;

        if (rc == SFS_OK)
        {
          response += " value=";
          response += value;
        }

        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
      else if (subcmd == "set")
      { //setxattr
        XrdOucString key = env.Get("mgm.xattrname");
        XrdOucString value = env.Get("mgm.xattrvalue");
        key.replace("user.admin.", "sys.");
        key.replace("user.eos.", "user.");
        int rc = gOFS->attr_set(spath.c_str(), error, client,
                                (const char *) 0, key.c_str(), value.c_str());

        XrdOucString response = "setxattr: retc=";
        response += rc;

        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
      else if (subcmd == "rm")
      { // rmxattr
        XrdOucString key = env.Get("mgm.xattrname");
        key.replace("user.admin.", "sys.");
        key.replace("user.eos.", "user.");
        int rc = gOFS->attr_rem(spath.c_str(), error, client,
                                (const char *) 0, key.c_str());

        XrdOucString response = "rmxattr: retc=";
        response += rc;

        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
    }
  }
  else if (!retc && S_ISREG(buf.st_mode))
  { //extended attributes for files

    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    eos::FileMD* fmd = 0;
    try
    {
      fmd = gOFS->eosView->getFile(spath.c_str());
    }
    catch (eos::MDException &e)
    {
      eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                       e.getErrno(), e.getMessage().str().c_str());
    }

    if ((sub_cmd = env.Get("mgm.subcmd")))
    {
      XrdOucString subcmd = sub_cmd;
      char* char_key = NULL;
      XrdOucString key;
      XrdOucString response;

      if (subcmd == "ls")
      { //listxattr
        response = "lsxattr: retc=0 ";
        response += "user.eos.cid";
        response += "&";
        response += "user.eos.fid";
        response += "&";
        response += "user.eos.lid";
        response += "&";
        response += "user.eos.XStype";
        response += "&";
        response += "user.eos.XS";
        response += "&";
        error.setErrInfo(response.length() + 1, response.c_str());
      }
      else if (subcmd == "get")
      { //getxattr
        char_key = env.Get("mgm.xattrname");
        key = char_key;
        response = "getxattr: retc=";

        if (key.find("eos.cid") != STR_NPOS)
        {
          XrdOucString sizestring;
          response += "0 ";
          response += "value=";
          response += eos::common::StringConversion::GetSizeString(sizestring,
                                                                   (unsigned long long) fmd->getContainerId());
        }
        else if (key.find("eos.fid") != STR_NPOS)
        {
          char fid[32];
          response += "0 ";
          response += "value=";
          snprintf(fid, 32, "%llu", (unsigned long long) fmd->getId());
          response += fid;
        }
        else if (key.find("eos.XStype") != STR_NPOS)
        {
          response += "0 ";
          response += "value=";
          response += eos::common::LayoutId::GetChecksumString(fmd->getLayoutId());
        }
        else if (key.find("eos.XS") != STR_NPOS)
        {
          response += "0 ";
          response += "value=";
          char hb[4];
          size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());
          for (unsigned int i = 0; i < cxlen; i++)
          {
            if ((i + 1) == cxlen)
              sprintf(hb, "%02x ", (unsigned char) (fmd->getChecksum().getDataPadded(i)));
            else
              sprintf(hb, "%02x_", (unsigned char) (fmd->getChecksum().getDataPadded(i)));
            response += hb;
          }

        }
        else if (key.find("eos.lid") != STR_NPOS)
        {
          response += "0 ";
          response += "value=";
          response += eos::common::LayoutId::GetLayoutTypeString(fmd->getLayoutId());
        }
        else
          response += "1 ";

        error.setErrInfo(response.length() + 1, response.c_str());
      }
      else if (subcmd == "rm")
      { //rmxattr
        response = "rmxattr: retc=38"; //error ENOSYS
        error.setErrInfo(response.length() + 1, response.c_str());
      }
      else if (subcmd == "set")
      { //setxattr
        response = "setxattr: retc=38"; //error ENOSYS
        error.setErrInfo(response.length() + 1, response.c_str());
      }

      return SFS_DATA;
    }
    return SFS_DATA;
  }
}