// ----------------------------------------------------------------------
// File: proc/user/Map.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Map ()
{

  if (mSubCmd == "ls")
  {
    eos::common::RWMutexReadLock lock(gOFS->PathMapMutex);
    std::map<std::string, std::string>::const_iterator it;
    for (it = gOFS->PathMap.begin(); it != gOFS->PathMap.end(); it++)
    {
      char mapline[16384];
      snprintf(mapline, sizeof (mapline) - 1, "%-64s => %s\n", it->first.c_str(), it->second.c_str());
      stdOut += mapline;
    }
    return SFS_OK;
  }

  if (mSubCmd == "link")
  {
    if ((!pVid->uid) ||
        eos::common::Mapping::HasUid(3, vid.uid_list) ||
        eos::common::Mapping::HasGid(4, vid.gid_list))
    {
      XrdOucString srcpath = pOpaque->Get("mgm.map.src");
      XrdOucString dstpath = pOpaque->Get("mgm.map.dest");
      if ((!srcpath.length()) || ((srcpath.find("..") != STR_NPOS))
          || ((srcpath.find("/../") != STR_NPOS))
          || ((srcpath.find(" ") != STR_NPOS))
          || ((srcpath.find("\\") != STR_NPOS))
          || ((srcpath.find("/./") != STR_NPOS))
          || ((!srcpath.beginswith("/")))
          || ((!srcpath.endswith("/")))
          || (!dstpath.length()) || ((dstpath.find("..") != STR_NPOS))
          || ((dstpath.find("/../") != STR_NPOS))
          || ((dstpath.find(" ") != STR_NPOS))
          || ((dstpath.find("\\") != STR_NPOS))
          || ((dstpath.find("/./") != STR_NPOS))
          || ((!dstpath.beginswith("/")))
          || ((!dstpath.endswith("/"))))
      {

        retc = EPERM;
        stdErr = "error: source and destination path has to start and end with '/', shouldn't contain spaces, '/./' or '/../' or backslash characters!";
      }
      else
      {
        if (gOFS->PathMap.count(srcpath.c_str()))
        {
          retc = EEXIST;
          stdErr = "error: there is already a mapping defined for '";
          stdErr += srcpath.c_str();
          stdErr += "' - remove the existing mapping using 'map unlink'!";
        }
        else
        {
          gOFS->PathMap[srcpath.c_str()] = dstpath.c_str();
          gOFS->ConfEngine->SetConfigValue("map", srcpath.c_str(), dstpath.c_str());
          stdOut = "success: added mapping '";
          stdOut += srcpath.c_str();
          stdOut += "'=>'";
          stdOut += dstpath.c_str();
          stdOut += "'";
        }
      }
    }
    else
    {
      // permission denied
      retc = EPERM;
      stdErr = "error: you don't have the required priviledges to execute 'map link'!";
    }
    return SFS_OK;
  }

  if (mSubCmd == "unlink")
  {
    XrdOucString path = pOpaque->Get("mgm.map.src");
    if ((!pVid->uid) ||
        eos::common::Mapping::HasUid(3, vid.uid_list) ||
        eos::common::Mapping::HasGid(4, vid.gid_list))
    {
      eos::common::RWMutexWriteLock lock(gOFS->PathMapMutex);
      if ((!path.length()) || (!gOFS->PathMap.count(path.c_str())))
      {
        retc = EINVAL;
        stdErr = "error: path '";
        stdErr += path.c_str();
        stdErr += "' is not in the path map!";
      }
      else
      {
        gOFS->PathMap.erase(path.c_str());
        gOFS->ConfEngine->DeleteConfigValue("map", path.c_str());
        stdOut = "success: removed mapping of path '";
        stdOut += path.c_str();
        stdOut += "'";
      }
    }
    else
    {
      // permission denied
      retc = EPERM;
      stdErr = "error: you don't have the required priviledges to execute 'map unlink'!";
    }
  }
  return SFS_OK;
}

EOSMGMNAMESPACE_END
