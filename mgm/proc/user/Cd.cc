// ----------------------------------------------------------------------
// File: proc/user/Cd.cc
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
ProcCommand::Cd ()
{
  gOFS->MgmStats.Add("Cd", pVid->uid, pVid->gid, 1);
  XrdOucString spath = pOpaque->Get("mgm.path");
  XrdOucString option = pOpaque->Get("mgm.option");

  const char* inpath = spath.c_str();

  NAMESPACEMAP;
  info = 0;
  if (info)info = 0; // for compiler happyness

  spath = path;

  if (!spath.length())
  {
    stdErr = "error: you have to give a path name to call 'cd'";
    retc = EINVAL;
  }
  else
  {
    XrdMgmOfsDirectory dir;
    struct stat buf;
    if (gOFS->_stat(spath.c_str(), &buf, *mError, *pVid, (const char*) 0))
    {
      stdErr = mError->getErrText();
      retc = errno;
    }
    else
    {
      // if this is a directory open it and list
      if (S_ISDIR(buf.st_mode))
      {
        retc = 0;
      }
      else
      {
        stdErr += "error: this is not a directory";
        retc = ENOTDIR;
      }
    }
  }
  return SFS_OK;
}

EOSMGMNAMESPACE_END
