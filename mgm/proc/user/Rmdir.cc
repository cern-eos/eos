// ----------------------------------------------------------------------
// File: proc/user/Rmdir.cc
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

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Rmdir()
{
  XrdOucString spath = pOpaque->Get("mgm.path");
  const char* inpath = spath.c_str();
  NAMESPACEMAP;
  NAMESPACE_NO_TRAILING_SLASH;
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;
  spath = path;
  PROC_TOKEN_SCOPE;

  if (!spath.length()) {
    stdErr = "error: you have to give a path name to call 'rmdir'";
    retc = EINVAL;
  } else {
    if (gOFS->_remdir(spath.c_str(), *mError, *pVid, (const char*) 0)) {
      stdErr += "error: unable to remove directory \"";
      stdErr += spath.c_str();
      stdErr +="\"";
      retc = errno;
    }
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
