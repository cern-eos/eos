// ----------------------------------------------------------------------
// File: proc/user/Mkdir.cc
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
#include "mgm/Access.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Mkdir ()
{
  XrdOucString spath = pOpaque->Get("mgm.path");
  XrdOucString option = pOpaque->Get("mgm.option");

  const char* inpath = spath.c_str();

  NAMESPACEMAP;
  info = 0;
  if (info)info = 0; // for compiler happyness
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;

  spath = path;

  if (!spath.length())
  {
    stdErr = "error: you have to give a path name to call 'mkdir'";
    retc = EINVAL;
  }
  else
  {
    XrdSfsMode mode = 0;
    if (option == "p")
    {
      mode |= SFS_O_MKPTH;
    }
    if (gOFS->_mkdir(spath.c_str(), mode, *mError, *pVid, (const char*) 0))
    {
      stdErr += "error: unable to create directory";
      retc = errno;
    }
  }
  return SFS_OK;
}

EOSMGMNAMESPACE_END
