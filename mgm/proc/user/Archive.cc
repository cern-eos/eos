//------------------------------------------------------------------------------
// File: Archive.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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


/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Archive()
{
  XrdOucString spath = pOpaque->Get("mgm.archive.path");
  const char* inpath = spath.c_str();
  NAMESPACEMAP;
  if (info) info = 0;
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;

  eos::common::Path cPath(path);
  spath = cPath.GetPath();

  if ((mSubCmd == "create") &&
      (!pOpaque->Get("mgm.archive.dst") || (!spath.length())))
  {
    stdErr = "error: need to provide 'mgm.archive.dst' and archive path for create";
    retc = EINVAL;
  }
  else if (((mSubCmd == "migrate") && (!spath.length())) ||
           ((mSubCmd == "stage")   && (!spath.length())))
  {
    stdErr = "error: need to provide a path for archive stage or migrate";
    retc = EINVAL;
  }
  else if ((mSubCmd == "list") && (!pOpaque->Get("mgm.archive.type")))
  {
    stdErr = "error: need to provide the archive listing type";
    retc = EINVAL;
  }
  else
  {
    stdErr = "error: operation not supported, needs to be one of the following: "
        "create, migrate, stage or list";
    retc = EINVAL;
  }

  // TODO: create a ZMQ connection and use it to send commands and get replies
  // + implement the method to create an archive file here on the server side

  // TODO: use the gOFS->_find command to get a list of all the files in the
  // current subtree

  return SFS_OK;
}

EOSMGMNAMESPACE_END
