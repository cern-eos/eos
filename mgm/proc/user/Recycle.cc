// ----------------------------------------------------------------------
// File: proc/user/Recycle.cc
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
#include "mgm/Recycle.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Recycle ()
{
 eos_info("");
 gOFS->MgmStats.Add("Recycle", pVid->uid, pVid->gid, 1);

 if (mSubCmd == "ls" || (mSubCmd == ""))
 {
   XrdOucString monitoring = pOpaque->Get("mgm.recycle.format");
   XrdOucString translateids = pOpaque->Get("mgm.recycle.printid");
   XrdOucString option = pOpaque->Get("mgm.option");

   Recycle::Print(stdOut, stdErr, *pVid, (monitoring == "m"), !(translateids == "n"), (mSubCmd == "ls"));
 }

 if (mSubCmd == "purge")
 {
   retc = Recycle::Purge(stdOut, stdErr, *pVid);
 }

 if (mSubCmd == "restore")
 {
   XrdOucString arg = pOpaque->Get("mgm.recycle.arg");
   XrdOucString option = pOpaque->Get("mgm.option");

   retc = Recycle::Restore(stdOut, stdErr, *pVid, arg.c_str(), option);
 }

 if (mSubCmd == "config")
 {
   XrdOucString arg = pOpaque->Get("mgm.recycle.arg");
   XrdOucString option = pOpaque->Get("mgm.option");
   
   retc = Recycle::Config(stdOut, stdErr, *pVid, arg.c_str(), option);
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
