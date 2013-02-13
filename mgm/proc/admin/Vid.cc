// ----------------------------------------------------------------------
// File: proc/admin/Vid.cc
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
#include "mgm/Vid.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Vid ()
{
 if (mSubCmd == "ls")
 {
   eos_notice("vid ls");
   Vid::Ls(*pOpaque, retc, stdOut, stdErr);
   mDoSort = true;
 }

 if ((mSubCmd == "set") || (mSubCmd == "rm"))
 {
   if (pVid->uid == 0)
   {
     if (mSubCmd == "set")
     {
       eos_notice("vid set");
       Vid::Set(*pOpaque, retc, stdOut, stdErr);
     }


     if (mSubCmd == "rm")
     {
       eos_notice("vid rm");
       Vid::Rm(*pOpaque, retc, stdOut, stdErr);
     }
   }
   else
   {
     retc = EPERM;
     stdErr = "error: you have to take role 'root' to execute this command";
   }
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
