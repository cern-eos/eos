// ----------------------------------------------------------------------
// File: proc/user/Version.cc
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
#include "mgm/Features.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Version ()
{
  gOFS->MgmStats.Add("Version", pVid->uid, pVid->gid, 1);
  eos_info("version");
  stdOut += "EOS_INSTANCE=";
  stdOut += gOFS->MgmOfsInstanceName;
  stdOut += "\nEOS_SERVER_VERSION=";
  stdOut += VERSION;
  stdOut += " EOS_SERVER_RELEASE=";
  stdOut += RELEASE;
  if(pOpaque->Get("mgm.option") && !strcmp(pOpaque->Get("mgm.option"),"f"))
  {
    stdOut += "\nEOS_SERVER_FEATURES=";
    for(auto it = Features::sMap.begin(); it!=Features::sMap.end(); it++)
    {
      stdOut += "\n";
      stdOut += it->first.c_str();
      stdOut += "  =>  ";
      stdOut += it->second.c_str();
    }
  }
  return SFS_OK;
}

EOSMGMNAMESPACE_END
