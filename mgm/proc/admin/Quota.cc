// ----------------------------------------------------------------------
// File: proc/admin/Quota.cc
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
#include "mgm/Quota.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::AdminQuota()
{
  if (mSubCmd == "rmnode")
  {
    eos_notice("quota rm");

    if ((pVid->prot != "sss") && (eos::common::Mapping::IsLocalhost(*pVid)))
    {
      std::string msg = "";
      std::string tag = "mgm.quota.space";

      if (!pOpaque->Get(tag.c_str()))
      {
	retc = EINVAL;
	stdErr = "error: no quota space specified";
	return SFS_OK;
      }

      std::string space = pOpaque->Get(tag.c_str());

      if (Quota::RmSpaceQuota(space, msg, retc))
	stdOut = msg.c_str();
      else
	stdErr = msg.c_str();
    }
    else
    {
      retc = EPERM;
      stdErr = "error: you cannot remove quota nodes from storage node with"
	       " 'sss' authentication!";
    }
  }
  else
  {
    stdErr = "error: unknown subcommand <";
    stdErr += mSubCmd;
    stdErr += ">";
    retc = EINVAL;
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
