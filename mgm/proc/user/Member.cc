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
ProcCommand::Member ()
{
  XrdOucString smember = pOpaque->Get("mgm.egroup");
  if (smember.length()) 
  {
    std::string egroup = smember.c_str();
    std::string rs = Egroup::DumpMember(vid.uid_string,  egroup);
    stdOut += rs.c_str();

  } 
  else
  {
    std::string rs = Egroup::DumpMembers();
    stdOut += rs.c_str();
  }
  return SFS_OK;
}

EOSMGMNAMESPACE_END
