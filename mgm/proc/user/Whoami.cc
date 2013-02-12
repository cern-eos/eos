// ----------------------------------------------------------------------
// File: proc/user/Whoami.cc
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
ProcCommand::Whoami ()
{
 gOFS->MgmStats.Add("WhoAmI", pVid->uid, pVid->gid, 1);
 stdOut += "Virtual Identity: uid=";
 stdOut += (int) pVid->uid;
 stdOut += " (";
 for (unsigned int i = 0; i < pVid->uid_list.size(); i++)
 {
   stdOut += (int) pVid->uid_list[i];
   stdOut += ",";
 }
 stdOut.erase(stdOut.length() - 1);
 stdOut += ") gid=";
 stdOut += (int) pVid->gid;
 stdOut += " (";
 for (unsigned int i = 0; i < pVid->gid_list.size(); i++)
 {
   stdOut += (int) pVid->gid_list[i];
   stdOut += ",";
 }
 stdOut.erase(stdOut.length() - 1);
 stdOut += ")";
 stdOut += " [authz:";
 stdOut += pVid->prot;
 stdOut += "]";
 if (pVid->sudoer)
   stdOut += " sudo*";

 stdOut += " host=";
 stdOut += pVid->host.c_str();
 if (pVid->geolocation.length())
 {
   stdOut += " geo-location=";
   stdOut += pVid->geolocation.c_str();
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
