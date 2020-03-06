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
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Stat.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Whoami()
{
  gOFS->MgmStats.Add("WhoAmI", pVid->uid, pVid->gid, 1);
  std::string option = (pOpaque->Get("mgm.option")) ? pOpaque->Get("mgm.option") :
                       "";

  if ((option.find("m")) != std::string::npos) {
    stdOut += "uid=";
    stdOut += (int) pVid->uid;
    stdOut += " uids=";

    for (const auto& uid : pVid->allowed_uids) {
      stdOut += (int)uid;
      stdOut += ",";
    }

    if (!pVid->allowed_uids.empty()) {
      stdOut.erase(stdOut.length() - 1);
    }

    stdOut += " gid=";
    stdOut += (int) pVid->gid;
    stdOut += " gids=";

    for (const auto& gid : pVid->allowed_gids) {
      stdOut += (int)gid;
      stdOut += ",";
    }

    if (!pVid->allowed_gids.empty()) {
      stdOut.erase(stdOut.length() - 1);
    }

    stdOut += " authz=";
    stdOut += pVid->prot;
    stdOut += " sudo=";

    if (pVid->sudoer) {
      stdOut += "true";
    } else {
      stdOut += "false";
    }

    //! the host/geo location is not reported
  } else {
    stdOut += "Virtual Identity: uid=";
    stdOut += (int) pVid->uid;
    stdOut += " (";

    for (const auto& uid : pVid->allowed_uids) {
      stdOut += (int)uid;
      stdOut += ",";
    }

    if (!pVid->allowed_uids.empty()) {
      stdOut.erase(stdOut.length() - 1);
    }

    stdOut += ") gid=";
    stdOut += (int) pVid->gid;
    stdOut += " (";

    for (const auto& gid : pVid->allowed_gids) {
      stdOut += (int)gid;
      stdOut += ",";
    }

    if (!pVid->allowed_gids.empty()) {
      stdOut.erase(stdOut.length() - 1);
    }

    stdOut += ")";
    stdOut += " [authz:";
    stdOut += pVid->prot;
    stdOut += "]";

    if (pVid->sudoer) {
      stdOut += " sudo*";
    }

    stdOut += " host=";
    stdOut += pVid->host.c_str();
    stdOut += " domain=";
    stdOut += pVid->domain.c_str();

    if (pVid->geolocation.length()) {
      stdOut += " geo-location=";
      stdOut += pVid->geolocation.c_str();
    }

    if (pVid->key.length()) {
      if (pVid->prot == "sss") {
        stdOut += " key=";
        stdOut += pVid->key.c_str();
      } else {
        stdOut += " key=<oauth2>";
      }
    }

    if (pVid->fullname.length()) {
      stdOut += " fullname='";
      stdOut += pVid->fullname.c_str();
      stdOut += "'";
    }

    if (pVid->federation.length()) {
      stdOut += " federation='";
      stdOut += pVid->federation.c_str();
      stdOut += "'";
    }

    if (pVid->email.length()) {
      stdOut += " email='";
      stdOut += pVid->email.c_str();
      stdOut += "'";
    }

    std::string tokenDump;

    if (pVid->token) {
      pVid->token->Dump(tokenDump, true, false);

      if (tokenDump.length() > 4) {
        stdOut += "\n";
        stdOut += tokenDump.c_str();
      }
    }
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
