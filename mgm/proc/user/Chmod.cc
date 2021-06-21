// ----------------------------------------------------------------------
// File: proc/user/Chmod.cc
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

#include "XrdOuc/XrdOucEnv.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Chmod()
{
  XrdOucString spath = pOpaque->Get("mgm.path");
  XrdOucString option = pOpaque->Get("mgm.option");
  XrdOucString mode = pOpaque->Get("mgm.chmod.mode");
  const char* inpath = spath.c_str();
  NAMESPACEMAP;
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;
  spath = path;

  PROC_TOKEN_SCOPE;

  if ((!spath.length()) || (!mode.length())) {
    stdErr = "error: you have to provide a path and the mode to set!\n";
    retc = EINVAL;
  } else {
    // find everything to be modified
    std::map<std::string, std::set<std::string> > found;
    std::map<std::string, std::set<std::string> >::const_iterator foundit;
    std::set<std::string>::const_iterator fileit;

    if (option == "r") {
      if (gOFS->_find(spath.c_str(), *mError, stdErr, *pVid, found, 0, 0, true)) {
        stdErr += "error: unable to search in path";
        retc = errno;
      }
    } else {
      // the single dir case
      (void)found[spath.c_str()].size();
    }

    char modecheck[1024];
    snprintf(modecheck, sizeof(modecheck) - 1, "%llu",
             (unsigned long long) strtoul(mode.c_str(), 0, 10));
    XrdOucString ModeCheck = modecheck;

    if (ModeCheck != mode) {
      stdErr = "error: mode has to be an octal number like 777, 2777, 755, 644 ...";
      retc = EINVAL;
    } else {
      XrdSfsMode Mode = (XrdSfsMode) strtoul(mode.c_str(), 0, 8);

      for (foundit = found.begin(); foundit != found.end(); foundit++) {
        {
          if (gOFS->_chmod(foundit->first.c_str(), Mode, *mError, *pVid, (char*) 0)) {
            stdErr += "error: unable to chmod of directory ";
            stdErr += foundit->first.c_str();
            stdErr += "\n";
            retc = errno;
          } else {
            char omode[64];
            snprintf(omode, sizeof(omode), "%o", (int)Mode);

            if (pVid->uid) {
              stdOut += "success: mode of file/directory ";
              stdOut += foundit->first.c_str();
              stdOut += " is now '2";
              stdOut += omode;
              stdOut += "'\n";
            } else {
              stdOut += "success: mode of file/directory ";
              stdOut += foundit->first.c_str();
              stdOut += " is now '";
              stdOut += omode;
              stdOut += "'\n";
            }
          }
        }
      }
    }
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
