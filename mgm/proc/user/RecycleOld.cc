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
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Recycle.hh"
#include "mgm/Stat.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Recycle()
{
  eos_info("");
  gOFS->MgmStats.Add("Recycle", pVid->uid, pVid->gid, 1);
  std::string std_out, std_err;

  if (mSubCmd == "ls" || (mSubCmd == "")) {
    XrdOucString monitoring = pOpaque->Get("mgm.recycle.format");
    XrdOucString translateids = pOpaque->Get("mgm.recycle.printid");
    XrdOucString option = pOpaque->Get("mgm.option");
    XrdOucString global = pOpaque->Get("mgm.recycle.global");
    XrdOucString date = pOpaque->Get("mgm.recycle.arg");

    if (!date.length()) {
      Recycle::PrintOld(std_out, std_err, *pVid, (monitoring == "m"),
                        !(translateids == "n"), (mSubCmd == "ls"));
      stdOut += std_out.c_str();
      stdErr += std_err.c_str();
    }

    Recycle::Print(std_out, std_err, *pVid, (monitoring == "m"),
                   !(translateids == "n"), (mSubCmd == "ls"),
                   date.length() ? date.c_str() : "", (global == "1"));
    stdOut = std_out.c_str();
    stdErr = std_err.c_str();
  }

  if (mSubCmd == "purge") {
    XrdOucString global = pOpaque->Get("mgm.recycle.global");
    XrdOucString date = pOpaque->Get("mgm.recycle.arg");
    Recycle::PurgeOld(std_out, std_err, *pVid);
    retc = Recycle::Purge(std_out, std_err, *pVid,
                          date.length() ? date.c_str() : "",
                          global == "1");
    stdOut = std_out.c_str();
    stdErr = std_err.c_str();
  }

  if (mSubCmd == "restore") {
    XrdOucString arg = pOpaque->Get("mgm.recycle.arg");
    XrdOucString option = pOpaque->Get("mgm.option");
    bool force_orig_name = (option.find("--force-original-name") != STR_NPOS);
    bool restore_versions = (option.find("--restore-versions") != STR_NPOS);
    retc = Recycle::Restore(std_out, std_err, *pVid, arg.c_str(), force_orig_name,
                            restore_versions);
    stdOut = std_out.c_str();
    stdErr = std_err.c_str();
  }

  if (mSubCmd == "config") {
    XrdOucString arg = pOpaque->Get("mgm.recycle.arg");
    XrdOucString option = pOpaque->Get("mgm.option");
    retc = Recycle::Config(std_out, std_err, *pVid, option.c_str(), arg.c_str());
    stdOut = std_out.c_str();
    stdErr = std_err.c_str();
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
