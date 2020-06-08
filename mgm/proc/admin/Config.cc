// ----------------------------------------------------------------------
// File: proc/admin/Config.cc
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
#include "mgm/FsView.hh"
#include "mgm/config/IConfigEngine.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Config()
{
  if (mSubCmd == "ls") {
    eos_notice("config ls");
    XrdOucString listing = "";
    bool showbackup = (bool)pOpaque->Get("mgm.config.showbackup");

    if (!(gOFS->ConfEngine->ListConfigs(listing, showbackup))) {
      stdErr += "error: listing of existing configs failed!";
      retc = errno;
    } else {
      stdOut += listing;
    }
  }

  int envlen;

  if (mSubCmd == "load") {
    if (pVid->uid == 0) {
      eos_notice("config load: %s", pOpaque->Env(envlen));
      eos::mgm::ConfigResetMonitor fsview_cfg_reset_monitor;

      std::string name = pOpaque->Get("mgm.config.file");

      if (!gOFS->ConfEngine->LoadConfig(name, stdErr)) {
        retc = errno;
      } else {
        stdOut = "success: configuration successfully loaded!";
      }
    } else {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }

  if (mSubCmd == "export") {
    retc = EINVAL;
    stdErr = "error: export command has been deprecated";
  }

  if (mSubCmd == "save") {
    eos_notice("config save: %s", pOpaque->Env(envlen));

    const char* name = pOpaque->Get("mgm.config.file");
    bool force = (bool)pOpaque->Get("mgm.config.force");
    bool autosave = (bool)pOpaque->Get("mgm.config.autosave");
    const char* comment = pOpaque->Get("mgm.config.comment");

    if (pVid->uid == 0) {
      if (!gOFS->ConfEngine->SaveConfig(name, force, autosave, comment, stdErr)) {
        retc = errno;
      } else {
        stdOut = "success: configuration successfully saved!";
      }
    } else {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }

  if (mSubCmd == "reset") {
    eos_notice("config reset");

    if (pVid->uid == 0) {
      gOFS->ConfEngine->ResetConfig();
      stdOut = "success: configuration has been reset(cleaned)!";
    } else {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }

  if (mSubCmd == "dump") {
    eos_notice("config dump");
    XrdOucString dump = "";

    const char* name = pOpaque->Get("mgm.config.file");

//    if (!gOFS->ConfEngine->DumpConfig(dump, *pOpaque)) {
    if (!gOFS->ConfEngine->DumpConfig(dump, name)) {
      stdErr += "error: listing of existing configs failed!";
      retc = errno;
    } else {
      stdOut += dump;
      mDoSort = true;
    }
  }

  if (mSubCmd == "changelog") {
    int nlines = 5;
    char* val;

    if ((val = pOpaque->Get("mgm.config.lines"))) {
      nlines = atoi(val);

      if (nlines < 1) {
        nlines = 1;
      }
    }

    std::string tailOut;
    gOFS->ConfEngine->Tail(nlines, tailOut);
    stdOut = tailOut.c_str();
    eos_notice("config changelog");
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
