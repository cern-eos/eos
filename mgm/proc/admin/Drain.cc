// ----------------------------------------------------------------------
// File: proc/admin/Drain.cc
// Author: Andrea Manzi - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/drain/Drainer.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Drain()
{
  if (mSubCmd == "start") {
    if (pVid->uid == 0) {
      eos_notice("drain start");

      if (!gOFS->DrainerEngine->StartFSDrain(*pOpaque, stdErr)) {
        retc = errno;
      } else {
        stdOut = "success: drain successfully started!";
      }
    } else {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }

  if (mSubCmd == "stop") {
    if (pVid->uid == 0) {
      eos_notice("drain stop");

      if (!gOFS->DrainerEngine->StopFSDrain(*pOpaque, stdErr)) {
        retc = errno;
      } else {
        stdOut = "success: drain successfully stopped!";
      }
    } else {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }

  if (mSubCmd == "status") {
    if ((pVid->uid == 0)) {
      eos_notice("drain status");
      XrdOucString status;

      if (!gOFS->DrainerEngine->GetDrainStatus(*pOpaque, status, stdErr)) {
        retc = errno;
      } else {
        stdOut += status;
      }

      //call to get the status of the drain
    } else {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }

  if (mSubCmd == "clear") {
    if ((pVid->uid == 0)) {
      eos_notice("drain status");
      XrdOucString status;

      if (!gOFS->DrainerEngine->ClearFSDrain(*pOpaque, stdErr)) {
        retc = errno;
      } else {
        stdOut += status;
      }

      //call to get the status of the drain
    } else {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
