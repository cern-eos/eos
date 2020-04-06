// ----------------------------------------------------------------------
// File: proc/admin/Debug.cc
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
#include "mgm/Messaging.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Debug()
{
  if (pVid->uid == 0) {
    if (mSubCmd == "getloglevel") {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      stdOut += "# ------------------------------------------------------------------------------------\n";
      stdOut += "# Debug log level\n";
      stdOut += "# ....................................................................................\n";
      eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
      stdOut += (XrdOucString) gOFS->HostName + ":" + std::to_string(
                  gOFS->ManagerPort).c_str() + "/mgm := \t" + g_logging.GetPriorityString(
                  g_logging.gPriorityLevel) + '\n';
      auto nodes = FsView::gFsView.mNodeView;

      for (auto node = nodes.begin(); node != nodes.end(); ++node) {
        stdOut += (node->first.substr(5) + " := \t" +
                   FsView::gFsView.mNodeView[node->first]->GetConfigMember("debug.state") +
                   '\n').c_str();
      }
    } else {
      XrdOucString debugnode = pOpaque->Get("mgm.nodename");
      XrdOucString debuglevel = pOpaque->Get("mgm.debuglevel");
      XrdOucString filterlist = pOpaque->Get("mgm.filter");
      int envlen;
      XrdOucString body = pOpaque->Env(envlen);
      // filter out several *'s ...
      int nstars = 0;
      int npos = 0;
      eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

      while ((npos = debugnode.find("*", npos)) != STR_NPOS) {
        npos++;
        nstars++;
      }

      if (nstars > 1) {
        stdErr = "error: debug level node can only contain one wildcard character (*) !";
        retc = EINVAL;
      } else {
        // always check debug level exists first
        int debugval = g_logging.GetPriorityByString(debuglevel.c_str());

        if (debugval < 0) {
          stdErr = "error: debug level ";
          stdErr += debuglevel;
          stdErr += " is not known!";
          retc = EINVAL;
        } else {
          if ((debugnode == "*") || (debugnode == "") ||
              (debugnode == gOFS->MgmOfsQueue)) {
            // this is for us!
            int debugval = g_logging.GetPriorityByString(debuglevel.c_str());
            g_logging.SetLogPriority(debugval);
            stdOut = "success: debug level is now <";
            stdOut += debuglevel.c_str();
            stdOut += ">";
            eos_notice("setting debug level to <%s>", debuglevel.c_str());

            if (filterlist.length()) {
              g_logging.SetFilter(filterlist.c_str());
              stdOut += " filter=";
              stdOut += filterlist;
              eos_notice("setting message logid filter to <%s>", filterlist.c_str());
            }

            if (debuglevel == "debug" &&
                ((g_logging.gAllowFilter.Num() &&
                  g_logging.gAllowFilter.Find("SharedHash")) ||
                 ((g_logging.gDenyFilter.Num() == 0) ||
                  (g_logging.gDenyFilter.Find("SharedHash") == 0)))
               ) {
              gOFS->ObjectManager.SetDebug(true);
            } else {
              gOFS->ObjectManager.SetDebug(false);
            }
          }

          if (debugnode == "*") {
            debugnode = "/eos/*/fst";

            if (!gOFS->mMessagingRealm->sendMessage("debug", body.c_str(), debugnode.c_str()).ok()) {
              stdErr = "error: could not send debug level to nodes mgm.nodename=";
              stdErr += debugnode;
              stdErr += "\n";
              retc = EINVAL;
            } else {
              stdOut = "success: switched to mgm.debuglevel=";
              stdOut += debuglevel;
              stdOut += " on nodes mgm.nodename=";
              stdOut += debugnode;
              stdOut += "\n";
              eos_notice("forwarding debug level <%s> to nodes mgm.nodename=%s",
                         debuglevel.c_str(), debugnode.c_str());
            }

            debugnode = "/eos/*/mgm";
            // Ignore return value as we've already set the loglevel for the
            // current instance. We're doing this only for the slave.
            (void) gOFS->mMessagingRealm->sendMessage("debug", body.c_str(), debugnode.c_str());
            stdOut += "success: switched to mgm.debuglevel=";
            stdOut += debuglevel;
            stdOut += " on nodes mgm.nodename=";
            stdOut += debugnode;
            eos_notice("forwarding debug level <%s> to nodes mgm.nodename=%s",
                       debuglevel.c_str(), debugnode.c_str());
          } else {
            if (debugnode != "") {
              // send to the specified list
              if (!gOFS->mMessagingRealm->sendMessage("debug", body.c_str(), debugnode.c_str()).ok()) {
                stdErr = "error: could not send debug level to nodes mgm.nodename=";
                stdErr += debugnode;
                retc = EINVAL;
              } else {
                stdOut = "success: switched to mgm.debuglevel=";
                stdOut += debuglevel;
                stdOut += " on nodes mgm.nodename=";
                stdOut += debugnode;
                eos_notice("forwarding debug level <%s> to nodes mgm.nodename=%s",
                           debuglevel.c_str(), debugnode.c_str());
              }
            }
          }
        }
      }
    }
  } else {
    retc = EPERM;
    stdErr = "error: you have to take role 'root' to execute this command";
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
