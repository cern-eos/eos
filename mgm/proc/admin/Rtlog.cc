// ----------------------------------------------------------------------
// File: proc/admin/Rtlog.cc
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
ProcCommand::Rtlog()
{
  if (pVid->uid) {
    retc = EPERM;
    stdErr = "error: you have to take role 'root' to execute this command";
    return SFS_OK;
  }

  mDoSort = 1;
  // this is just to identify a new queue for reach request
  static int bccount = 0;
  bccount++;
  XrdOucString queue = pOpaque->Get("mgm.rtlog.queue");
  XrdOucString lines = pOpaque->Get("mgm.rtlog.lines");
  XrdOucString tag = pOpaque->Get("mgm.rtlog.tag");
  XrdOucString filter = pOpaque->Get("mgm.rtlog.filter");
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

  if (!filter.length()) {
    filter = " ";
  }

  if ((!queue.length()) || (!lines.length()) || (!tag.length())) {
    stdErr = "error: mgm.rtlog.queue, mgm.rtlog.lines, mgm.rtlog.tag have to be given as input paramters!";
    retc = EINVAL;
    return SFS_OK;
  }

  if ((g_logging.GetPriorityByString(tag.c_str())) == -1) {
    stdErr = "error: mgm.rtlog.tag must be info, debug, err, emerg, alert, crit, warning or notice";
    retc = EINVAL;
    return SFS_OK;
  }

  // Grab the logs from the current MGM
  if ((queue == ".") || (queue == "*") || (queue == gOFS->MgmOfsQueue)) {
    int logtagindex = g_logging.GetPriorityByString(tag.c_str());

    for (int j = 0; j <= logtagindex; j++) {
      g_logging.gMutex.Lock();

      for (int i = 1; i <= atoi(lines.c_str()); i++) {
        XrdOucString logline = g_logging.gLogMemory[j][(g_logging.gLogCircularIndex[j] -
                               i + g_logging.gCircularIndexSize) % g_logging.gCircularIndexSize].c_str();

        if (logline.length() && ((logline.find(filter.c_str())) != STR_NPOS)) {
          stdOut += logline;
          stdOut += "\n";
        }

        if (!logline.length()) {
          break;
        }
      }

      g_logging.gMutex.UnLock();
    }
  }

  // Grab the logs from the FSTs
  if ((queue == "*") || ((queue != gOFS->MgmOfsQueue) && (queue != "."))) {
    std::set<std::string> endpoints = FsView::gFsView.CollectEndpoints(
                                        queue.c_str());

    if (endpoints.empty()) {
      eos_static_err("msg=\"no matching endpoints\" queue=\"%s\"", queue.c_str());
      stdErr = "error: not matching endpoints for given queue";
      retc = EINVAL;
    } else {
      std::ostringstream oss;
      oss << "/?fst.pcmd=rtlog"
          << "&mgm.rtlog.lines=" << lines
          << "&mgm.rtlog.tag=" << tag;

      if (filter != " ") {
        oss << "&mgm.rtlog.filter=" << filter;
      }

      std::string request = oss.str();
      std::map<std::string, std::pair<int, std::string>> responses;
      int query_retc = gOFS->BroadcastQuery(request, endpoints, responses, 10);

      if (query_retc == 0) {
        for (const auto& resp : responses) {
          stdOut += resp.second.second.c_str();
        }
      } else {
        // Fallback to old method if we got any error
        XrdOucString broadcastresponsequeue = gOFS->MgmOfsBrokerUrl;
        broadcastresponsequeue += "-rtlog-";
        broadcastresponsequeue += bccount;
        XrdOucString broadcasttargetqueue = gOFS->MgmDefaultReceiverQueue;

        if (queue != "*") {
          broadcasttargetqueue = queue;
        }

        int envlen;
        XrdOucString msgbody;
        msgbody = pOpaque->Env(envlen);

        if (!gOFS->MgmOfsMessaging->BroadCastAndCollect(broadcastresponsequeue,
            broadcasttargetqueue, msgbody, stdOut, 2)) {
          eos_err("failed to broad cast and collect rtlog from [%s]:[%s]",
                  broadcastresponsequeue.c_str(), broadcasttargetqueue.c_str());
          stdErr = "error: broadcast failed\n";
          retc = EFAULT;
        }
      }
    }
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
