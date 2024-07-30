// ----------------------------------------------------------------------
// File: ErrorReport.cc
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

#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/Config.hh"
#include "mq/MessagingRealm.hh"
#include <deque>
#include <string>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method sending error reports
//------------------------------------------------------------------------------
void
Storage::ErrorReport(ThreadAssistant& assistant) noexcept
{
  bool failure = false;
  XrdOucString errorReceiver = gConfig.FstDefaultReceiverQueue;
  errorReceiver.replace("*/mgm", "*/errorreport");
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  eos::common::Logging::LogCircularIndex localCircularIndex;
  localCircularIndex.resize(LOG_DEBUG + 1);
  std::deque<XrdOucString> ErrorReportQueue;
  eos_static_info("%s", "msg=\"starting error report thread\"");

  // initialize with the current positions of the circular index
  for (size_t i = LOG_EMERG; i <= LOG_DEBUG; i++) {
    localCircularIndex[i] = g_logging.gLogCircularIndex[i];
  }

  while (!assistant.terminationRequested()) {
    failure = false;

    // push messages from the circular buffers to the error queue
    for (size_t i = LOG_EMERG; i <= LOG_ERR; i++) {
      g_logging.gMutex.Lock();
      size_t endpos = g_logging.gLogCircularIndex[i];
      g_logging.gMutex.UnLock();

      if (endpos > localCircularIndex[i]) {
        // we have to follow the messages and add them to the queue
        for (unsigned long j = localCircularIndex[i]; j < endpos; j++) {
          // copy the messages to the queue
          g_logging.gMutex.Lock();
          ErrorReportQueue.push_back(g_logging.gLogMemory[i][j %
                                     g_logging.gCircularIndexSize]);
          g_logging.gMutex.UnLock();
        }

        localCircularIndex[i] = endpos;
      }
    }

    while (ErrorReportQueue.size() > 0) {
      // send all reports away and dump them into the log
      XrdOucString report = ErrorReportQueue.front().c_str();
      std::string truncationmessage;
      eos_debug("broadcasting errorreport message: %s", report.c_str());

      if (ErrorReportQueue.size() > 5) {
        // don't keep long error queues, send a suppression marker and clean the queue, the errors are anyway in the local log files
        truncationmessage = " ... [ ErrorReport ] suppressing " + std::to_string(
                              ErrorReportQueue.size() - 1) + " error messages!";
        ErrorReportQueue.clear();
      }

      // evt. exclude some messages from upstream reporting if the contain [NB]
      if (report.find("[NB]") == STR_NPOS) {
        report += truncationmessage.c_str();
        mq::MessagingRealm::Response response =
          gOFS.mMessagingRealm->sendMessage("errorreport", report.c_str(),
                                            errorReceiver.c_str(), true);

        if (!response.ok()) {
          // display communication error
          eos_err("%s", "msg=\"cannot send errorreport broadcast\"");
          failure = true;
          break;
        }
      }

      if (ErrorReportQueue.size()) {
        ErrorReportQueue.pop_front();
      }
    }

    if (failure) {
      assistant.wait_for(std::chrono::seconds(10));
    } else {
      assistant.wait_for(std::chrono::seconds(1));
    }
  }

  eos_static_info("%s", "msg=\"stopped error report thread\"");
}

EOSFSTNAMESPACE_END


