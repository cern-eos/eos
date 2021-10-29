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

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::ErrorReport()
{
  // this thread send's error report messages from the error queue
  bool failure;
  XrdOucString errorReceiver = gConfig.FstDefaultReceiverQueue;
  errorReceiver.replace("*/mgm", "*/errorreport");
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  eos::common::Logging::LogCircularIndex localCircularIndex;
  localCircularIndex.resize(LOG_DEBUG + 1);

  // initialize with the current positions of the circular index
  for (size_t i = LOG_EMERG; i <= LOG_DEBUG; i++) {
    localCircularIndex[i] = g_logging.gLogCircularIndex[i];
  }

  while (1) {
    failure = false;

    // push messages from the circular buffers to the error queue
    for (size_t i = LOG_EMERG; i <= LOG_ERR; i++) {
      g_logging.gMutex.Lock();
      size_t endpos = g_logging.gLogCircularIndex[i];
      g_logging.gMutex.UnLock();

      if (endpos > localCircularIndex[i]) {
        // we have to follow the messages and add them to the queue
        gOFS.ErrorReportQueueMutex.Lock();

        for (unsigned long j = localCircularIndex[i]; j < endpos; j++) {
          // copy the messages to the queue
          g_logging.gMutex.Lock();
          gOFS.ErrorReportQueue.push(g_logging.gLogMemory[i][j %
                                     g_logging.gCircularIndexSize]);
          g_logging.gMutex.UnLock();
        }

        localCircularIndex[i] = endpos;
        gOFS.ErrorReportQueueMutex.UnLock();
      }
    }

    gOFS.ErrorReportQueueMutex.Lock();

    while (gOFS.ErrorReportQueue.size() > 0) {
      gOFS.ErrorReportQueueMutex.UnLock();
      gOFS.ErrorReportQueueMutex.Lock();
      // send all reports away and dump them into the log
      XrdOucString report = gOFS.ErrorReportQueue.front().c_str();
      gOFS.ErrorReportQueueMutex.UnLock();
      eos_debug("broadcasting errorreport message: %s", report.c_str());

      // evt. exclude some messages from upstream reporting if the contain [NB]
      if (report.find("[NB]") == STR_NPOS) {
        mq::MessagingRealm::Response response =
          gOFS.mMessagingRealm->sendMessage("errorreport", report.c_str(),
                                            errorReceiver.c_str(), true);

        if (!response.ok()) {
          // display communication error
          eos_err("%s", "msg=\"cannot send errorreport broadcast\"");
          failure = true;
          gOFS.ErrorReportQueueMutex.Lock();
          break;
        }
      }

      gOFS.ErrorReportQueueMutex.Lock();
      gOFS.ErrorReportQueue.pop();
    }

    gOFS.ErrorReportQueueMutex.UnLock();

    if (failure) {
      std::this_thread::sleep_for(std::chrono::seconds(10));
    } else {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

EOSFSTNAMESPACE_END


