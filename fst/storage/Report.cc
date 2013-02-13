// ----------------------------------------------------------------------
// File: Report.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::Report ()
{
 // this thread send's report messages from the report queue
 bool failure;

 XrdOucString monitorReceiver = Config::gConfig.FstDefaultReceiverQueue;
 monitorReceiver.replace("*/mgm", "*/report");

 while (1)
 {
   failure = false;

   gOFS.ReportQueueMutex.Lock();
   while (gOFS.ReportQueue.size() > 0)
   {
     gOFS.ReportQueueMutex.UnLock();

     gOFS.ReportQueueMutex.Lock();
     // send all reports away and dump them into the log
     XrdOucString report = gOFS.ReportQueue.front();
     gOFS.ReportQueueMutex.UnLock();
     eos_static_info("%s", report.c_str());

     // this type of messages can have no receiver
     XrdMqMessage message("report");
     message.MarkAsMonitor();

     XrdOucString msgbody;
     message.SetBody(report.c_str());

     eos_debug("broadcasting report message: %s", msgbody.c_str());


     if (!XrdMqMessaging::gMessageClient.SendMessage(message, monitorReceiver.c_str()))
     {
       // display communication error
       eos_err("cannot send report broadcast");
       failure = true;
       gOFS.ReportQueueMutex.Lock();
       break;
     }
     gOFS.ReportQueueMutex.Lock();
     gOFS.ReportQueue.pop();
   }
   gOFS.ReportQueueMutex.UnLock();

   if (failure)
   {
     XrdSysTimer sleeper;
     sleeper.Snooze(10);
   }
   else
   {
     XrdSysTimer sleeper;
     sleeper.Snooze(1);
   }
 }
}

EOSFSTNAMESPACE_END


