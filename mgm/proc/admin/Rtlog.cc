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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Rtlog ()
{
 if (pVid->uid == 0)
 {
   mDoSort = 1;
   // this is just to identify a new queue for reach request
   static int bccount = 0;
   bccount++;
   XrdOucString queue = pOpaque->Get("mgm.rtlog.queue");
   XrdOucString lines = pOpaque->Get("mgm.rtlog.lines");
   XrdOucString tag = pOpaque->Get("mgm.rtlog.tag");
   XrdOucString filter = pOpaque->Get("mgm.rtlog.filter");
   if (!filter.length()) filter = " ";
   if ((!queue.length()) || (!lines.length()) || (!tag.length()))
   {
     stdErr = "error: mgm.rtlog.queue, mgm.rtlog.lines, mgm.rtlog.tag have to be given as input paramters!";
     retc = EINVAL;
   }
   else
   {
     if ((eos::common::Logging::GetPriorityByString(tag.c_str())) == -1)
     {
       stdErr = "error: mgm.rtlog.tag must be info,debug,err,emerg,alert,crit,warning or notice";
       retc = EINVAL;
     }
     else
     {
       if ((queue == ".") || (queue == "*") || (queue == gOFS->MgmOfsQueue))
       {
         int logtagindex = eos::common::Logging::GetPriorityByString(tag.c_str());
         for (int j = 0; j <= logtagindex; j++)
         {
           eos::common::Logging::gMutex.Lock();
           for (int i = 1; i <= atoi(lines.c_str()); i++)
           {
             XrdOucString logline = eos::common::Logging::gLogMemory[j][(eos::common::Logging::gLogCircularIndex[j] - i + eos::common::Logging::gCircularIndexSize) % eos::common::Logging::gCircularIndexSize].c_str();
             if (logline.length() && ((logline.find(filter.c_str())) != STR_NPOS))
             {
               stdOut += logline;
               stdOut += "\n";
             }
             if (!logline.length())
               break;
           }
           eos::common::Logging::gMutex.UnLock();
         }
       }
       if ((queue == "*") || ((queue != gOFS->MgmOfsQueue) && (queue != ".")))
       {
         XrdOucString broadcastresponsequeue = gOFS->MgmOfsBrokerUrl;
         broadcastresponsequeue += "-rtlog-";
         broadcastresponsequeue += bccount;
         XrdOucString broadcasttargetqueue = gOFS->MgmDefaultReceiverQueue;
         if (queue != "*")
           broadcasttargetqueue = queue;

         int envlen;
         XrdOucString msgbody;
         msgbody = pOpaque->Env(envlen);

         if (!gOFS->MgmOfsMessaging->BroadCastAndCollect(broadcastresponsequeue, broadcasttargetqueue, msgbody, stdOut, 2))
         {
           eos_err("failed to broad cast and collect rtlog from [%s]:[%s]", broadcastresponsequeue.c_str(), broadcasttargetqueue.c_str());
           stdErr = "error: broadcast failed\n";
           retc = EFAULT;
         }
       }
     }
   }
 }
 else
 {
   retc = EPERM;
   stdErr = "error: you have to take role 'root' to execute this command";
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
