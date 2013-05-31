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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Debug ()
{
 if (pVid->uid == 0)
 {
   XrdOucString debugnode = pOpaque->Get("mgm.nodename");
   XrdOucString debuglevel = pOpaque->Get("mgm.debuglevel");
   XrdOucString filterlist = pOpaque->Get("mgm.filter");

   XrdMqMessage message("debug");
   int envlen;
   XrdOucString body = pOpaque->Env(envlen);
   message.SetBody(body.c_str());
   // filter out several *'s ...
   int nstars = 0;
   int npos = 0;
   while ((npos = debugnode.find("*", npos)) != STR_NPOS)
   {
     npos++;
     nstars++;
   }
   if (nstars > 1)
   {
     stdErr = "error: debug level node can only contain one wildcard character (*) !";
     retc = EINVAL;
   }
   else
   {
     if ((debugnode == "*") || (debugnode == "") || (debugnode == gOFS->MgmOfsQueue))
     {
       // this is for us!
       int debugval = eos::common::Logging::GetPriorityByString(debuglevel.c_str());
       if (debugval < 0)
       {
         stdErr = "error: debug level ";
         stdErr += debuglevel;
         stdErr += " is not known!";
         retc = EINVAL;
       }
       else
       {
         eos::common::Logging::SetLogPriority(debugval);
         stdOut = "success: debug level is now <";
         stdOut += debuglevel.c_str();
         stdOut += ">";
         eos_notice("setting debug level to <%s>", debuglevel.c_str());
         if (filterlist.length())
         {
           eos::common::Logging::SetFilter(filterlist.c_str());
           stdOut += " filter=";
           stdOut += filterlist;
           eos_notice("setting message logid filter to <%s>", filterlist.c_str());
         }
         if (debuglevel == "debug" && 
	     (
	      ((eos::common::Logging::gFilter.find("PASS:") == STR_NPOS) && (eos::common::Logging::gFilter.find("SharedHash") == STR_NPOS)) ||
	      ((eos::common::Logging::gFilter.find("PASS:") != STR_NPOS) && (eos::common::Logging::gFilter.find("SharedHash") != STR_NPOS))
	     )
	    )
         {
           gOFS->ObjectManager.SetDebug(true);
         }
         else
         {
           gOFS->ObjectManager.SetDebug(false);
         }
       }
     }
     if (debugnode == "*")
     {
       debugnode = "/eos/*/fst";
       if (!Messaging::gMessageClient.SendMessage(message, debugnode.c_str()))
       {
         stdErr = "error: could not send debug level to nodes mgm.nodename=";
         stdErr += debugnode;
         stdErr += "\n";
         retc = EINVAL;
       }
       else
       {
         stdOut = "success: switched to mgm.debuglevel=";
         stdOut += debuglevel;
         stdOut += " on nodes mgm.nodename=";
         stdOut += debugnode;
         stdOut += "\n";
         eos_notice("forwarding debug level <%s> to nodes mgm.nodename=%s", debuglevel.c_str(), debugnode.c_str());
       }
       debugnode = "/eos/*/mgm";
       if (!Messaging::gMessageClient.SendMessage(message, debugnode.c_str()))
       {
         stdErr += "error: could not send debug level to nodes mgm.nodename=";
         stdErr += debugnode;
         retc = EINVAL;
       }
       else
       {
         stdOut += "success: switched to mgm.debuglevel=";
         stdOut += debuglevel;
         stdOut += " on nodes mgm.nodename=";
         stdOut += debugnode;
         eos_notice("forwarding debug level <%s> to nodes mgm.nodename=%s", debuglevel.c_str(), debugnode.c_str());
       }
     }
     else
     {
       if (debugnode != "")
       {
         // send to the specified list
         if (!Messaging::gMessageClient.SendMessage(message, debugnode.c_str()))
         {
           stdErr = "error: could not send debug level to nodes mgm.nodename=";
           stdErr += debugnode;
           retc = EINVAL;
         }
         else
         {
           stdOut = "success: switched to mgm.debuglevel=";
           stdOut += debuglevel;
           stdOut += " on nodes mgm.nodename=";
           stdOut += debugnode;
           eos_notice("forwarding debug level <%s> to nodes mgm.nodename=%s", debuglevel.c_str(), debugnode.c_str());
         }
       }
     }
   }
   //      stdOut+="\n==== debug done ====";
 }
 else
 {
   retc = EPERM;
   stdErr = "error: you have to take role 'root' to execute this command";
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
