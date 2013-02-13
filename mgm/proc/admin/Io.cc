// ----------------------------------------------------------------------
// File: proc/admin/Io.cc
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
ProcCommand::Io ()
{
 if (pVid->uid == 0)
 {
   if (mSubCmd == "report")
   {
     XrdOucString path = pOpaque->Get("mgm.io.path");
     retc = Iostat::NamespaceReport(path.c_str(), stdOut, stdErr);
   }
   else
   {
     XrdOucString option = pOpaque->Get("mgm.option");
     XrdOucString target = pOpaque->Get("mgm.udptarget");
     bool reports = false;
     bool reportnamespace = false;
     bool popularity = false;

     if ((option.find("r") != STR_NPOS))
       reports = true;

     if ((option.find("n") != STR_NPOS))
       reportnamespace = true;

     if ((option.find("p") != STR_NPOS))
       popularity = true;

     if ((!reports) && (!reportnamespace))
     {
       if (mSubCmd == "enable")
       {
         if (target.length())
         {
           if (gOFS->IoStats.AddUdpTarget(target.c_str()))
           {
             stdOut += "success: enabled IO udp target ";
             stdOut += target.c_str();
           }
           else
           {
             stdErr += "error: IO udp target was not configured ";
             stdErr += target.c_str();
             retc = EINVAL;
           }
         }
         else
         {
           if (popularity)
           {
             gOFS->IoStats.Start(); // always enable collection otherwise we don't get anything for popularity reporting
             if (gOFS->IoStats.StartPopularity())
             {
               stdOut += "success: enabled IO popularity collection";
             }
             else
             {
               stdErr += "error: IO popularity collection already enabled";
               ;
               retc = EINVAL;
             }
           }
           else
           {
             if (gOFS->IoStats.StartCollection())
             {
               stdOut += "success: enabled IO report collection";
             }
             else
             {
               stdErr += "error: IO report collection already enabled";
               ;
               retc = EINVAL;
             }
           }
         }
       }
       if (mSubCmd == "disable")
       {
         if (target.length())
         {
           if (gOFS->IoStats.RemoveUdpTarget(target.c_str()))
           {
             stdOut += "success: disabled IO udp target ";
             stdOut += target.c_str();
           }
           else
           {
             stdErr += "error: IO udp target was not configured ";
             stdErr += target.c_str();
             retc = EINVAL;
           }
         }
         else
         {
           if (popularity)
           {
             if (gOFS->IoStats.StopPopularity())
             {
               stdOut += "success: disabled IO popularity collection";
             }
             else
             {
               stdErr += "error: IO popularity collection already disabled";
               ;
               retc = EINVAL;
             }
           }
           else
           {
             if (gOFS->IoStats.StopCollection())
             {
               stdOut += "success: disabled IO report collection";
             }
             else
             {
               stdErr += "error: IO report collection was already disabled";
               retc = EINVAL;
             }
           }
         }
       }
     }
     else
     {
       if (reports)
       {
         if (mSubCmd == "enable")
         {
           if (gOFS->IoStats.StartReport())
           {
             stdErr += "error: IO report store already enabled";
             retc = EINVAL;
           }
           else
           {
             stdOut += "success: enabled IO report store";
           }
         }
         if (mSubCmd == "disable")
         {
           if (!gOFS->IoStats.StopReport())
           {
             stdErr += "error: IO report store already disabled";
             retc = EINVAL;
           }
           else
           {
             stdOut += "success: disabled IO report store";
           }
         }
       }
       if (reportnamespace)
       {
         if (mSubCmd == "enable")
         {
           if (gOFS->IoStats.StartReportNamespace())
           {
             stdErr += "error: IO report namespace already enabled";
             retc = EINVAL;
           }
           else
           {
             stdOut += "success: enabled IO report namespace";
           }
         }
         if (mSubCmd == "disable")
         {
           if (!gOFS->IoStats.StopReportNamespace())
           {
             stdErr += "error: IO report namespace already disabled";
             ;
             retc = EINVAL;
           }
           else
           {
             stdOut += "success: disabled IO report namespace";
           }
         }
       }
     }
   }
 }

 if (mSubCmd == "stat")
 {
   XrdOucString option = pOpaque->Get("mgm.option");
   bool details = false;
   bool monitoring = false;
   bool numerical = false;
   bool top = false;
   bool domain = false;
   bool apps = false;
   bool summary = false;

   if ((option.find("a") != STR_NPOS))
     details = true;
   if ((option.find("m") != STR_NPOS))
     monitoring = true;
   if ((option.find("n") != STR_NPOS))
     numerical = true;
   if ((option.find("t") != STR_NPOS))
     top = true;
   if ((option.find("d") != STR_NPOS))
     domain = true;
   if ((option.find("x") != STR_NPOS))
     apps = true;
   if ((option.find("l") != STR_NPOS))
     summary = true;

   if (!(apps | domain | top | details))
   {
     // if nothing is selected, we show the summary information
     summary = true;
   }

   eos_info("io stat");

   gOFS->IoStats.PrintOut(stdOut, summary, details, monitoring, numerical, top, domain, apps, option);
 }

 if (mSubCmd == "ns")
 {
   XrdOucString option = pOpaque->Get("mgm.option");
   eos_info("io ns");

   gOFS->IoStats.PrintNs(stdOut, option);
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
