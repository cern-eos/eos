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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Config ()
{
 if (subcmd == "ls")
 {
   eos_notice("config ls");
   XrdOucString listing = "";
   bool showbackup = (bool)opaque->Get("mgm.config.showbackup");

   if (!(gOFS->ConfEngine->ListConfigs(listing, showbackup)))
   {
     stdErr += "error: listing of existing configs failed!";
     retc = errno;
   }
   else
   {
     stdOut += listing;
   }
 }

 if (subcmd == "autosave")
 {
   eos_notice("config autosave");
   XrdOucString onoff = opaque->Get("mgm.config.state") ? opaque->Get("mgm.config.state") : "";
   if (!onoff.length())
   {
     if (gOFS->ConfEngine->GetAutoSave())
     {
       stdOut += "<autosave> is enabled\n";
       retc = 0;
     }
     else
     {
       stdOut += "<autosave> is disabled\n";
       retc = 0;
     }
   }
   else
   {
     if ((onoff != "on") && (onoff != "off"))
     {
       stdErr += "error: state must be either 'on' or 'off' or empty to read the current setting!\n";
       retc = EINVAL;
     }
     else
     {
       if (onoff == "on")
       {
         gOFS->ConfEngine->SetAutoSave(true);
       }
       else
       {
         gOFS->ConfEngine->SetAutoSave(false);
       }
     }
   }
 }

 int envlen;
 if (subcmd == "load")
 {
   if (pVid->uid == 0)
   {
     eos_notice("config load: %s", opaque->Env(envlen));
     if (!gOFS->ConfEngine->LoadConfig(*opaque, stdErr))
     {
       retc = errno;
     }
     else
     {
       stdOut = "success: configuration successfully loaded!";
     }
   }
   else
   {
     retc = EPERM;
     stdErr = "error: you have to take role 'root' to execute this command";
   }
 }

 if (subcmd == "save")
 {
   eos_notice("config save: %s", opaque->Env(envlen));
   if (pVid->uid == 0)
   {
     if (!gOFS->ConfEngine->SaveConfig(*opaque, stdErr))
     {
       retc = errno;
     }
     else
     {
       stdOut = "success: configuration successfully saved!";
     }
   }
   else
   {
     retc = EPERM;
     stdErr = "error: you have to take role 'root' to execute this command";
   }
 }

 if (subcmd == "reset")
 {
   eos_notice("config reset");
   if (pVid->uid == 0)
   {
     gOFS->ConfEngine->ResetConfig();
     stdOut = "success: configuration has been reset(cleaned)!";
   }
   else
   {
     retc = EPERM;
     stdErr = "error: you have to take role 'root' to execute this command";
   }
 }

 if (subcmd == "dump")
 {
   eos_notice("config dump");
   XrdOucString dump = "";
   if (!gOFS->ConfEngine->DumpConfig(dump, *opaque))
   {
     stdErr += "error: listing of existing configs failed!";
     retc = errno;
   }
   else
   {
     stdOut += dump;
     dosort = true;
   }
 }

 if (subcmd == "diff")
 {
   eos_notice("config diff");
   gOFS->ConfEngine->Diffs(stdOut);
 }

 if (subcmd == "changelog")
 {
   int nlines = 5;
   char* val;
   if ((val = opaque->Get("mgm.config.lines")))
   {
     nlines = atoi(val);
     if (nlines < 1) nlines = 1;
   }
   gOFS->ConfEngine->GetChangeLog()->Tail(nlines, stdOut);
   eos_notice("config changelog");
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
