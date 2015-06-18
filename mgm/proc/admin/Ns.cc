// ----------------------------------------------------------------------
// File: proc/admin/Ns.cc
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
#include "mgm/Quota.hh"
#include "common/LinuxMemConsumption.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Ns ()
{
#ifdef EOS_INSTRUMENTED_RWMUTEX
 if (mSubCmd == "mutex")
 {
   if (pVid->uid == 0)
   {
     XrdOucString option = pOpaque->Get("mgm.option");
     bool toggletiming = false;
     bool toggleorder = false;
     bool smplrate1 = false;
     bool smplrate10 = false;
     bool smplrate100 = false;
     bool nooption = true;
     if ((option.find("t") != STR_NPOS))
       toggletiming = true;
     if ((option.find("o") != STR_NPOS))
       toggleorder = true;
     if ((option.find("1") != STR_NPOS))
       smplrate1 = true;
     if ((option.find("s") != STR_NPOS))
       smplrate10 = true;
     if ((option.find("f") != STR_NPOS))
       smplrate100 = true;
     if (smplrate1 || smplrate10 || smplrate100 || toggleorder || toggletiming) nooption = false;

     if (nooption)
     {
       stdOut += "# ------------------------------------------------------------------------------------\n";
       stdOut += "# Mutex Monitoring Management\n";
       stdOut += "# ------------------------------------------------------------------------------------\n";

       size_t cycleperiod = eos::common::RWMutex::GetLockUnlockDuration();

       stdOut += "order checking is : ";
       stdOut += (eos::common::RWMutex::GetOrderCheckingGlobal() ? "on " : "off");
       stdOut += " (estimated order checking latency for 1 rule ";
       size_t orderlatency = eos::common::RWMutex::GetOrderCheckingLatency();
       stdOut += (int) orderlatency;
       stdOut += " nsec / ";
       stdOut += int(double(orderlatency) / cycleperiod * 100);
       stdOut += "% of the mutex lock/unlock cycle duration)\n";

       stdOut += "timing         is : ";
       stdOut += (FsView::gFsView.ViewMutex.GetTiming() ? "on " : "off");
       stdOut += " (estimated timing latency for 1 lock ";
       size_t timinglatency = eos::common::RWMutex::GetTimingLatency();
       stdOut += (int) timinglatency;
       stdOut += " nsec / ";
       stdOut += int(double(timinglatency) / cycleperiod * 100);
       stdOut += "% of the mutex lock/unlock cycle duration)\n";

       stdOut += "sampling rate  is : ";
       float sr = FsView::gFsView.ViewMutex.GetSampling();
       char ssr[32];
       sprintf(ssr, "%f", sr);
       stdOut += (sr < 0 ? "NA" : ssr);
       if (sr > 0)
       {
         stdOut += " (estimated average timing latency ";
         stdOut += int(double(timinglatency) * sr);
         stdOut += " nsec / ";
         stdOut += int((timinglatency * sr) / cycleperiod * 100);
         stdOut += "% of the mutex lock/unlock cycle duration)";
       }
       stdOut += "\n";
     }
     if (toggletiming)
     {
       if (FsView::gFsView.ViewMutex.GetTiming())
       {
         FsView::gFsView.ViewMutex.SetTiming(false);
         Quota::gQuotaMutex.SetTiming(false);
         gOFS->eosViewRWMutex.SetTiming(false);
         stdOut += "mutex timing is off\n";
       }
       else
       {
         FsView::gFsView.ViewMutex.SetTiming(true);
         Quota::gQuotaMutex.SetTiming(true);
         gOFS->eosViewRWMutex.SetTiming(true);
         stdOut += "mutex timing is on\n";
       }
     }
     if (toggleorder)
     {
       if (eos::common::RWMutex::GetOrderCheckingGlobal())
       {
         eos::common::RWMutex::SetOrderCheckingGlobal(false);
         stdOut += "mutex order checking is off\n";
       }
       else
       {
         eos::common::RWMutex::SetOrderCheckingGlobal(true);
         stdOut += "mutex order checking is on\n";
       }
     }
     if (smplrate1 || smplrate10 || smplrate100)
     {
       float rate = 0.0;
       if (smplrate1) rate = 0.01;
       if (smplrate10) rate = 0.1;
       if (smplrate100) rate = 1.0;
       FsView::gFsView.ViewMutex.SetSampling(true, rate);
       Quota::gQuotaMutex.SetSampling(true, rate);
       gOFS->eosViewRWMutex.SetSampling(true, rate);
     }

   }
   else
   {
     retc = EPERM;
     stdErr = "error: you have to take role 'root' to execute this command";
   }
 }
#endif
 if ((mSubCmd != "mutex") && (mSubCmd != "compact"))
 {
   XrdOucString option = pOpaque->Get("mgm.option");
   bool details = false;
   bool monitoring = false;
   bool numerical = false;
   if ((option.find("a") != STR_NPOS))
     details = true;
   if ((option.find("m") != STR_NPOS))
     monitoring = true;
   if ((option.find("n") != STR_NPOS))
     numerical = true;

   eos_info("ns stat");
   unsigned long long f = (unsigned long long) gOFS->eosFileService->getNumFiles();
   unsigned long long d = (unsigned long long) gOFS->eosDirectoryService->getNumContainers();
   char files[1024];
   sprintf(files, "%llu", f);
   char dirs[1024];
   sprintf(dirs, "%llu", d);

   // stat the size of the changelog files
   struct stat statf;
   struct stat statd;
   memset(&statf, 0, sizeof (struct stat));
   memset(&statd, 0, sizeof (struct stat));
   XrdOucString clfsize;
   XrdOucString cldsize;
   XrdOucString clfratio;
   XrdOucString cldratio;
   XrdOucString sizestring;

   // statistic for the changelog files
   if ((!::stat(gOFS->MgmNsFileChangeLogFile.c_str(), &statf)) && (!::stat(gOFS->MgmNsDirChangeLogFile.c_str(), &statd)))
   {
     eos::common::StringConversion::GetReadableSizeString(clfsize, (unsigned long long) statf.st_size, "B");
     eos::common::StringConversion::GetReadableSizeString(cldsize, (unsigned long long) statd.st_size, "B");
     eos::common::StringConversion::GetReadableSizeString(clfratio, (unsigned long long) f ? (1.0 * statf.st_size) / f : 0, "B");
     eos::common::StringConversion::GetReadableSizeString(cldratio, (unsigned long long) d ? (1.0 * statd.st_size) / d : 0, "B");
   }

   // statistic for the memory usage
   eos::common::LinuxMemConsumption::linux_mem_t mem;

   if (!eos::common::LinuxMemConsumption::GetMemoryFootprint(mem))
   {
     stdErr += "failed to get the memory usage information\n";
   }

   eos::common::LinuxStat::linux_stat_t pstat;

   if (!eos::common::LinuxStat::GetStat(pstat))
   {
     stdErr += "failed to get the process stat information\n";
   }

   XrdOucString bootstring;
   time_t boottime;

   {
     XrdSysMutexHelper lock(gOFS->InitializationMutex);
     bootstring = gOFS->gNameSpaceState[gOFS->Initialized];
     boottime = 0;
     if (bootstring == "booting")
     {
       boottime = (time(NULL) - gOFS->InitializationTime);
     }
     else
     {
       boottime = gOFS->InitializationTime;
     }
   }

   double avg = 0;
   double sigma = 0;

   // TODO: Lukasz has removed this from the class 
   //      if (!gOFS->MgmMaster.IsMaster()) {
   //gOFS->eosFileService->getLatency(avg, sigma);
   //      }

   if (!monitoring)
   {
     stdOut += "# ------------------------------------------------------------------------------------\n";
     stdOut += "# Namespace Statistic\n";
     stdOut += "# ------------------------------------------------------------------------------------\n";

     stdOut += "ALL      Files                            ";
     stdOut += files;
     stdOut += " [";
     stdOut += bootstring;
     stdOut += "] (";
     stdOut += (int) boottime;
     stdOut += "s)";
     stdOut += "\n";

     stdOut += "ALL      Directories                      ";
     stdOut += dirs;
     stdOut += "\n";
     stdOut += "# ....................................................................................\n";
     stdOut += "ALL      Compactification                 ";
     gOFS->MgmMaster.PrintOutCompacting(stdOut);
     stdOut += "\n";
     stdOut += "# ....................................................................................\n";
     stdOut += "ALL      Replication                      ";
     gOFS->MgmMaster.PrintOut(stdOut);
     stdOut += "\n";
     if (!gOFS->MgmMaster.IsMaster())
     {
       char slatency[1024];
       snprintf(slatency, sizeof (slatency) - 1, "%.02f += %.02f ms", avg, sigma);
       stdOut += "ALL      Namespace Latency                ";
       stdOut += slatency;
       stdOut += "\n";
     }
     stdOut += "# ....................................................................................\n";
     stdOut += "ALL      File Changelog Size              ";
     stdOut += clfsize;
     stdOut += "\n";
     stdOut += "ALL      Dir  Changelog Size              ";
     stdOut += cldsize;
     stdOut += "\n";
     stdOut += "# ....................................................................................\n";
     stdOut += "ALL      avg. File Entry Size             ";
     stdOut += clfratio;
     stdOut += "\n";
     stdOut += "ALL      avg. Dir  Entry Size             ";
     stdOut += cldratio;
     stdOut += "\n";
     stdOut += "# ------------------------------------------------------------------------------------\n";
     stdOut += "ALL      memory virtual                   ";
     stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring, (unsigned long long) mem.vmsize, "B");
     stdOut += "\n";
     stdOut += "ALL      memory resident                  ";
     stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring, (unsigned long long) mem.resident, "B");
     stdOut += "\n";
     stdOut += "ALL      memory share                     ";
     stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring, (unsigned long long) mem.share, "B");
     stdOut += "\n";
     if (pstat.vsize > gOFS->LinuxStatsStartup.vsize)
     {
       stdOut += "ALL      memory growths                   ";
       stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring, (unsigned long long) (pstat.vsize - gOFS->LinuxStatsStartup.vsize), "B");
       stdOut += "\n";
     }
     else
     {
       stdOut += "ALL      memory growths                  -";
       stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring, (unsigned long long) (-pstat.vsize + gOFS->LinuxStatsStartup.vsize), "B");
       stdOut += "\n";
     }
     stdOut += "ALL      threads                          ";
     stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) pstat.threads);
     stdOut += "\n";
     stdOut += "ALL      uptime                           ";
     stdOut += (int)(time(NULL)-gOFS->StartTime);
     stdOut += "\n";

     stdOut += "# ------------------------------------------------------------------------------------\n";
   }
   else
   {
     stdOut += "uid=all gid=all ns.total.files=";
     stdOut += files;
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.total.directories=";
     stdOut += dirs;
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.total.files.changelog.size=";
     stdOut += eos::common::StringConversion::GetSizeString(clfsize, (unsigned long long) statf.st_size);
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.total.directories.changelog.size=";
     stdOut += eos::common::StringConversion::GetSizeString(cldsize, (unsigned long long) statd.st_size);
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.total.files.changelog.avg_entry_size=";
     stdOut += eos::common::StringConversion::GetSizeString(clfratio, (unsigned long long) f ? (1.0 * statf.st_size) / f : 0);
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.total.directories.changelog.avg_entry_size=";
     stdOut += eos::common::StringConversion::GetSizeString(cldratio, (unsigned long long) d ? (1.0 * statd.st_size) / d : 0);
     stdOut += "\n";
     stdOut += "uid=all gid=all ";
     gOFS->MgmMaster.PrintOutCompacting(stdOut);
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.boot.status=";
     stdOut += bootstring;
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.boot.time=";
     stdOut += (int) boottime;
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.latency.avg=";
     char savg[1024];
     snprintf(savg, sizeof (savg) - 1, "%.02f", avg);
     stdOut += savg;
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.latency.sig=";
     char ssig[1024];
     snprintf(ssig, sizeof (ssig) - 1, "%.02f", sigma);
     stdOut += ssig;
     stdOut += "\n";
     stdOut += "uid=all gid=all ";
     gOFS->MgmMaster.PrintOut(stdOut);
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.memory.virtual=";
     stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mem.vmsize);
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.memory.resident=";
     stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mem.resident);
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.memory.share=";
     stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mem.share);
     stdOut += "\n";
     stdOut += "uid=all gid=all ns.stat.threads=";
     stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) pstat.threads);
     stdOut += "\n";
     if (pstat.vsize > gOFS->LinuxStatsStartup.vsize)
     {
       stdOut += "uid=all gid=all ns.memory.growth=";
       stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) (pstat.vsize - gOFS->LinuxStatsStartup.vsize));
       stdOut += "\n";
     }
     else
     {
       stdOut += "uid=all gid=all ns.memory.growth=-";
       stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) (-pstat.vsize + gOFS->LinuxStatsStartup.vsize));
       stdOut += "\n";
     }
     stdOut += "uid=all gid=all ns.uptime=";
     stdOut += (int)(time(NULL)-gOFS->StartTime);
     stdOut += "\n";
   }

   if (mSubCmd == "stat")
   {
     if (option.find("r") != STR_NPOS)
     {
       gOFS->MgmStats.Clear();
       stdOut += "success: all counters have been reset";
     }
     gOFS->MgmStats.PrintOutTotal(stdOut, details, monitoring, numerical);
   }

   if (mSubCmd == "master")
   {
     XrdOucString masterhost = pOpaque->Get("mgm.master");

     if (masterhost == "--disable")
     {
       // just disable the master heart beat thread to do remote checks
       if (!gOFS->MgmMaster.DisableRemoteCheck())
       {
         stdErr += "warning: master heartbeat was already disabled!\n";
         retc = EINVAL;
       }
       else
       {
         stdOut += "success: disabled master heartbeat check\n";
       }
       mDoSort = false;
       return SFS_OK;
     }

     if (masterhost == "--enable")
     {
       // just enable the master heart beat thread to do remote checks
       if (!gOFS->MgmMaster.EnableRemoteCheck())
       {
         stdErr += "warning: master heartbeat was already enabled!\n";
         retc = EINVAL;
       }
       else
       {
         stdOut += "success: enabled master heartbeat check\n";
       }
       mDoSort = false;
       return SFS_OK;
     }

     if ((masterhost == "--log") || (!masterhost.length()))
     {
       gOFS->MgmMaster.GetLog(stdOut);
       mDoSort = false;
       return SFS_OK;
     }

     if ((masterhost == "--log-clear"))
     {
       gOFS->MgmMaster.ResetLog();
       stdOut += "success: cleaned the master log";
       mDoSort = false;
       return SFS_OK;
     }

     if (!gOFS->MgmMaster.Set(masterhost, stdOut, stdErr))
     {
       retc = EIO;
     }
     else
     {
       stdOut += "success: <";
       stdOut += gOFS->MgmMaster.GetMasterHost();
       stdOut += "> is now the master\n";
     }
     mDoSort = false;
     return SFS_OK;
   }
 }

 if (mSubCmd == "compact")
 {
   if (pVid->uid == 0)
   {
     XrdOucString action = pOpaque->Get("mgm.ns.compact");
     XrdOucString delay = pOpaque->Get("mgm.ns.compact.delay");
     XrdOucString interval = pOpaque->Get("mgm.ns.compact.interval");
     XrdOucString type = pOpaque->Get("mgm.ns.compact.type");

     if ((!action.length()))
     {
       retc = EINVAL;
       stdErr += "error: invalid arguments specified\n";
     }
     else
     {
       if ((action != "on") && (action != "off"))
       {
         retc = EINVAL;
         stdErr += "error: invalid arguments specified\n";
       }
       else
       {
         if (action == "on")
         {
	   if ( ( type != "files") &&
		( type != "directories") &&
		( type != "all") &&
		( type != "files") &&
		( type != "directories") &&
		( type != "all") )
           {
	     retc = EINVAL;
	     stdErr += "error: invalid arguments specified - type must be 'files','files-repair','directories','directories-repair' or 'all','all-repair'\n";
	   } 
	   else 
	   {
	     if (!delay.length()) delay = "0";
	     if (!interval.length()) interval = "0";
	     gOFS->MgmMaster.ScheduleOnlineCompacting((time(NULL) + atoi(delay.c_str())), atoi(interval.c_str()));
	     if ( type == "files" )
	       gOFS->MgmMaster.SetCompactingType(true, false, false);
	     else if ( type == "directories" ) 
	       gOFS->MgmMaster.SetCompactingType(false, true, false);
	     else if ( type == "all" ) 
	       gOFS->MgmMaster.SetCompactingType(true, true, false);
	     else if ( type == "files-repair" )
	       gOFS->MgmMaster.SetCompactingType(true, false, true);
	     else if ( type == "directories-repair" ) 
	       gOFS->MgmMaster.SetCompactingType(false, true, true);
	     else if ( type == "all-repair" ) 
	       gOFS->MgmMaster.SetCompactingType(true, true, true);

	     stdOut += "success: configured online compacting to run in ";
	     stdOut += delay.c_str();
	     stdOut += " seconds from now ( might be delayed upto 60 seconds )";
	     if (interval != "0")
	     {
	       stdOut += " (re-compact every ";
	       stdOut += interval;
	       stdOut += " seconds)\n";
	     }
	     else
	     {
	       stdOut += "\n";
	     }
	   }
         }
         if (action == "off")
         {
           gOFS->MgmMaster.ScheduleOnlineCompacting(0, 0);
           stdOut += "success: disabled online compacting\n";
         }
       }
     }
     //-------------------------------------------
   }
   else
   {
     retc = EPERM;
     stdErr = "error: you have to take role 'root' to execute this command";
   }
   mDoSort = false;
 }
 return SFS_OK;
}

EOSMGMNAMESPACE_END
