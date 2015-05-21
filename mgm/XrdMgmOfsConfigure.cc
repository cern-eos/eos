// ----------------------------------------------------------------------
// File: XrdMgmOfsConfigure.cc
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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <dirent.h>
#include <string.h>
/*----------------------------------------------------------------------------*/
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsTrace.hh"
#include "mgm/txengine/TransferEngine.hh"
#include "mgm/Quota.hh"
#include "mgm/Access.hh"
#include "mgm/Recycle.hh"
#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogFileMDSvc.hh"
#include "namespace/views/HierarchicalView.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdSys/XrdSysDNS.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysPlugin.hh"
/*----------------------------------------------------------------------------*/
extern XrdOucTrace gMgmOfsTrace;
extern void xrdmgmofs_shutdown (int sig);
extern void xrdmgmofs_stacktrace (int sig);

/*----------------------------------------------------------------------------*/

USE_EOSMGMNAMESPACE

/*----------------------------------------------------------------------------*/
void*
XrdMgmOfs::StaticInitializeFileView (void* arg)
{
  //----------------------------------------------------------------
  //! static thread startup function calling Drain
  //----------------------------------------------------------------
  return reinterpret_cast<XrdMgmOfs*> (arg)->InitializeFileView();
}

/*----------------------------------------------------------------------------*/
void*
XrdMgmOfs::InitializeFileView ()
{
  {
    XrdSysMutexHelper lock(InitializationMutex);
    Initialized = kBooting;
    InitializationTime = time(0);
    RemoveStallRuleAfterBoot = false;
  }
  time_t tstart = time(0);
  std::string oldstallrule = "";
  std::string oldstallcomment = "";
  bool oldstallglobal = false;
  // set the client stall
  {
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
    if (Access::gStallRules.count(std::string("*")))
    {
      if (!RemoveStallRuleAfterBoot)
      {
        oldstallrule = Access::gStallRules[std::string("*")];
        oldstallcomment = Access::gStallComment[std::string("*")];
        oldstallglobal = Access::gStallGlobal;
      }
      else
      {
        RemoveStallRuleAfterBoot = false;
      }
    }
    Access::gStallRules[std::string("*")] = "100";
    Access::gStallGlobal = true;
    Access::gStallComment[std::string("*")] = "namespace is booting";
  }

  try
  {
    gOFS->eosView->initialize2();
    {
      gOFS->eosViewRWMutex.LockWrite();
      gOFS->eosView->initialize3();

      if (MgmMaster.IsMaster())
      {
        // create ../proc/<x> files
        XrdOucString procpathwhoami = MgmProcPath;
        procpathwhoami += "/whoami";
        XrdOucString procpathwho = MgmProcPath;
        procpathwho += "/who";
        XrdOucString procpathquota = MgmProcPath;
        procpathquota += "/quota";
        XrdOucString procpathreconnect = MgmProcPath;
        procpathreconnect += "/reconnect";
        XrdOucString procpathmaster = MgmProcPath;
        procpathmaster += "/master";

        XrdOucErrInfo error;
        eos::common::Mapping::VirtualIdentity vid;
        eos::common::Mapping::Root(vid);
        eos::FileMD* fmd = 0;

        try
        {
          fmd = gOFS->eosView->getFile(procpathwhoami.c_str());
          fmd = 0;
        }
        catch (eos::MDException &e)
        {
          fmd = gOFS->eosView->createFile(procpathwhoami.c_str(), 0, 0);
        }

        if (fmd)
        {
          fmd->setSize(4096);
          gOFS->eosView->updateFileStore(fmd);
        }

        try
        {
          fmd = gOFS->eosView->getFile(procpathwho.c_str());
          fmd = 0;
        }
        catch (eos::MDException &e)
        {
          fmd = gOFS->eosView->createFile(procpathwho.c_str(), 0, 0);
        }

        if (fmd)
        {
          fmd->setSize(4096);
          gOFS->eosView->updateFileStore(fmd);
        }

        try
        {
          fmd = gOFS->eosView->getFile(procpathquota.c_str());
          fmd = 0;
        }
        catch (eos::MDException &e)
        {
          fmd = gOFS->eosView->createFile(procpathquota.c_str(), 0, 0);
        }

        if (fmd)
        {
          fmd->setSize(4096);
          gOFS->eosView->updateFileStore(fmd);
        }

        try
        {
          fmd = gOFS->eosView->getFile(procpathreconnect.c_str());
          fmd = 0;
        }
        catch (eos::MDException &e)
        {
          fmd = gOFS->eosView->createFile(procpathreconnect.c_str(), 0, 0);
        }

        if (fmd)
        {
          fmd->setSize(4096);
          gOFS->eosView->updateFileStore(fmd);
        }

        try
        {
          fmd = gOFS->eosView->getFile(procpathmaster.c_str());
          fmd = 0;
        }
        catch (eos::MDException &e)
        {
          fmd = gOFS->eosView->createFile(procpathmaster.c_str(), 0, 0);
        }

        if (fmd)
        {
          fmd->setSize(4096);
          gOFS->eosView->updateFileStore(fmd);
        }

        {
          XrdSysMutexHelper lock(InitializationMutex);
          Initialized = kBooted;
	  eos_static_alert("msg=\"namespace booted (as master)\"");
        }
      }
    }

    gOFS->eosViewRWMutex.UnLockWrite();

    if (!MgmMaster.IsMaster())
    {
      eos_static_info("msg=\"starting slave listener\"");

      struct stat buf;
      buf.st_size = 0;
      ::stat(gOFS->MgmNsFileChangeLogFile.c_str(), &buf);


      gOFS->eosFileService->startSlave();
      gOFS->eosDirectoryService->startSlave();

      // wait that the follower reaches the offset seen now
      while (gOFS->eosFileService->getFollowOffset() < (uint64_t) buf.st_size)
      {
        XrdSysTimer sleeper;
        sleeper.Wait(200);
        eos_static_debug("msg=\"waiting for the namespace to reach the follow point\" is-offset=%llu follow-offset=%llu", gOFS->eosFileService->getFollowOffset(), (uint64_t) buf.st_size);
      }

      {
        XrdSysMutexHelper lock(InitializationMutex);
        Initialized = kBooted;
	eos_static_alert("msg=\"namespace booted (as slave)\"");
      }
    }

    time_t tstop = time(0);

    gOFS->MgmMaster.MasterLog(eos_notice("eos namespace file loading stopped after %d seconds", (tstop - tstart)));

    {
      eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
      if (oldstallrule.length())
      {
        Access::gStallRules[std::string("*")] = oldstallrule;
      }
      else
      {
        Access::gStallRules.erase(std::string("*"));
      }
      if (oldstallcomment.length())
      {
        Access::gStallComment[std::string("*")] = oldstallcomment;
      }
      else
      {
        Access::gStallComment.erase(std::string("*"));
      }
      Access::gStallGlobal = oldstallglobal;
    }
  }
  catch (eos::MDException &e)
  {
    {
      XrdSysMutexHelper lock(InitializationMutex);
      Initialized = kFailed;
    }
    time_t tstop = time(0);
    eos_crit("eos namespace file loading initialization failed after %d seconds", (tstop - tstart));
    errno = e.getErrno();
    eos_crit("initialization returnd ec=%d %s\n", e.getErrno(), e.getMessage().str().c_str());
  };

  {
    InitializationTime = (time(0) - InitializationTime);
    XrdSysMutexHelper lock(InitializationMutex);

    // grab process status after boot
    if (!eos::common::LinuxStat::GetStat(gOFS->LinuxStatsStartup))
    {
      eos_crit("failed to grab /proc/self/stat information");
    }
  }

  // fill the current accounting
  // load all the quota nodes from the namespace
  Quota::LoadNodes();
  Quota::NodesToSpaceQuota();


  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::Configure (XrdSysError &Eroute)
{
  char *var;
  const char *val;
  int cfgFD, retc, NoGo = 0;
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"));
  XrdOucString role = "server";
  bool authorize = false;
  AuthLib = "";
  Authorization = 0;
  pthread_t tid = 0;
  IssueCapability = false;
  MgmRedirector = false;
  StartTime = time(NULL);

  // set short timeouts in the new XrdCl class
  XrdCl::DefaultEnv::GetEnv()->PutInt("TimeoutResolution", 1);
  // set connection window short
  XrdCl::DefaultEnv::GetEnv()->PutInt("ConnectionWindow", 5);
  // set connection retry to one
  XrdCl::DefaultEnv::GetEnv()->PutInt("ConnectionRetry", 1);
  // set stream error window
  XrdCl::DefaultEnv::GetEnv()->PutInt("StreamErrorWindow", 0);

  UTF8 = getenv("EOS_UTF8")?true:false;

  Shutdown = false;

  setenv("XrdSecPROTOCOL", "sss", 1);
  Eroute.Say("=====> mgmofs enforces SSS authentication for XROOT clients");

  MgmOfsTargetPort = "1094";
  MgmOfsName = "";
  MgmOfsAlias = "";
  MgmOfsBrokerUrl = "root://localhost:1097//eos/";
  MgmOfsInstanceName = "testinstance";

  MgmConfigDir = "";
  MgmMetaLogDir = "";
  MgmTxDir = "";
  MgmAuthDir = "";
  MgmArchiveDir = "";

  MgmHealMap.set_deleted_key(0);
  MgmDirectoryModificationTime.set_deleted_key(0);

  IoReportStorePath = "/var/tmp/eos/report";
  MgmOfsVstBrokerUrl = "";
  MgmArchiveDstUrl = "";
  MgmArchiveSvcClass = "default";

  if (getenv("EOS_VST_BROKER_URL"))
    MgmOfsVstBrokerUrl = getenv("EOS_VST_BROKER_URL");

  if (getenv("EOS_ARCHIVE_URL"))
  {
    MgmArchiveDstUrl = getenv("EOS_ARCHIVE_URL");

    // Make sure it ends with a '/'
    if (MgmArchiveDstUrl[MgmArchiveDstUrl.length() - 1] != '/')
      MgmArchiveDstUrl += '/';
  }

  if (getenv("EOS_ARCHIVE_SVCCLASS"))
  {
    MgmArchiveSvcClass = getenv("EOS_ARCHIVE_SVCCLASS");
  }

  // Create and own the output cache directory or clean it up if it exists -
  // this is used to store temporary results for commands like find, backup
  // or achive
  struct stat dir_stat;
  if (!::stat("/tmp/eos.mgm/", &dir_stat) && S_ISDIR(dir_stat.st_mode))
  {
    XrdOucString systemline = "rm -rf /tmp/eos.mgm/* >& /dev/null &";
    int rrc = system(systemline.c_str());

    if (WEXITSTATUS(rrc))
      eos_err("%s returned %d", systemline.c_str(), rrc);
  }
  else
  {
    eos::common::Path out_dir("/tmp/eos.mgm/empty");

    if (!out_dir.MakeParentPath(S_IRWXU))
    {
      eos_err("Unable to create temporary output file directory /tmp/eos.mgm/");
      Eroute.Emsg("Config", errno, "create temporary outputfile"
                  " directory /tmp/eos.mgm/");
      NoGo = 1;
      return NoGo;;
    }

    // Own the directory by daemon
    if (::chown(out_dir.GetParentPath(), 2, 2))
    {
      eos_err("Unable to own temporary outputfile directory %s", out_dir.GetParentPath());
      Eroute.Emsg("Config", errno, "own outputfile directory /tmp/eos.mgm/");
      NoGo = 1;
      return NoGo;;
    }
  }

  ErrorLog = true;
  bool ConfigAutoSave = false;
  MgmConfigAutoLoad = "";

  long myPort = 0;

  if (getenv("XRDDEBUG")) gMgmOfsTrace.What = TRACE_MOST | TRACE_debug;

  {
    // borrowed from XrdOfs
    unsigned int myIPaddr = 0;

    char buff[256], *bp;
    int i;

    // Obtain port number we will be using
    //
    myPort = (bp = getenv("XRDPORT")) ? strtol(bp, (char **) 0, 10) : 0;

    // Establish our hostname and IPV4 address
    //
    HostName = XrdSysDNS::getHostName();

    if (!XrdSysDNS::Host2IP(HostName, &myIPaddr)) myIPaddr = 0x7f000001;
    strcpy(buff, "[::");
    bp = buff + 3;
    bp += XrdSysDNS::IP2String(myIPaddr, 0, bp, 128);
    *bp++ = ']';
    *bp++ = ':';
    sprintf(bp, "%ld", myPort);
    for (i = 0; HostName[i] && HostName[i] != '.'; i++);
    HostName[i] = '\0';
    HostPref = strdup(HostName);
    HostName[i] = '.';
    Eroute.Say("=====> mgmofs.hostname: ", HostName, "");
    Eroute.Say("=====> mgmofs.hostpref: ", HostPref, "");
    ManagerId = HostName;
    ManagerId += ":";
    ManagerId += (int) myPort;
    unsigned int ip = 0;

    if (XrdSysDNS::Host2IP(HostName, &ip))
    {
      char buff[1024];
      XrdSysDNS::IP2String(ip, 0, buff, 1024);
      ManagerIp = buff;
      ManagerPort = myPort;
    }
    else
    {
      return Eroute.Emsg("Config", errno, "convert hostname to IP address", HostName);
    }


    Eroute.Say("=====> mgmofs.managerid: ", ManagerId.c_str(), "");
  }

  if (!ConfigFN || !*ConfigFN)
  {
    Eroute.Emsg("Config", "Configuration file not specified.");
  }
  else
  {
    // Try to open the configuration file.
    //
    if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return Eroute.Emsg("Config", errno, "open config file", ConfigFN);
    Config.Attach(cfgFD);
    // Now start reading records until eof.
    //
    XrdOucString nsin;
    XrdOucString nsout;

    while ((var = Config.GetMyFirstWord()))
    {
      if (!strncmp(var, "all.", 4))
      {
        var += 4;
        if (!strcmp("role", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument for all.role missing.");
            NoGo = 1;
          }
          else
          {
            XrdOucString lrole = val;

            if ((val = Config.GetWord()))
            {
              if (!strcmp(val, "if"))
              {
                if ((val = Config.GetWord()))
                {
                  if (!strcmp(val, HostName))
                  {
                    role = lrole;
                  }
                  if (!strcmp(val, HostPref))
                  {
                    role = lrole;
                  }
                }
              }
              else
              {
                role = lrole;
              }
            }
            else
            {
              role = lrole;
            }
          }
        }
      }
      if (!strncmp(var, "mgmofs.", 7))
      {
        var += 7;

        if (!strcmp("fs", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument for fs invalid.");
            NoGo = 1;
          }
          else
          {
            Eroute.Say("=====> mgmofs.fs: ", val, "");
            MgmOfsName = val;
          }
        }

        if (!strcmp("targetport", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument for fs invalid.");
            NoGo = 1;
          }
          else
          {
            Eroute.Say("=====> mgmofs.targetport: ", val, "");
            MgmOfsTargetPort = val;
          }
        }

        if (!strcmp("capability", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for capbility missing. Can be true/lazy/1 or false/0");
            NoGo = 1;
          }
          else
          {
            if ((!(strcmp(val, "true"))) || (!(strcmp(val, "1"))) || (!(strcmp(val, "lazy"))))
            {
              IssueCapability = true;
            }
            else
            {
              if ((!(strcmp(val, "false"))) || (!(strcmp(val, "0"))))
              {
                IssueCapability = false;
              }
              else
              {
                Eroute.Emsg("Config", "argument 2 for capbility invalid. Can be <true>/1 or <false>/0");
                NoGo = 1;
              }
            }
          }
        }

        if (!strcmp("broker", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for broker missing. Should be URL like root://<host>/<queue>/");
            NoGo = 1;
          }
          else
          {
            if (getenv("EOS_BROKER_URL"))
            {
              MgmOfsBrokerUrl = getenv("EOS_BROKER_URL");
            }
            else
            {
              MgmOfsBrokerUrl = val;
            }
          }
        }

        if (!strcmp("instance", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for instance missing. Should be the name of the EOS cluster");
            NoGo = 1;
          }
          else
          {
            if (getenv("EOS_INSTANCE_NAME"))
            {
              MgmOfsInstanceName = getenv("EOS_INSTANCE_NAME");
            }
            else
            {
              MgmOfsInstanceName = val;
            }
          }
          Eroute.Say("=====> mgmofs.instance : ", MgmOfsInstanceName.c_str(), "");
        }

        if (!strcmp("authlib", var))
        {
          if ((!(val = Config.GetWord())) || (::access(val, R_OK)))
          {
            Eroute.Emsg("Config", "I cannot acccess you authorization library!");
            NoGo = 1;
          }
          else
          {
            AuthLib = val;
          }
          Eroute.Say("=====> mgmofs.authlib : ", AuthLib.c_str());
        }

        if (!strcmp("authorize", var))
        {
          if ((!(val = Config.GetWord())) ||
              (strcmp("true", val) && strcmp("false", val) &&
               strcmp("1", val) && strcmp("0", val)))
          {
            Eroute.Emsg("Config", "argument 2 for authorize illegal or missing. "
                        "Must be <true>, <false>, <1> or <0>!");
            NoGo = 1;
          }
          else
          {
            if ((!strcmp("true", val) || (!strcmp("1", val))))
            {
              authorize = true;
            }
          }

          if (authorize)
            Eroute.Say("=====> mgmofs.authorize : true");
          else
            Eroute.Say("=====> mgmofs.authorize : false");
        }

        if (!strcmp("errorlog", var))
        {
          if ((!(val = Config.GetWord())) ||
              (strcmp("true", val) && strcmp("false", val) &&
               strcmp("1", val) && strcmp("0", val)))
          {
            Eroute.Emsg("Config", "argument 2 for errorlog illegal or missing. "
                        "Must be <true>, <false>, <1> or <0>!");
            NoGo = 1;
          }
          else
          {
            if ((!strcmp("true", val) || (!strcmp("1", val))))
            {
              ErrorLog = true;
            }
            else
            {
              ErrorLog = false;
            }
          }
          if (ErrorLog)
            Eroute.Say("=====> mgmofs.errorlog : true");
          else
            Eroute.Say("=====> mgmofs.errorlog : false");
        }

        if (!strcmp("redirector", var))
        {
          if ((!(val = Config.GetWord())) ||
              (strcmp("true", val) && strcmp("false", val) &&
               strcmp("1", val) && strcmp("0", val)))
          {
            Eroute.Emsg("Config", "argument 2 for redirector illegal or missing. "
                        "Must be <true>,<false>,<1> or <0>!");
            NoGo = 1;
          }
          else
          {
            if ((!strcmp("true", val) || (!strcmp("1", val))))
            {
              MgmRedirector = true;
            }
            else
            {
              MgmRedirector = false;
            }
          }

          if (ErrorLog)
            Eroute.Say("=====> mgmofs.errorlog   : true");
          else
            Eroute.Say("=====> mgmofs.errorlog   : false");
        }

        if (!strcmp("configdir", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument for configdir invalid.");
            NoGo = 1;
          }
          else
          {
            MgmConfigDir = val;

            if (!MgmConfigDir.endswith("/"))
              MgmConfigDir += "/";
          }
        }

        if (!strcmp("archivedir", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument for archivedir invalid.");
            NoGo = 1;
          }
          else
          {
            MgmArchiveDir = val;

            if (!MgmArchiveDir.endswith("/"))
              MgmArchiveDir += "/";
          }
        }

        if (!strcmp("autosaveconfig", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for autosaveconfig missing. Can be true/1 or false/0");
            NoGo = 1;
          }
          else
          {
            if ((!(strcmp(val, "true"))) || (!(strcmp(val, "1"))))
            {
              ConfigAutoSave = true;
            }
            else
            {
              if ((!(strcmp(val, "false"))) || (!(strcmp(val, "0"))))
              {
                ConfigAutoSave = false;
              }
              else
              {
                Eroute.Emsg("Config", "argument 2 for autosaveconfig invalid. Can be <true>/1 or <false>/0");
                NoGo = 1;
              }
            }
          }
        }

        if (!strcmp("autoloadconfig", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument for autoloadconfig invalid.");
            NoGo = 1;
          }
          else
          {
            MgmConfigAutoLoad = val;
          }
        }

        if (!strcmp("alias", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument for alias missing.");
            NoGo = 1;
          }
          else
          {
            MgmOfsAlias = val;
          }
        }

        if (!strcmp("metalog", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for metalog missing");
            NoGo = 1;
          }
          else
          {
            MgmMetaLogDir = val;
            // just try to create it in advance
            XrdOucString makeit = "mkdir -p ";
            makeit += MgmMetaLogDir;
            int src = system(makeit.c_str());
            if (src)
              eos_err("%s returned %d", makeit.c_str(), src);
            XrdOucString chownit = "chown -R ";
            chownit += (int) geteuid();
            chownit += " ";
            chownit += MgmMetaLogDir;
            src = system(chownit.c_str());
            if (src)
              eos_err("%s returned %d", chownit.c_str(), src);

            if (::access(MgmMetaLogDir.c_str(), W_OK | R_OK | X_OK))
            {
              Eroute.Emsg("Config", "cannot acccess the meta data changelog "
                          "directory for r/w!", MgmMetaLogDir.c_str());
              NoGo = 1;
            }
            else
            {
              Eroute.Say("=====> mgmofs.metalog: ", MgmMetaLogDir.c_str(), "");
            }
          }
        }

        if (!strcmp("txdir", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for txdir missing");
            NoGo = 1;
          }
          else
          {
            MgmTxDir = val;
            // just try to create it in advance
            XrdOucString makeit = "mkdir -p ";
            makeit += MgmTxDir;
            int src = system(makeit.c_str());
            if (src)
              eos_err("%s returned %d", makeit.c_str(), src);
            XrdOucString chownit = "chown -R ";
            chownit += (int) geteuid();
            chownit += " ";
            chownit += MgmTxDir;
            src = system(chownit.c_str());
            if (src)
              eos_err("%s returned %d", chownit.c_str(), src);

            if (::access(MgmTxDir.c_str(), W_OK | R_OK | X_OK))
            {
              Eroute.Emsg("Config", "cannot acccess the transfer directory for r/w:", MgmTxDir.c_str());
              NoGo = 1;
            }
            else
            {
              Eroute.Say("=====> mgmofs.txdir:   ", MgmTxDir.c_str(), "");
            }
          }
        }

        if (!strcmp("authdir", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for authdir missing");
            NoGo = 1;
          }
          else
          {
            MgmAuthDir = val;
            // just try to create it in advance
            XrdOucString makeit = "mkdir -p ";
            makeit += MgmAuthDir;
            int src = system(makeit.c_str());
            if (src)
              eos_err("%s returned %d", makeit.c_str(), src);
            XrdOucString chownit = "chown -R ";
            chownit += (int) geteuid();
            chownit += " ";
            chownit += MgmAuthDir;
            src = system(chownit.c_str());
            if (src)
              eos_err("%s returned %d", chownit.c_str(), src);

            if ((src = ::chmod(MgmAuthDir.c_str(), S_IRUSR | S_IWUSR | S_IXUSR)))
            {
              eos_err("chmod 700 %s returned %d", MgmAuthDir.c_str(), src);
              NoGo = 1;
            }

            if (::access(MgmAuthDir.c_str(), W_OK | R_OK | X_OK))
            {
              Eroute.Emsg("Config", "cannot acccess the authentication directory "
                          "for r/w:", MgmAuthDir.c_str());
              NoGo = 1;
            }
            else
            {
              Eroute.Say("=====> mgmofs.authdir:   ", MgmAuthDir.c_str(), "");
            }
          }
        }

        if (!strcmp("reportstorepath", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for reportstorepath missing");
            NoGo = 1;
          }
          else
          {
            IoReportStorePath = val;
            // just try to create it in advance
            XrdOucString makeit = "mkdir -p ";
            makeit += IoReportStorePath;
            int src = system(makeit.c_str());
            if (src)
              eos_err("%s returned %d", makeit.c_str(), src);
            XrdOucString chownit = "chown -R ";
            chownit += (int) geteuid();
            chownit += " ";
            chownit += IoReportStorePath;
            src = system(chownit.c_str());
            if (src)
              eos_err("%s returned %d", chownit.c_str(), src);

            if (::access(IoReportStorePath.c_str(), W_OK | R_OK | X_OK))
            {
              Eroute.Emsg("Config", "cannot acccess the reportstore directory "
                          "for r/w:", IoReportStorePath.c_str());
              NoGo = 1;
            }
            else
            {
              Eroute.Say("=====> mgmofs.reportstorepath: ", IoReportStorePath.c_str(), "");
            }
          }
        }

        // Get the fst gateway hostname and port
        if (!strcmp("fstgw", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "fst gateway value not specified");
            NoGo = 1;
          }
          else
          {
            mFstGwHost = val;
            size_t pos = mFstGwHost.find(':');

            if (pos == std::string::npos)
            {
              // Use a default value if no port is specified
              mFstGwPort = 1094;
            }
            else
            {
              mFstGwPort = atoi(mFstGwHost.substr(pos + 1).c_str());
              mFstGwHost = mFstGwHost.erase(pos);
            }

            Eroute.Say("=====> mgmofs.fstgw: ", mFstGwHost.c_str(), ":",
                       std::to_string((long long int)mFstGwPort).c_str());
          }
        }

        if (!strcmp("trace", var))
        {
          static struct traceopts
          {
            const char *opname;
            int opval;
          } tropts[] = {
            {"aio", TRACE_aio},
            {"all", TRACE_ALL},
            {"chmod", TRACE_chmod},
            {"close", TRACE_close},
            {"closedir", TRACE_closedir},
            {"debug", TRACE_debug},
            {"delay", TRACE_delay},
            {"dir", TRACE_dir},
            {"exists", TRACE_exists},
            {"getstats", TRACE_getstats},
            {"fsctl", TRACE_fsctl},
            {"io", TRACE_IO},
            {"mkdir", TRACE_mkdir},
            {"most", TRACE_MOST},
            {"open", TRACE_open},
            {"opendir", TRACE_opendir},
            {"qscan", TRACE_qscan},
            {"read", TRACE_read},
            {"readdir", TRACE_readdir},
            {"redirect", TRACE_redirect},
            {"remove", TRACE_remove},
            {"rename", TRACE_rename},
            {"sync", TRACE_sync},
            {"truncate", TRACE_truncate},
            {"write", TRACE_write},
            {"authorize", TRACE_authorize},
            {"map", TRACE_map},
            {"role", TRACE_role},
            {"access", TRACE_access},
            {"attributes", TRACE_attributes},
            {"allows", TRACE_allows}
          };
          int i, neg, trval = 0, numopts = sizeof (tropts) / sizeof (struct traceopts);

          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "trace option not specified");
            return 1;
          }

          while (val)
          {
            Eroute.Say("=====> mgmofs.trace: ", val, "");
            if (!strcmp(val, "off")) trval = 0;
            else
            {
              if ((neg = (val[0] == '-' && val[1]))) val++;
              for (i = 0; i < numopts; i++)
              {
                if (!strcmp(val, tropts[i].opname))
                {
                  if (neg) trval &= ~tropts[i].opval;
                  else trval |= tropts[i].opval;
                  break;
                }
              }
              if (i >= numopts)
                Eroute.Say("Config warning: ignoring invalid trace option '", val, "'.");
            }
            val = Config.GetWord();
          }

          gMgmOfsTrace.What = trval;
        }
      }
    }
  }

  if (MgmRedirector)
    Eroute.Say("=====> mgmofs.redirector : true");
  else
    Eroute.Say("=====> mgmofs.redirector : false");

  if (!MgmOfsBrokerUrl.endswith("/"))
  {
    MgmOfsBrokerUrl += "/";
  }

  if (!MgmOfsBrokerUrl.endswith("//eos/"))
  {
    Eroute.Say("Config error: the broker url has to be of the form <root://<hostname>[:<port>]//");
    return 1;
  }

  if (MgmOfsVstBrokerUrl.length() && (!MgmOfsVstBrokerUrl.endswith("//eos/")))
  {
    Eroute.Say("Config error: the vst broker url has to be of the form <root://<hostname>[:<port>]//");
    return 1;
  }

  if (!MgmConfigDir.length())
  {
    Eroute.Say("Config error: configuration directory is not defined : mgm.configdir=</var/eos/config/>");
    return 1;
  }

  if (!MgmMetaLogDir.length())
  {
    Eroute.Say("Config error: meta data log directory is not defined : mgm.metalog=</var/eos/md/>");
    return 1;
  }

  if (!MgmTxDir.length())
  {
    Eroute.Say("Config error: transfer directory is not defined : mgm.txdir=</var/eos/tx/>");
    return 1;
  }

  if (!MgmAuthDir.length())
  {
    Eroute.Say("Config error: auth directory is not defined: mgm.authdir=</var/eos/auth/>");
    return 1;
  }

  if (!MgmArchiveDir.length())
  {
    Eroute.Say("Config notice: archive directory is not defined - archiving is disabled");
  }

  MgmOfsBroker = MgmOfsBrokerUrl;
  MgmDefaultReceiverQueue = MgmOfsBrokerUrl;
  MgmDefaultReceiverQueue += "*/fst";

  MgmOfsBrokerUrl += HostName;
  MgmOfsBrokerUrl += "/mgm";

  if (MgmOfsVstBrokerUrl.length())
  {
    MgmOfsVstBrokerUrl += MgmOfsInstanceName;
    MgmOfsVstBrokerUrl += "/";
    MgmOfsVstBrokerUrl += HostName;
    MgmOfsVstBrokerUrl += "/vst";
  }

  MgmOfsQueue = "/eos/";
  MgmOfsQueue += ManagerId;
  MgmOfsQueue += "/mgm";

  // setup the circular in-memory logging buffer
  eos::common::Logging::Init();

  // configure log-file fan out

  std::vector<std::string> lFanOutTags;

  lFanOutTags.push_back("Balancer");
  lFanOutTags.push_back("Converter");
  lFanOutTags.push_back("DrainJob");
  lFanOutTags.push_back("Http");
  lFanOutTags.push_back("Master");
  lFanOutTags.push_back("Recycle");
  lFanOutTags.push_back("LRU");
  lFanOutTags.push_back("GroupBalancer");
  lFanOutTags.push_back("GeoBalancer");
  lFanOutTags.push_back("#");

  // get the XRootD log directory
  char *logdir = 0;
  XrdOucEnv::Import("XRDLOGDIR", logdir);

  if (logdir)
  {
    for (size_t i = 0; i < lFanOutTags.size(); i++)
    {
      std::string lLogFile = logdir;
      lLogFile += "/";
      if (lFanOutTags[i] == "#")
      {
        lLogFile += "Clients";
      }
      else
      {
        lLogFile += lFanOutTags[i];
      }
      lLogFile += ".log";
      FILE* fp = fopen(lLogFile.c_str(), "a+");
      if (fp)
      {
        eos::common::Logging::AddFanOut(lFanOutTags[i].c_str(), fp);
      }
      else
      {
        fprintf(stderr, "error: failed to open sub-logfile=%s", lLogFile.c_str());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // add some alias for the logging
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // HTTP module
  // ---------------------------------------------------------------------------
  eos::common::Logging::AddFanOutAlias("HttpHandler", "Http");
  eos::common::Logging::AddFanOutAlias("HttpServer", "Http");
  eos::common::Logging::AddFanOutAlias("ProtocolHandler", "Http");
  eos::common::Logging::AddFanOutAlias("S3", "Http");
  eos::common::Logging::AddFanOutAlias("S3Store", "Http");
  eos::common::Logging::AddFanOutAlias("WebDAV", "Http");
  eos::common::Logging::AddFanOutAlias("PropFindResponse", "Http");
  eos::common::Logging::AddFanOutAlias("WebDAVHandler", "Http");
  eos::common::Logging::AddFanOutAlias("WebDAVReponse", "Http");
  eos::common::Logging::AddFanOutAlias("S3Handler", "Http");
  eos::common::Logging::AddFanOutAlias("S3Store", "Http");

  eos::common::Logging::SetUnit(MgmOfsBrokerUrl.c_str());

  Eroute.Say("=====> mgmofs.broker : ", MgmOfsBrokerUrl.c_str(), "");

  XrdOucString ttybroadcastkillline = "pkill -9 -f \"eos-tty-broadcast\"";
  int rrc = system(ttybroadcastkillline.c_str());
  if (WEXITSTATUS(rrc))
  {
    eos_info("%s returned %d", ttybroadcastkillline.c_str(), rrc);
  }
  
  if (getenv("EOS_TTY_BROADCAST_LISTEN_LOGFILE") && getenv("EOS_TTY_BROADCAST_EGREP"))
  {
    XrdOucString ttybroadcastline = "eos-tty-broadcast ";
    ttybroadcastline += getenv("EOS_TTY_BROADCAST_LISTEN_LOGFILE");
    ttybroadcastline += " ";
    ttybroadcastline += getenv("EOS_TTY_BROADCAST_EGREP");
    ttybroadcastline += " >& /dev/null &";
    eos_info("%s\n", ttybroadcastline.c_str());
    rrc = system(ttybroadcastline.c_str());
    if (WEXITSTATUS(rrc))
    {
      eos_info("%s returned %d", ttybroadcastline.c_str(), rrc);
    }
  }

  int pos1 = MgmDefaultReceiverQueue.find("//");
  int pos2 = MgmDefaultReceiverQueue.find("//", pos1 + 2);
  if (pos2 != STR_NPOS)
  {
    MgmDefaultReceiverQueue.erase(0, pos2 + 1);
  }

  Eroute.Say("=====> mgmofs.defaultreceiverqueue : ", MgmDefaultReceiverQueue.c_str(), "");

  // set our Eroute for XrdMqMessage
  XrdMqMessage::Eroute = *eDest;

  // check if mgmofsfs has been set

  if (!MgmOfsName.length())
  {
    Eroute.Say("Config error: no mgmofs fs has been defined (mgmofs.fs /...)", "", "");
  }
  else
  {
    Eroute.Say("=====> mgmofs.fs: ", MgmOfsName.c_str(), "");
  }

  if (ErrorLog)
    Eroute.Say("=====> mgmofs.errorlog : enabled");
  else
    Eroute.Say("=====> mgmofs.errorlog : disabled");

  // we need to specify this if the server was not started with the explicit manager option ... e.g. see XrdOfs

  Eroute.Say("=====> all.role: ", role.c_str(), "");

  if (role == "manager")
  {
    putenv((char *) "XRDREDIRECT=R");
  }

  if ((AuthLib != "") && (authorize))
  {
    // load the authorization plugin
    XrdSysPlugin *myLib;
    XrdAccAuthorize * (*ep)(XrdSysLogger *, const char *, const char *);

    // Authorization comes from the library or we use the default
    //
    Authorization = XrdAccAuthorizeObject(Eroute.logger(), ConfigFN, 0);

    if (!(myLib = new XrdSysPlugin(&Eroute, AuthLib.c_str())))
    {
      Eroute.Emsg("Config", "Failed to load authorization library!");
      NoGo = 1;
    }
    else
    {
      ep = (XrdAccAuthorize * (*)(XrdSysLogger *, const char *, const char *))
        (myLib->getPlugin("XrdAccAuthorizeObject"));
      if (!ep)
      {
        Eroute.Emsg("Config", "Failed to get authorization library plugin!");
        NoGo = 1;
      }
      else
      {
        Authorization = ep(Eroute.logger(), ConfigFN, 0);
      }
    }
  }

  if ((retc = Config.LastError()))
    NoGo = Eroute.Emsg("Config", -retc, "read config file", ConfigFN);
  Config.Close();

  XrdOucString unit = "mgm@";
  unit += ManagerId;

  eos::common::Logging::SetLogPriority(LOG_INFO);
  eos::common::Logging::SetUnit(unit.c_str());
  std::string filter = "Process,AddQuota,UpdateHint,Update,UpdateQuotaStatus,SetConfigValue,"
    "Deletion,GetQuota,PrintOut,RegisterNode,SharedHash";
  eos::common::Logging::SetFilter(filter.c_str());
  Eroute.Say("=====> setting message filter: Process,AddQuota,UpdateHint,Update"
             "UpdateQuotaStatus,SetConfigValue,Deletion,GetQuota,PrintOut,"
             "RegisterNode,SharedHash");

  // we automatically append the host name to the config dir now !!!
  MgmConfigDir += HostName;
  MgmConfigDir += "/";

  XrdOucString makeit = "mkdir -p ";
  makeit += MgmConfigDir;
  int src = system(makeit.c_str());
  if (src)
    eos_err("%s returned %d", makeit.c_str(), src);

  XrdOucString chownit = "chown -R ";
  chownit += (int) geteuid();
  chownit += " ";
  chownit += MgmConfigDir;
  src = system(chownit.c_str());
  if (src)
    eos_err("%s returned %d", chownit.c_str(), src);

  // check config directory access
  if (::access(MgmConfigDir.c_str(), W_OK | R_OK | X_OK))
  {
    Eroute.Emsg("Config", "I cannot acccess the configuration directory for r/w!", MgmConfigDir.c_str());
    NoGo = 1;
  }
  else
  {
    Eroute.Say("=====> mgmofs.configdir: ", MgmConfigDir.c_str(), "");
  }

  // start the config enging
  ConfEngine = new ConfigEngine(MgmConfigDir.c_str());

  // create comment log
  commentLog = new eos::common::CommentLog("/var/log/eos/mgm/logbook.log");
  if (commentLog && commentLog->IsValid())
  {
    Eroute.Say("=====> comment log in /var/log/eos/mgm/logbook.log");
  }
  else
  {
    Eroute.Emsg("Config", "I cannot create/open the comment log file /var/log/eos/mgm/logbook.log");
    NoGo = 1;
  }

  if (ConfigAutoSave && (!getenv("EOS_AUTOSAVE_CONFIG")))
  {
    Eroute.Say("=====> mgmofs.autosaveconfig: true", "");
    ConfEngine->SetAutoSave(true);
  }
  else
  {
    if (getenv("EOS_AUTOSAVE_CONFIG"))
    {
      eos_info("autosave config=%s", getenv("EOS_AUTOSAVE_CONFIG"));
      XrdOucString autosave = getenv("EOS_AUTOSAVE_CONFIG");
      if ((autosave == "1") || (autosave == "true"))
      {
        Eroute.Say("=====> mgmofs.autosaveconfig: true", "");
        ConfEngine->SetAutoSave(true);
      }
      else
      {
        Eroute.Say("=====> mgmofs.autosaveconfig: false", "");
        ConfEngine->SetAutoSave(false);
      }
    }
    else
    {
      Eroute.Say("=====> mgmofs.autosaveconfig: false", "");
    }
  }

  if (getenv("EOS_MGM_ALIAS"))
  {
    MgmOfsAlias = getenv("EOS_MGM_ALIAS");
  }

  // we don't put the alias we need call-back's to appear on our node
  if (MgmOfsAlias.length())
  {
    Eroute.Say("=====> mgmofs.alias: ", MgmOfsAlias.c_str());
  }

  XrdOucString keytabcks = "unaccessible";
  // ----------------------------------------------------------
  // build the adler & sha1 checksum of the default keytab file
  // ----------------------------------------------------------
  int fd = ::open("/etc/eos.keytab", O_RDONLY);

  XrdOucString symkey = "";

  if (fd > 0)
  {
    char buffer[65535];
    char keydigest[SHA_DIGEST_LENGTH + 1];

    SHA_CTX sha1;
    SHA1_Init(&sha1);



    size_t nread = ::read(fd, buffer, sizeof (buffer));
    if (nread > 0)
    {
      unsigned int adler;
      SHA1_Update(&sha1, (const char*) buffer, nread);
      adler = adler32(0L, Z_NULL, 0);
      adler = adler32(adler, (const Bytef*) buffer, nread);
      char sadler[1024];
      snprintf(sadler, sizeof (sadler) - 1, "%08x", adler);
      keytabcks = sadler;
    }
    SHA1_Final((unsigned char*) keydigest, &sha1);
    eos::common::SymKey::Base64Encode(keydigest, SHA_DIGEST_LENGTH, symkey);
    close(fd);
  }

  eos_notice("MGM_HOST=%s MGM_PORT=%ld VERSION=%s RELEASE=%s KEYTABADLER=%s SYMKEY=%s", HostName, myPort, VERSION, RELEASE, keytabcks.c_str(), symkey.c_str());

  if (!eos::common::gSymKeyStore.SetKey64(symkey.c_str(), 0))
  {
    eos_crit("unable to store the created symmetric key %s", symkey.c_str());
    return 1;
  }

  // ----------------------------------------------------------
  // create global visible configuration parameters
  // we create 3 queues
  // "/eos/<instance>/"
  // ----------------------------------------------------------
  XrdOucString configbasequeue = "/config/";
  configbasequeue += MgmOfsInstanceName.c_str();

  MgmConfigQueue = configbasequeue;
  MgmConfigQueue += "/mgm/";
  AllConfigQueue = configbasequeue;
  AllConfigQueue += "/all/";
  FstConfigQueue = configbasequeue;
  FstConfigQueue += "/fst/";

  SpaceConfigQueuePrefix = configbasequeue;
  SpaceConfigQueuePrefix += "/space/";
  NodeConfigQueuePrefix = "/config/";
  NodeConfigQueuePrefix += MgmOfsInstanceName.c_str();
  NodeConfigQueuePrefix += "/node/";
  GroupConfigQueuePrefix = configbasequeue;
  GroupConfigQueuePrefix += "/group/";

  FsNode::gManagerId = ManagerId.c_str();

  FsView::gFsView.SetConfigQueues(MgmConfigQueue.c_str(), NodeConfigQueuePrefix.c_str(), GroupConfigQueuePrefix.c_str(), SpaceConfigQueuePrefix.c_str());
  FsView::gFsView.SetConfigEngine(ConfEngine);

  // we need to set the shared object manager to be used
  eos::common::GlobalConfig::gConfig.SetSOM(&ObjectManager);

  // set the object manager to listener only
  ObjectManager.EnableBroadCast(false);

  // setup the modifications which the fs listener thread is waiting for
  ObjectManager.SubjectsMutex.Lock();
  std::string watch_errc = "stat.errc";

  ObjectManager.ModificationWatchKeys.insert(watch_errc); // we need to take action an filesystem errors
  ObjectManager.ModificationWatchSubjects.insert(MgmConfigQueue.c_str()); // we need to apply remote configuration changes

  ObjectManager.SubjectsMutex.UnLock();

  ObjectManager.SetDebug(false);

  if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(MgmConfigQueue.c_str(), "/eos/*/mgm"))
  {
    eos_crit("Cannot add global config queue %s\n", MgmConfigQueue.c_str());
  }
  if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(AllConfigQueue.c_str(), "/eos/*"))
  {
    eos_crit("Cannot add global config queue %s\n", AllConfigQueue.c_str());
  }
  if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(FstConfigQueue.c_str(), "/eos/*/fst"))
  {
    eos_crit("Cannot add global config queue %s\n", FstConfigQueue.c_str());
  }

  std::string out = "";
  eos::common::GlobalConfig::gConfig.PrintBroadCastMap(out);
  fprintf(stderr, "%s", out.c_str());

  // eventuall autoload a configuration
  if (getenv("EOS_AUTOLOAD_CONFIG"))
  {
    MgmConfigAutoLoad = getenv("EOS_AUTOLOAD_CONFIG");
  }

  XrdOucString instancepath = "/eos/";
  MgmProcPath = "/eos/";
  XrdOucString subpath = MgmOfsInstanceName;
  if (subpath.beginswith("eos"))
  {
    subpath.replace("eos", "");
  }
  MgmProcPath += subpath;
  MgmProcPath += "/proc";
  // This path is used for temporary output files for layout conversions
  MgmProcConversionPath = MgmProcPath;
  MgmProcConversionPath += "/conversion";
  MgmProcMasterPath = MgmProcPath;
  MgmProcMasterPath += "/master";
  MgmProcArchivePath = MgmProcPath;
  MgmProcArchivePath += "/archive";

  Recycle::gRecyclingPrefix.insert(0, MgmProcPath.c_str());

  instancepath += subpath;

  //  eos_emerg("%s",(char*)"test emerg");
  //  eos_alert("%s",(char*)"test alert");
  //  eos_crit("%s", (char*)"test crit");
  //  eos_err("%s",  (char*)"test err");
  //  eos_warning("%s",(char*)"test warning");
  //  eos_notice("%s",(char*)"test notice");
  //  eos_info("%s",(char*)"test info");
  //  eos_debug("%s",(char*)"test debug");

  // ----------------------------------------------------------
  // initialize user mapping
  // ----------------------------------------------------------
  eos::common::Mapping::Init();

  // ----------------------------------------------------------
  // initialize the master/slave class
  // ----------------------------------------------------------
  if (!MgmMaster.Init())
  {
    return 1;
  }

  // ----------------------------------------------------------
  // configure the meta data catalog
  // ----------------------------------------------------------
  gOFS->eosViewRWMutex.SetBlocking(true);

  if (!MgmMaster.BootNamespace())
  {
    return 1;
  }

  // ----------------------------------------------------------
  // check the '/' directory
  // ----------------------------------------------------------

  eos::ContainerMD* rootmd;
  try
  {
    rootmd = gOFS->eosView->getContainer("/");
  }
  catch (eos::MDException &e)
  {
    Eroute.Emsg("Config", "cannot get the / directory meta data");
    eos_crit("eos view cannot retrieve the / directory");
    return 1;
  }

  // ----------------------------------------------------------
  // check the '/' directory permissions
  // ----------------------------------------------------------
  if (!rootmd->getMode())
  {
    if (MgmMaster.IsMaster())
    {
      // no permissions set yet
      try
      {
        rootmd->setMode(S_IFDIR | S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP | S_IWGRP | S_IXGRP | S_ISGID);
      }
      catch (eos::MDException &e)
      {
        Eroute.Emsg("Config", "cannot set the / directory mode to inital mode");
        eos_crit("cannot set the / directory mode to 755");
        return 1;
      }
    }
    else
    {
      Eroute.Emsg("Config", "/ directory has no 755 permissions set");
      eos_crit("cannot see / directory with mode to 755");
      return 1;
    }
  }

  eos_info("/ permissions are %o", rootmd->getMode());

  if (MgmMaster.IsMaster())
  {
    // create /eos
    eos::ContainerMD* eosmd = 0;
    try
    {
      eosmd = gOFS->eosView->getContainer("/eos/");
    }
    catch (eos::MDException &e)
    {
      // nothing in this case
      eosmd = 0;
    }

    if (!eosmd)
    {
      try
      {
        eosmd = gOFS->eosView->createContainer("/eos/", true);
        // set attribute inheritance
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP | S_IWGRP | S_IXGRP | S_ISGID);
        // set default checksum 'adler'
        eosmd->setAttribute("sys.forced.checksum", "adler");
        gOFS->eosView->updateContainerStore(eosmd);
        eos_info("/eos permissions are %o checksum is set <adler>", eosmd->getMode());
      }
      catch (eos::MDException &e)
      {
        Eroute.Emsg("Config", "cannot set the /eos/ directory mode to inital mode");
        eos_crit("cannot set the /eos/ directory mode to 755");
        return 1;
      }
    }

    // check recycle directory
    try
    {
      eosmd = gOFS->eosView->getContainer(Recycle::gRecyclingPrefix.c_str());
    }
    catch (eos::MDException &e)
    {
      // nothing in this case
      eosmd = 0;
    }

    if (!eosmd)
    {
      try
      {
        eosmd = gOFS->eosView->createContainer(Recycle::gRecyclingPrefix.c_str(), true);
        // set attribute inheritance
        eosmd->setMode(S_IFDIR | S_IRWXU);

        gOFS->eosView->updateContainerStore(eosmd);
        eos_info("%s permissions are %o", Recycle::gRecyclingPrefix.c_str(), eosmd->getMode());
      }
      catch (eos::MDException &e)
      {
        Eroute.Emsg("Config", "cannot set the recycle directory mode to inital mode");
        eos_crit("cannot set the %s directory mode to 700", Recycle::gRecyclingPrefix.c_str());
        return 1;
      }
    }

    try
    {
      eosmd = gOFS->eosView->getContainer(MgmProcPath.c_str());
    }
    catch (eos::MDException &e)
    {
      eosmd = 0;
    }

    if (!eosmd)
    {
      try
      {
        eosmd = gOFS->eosView->createContainer(MgmProcPath.c_str(), true);
        // set attribute inheritance
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP | S_IXGRP);
        gOFS->eosView->updateContainerStore(eosmd);
      }
      catch (eos::MDException &e)
      {
        Eroute.Emsg("Config", "cannot set the /eos/proc directory mode to inital mode");
        eos_crit("cannot set the /eos/proc directory mode to 755");
        return 1;
      }
    }

    // Create output directory layout conversions
    try
    {
      eosmd = gOFS->eosView->getContainer(MgmProcConversionPath.c_str());
      eosmd->setMode(S_IFDIR | S_IRWXU);
      eosmd->setCUid(2); // conversion directory is owned by daemon
      gOFS->eosView->updateContainerStore(eosmd);
    }
    catch (eos::MDException &e)
    {
      eosmd = 0;
    }

    if (!eosmd)
    {
      try
      {
        eosmd = gOFS->eosView->createContainer(MgmProcConversionPath.c_str(), true);
        // set attribute inheritance
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IRWXG);
        gOFS->eosView->updateContainerStore(eosmd);
      }
      catch (eos::MDException &e)
      {
        Eroute.Emsg("Config", "cannot set the /eos/../proc/conversion directory mode to inital mode");
        eos_crit("cannot set the /eos/../proc/conversion directory mode to 700");
        return 1;
      }
    }

    // Create directory for fast find functionality of archived dirs
    try
    {
      eosmd = gOFS->eosView->getContainer(MgmProcArchivePath.c_str());
      eosmd->setMode(S_IFDIR | S_IRWXU);
      eosmd->setCUid(2); // archive directory is owned by daemon
      gOFS->eosView->updateContainerStore(eosmd);
    }
    catch (eos::MDException &e)
    {
      eosmd = 0;
    }

    if (!eosmd)
    {
      try
      {
        eosmd = gOFS->eosView->createContainer(MgmProcArchivePath.c_str(), true);
        // Set attribute inheritance
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IRWXG);
        gOFS->eosView->updateContainerStore(eosmd);
      }
      catch (eos::MDException &e)
      {
        Eroute.Emsg("Config", "cannot set the /eos/../proc/archive directory mode to inital mode");
        eos_crit("cannot set the /eos/../proc/archive directory mode to 700");
        return 1;
      }
    }

    // Set also the archiverd ZMQ endpoint were client requests are sent
    std::ostringstream oss;
    oss << "ipc://" << MgmArchiveDir.c_str() << "archive_frontend.ipc";
    mArchiveEndpoint = oss.str();
  }
  //-------------------------------------------

  XrdMqSharedHash* hash = 0;

  // - we disable a lot of features if we are only a redirector
  if (!MgmRedirector)
  {
    // create the specific listener class
    MgmOfsMessaging = new Messaging(MgmOfsBrokerUrl.c_str(), MgmDefaultReceiverQueue.c_str(), true, true, &ObjectManager);
    if (!MgmOfsMessaging->StartListenerThread()) NoGo = 1;
    MgmOfsMessaging->SetLogId("MgmOfsMessaging");

    if ((!MgmOfsMessaging) || (MgmOfsMessaging->IsZombie()))
    {
      Eroute.Emsg("Config", "cannot create messaging object(thread)");
      return NoGo;
    }

    if (MgmOfsVstBrokerUrl.length() &&
        (!getenv("EOS_VST_BROKER_DISABLE") || (strcmp(getenv("EOS_VST_BROKER_DISABLE"), "1"))))
    {
      MgmOfsVstMessaging = new VstMessaging(MgmOfsVstBrokerUrl.c_str(), "/eos/*/vst", true, true, 0);
      if (!MgmOfsVstMessaging->StartListenerThread()) NoGo = 1;
      MgmOfsMessaging->SetLogId("MgmOfsVstMessaging");

      if ((!MgmOfsVstMessaging) || (MgmOfsVstMessaging->IsZombie()))
      {
        Eroute.Emsg("Config", "cannot create vst messaging object(thread)");
        return NoGo;
      }
    }

#ifdef HAVE_ZMQ
    //-------------------------------------------
    // create the ZMQ processor
    zMQ = new ZMQ("tcp://*:5555");
    if (!zMQ || zMQ->IsZombie())
    {
      Eroute.Emsg("Config", "cannto start ZMQ processor");
      return 1;
    }
#endif

    ObjectManager.CreateSharedHash("/eos/*", "/eos/*/fst");
    ObjectManager.HashMutex.LockRead();
    hash = ObjectManager.GetHash("/eos/*");


    ObjectManager.HashMutex.UnLockRead();

    XrdOucString dumperfile = MgmMetaLogDir;
    dumperfile += "/so.mgm.dump";

    ObjectManager.StartDumper(dumperfile.c_str());
    ObjectManager.SetAutoReplyQueueDerive(true);
  }

  {
    // hook to the appropiate config file
    XrdOucString stdOut;
    XrdOucString stdErr;
    MgmMaster.ApplyMasterConfig(stdOut, stdErr, 0);
  }


  /*
  if (MgmConfigAutoLoad.length()) {
    eos_info("autoload config=%s", MgmConfigAutoLoad.c_str());
    XrdOucString configloader = "mgm.config.file=";
    configloader += MgmConfigAutoLoad;
    XrdOucEnv configenv(configloader.c_str());
    XrdOucString stdErr="";
    if (!ConfEngine->LoadConfig(configenv, stdErr)) {
      eos_crit("Unable to auto-load config %s - fix your configuration file!", MgmConfigAutoLoad.c_str());
      eos_crit("%s\n", stdErr.c_str());
      return 1;
    } else {
      eos_info("Successful auto-load config %s", MgmConfigAutoLoad.c_str());
    }
  }
   */

  if (!MgmRedirector)
  {
    if (ErrorLog)
    {
      // run the error log console
      XrdOucString errorlogkillline = "pkill -9 -f \"eos -b console log _MGMID_\"";
      int rrc = system(errorlogkillline.c_str());
      if (WEXITSTATUS(rrc))
      {
        eos_info("%s returned %d", errorlogkillline.c_str(), rrc);
      }

      XrdOucString errorlogline = "eos -b console log _MGMID_ >& /dev/null &";
      rrc = system(errorlogline.c_str());
      if (WEXITSTATUS(rrc))
      {
        eos_info("%s returned %d", errorlogline.c_str(), rrc);
      }
    }

    eos_info("starting file view loader thread");
    if ((XrdSysThread::Run(&tid, XrdMgmOfs::StaticInitializeFileView, static_cast<void *> (this),
                           0, "File View Loader")))
    {
      eos_crit("cannot start file view loader");
      NoGo = 1;
    }
  }
#ifdef EOS_INSTRUMENTED_RWMUTEX
  eos::common::RWMutex::EstimateLatenciesAndCompensation();
  FsView::gFsView.ViewMutex.SetDebugName("FsView");
  FsView::gFsView.ViewMutex.SetTiming(false);
  FsView::gFsView.ViewMutex.SetSampling(true, 0.01);
  Quota::gQuotaMutex.SetDebugName("QuotaView");
  Quota::gQuotaMutex.SetTiming(false);
  Quota::gQuotaMutex.SetSampling(true, 0.01);
  gOFS->eosViewRWMutex.SetDebugName("eosView");
  gOFS->eosViewRWMutex.SetTiming(false);
  gOFS->eosViewRWMutex.SetSampling(true, 0.01);
  std::vector<eos::common::RWMutex*> order;
  order.push_back(&FsView::gFsView.ViewMutex);
  order.push_back(&Quota::gQuotaMutex);
  order.push_back(&gOFS->eosViewRWMutex);
  eos::common::RWMutex::AddOrderRule("Eos Mgm Mutexes", order);
#endif

  eos_info("starting statistics thread");
  if ((XrdSysThread::Run(&stats_tid, XrdMgmOfs::StartMgmStats, static_cast<void *> (this),
                         0, "Statistics Thread")))
  {
    eos_crit("cannot start statistics thread");
    NoGo = 1;
  }


  if (!MgmRedirector)
  {
    eos_info("starting fs listener thread");
    if ((XrdSysThread::Run(&fsconfiglistener_tid, XrdMgmOfs::StartMgmFsConfigListener, static_cast<void *> (this),
                           0, "FsListener Thread")))
    {
      eos_crit("cannot start fs listener thread");
      NoGo = 1;
    }

  }

  // initialize the transfer database
  if (!gTransferEngine.Init("/var/eos/tx"))
  {
    eos_crit("cannot intialize transfer database");
    NoGo = 1;
  }

  // create the 'default' quota space which is needed if quota is disabled!
  {
    eos::common::RWMutexReadLock qLock(Quota::gQuotaMutex);
    if (!Quota::GetSpaceQuota("default"))
    {
      eos_crit("failed to get default quota space");
    }
  }

  // start the Httpd if available
  if (!gOFS->Httpd.Start())
  {
    eos_warning("msg=\"cannot start httpd darmon\"");
  }

  // start the Egroup fetching
  if (!gOFS->EgroupRefresh.Start())
  {
    eos_warning("msg=\"cannot start egroup thread\"");
  }

  // start the LRU daemon
  if (!gOFS->LRUd.Start())
  {
    eos_warning("msg=\"cannot start LRU thread\"");
  }
  // start the recycler garbage collection thread on a master machine
  if ((MgmMaster.IsMaster()) && (!gOFS->Recycler.Start()))
  {
    eos_warning("msg=\"cannot start recycle thread\"");
  }

  // add all stat entries with 0
  gOFS->MgmStats.Add("HashSet", 0, 0, 0);
  gOFS->MgmStats.Add("HashSetNoLock", 0, 0, 0);
  gOFS->MgmStats.Add("HashGet", 0, 0, 0);

  gOFS->MgmStats.Add("ViewLockR", 0, 0, 0);
  gOFS->MgmStats.Add("ViewLockW", 0, 0, 0);
  gOFS->MgmStats.Add("NsLockR", 0, 0, 0);
  gOFS->MgmStats.Add("NsLockW", 0, 0, 0);
  gOFS->MgmStats.Add("QuotaLockR", 0, 0, 0);
  gOFS->MgmStats.Add("QuotaLockW", 0, 0, 0);

  gOFS->MgmStats.Add("Access", 0, 0, 0);
  gOFS->MgmStats.Add("AdjustReplica", 0, 0, 0);
  gOFS->MgmStats.Add("AttrGet", 0, 0, 0);
  gOFS->MgmStats.Add("AttrLs", 0, 0, 0);
  gOFS->MgmStats.Add("AttrRm", 0, 0, 0);
  gOFS->MgmStats.Add("AttrSet", 0, 0, 0);
  gOFS->MgmStats.Add("Cd", 0, 0, 0);
  gOFS->MgmStats.Add("Checksum", 0, 0, 0);
  gOFS->MgmStats.Add("Chmod", 0, 0, 0);
  gOFS->MgmStats.Add("Chown", 0, 0, 0);
  gOFS->MgmStats.Add("Commit", 0, 0, 0);
  gOFS->MgmStats.Add("CommitFailedFid", 0, 0, 0);
  gOFS->MgmStats.Add("CommitFailedNamespace", 0, 0, 0);
  gOFS->MgmStats.Add("CommitFailedParameters", 0, 0, 0);
  gOFS->MgmStats.Add("CommitFailedUnlinked", 0, 0, 0);
  gOFS->MgmStats.Add("ConversionDone", 0, 0, 0);
  gOFS->MgmStats.Add("ConversionFailed", 0, 0, 0);
  gOFS->MgmStats.Add("CopyStripe", 0, 0, 0);
  gOFS->MgmStats.Add("DumpMd", 0, 0, 0);
  gOFS->MgmStats.Add("Drop", 0, 0, 0);
  gOFS->MgmStats.Add("DropStripe", 0, 0, 0);
  gOFS->MgmStats.Add("Exists", 0, 0, 0);
  gOFS->MgmStats.Add("Exists", 0, 0, 0);
  gOFS->MgmStats.Add("FileInfo", 0, 0, 0);
  gOFS->MgmStats.Add("FindEntries", 0, 0, 0);
  gOFS->MgmStats.Add("Find", 0, 0, 0);
  gOFS->MgmStats.Add("Fuse", 0, 0, 0);
  gOFS->MgmStats.Add("Fuse-Statvfs", 0, 0, 0);
  gOFS->MgmStats.Add("Fuse-Mkdir", 0, 0, 0);
  gOFS->MgmStats.Add("Fuse-Stat", 0, 0, 0);
  gOFS->MgmStats.Add("Fuse-Chmod", 0, 0, 0);
  gOFS->MgmStats.Add("Fuse-Chown", 0, 0, 0);
  gOFS->MgmStats.Add("Fuse-Access", 0, 0, 0);
  gOFS->MgmStats.Add("Fuse-Access", 0, 0, 0);
  gOFS->MgmStats.Add("Fuse-Checksum", 0, 0, 0);
  gOFS->MgmStats.Add("Fuse-XAttr", 0, 0, 0);
  gOFS->MgmStats.Add("Fuse-Utimes", 0, 0, 0);
  gOFS->MgmStats.Add("GetMdLocation", 0, 0, 0);
  gOFS->MgmStats.Add("GetMd", 0, 0, 0);
  gOFS->MgmStats.Add("Http-COPY", 0, 0, 0);
  gOFS->MgmStats.Add("Http-DELETE", 0, 0, 0);
  gOFS->MgmStats.Add("Http-GET", 0, 0, 0);
  gOFS->MgmStats.Add("Http-HEAD", 0, 0, 0);
  gOFS->MgmStats.Add("Http-LOCK", 0, 0, 0);
  gOFS->MgmStats.Add("Http-MKCOL", 0, 0, 0);
  gOFS->MgmStats.Add("Http-MOVE", 0, 0, 0);
  gOFS->MgmStats.Add("Http-OPTIONS", 0, 0, 0);
  gOFS->MgmStats.Add("Http-POST", 0, 0, 0);
  gOFS->MgmStats.Add("Http-PROPFIND", 0, 0, 0);
  gOFS->MgmStats.Add("Http-PROPPATCH", 0, 0, 0);
  gOFS->MgmStats.Add("Http-PUT", 0, 0, 0);
  gOFS->MgmStats.Add("Http-TRACE", 0, 0, 0);
  gOFS->MgmStats.Add("Http-UNLOCK", 0, 0, 0);
  gOFS->MgmStats.Add("IdMap", 0, 0, 0);
  gOFS->MgmStats.Add("Ls", 0, 0, 0);
  gOFS->MgmStats.Add("LRUFind", 0, 0, 0);
  gOFS->MgmStats.Add("MarkDirty", 0, 0, 0);
  gOFS->MgmStats.Add("MarkClean", 0, 0, 0);
  gOFS->MgmStats.Add("Mkdir", 0, 0, 0);
  gOFS->MgmStats.Add("Motd", 0, 0, 0);
  gOFS->MgmStats.Add("MoveStripe", 0, 0, 0);
  gOFS->MgmStats.Add("OpenDir", 0, 0, 0);
  gOFS->MgmStats.Add("OpenDir-Entry", 0, 0, 0);
  gOFS->MgmStats.Add("OpenFailedCreate", 0, 0, 0);
  gOFS->MgmStats.Add("OpenFailedENOENT", 0, 0, 0);
  gOFS->MgmStats.Add("OpenFailedExists", 0, 0, 0);
  gOFS->MgmStats.Add("OpenFailedHeal", 0, 0, 0);
  gOFS->MgmStats.Add("OpenFailedPermission", 0, 0, 0);
  gOFS->MgmStats.Add("OpenFailedQuota", 0, 0, 0);
  gOFS->MgmStats.Add("OpenFailedNoUpdate", 0, 0, 0);
  gOFS->MgmStats.Add("OpenFailedReconstruct", 0, 0, 0);
  gOFS->MgmStats.Add("OpenFileOffline", 0, 0, 0);
  gOFS->MgmStats.Add("OpenProc", 0, 0, 0);
  gOFS->MgmStats.Add("OpenRead", 0, 0, 0);
  gOFS->MgmStats.Add("OpenShared", 0, 0, 0);
  gOFS->MgmStats.Add("OpenStalledHeal", 0, 0, 0);
  gOFS->MgmStats.Add("OpenStalled", 0, 0, 0);
  gOFS->MgmStats.Add("OpenStalled", 0, 0, 0);
  gOFS->MgmStats.Add("Open", 0, 0, 0);
  gOFS->MgmStats.Add("OpenWriteCreate", 0, 0, 0);
  gOFS->MgmStats.Add("OpenWriteTruncate", 0, 0, 0);
  gOFS->MgmStats.Add("OpenWrite", 0, 0, 0);
  gOFS->MgmStats.Add("ReadLink", 0, 0, 0);
  gOFS->MgmStats.Add("Recycle", 0, 0, 0);
  gOFS->MgmStats.Add("ReplicaFailedSize", 0, 0, 0);
  gOFS->MgmStats.Add("ReplicaFailedChecksum", 0, 0, 0);
  gOFS->MgmStats.Add("Redirect", 0, 0, 0);
  gOFS->MgmStats.Add("RedirectR", 0, 0, 0);
  gOFS->MgmStats.Add("RedirectW", 0, 0, 0);
  gOFS->MgmStats.Add("RedirectR-Master", 0, 0, 0);
  gOFS->MgmStats.Add("RedirectENOENT", 0, 0, 0);
  gOFS->MgmStats.Add("RedirectENONET", 0, 0, 0);
  gOFS->MgmStats.Add("Rename", 0, 0, 0);
  gOFS->MgmStats.Add("RmDir", 0, 0, 0);
  gOFS->MgmStats.Add("Rm", 0, 0, 0);
  gOFS->MgmStats.Add("Schedule2Drain", 0, 0, 0);
  gOFS->MgmStats.Add("Schedule2Balance", 0, 0, 0);
  gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 0);
  gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 0);
  gOFS->MgmStats.Add("Scheduled2Balance", 0, 0, 0);
  gOFS->MgmStats.Add("Scheduled2Drain", 0, 0, 0);
  gOFS->MgmStats.Add("Schedule2Delete", 0, 0, 0);
  gOFS->MgmStats.Add("Scheduled2Delete", 0, 0, 0);
  gOFS->MgmStats.Add("SendResync", 0, 0, 0);
  gOFS->MgmStats.Add("Stall", 0, 0, 0);
  gOFS->MgmStats.Add("Stat", 0, 0, 0);
  gOFS->MgmStats.Add("Symlink", 0, 0, 0);
  gOFS->MgmStats.Add("Touch", 0, 0, 0);
  gOFS->MgmStats.Add("TxState", 0, 0, 0);
  gOFS->MgmStats.Add("Truncate", 0, 0, 0);
  gOFS->MgmStats.Add("VerifyStripe", 0, 0, 0);
  gOFS->MgmStats.Add("Version", 0, 0, 0);
  gOFS->MgmStats.Add("Versioning", 0, 0, 0);
  gOFS->MgmStats.Add("WhoAmI", 0, 0, 0);

  // set IO accounting file
  XrdOucString ioaccounting = MgmMetaLogDir;
  ioaccounting += "/iostat.";
  ioaccounting += HostName;
  ioaccounting += ".dump";

  eos_notice("Setting IO dump store file to %s", ioaccounting.c_str());
  if (!gOFS->IoStats.SetStoreFileName(ioaccounting.c_str()))
  {
    eos_warning("couldn't load anything from the io stat dump file %s", ioaccounting.c_str());
  }
  else
  {
    eos_notice("loaded io stat dump file %s", ioaccounting.c_str());
  }
  // start IO ciruclate thread
  gOFS->IoStats.StartCirculate();


  if (!MgmRedirector)
  {
    if (hash)
    {
      // ask for a broadcast from fst's
      hash->BroadCastRequest("/eos/*/fst");
    }
  }

  if (!getenv("EOS_NO_SHUTDOWN"))
  {
    // add shutdown handler
    (void) signal(SIGINT, xrdmgmofs_shutdown);
    (void) signal(SIGTERM, xrdmgmofs_shutdown);
    (void) signal(SIGQUIT, xrdmgmofs_shutdown);

    // add SEGV handler
    if (!getenv("EOS_NO_STACKTRACE"))
    {
      (void) signal(SIGSEGV, xrdmgmofs_stacktrace);
      (void) signal(SIGABRT, xrdmgmofs_stacktrace);
      (void) signal(SIGBUS, xrdmgmofs_stacktrace);
    }
  }

  XrdSysTimer sleeper;
  sleeper.Wait(200);

  return NoGo;
}
/*----------------------------------------------------------------------------*/
