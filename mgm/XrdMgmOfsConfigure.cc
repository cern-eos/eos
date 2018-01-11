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
#include <sys/fsuid.h>
/*----------------------------------------------------------------------------*/
#include "authz/XrdCapability.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsTrace.hh"
#include "mgm/txengine/TransferEngine.hh"
#include "mgm/Quota.hh"
#include "mgm/Access.hh"
#include "mgm/Recycle.hh"
#include "mgm/drain/Drainer.hh"
#include "mgm/FileConfigEngine.hh"
#include "mgm/VstMessaging.hh"
#include "mgm/Egroup.hh"
#include "mgm/GeoTreeEngine.hh"
#include "mgm/http/HttpServer.hh"
#include "mgm/ZMQ.hh"
#include "mgm/Iostat.hh"
#ifdef HAVE_QCLIENT
#include "mgm/RedisConfigEngine.hh"
#endif
#include "common/plugin_manager/PluginManager.hh"
#include "common/CommentLog.hh"
#include "common/ZMQ.hh"
#include "common/Path.hh"
#include "namespace/interface/IChLogFileMDSvc.hh"
#include "namespace/interface/IChLogContainerMDSvc.hh"
#include "namespace/interface/IView.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdSys/XrdSysDNS.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysPlugin.hh"
/*----------------------------------------------------------------------------*/
extern XrdOucTrace gMgmOfsTrace;
extern void xrdmgmofs_shutdown(int sig);
extern void xrdmgmofs_stacktrace(int sig);

/*----------------------------------------------------------------------------*/

USE_EOSMGMNAMESPACE

//------------------------------------------------------------------------------
// Static method used to start the FileView initialization thread
//------------------------------------------------------------------------------
void*
XrdMgmOfs::StaticInitializeFileView(void* arg)
{
  return reinterpret_cast<XrdMgmOfs*>(arg)->InitializeFileView();
}

//------------------------------------------------------------------------------
// Method ran by the FileView initialization thread
//------------------------------------------------------------------------------
void*
XrdMgmOfs::InitializeFileView()
{
  {
    XrdSysMutexHelper lock(InitializationMutex);
    Initialized = kBooting;
    InitializationTime = time(0);
    RemoveStallRuleAfterBoot = false;
    BootFileId = 0;
  }
  time_t tstart = time(0);
  std::string oldstallrule = "";
  std::string oldstallcomment = "";
  bool oldstallglobal = false;
  // set the client stall
  {
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

    if (Access::gStallRules.count(std::string("*"))) {
      if (!RemoveStallRuleAfterBoot) {
        oldstallrule = Access::gStallRules[std::string("*")];
        oldstallcomment = Access::gStallComment[std::string("*")];
        oldstallglobal = Access::gStallGlobal;
      } else {
        RemoveStallRuleAfterBoot = false;
      }
    }

    Access::gStallRules[std::string("*")] = "100";
    Access::gStallGlobal = true;
    Access::gStallComment[std::string("*")] = "namespace is booting";
  }
  eos_notice("starting eos file view initialize2");

  try {
    time_t t1 = time(0);
    eosView->initialize2();
    time_t t2 = time(0);
    {
      eos_notice("eos file view after initialize2");
      eos::common::RWMutexWriteLock view_lock(eosViewRWMutex);
      eos_notice("starting eos file view initialize3");
      eosView->initialize3();
      time_t t3 = time(0);
      eos_notice("eos file view initialize2: %d seconds", t2 - t1);
      eos_notice("eos file view initialize3: %d seconds", t3 - t2);
      BootFileId = gOFS->eosFileService->getFirstFreeId();

      if (MgmMaster.IsMaster()) {
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
        std::shared_ptr<eos::IFileMD> fmd;

        try {
          fmd = eosView->getFile(procpathwhoami.c_str());
          fmd.reset();
        } catch (eos::MDException& e) {
          fmd = eosView->createFile(procpathwhoami.c_str(), 0, 0);
        }

        if (fmd) {
          fmd->setSize(4096);
          eosView->updateFileStore(fmd.get());
        }

        try {
          fmd = eosView->getFile(procpathwho.c_str());
          fmd.reset();
        } catch (eos::MDException& e) {
          fmd = eosView->createFile(procpathwho.c_str(), 0, 0);
        }

        if (fmd) {
          fmd->setSize(4096);
          eosView->updateFileStore(fmd.get());
        }

        try {
          fmd = eosView->getFile(procpathquota.c_str());
          fmd.reset();
        } catch (eos::MDException& e) {
          fmd = eosView->createFile(procpathquota.c_str(), 0, 0);
        }

        if (fmd) {
          fmd->setSize(4096);
          eosView->updateFileStore(fmd.get());
        }

        try {
          fmd = eosView->getFile(procpathreconnect.c_str());
          fmd.reset();
        } catch (eos::MDException& e) {
          fmd = eosView->createFile(procpathreconnect.c_str(), 0, 0);
        }

        if (fmd) {
          fmd->setSize(4096);
          eosView->updateFileStore(fmd.get());
        }

        try {
          fmd = eosView->getFile(procpathmaster.c_str());
          fmd.reset();
        } catch (eos::MDException& e) {
          fmd = eosView->createFile(procpathmaster.c_str(), 0, 0);
        }

        if (fmd) {
          fmd->setSize(4096);
          eosView->updateFileStore(fmd.get());
        }

        {
          XrdSysMutexHelper lock(InitializationMutex);
          Initialized = kBooted;
          eos_static_alert("msg=\"namespace booted (as master)\"");
        }
      }
    }

    if (!MgmMaster.IsMaster()) {
      eos_static_info("msg=\"starting slave listener\"");
      struct stat f_buf;
      struct stat c_buf;
      f_buf.st_size = 0;
      c_buf.st_size = 0;

      if (::stat(gOFS->MgmNsFileChangeLogFile.c_str(), &f_buf) == -1) {
        eos_static_alert("msg=\"failed to stat the file changlog\"");
        Initialized = kFailed;
        return 0;
      }

      if (::stat(gOFS->MgmNsDirChangeLogFile.c_str(), &c_buf) == -1) {
        eos_static_alert("msg=\"failed to stat the container changlog\"");
        Initialized = kFailed;
        return 0;
      }

      eos::IChLogContainerMDSvc* eos_chlog_dirsvc =
        dynamic_cast<eos::IChLogContainerMDSvc*>(gOFS->eosDirectoryService);
      eos::IChLogFileMDSvc* eos_chlog_filesvc =
        dynamic_cast<eos::IChLogFileMDSvc*>(gOFS->eosFileService);

      if (eos_chlog_dirsvc && eos_chlog_filesvc) {
        eos_chlog_filesvc->startSlave();
        eos_chlog_dirsvc->startSlave();

        // Wait that the follower reaches the offset seen now
        while ((eos_chlog_filesvc->getFollowOffset() < (uint64_t) f_buf.st_size) ||
               (eos_chlog_dirsvc->getFollowOffset() < (uint64_t) c_buf.st_size) ||
               (eos_chlog_filesvc->getFollowPending())) {
          XrdSysTimer sleeper;
          sleeper.Wait(5000);
          eos_static_info("msg=\"waiting for the namespace to reach the follow "
                          "point\" is-file-offset=%llu, target-file-offset=%llu, "
                          "is-dir-offset=%llu, target-dir-offset=%llu, files-pending=%llu",
                          eos_chlog_filesvc->getFollowOffset(), (uint64_t) f_buf.st_size,
                          eos_chlog_dirsvc->getFollowOffset(), (uint64_t) c_buf.st_size,
                          eos_chlog_filesvc->getFollowPending());
        }
      }

      {
        XrdSysMutexHelper lock(InitializationMutex);
        Initialized = kBooted;
        eos_static_alert("msg=\"namespace booted (as slave)\"");
      }
    }

    time_t tstop = time(0);
    MgmMaster.MasterLog(eos_notice("eos namespace file loading stopped after %d "
                                   "seconds", (tstop - tstart)));
    {
      eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

      if (oldstallrule.length()) {
        Access::gStallRules[std::string("*")] = oldstallrule;
      } else {
        Access::gStallRules.erase(std::string("*"));
      }

      if (oldstallcomment.length()) {
        Access::gStallComment[std::string("*")] = oldstallcomment;
      } else {
        Access::gStallComment.erase(std::string("*"));
      }

      Access::gStallGlobal = oldstallglobal;
    }
  } catch (eos::MDException& e) {
    {
      XrdSysMutexHelper lock(InitializationMutex);
      Initialized = kFailed;
    }
    time_t tstop = time(0);
    errno = e.getErrno();
    eos_crit("namespace file loading initialization failed after %d seconds",
             (tstop - tstart));
    eos_crit("initialization returnd ec=%d %s\n", e.getErrno(),
             e.getMessage().str().c_str());
  }

  {
    XrdSysMutexHelper lock(InitializationMutex);
    InitializationTime = (time(0) - InitializationTime);

    // grab process status after boot
    if (!eos::common::LinuxStat::GetStat(LinuxStatsStartup)) {
      eos_crit("failed to grab /proc/self/stat information");
    }
  }

  // Load all the quota nodes from the namespace
  Quota::LoadNodes();
  // Force refresh the GeoTreeEngine info at the end of the mgm booting process
  gGeoTreeEngine.forceRefresh();
  return 0;
}

void XrdMgmOfs::StartHeapProfiling(int sig)
{
  if (!gOFS->mJeMallocHandler.CanProfile()) {
    eos_static_crit("cannot run heap profiling");
    return;
  }

  if (gOFS->mJeMallocHandler.StartProfiling()) {
    eos_static_warning("started jemalloc heap profiling");
  } else {
    eos_static_warning("failed to start jemalloc heap profiling");
  }
}

void XrdMgmOfs::StopHeapProfiling(int sig)
{
  if (!gOFS->mJeMallocHandler.CanProfile()) {
    eos_static_crit("cannot run heap profiling");
    return;
  }

  if (gOFS->mJeMallocHandler.StopProfiling()) {
    eos_static_warning("stopped jemalloc heap profiling");
  } else {
    eos_static_warning("failed to stop jemalloc heap profiling");
  }
}

void XrdMgmOfs::DumpHeapProfile(int sig)
{
  if (!gOFS->mJeMallocHandler.ProfRunning()) {
    eos_static_crit("profiling is not running");
    return;
  }

  if (gOFS->mJeMallocHandler.DumpProfile()) {
    eos_static_warning("dumped heap profile");
  } else {
    eos_static_warning("failed to sump heap profile");
  }
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::Configure(XrdSysError& Eroute)
{
  // the process run's as root, but acts on the filesystem as daemon
  char* var;
  const char* val;
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
  UTF8 = getenv("EOS_UTF8") ? true : false;
  Shutdown = false;
  setenv("XrdSecPROTOCOL", "sss", 1);
  Eroute.Say("=====> mgmofs enforces SSS authentication for XROOT clients");
  MgmOfsTargetPort = "1094";
  MgmOfsName = "";
  MgmOfsAlias = "";
  MgmOfsBrokerUrl = "root://localhost:1097//eos/";
  MgmOfsInstanceName = "testinstance";
  MgmOfsConfigEngineType = "file";
  MgmOfsConfigEngineRedisHost = "localhost";
  MgmOfsConfigEngineRedisPort = 6379;
  MgmOfsCentralDraining = false;
  MgmConfigDir = "";
  MgmMetaLogDir = "";
  MgmTxDir = "";
  MgmAuthDir = "";
  MgmArchiveDir = "";
  MgmHealMap.set_deleted_key(0);
  IoReportStorePath = "/var/tmp/eos/report";
  MgmOfsVstBrokerUrl = "";
  MgmArchiveDstUrl = "";
  MgmArchiveSvcClass = "default";
  eos::common::StringConversion::InitLookupTables();

  if (getenv("EOS_VST_BROKER_URL")) {
    MgmOfsVstBrokerUrl = getenv("EOS_VST_BROKER_URL");
  }

  if (getenv("EOS_ARCHIVE_URL")) {
    MgmArchiveDstUrl = getenv("EOS_ARCHIVE_URL");

    // Make sure it ends with a '/'
    if (MgmArchiveDstUrl[MgmArchiveDstUrl.length() - 1] != '/') {
      MgmArchiveDstUrl += '/';
    }
  }

  if (getenv("EOS_ARCHIVE_SVCCLASS")) {
    MgmArchiveSvcClass = getenv("EOS_ARCHIVE_SVCCLASS");
  }

  // configure heap profiling if any
  if (mJeMallocHandler.JeMallocLoaded()) {
    eos_warning("jemalloc is loaded!");
    Eroute.Say("jemalloc is loaded!");

    if (mJeMallocHandler.CanProfile()) {
      Eroute.Say(mJeMallocHandler.ProfRunning() ?
                 "jemalloc heap profiling enabled and running" :
                 "jemalloc heap profiling enabled and NOT running");
      eos_warning("jemalloc heap profiling enabled and %srunning. will start "
                  "running on signal 40 and will stop running on signal 41",
                  mJeMallocHandler.ProfRunning() ? "" : "NOT ");
    } else {
      eos_warning("jemalloc heap profiling is disabled");
      Eroute.Say("jemalloc heap profiling is disabled");
    }
  } else {
    eos_warning("jemalloc is NOT loaded!");
    Eroute.Say("jemalloc is NOT loaded!");
  }

  if (SIGRTMIN <= 40 && 42 <= SIGRTMAX) {
    signal(40, StartHeapProfiling);
    signal(41, StopHeapProfiling);
    signal(42, DumpHeapProfile);
  } else {
    eos_static_warning("cannot install signal handlers for heap profiling as "
                       "ports 40, 41, 42 don't fit in the allowed realtime "
                       "dignal range. SIGRTMIN=%d  SIGRTMAX=%d",
                       (int)SIGRTMIN, (int)SIGRTMAX);
  }

  // Create and own the output cache directory or clean it up if it exists -
  // this is used to store temporary results for commands like find, backup
  // or achive
  struct stat dir_stat;

  if (!::stat("/tmp/eos.mgm/", &dir_stat) && S_ISDIR(dir_stat.st_mode)) {
    XrdOucString systemline = "rm -rf /tmp/eos.mgm/* >& /dev/null &";
    int rrc = system(systemline.c_str());

    if (WEXITSTATUS(rrc)) {
      eos_err("%s returned %d", systemline.c_str(), rrc);
    }
  } else {
    eos::common::Path out_dir("/tmp/eos.mgm/empty");

    if (!out_dir.MakeParentPath(S_IRWXU)) {
      eos_err("Unable to create temporary output file directory /tmp/eos.mgm/");
      Eroute.Emsg("Config", errno, "create temporary outputfile"
                  " directory /tmp/eos.mgm/");
      NoGo = 1;
      return NoGo;
      ;
    }

    // Own the directory by daemon
    if (::chown(out_dir.GetParentPath(), 2, 2)) {
      eos_err("Unable to own temporary outputfile directory %s",
              out_dir.GetParentPath());
      Eroute.Emsg("Config", errno, "own outputfile directory /tmp/eos.mgm/");
      NoGo = 1;
      return NoGo;
      ;
    }
  }

  ErrorLog = true;
  bool ConfigAutoSave = false;
  MgmConfigAutoLoad = "";
  long myPort = 0;
  std::string ns_lib_path {""};

  if (getenv("XRDDEBUG")) {
    gMgmOfsTrace.What = TRACE_MOST | TRACE_debug;
  }

  {
    // borrowed from XrdOfs
    unsigned int myIPaddr = 0;
    char buff[256], *bp;
    int i;
    // Obtain port number we will be using
    //
    myPort = (bp = getenv("XRDPORT")) ? strtol(bp, (char**) 0, 10) : 0;
    // Establish our hostname and IPV4 address
    //
    char* errtext = 0;
    HostName = XrdSysDNS::getHostName(0, &errtext);

    if (!HostName || std::string(HostName) == "0.0.0.0") {
      return Eroute.Emsg("Config", errno, "cannot get hostname : %s", errtext);
    }

    if (!XrdSysDNS::Host2IP(HostName, &myIPaddr)) {
      myIPaddr = 0x7f000001;
    }

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

    if (XrdSysDNS::Host2IP(HostName, &ip)) {
      char buff[1024];
      XrdSysDNS::IP2String(ip, 0, buff, 1024);
      ManagerIp = buff;
      ManagerPort = myPort;
    } else {
      return Eroute.Emsg("Config", errno, "convert hostname to IP address", HostName);
    }

    Eroute.Say("=====> mgmofs.managerid: ", ManagerId.c_str(), "");
  }

  if (!ConfigFN || !*ConfigFN) {
    Eroute.Emsg("Config", "Configuration file not specified.");
  } else {
    // Try to open the configuration file.
    //
    if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0) {
      return Eroute.Emsg("Config", errno, "open config file", ConfigFN);
    }

    Config.Attach(cfgFD);
    // Now start reading records until eof.
    //
    XrdOucString nsin;
    XrdOucString nsout;

    while ((var = Config.GetMyFirstWord())) {
      if (!strncmp(var, "all.", 4)) {
        var += 4;

        if (!strcmp("role", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for all.role missing.");
            NoGo = 1;
          } else {
            XrdOucString lrole = val;

            if ((val = Config.GetWord())) {
              if (!strcmp(val, "if")) {
                if ((val = Config.GetWord())) {
                  if (!strcmp(val, HostName)) {
                    role = lrole;
                  }

                  if (!strcmp(val, HostPref)) {
                    role = lrole;
                  }
                }
              } else {
                role = lrole;
              }
            } else {
              role = lrole;
            }
          }
        }
      }

      if (!strncmp(var, "mgmofs.", 7)) {
        var += 7;

        if (!strcmp("fs", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for fs invalid.");
            NoGo = 1;
          } else {
            Eroute.Say("=====> mgmofs.fs: ", val, "");
            MgmOfsName = val;
          }
        }

        //added conf for ConfigEngine
        if (!strcmp("cfgtype", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for cfgtype invalid.");
            NoGo = 1;
          } else {
            Eroute.Say("=====> mgmofs.cfgtype: ", val, "");
            MgmOfsConfigEngineType = val;
          }
        }

        if (!strcmp("cfgredishost", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for cfgredishost invalid.");
            NoGo = 1;
          } else {
            Eroute.Say("=====> mgmofs.cfgredishost: ", val, "");
            MgmOfsConfigEngineRedisHost = val;
          }
        }

        if (!strcmp("cfgredisport", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for cfgredisport invalid.");
            NoGo = 1;
          } else {
            Eroute.Say("=====> mgmofs.cfgredisport: ", val, "");
            MgmOfsConfigEngineRedisPort = atoi(val);
          }
        }

        if (!strcmp("centraldraining", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for centraldraining invalid.");
            NoGo = 1;
          } else {
            Eroute.Say("=====> mgmofs.centraldraining: ", val, "");

            if ((!strcmp("true", val) || (!strcmp("1", val)))) {
              MgmOfsCentralDraining = true;
            }
          }
        }

        if (!strcmp("targetport", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for fs invalid.");
            NoGo = 1;
          } else {
            Eroute.Say("=====> mgmofs.targetport: ", val, "");
            MgmOfsTargetPort = val;
          }
        }

        if (!strcmp("capability", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config",
                        "argument 2 for capbility missing. Can be true/lazy/1 or false/0");
            NoGo = 1;
          } else {
            if ((!(strcmp(val, "true"))) || (!(strcmp(val, "1")))
                || (!(strcmp(val, "lazy")))) {
              IssueCapability = true;
            } else {
              if ((!(strcmp(val, "false"))) || (!(strcmp(val, "0")))) {
                IssueCapability = false;
              } else {
                Eroute.Emsg("Config",
                            "argument 2 for capbility invalid. Can be <true>/1 or <false>/0");
                NoGo = 1;
              }
            }
          }
        }

        if (!strcmp("broker", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config",
                        "argument 2 for broker missing. Should be URL like root://<host>/<queue>/");
            NoGo = 1;
          } else {
            if (getenv("EOS_BROKER_URL")) {
              MgmOfsBrokerUrl = getenv("EOS_BROKER_URL");
            } else {
              MgmOfsBrokerUrl = val;
            }
          }
        }

        if (!strcmp("instance", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config",
                        "argument 2 for instance missing. Should be the name of the EOS cluster");
            NoGo = 1;
          } else {
            if (getenv("EOS_INSTANCE_NAME")) {
              MgmOfsInstanceName = getenv("EOS_INSTANCE_NAME");
            } else {
              MgmOfsInstanceName = val;
            }
          }

          Eroute.Say("=====> mgmofs.instance : ", MgmOfsInstanceName.c_str(), "");
        }

        if (!strcmp("nslib", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "no namespace library path provided");
            NoGo = 1;
          } else {
            ns_lib_path = val;
          }

          Eroute.Say("=====> mgmofs.nslib : ", ns_lib_path.c_str());
        }

        if (!strcmp("qdbcluster", var)) {
          while ((val = Config.GetWord())) {
            mQdbCluster += val;
            mQdbCluster += " ";
          }

          Eroute.Say("=====> mgmofs.qdbcluster : ", mQdbCluster.c_str());
        }

        if (!strcmp("authlib", var)) {
          if ((!(val = Config.GetWord())) || (::access(val, R_OK))) {
            Eroute.Emsg("Config", "I cannot acccess you authorization library!");
            NoGo = 1;
          } else {
            AuthLib = val;
          }

          Eroute.Say("=====> mgmofs.authlib : ", AuthLib.c_str());
        }

        if (!strcmp("authorize", var)) {
          if ((!(val = Config.GetWord())) ||
              (strcmp("true", val) && strcmp("false", val) &&
               strcmp("1", val) && strcmp("0", val))) {
            Eroute.Emsg("Config", "argument 2 for authorize illegal or missing. "
                        "Must be <true>, <false>, <1> or <0>!");
            NoGo = 1;
          } else {
            if ((!strcmp("true", val) || (!strcmp("1", val)))) {
              authorize = true;
            }
          }

          if (authorize) {
            Eroute.Say("=====> mgmofs.authorize : true");
          } else {
            Eroute.Say("=====> mgmofs.authorize : false");
          }
        }

        if (!strcmp("errorlog", var)) {
          if ((!(val = Config.GetWord())) ||
              (strcmp("true", val) && strcmp("false", val) &&
               strcmp("1", val) && strcmp("0", val))) {
            Eroute.Emsg("Config", "argument 2 for errorlog illegal or missing. "
                        "Must be <true>, <false>, <1> or <0>!");
            NoGo = 1;
          } else {
            if ((!strcmp("true", val) || (!strcmp("1", val)))) {
              ErrorLog = true;
            } else {
              ErrorLog = false;
            }
          }

          if (ErrorLog) {
            Eroute.Say("=====> mgmofs.errorlog : true");
          } else {
            Eroute.Say("=====> mgmofs.errorlog : false");
          }
        }

        if (!strcmp("redirector", var)) {
          if ((!(val = Config.GetWord())) ||
              (strcmp("true", val) && strcmp("false", val) &&
               strcmp("1", val) && strcmp("0", val))) {
            Eroute.Emsg("Config", "argument 2 for redirector illegal or missing. "
                        "Must be <true>,<false>,<1> or <0>!");
            NoGo = 1;
          } else {
            if ((!strcmp("true", val) || (!strcmp("1", val)))) {
              MgmRedirector = true;
            } else {
              MgmRedirector = false;
            }
          }

          if (MgmRedirector) {
            Eroute.Say("=====> mgmofs.redirector : true");
          } else {
            Eroute.Say("=====> mgmofs.redirector : false");
          }
        }

        if (!strcmp("configdir", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for configdir invalid.");
            NoGo = 1;
          } else {
            MgmConfigDir = val;

            if (!MgmConfigDir.endswith("/")) {
              MgmConfigDir += "/";
            }
          }
        }

        if (!strcmp("archivedir", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for archivedir invalid.");
            NoGo = 1;
          } else {
            MgmArchiveDir = val;

            if (!MgmArchiveDir.endswith("/")) {
              MgmArchiveDir += "/";
            }
          }
        }

        if (!strcmp("autosaveconfig", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config",
                        "argument 2 for autosaveconfig missing. Can be true/1 or false/0");
            NoGo = 1;
          } else {
            if ((!(strcmp(val, "true"))) || (!(strcmp(val, "1")))) {
              ConfigAutoSave = true;
            } else {
              if ((!(strcmp(val, "false"))) || (!(strcmp(val, "0")))) {
                ConfigAutoSave = false;
              } else {
                Eroute.Emsg("Config",
                            "argument 2 for autosaveconfig invalid. Can be <true>/1 or <false>/0");
                NoGo = 1;
              }
            }
          }
        }

        if (!strcmp("autoloadconfig", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for autoloadconfig invalid.");
            NoGo = 1;
          } else {
            MgmConfigAutoLoad = val;
          }
        }

        if (!strcmp("alias", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for alias missing.");
            NoGo = 1;
          } else {
            MgmOfsAlias = val;
          }
        }

        if (!strcmp("metalog", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument 2 for metalog missing");
            NoGo = 1;
          } else {
            MgmMetaLogDir = val;
            // just try to create it in advance
            XrdOucString makeit = "mkdir -p ";
            makeit += MgmMetaLogDir;
            int src = system(makeit.c_str());

            if (src) {
              eos_err("%s returned %d", makeit.c_str(), src);
            }

            XrdOucString chownit = "chown -R daemon ";
            chownit += MgmMetaLogDir;
            src = system(chownit.c_str());

            if (src) {
              eos_err("%s returned %d", chownit.c_str(), src);
            }

            if (::access(MgmMetaLogDir.c_str(), W_OK | R_OK | X_OK)) {
              Eroute.Emsg("Config", "cannot acccess the meta data changelog "
                          "directory for r/w!", MgmMetaLogDir.c_str());
              NoGo = 1;
            } else {
              Eroute.Say("=====> mgmofs.metalog: ", MgmMetaLogDir.c_str(), "");
            }
          }
        }

        if (!strcmp("txdir", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument 2 for txdir missing");
            NoGo = 1;
          } else {
            MgmTxDir = val;
            // just try to create it in advance
            XrdOucString makeit = "mkdir -p ";
            makeit += MgmTxDir;
            int src = system(makeit.c_str());

            if (src) {
              eos_err("%s returned %d", makeit.c_str(), src);
            }

            XrdOucString chownit = "chown -R daemon ";
            chownit += MgmTxDir;
            src = system(chownit.c_str());

            if (src) {
              eos_err("%s returned %d", chownit.c_str(), src);
            }

            if (::access(MgmTxDir.c_str(), W_OK | R_OK | X_OK)) {
              Eroute.Emsg("Config", "cannot acccess the transfer directory for r/w:",
                          MgmTxDir.c_str());
              NoGo = 1;
            } else {
              Eroute.Say("=====> mgmofs.txdir:   ", MgmTxDir.c_str(), "");
            }
          }
        }

        if (!strcmp("authdir", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument 2 for authdir missing");
            NoGo = 1;
          } else {
            MgmAuthDir = val;
            // just try to create it in advance
            XrdOucString makeit = "mkdir -p ";
            makeit += MgmAuthDir;
            int src = system(makeit.c_str());

            if (src) {
              eos_err("%s returned %d", makeit.c_str(), src);
            }

            XrdOucString chownit = "chown -R daemon ";
            chownit += MgmAuthDir;
            src = system(chownit.c_str());

            if (src) {
              eos_err("%s returned %d", chownit.c_str(), src);
            }

            if ((src = ::chmod(MgmAuthDir.c_str(), S_IRUSR | S_IWUSR | S_IXUSR))) {
              eos_err("chmod 700 %s returned %d", MgmAuthDir.c_str(), src);
              NoGo = 1;
            }

            if (::access(MgmAuthDir.c_str(), W_OK | R_OK | X_OK)) {
              Eroute.Emsg("Config", "cannot acccess the authentication directory "
                          "for r/w:", MgmAuthDir.c_str());
              NoGo = 1;
            } else {
              Eroute.Say("=====> mgmofs.authdir:   ", MgmAuthDir.c_str(), "");
            }
          }
        }

        if (!strcmp("reportstorepath", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument 2 for reportstorepath missing");
            NoGo = 1;
          } else {
            IoReportStorePath = val;
            // just try to create it in advance
            XrdOucString makeit = "mkdir -p ";
            makeit += IoReportStorePath;
            int src = system(makeit.c_str());

            if (src) {
              eos_err("%s returned %d", makeit.c_str(), src);
            }

            XrdOucString chownit = "chown -R daemon ";
            chownit += IoReportStorePath;
            src = system(chownit.c_str());

            if (src) {
              eos_err("%s returned %d", chownit.c_str(), src);
            }

            if (::access(IoReportStorePath.c_str(), W_OK | R_OK | X_OK)) {
              Eroute.Emsg("Config", "cannot acccess the reportstore directory "
                          "for r/w:", IoReportStorePath.c_str());
              NoGo = 1;
            } else {
              Eroute.Say("=====> mgmofs.reportstorepath: ", IoReportStorePath.c_str(), "");
            }
          }
        }

        // Get the fst gateway hostname and port
        if (!strcmp("fstgw", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "fst gateway value not specified");
            NoGo = 1;
          } else {
            mFstGwHost = val;
            size_t pos = mFstGwHost.find(':');

            if (pos == std::string::npos) {
              // Use a default value if no port is specified
              mFstGwPort = 1094;
            } else {
              mFstGwPort = atoi(mFstGwHost.substr(pos + 1).c_str());
              mFstGwHost = mFstGwHost.erase(pos);
            }

            Eroute.Say("=====> mgmofs.fstgw: ", mFstGwHost.c_str(), ":",
                       std::to_string((long long int)mFstGwPort).c_str());
          }
        }

        if (!strcmp("trace", var)) {
          static struct traceopts {
            const char* opname;
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
          int i, neg, trval = 0, numopts = sizeof(tropts) / sizeof(struct traceopts);

          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "trace option not specified");
            close(cfgFD);
            return 1;
          }

          while (val) {
            Eroute.Say("=====> mgmofs.trace: ", val, "");

            if (!strcmp(val, "off")) {
              trval = 0;
            } else {
              if ((neg = (val[0] == '-' && val[1]))) {
                val++;
              }

              for (i = 0; i < numopts; i++) {
                if (!strcmp(val, tropts[i].opname)) {
                  if (neg) {
                    trval &= ~tropts[i].opval;
                  } else {
                    trval |= tropts[i].opval;
                  }

                  break;
                }
              }

              if (i >= numopts) {
                Eroute.Say("Config warning: ignoring invalid trace option '", val, "'.");
              }
            }

            val = Config.GetWord();
          }

          gMgmOfsTrace.What = trval;
        }

        if (!strcmp("auththreads", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for number of auth threads is invalid.");
            NoGo = 1;
          } else {
            Eroute.Say("=====> mgmofs.auththreads: ", val, "");
            mNumAuthThreads = atoi(val);
          }
        }

        // Configure frontend port number on which clients submit requests
        if (!strcmp("authport", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for frontend port invalid.");
            NoGo = 1;
          } else {
            Eroute.Say("=====> mgmofs.authport: ", val, "");
            mFrontendPort = atoi(val);
          }
        }
      }
    }

    close(cfgFD);
  }

  if (MgmRedirector) {
    Eroute.Say("=====> mgmofs.redirector : true");
  } else {
    Eroute.Say("=====> mgmofs.redirector : false");
  }

  if (!MgmOfsBrokerUrl.endswith("/")) {
    MgmOfsBrokerUrl += "/";
  }

  if (!MgmOfsBrokerUrl.endswith("//eos/")) {
    Eroute.Say("Config error: the broker url has to be of the form <root://<hostname>[:<port>]//");
    return 1;
  }

  if (MgmOfsVstBrokerUrl.length() && (!MgmOfsVstBrokerUrl.endswith("//eos/"))) {
    Eroute.Say("Config error: the vst broker url has to be of the form <root://<hostname>[:<port>]//");
    return 1;
  }

  if (!MgmConfigDir.length()) {
    Eroute.Say("Config error: configuration directory is not defined : mgm.configdir=</var/eos/config/>");
    return 1;
  }

  if (!MgmMetaLogDir.length()) {
    Eroute.Say("Config error: meta data log directory is not defined : mgm.metalog=</var/eos/md/>");
    return 1;
  }

  if (!MgmTxDir.length()) {
    Eroute.Say("Config error: transfer directory is not defined : mgm.txdir=</var/eos/tx/>");
    return 1;
  }

  if (!MgmAuthDir.length()) {
    Eroute.Say("Config error: auth directory is not defined: mgm.authdir=</var/eos/auth/>");
    return 1;
  }

  if (!MgmArchiveDir.length()) {
    Eroute.Say("Config notice: archive directory is not defined - archiving is disabled");
  }

  if (!ns_lib_path.empty()) {
    eos::common::PluginManager& pm = eos::common::PluginManager::GetInstance();
    pm.LoadByPath(ns_lib_path);
  }

  MgmOfsBroker = MgmOfsBrokerUrl;
  MgmDefaultReceiverQueue = MgmOfsBrokerUrl;
  MgmDefaultReceiverQueue += "*/fst";
  MgmOfsBrokerUrl += HostName;
  MgmOfsBrokerUrl += "/mgm";

  if (MgmOfsVstBrokerUrl.length()) {
    MgmOfsVstBrokerUrl += MgmOfsInstanceName;
    MgmOfsVstBrokerUrl += "/";
    MgmOfsVstBrokerUrl += HostName;
    MgmOfsVstBrokerUrl += "/vst";
  }

  MgmOfsQueue = "/eos/";
  MgmOfsQueue += ManagerId;
  MgmOfsQueue += "/mgm";
  // Setup the circular in-memory logging buffer
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  // Configure log-file fan out
  std::vector<std::string> lFanOutTags;
  lFanOutTags.push_back("Balancer");
  lFanOutTags.push_back("Converter");
  lFanOutTags.push_back("DrainJob");
  lFanOutTags.push_back("ZMQ");
  lFanOutTags.push_back("MetadataFlusher");
  lFanOutTags.push_back("Http");
  lFanOutTags.push_back("Master");
  lFanOutTags.push_back("Recycle");
  lFanOutTags.push_back("LRU");
  lFanOutTags.push_back("WFE");
  lFanOutTags.push_back("WFE::Job");
  lFanOutTags.push_back("GroupBalancer");
  lFanOutTags.push_back("GeoBalancer");
  lFanOutTags.push_back("GeoTreeEngine");
  lFanOutTags.push_back("#");
  // get the XRootD log directory
  char* logdir = 0;
  XrdOucEnv::Import("XRDLOGDIR", logdir);

  if (logdir) {
    for (size_t i = 0; i < lFanOutTags.size(); i++) {
      std::string lLogFile = logdir;
      lLogFile += "/";

      if (lFanOutTags[i] == "#") {
        lLogFile += "Clients";
      } else {
        lLogFile += lFanOutTags[i];
      }

      lLogFile += ".log";
      FILE* fp = fopen(lLogFile.c_str(), "a+");

      if (fp) {
        g_logging.AddFanOut(lFanOutTags[i].c_str(), fp);
      } else {
        fprintf(stderr, "error: failed to open sub-logfile=%s", lLogFile.c_str());
      }
    }
  }

  // Add some alias for the logging
  g_logging.AddFanOutAlias("HttpHandler", "Http");
  g_logging.AddFanOutAlias("HttpServer", "Http");
  g_logging.AddFanOutAlias("ProtocolHandler", "Http");
  g_logging.AddFanOutAlias("S3", "Http");
  g_logging.AddFanOutAlias("S3Store", "Http");
  g_logging.AddFanOutAlias("WebDAV", "Http");
  g_logging.AddFanOutAlias("PropFindResponse", "Http");
  g_logging.AddFanOutAlias("WebDAVHandler", "Http");
  g_logging.AddFanOutAlias("WebDAVReponse", "Http");
  g_logging.AddFanOutAlias("S3Handler", "Http");
  g_logging.AddFanOutAlias("S3Store", "Http");
  g_logging.SetUnit(MgmOfsBrokerUrl.c_str());
  Eroute.Say("=====> mgmofs.broker : ", MgmOfsBrokerUrl.c_str(), "");
  XrdOucString ttybroadcastkillline = "pkill -9 -f \"eos-tty-broadcast\"";
  int rrc = system(ttybroadcastkillline.c_str());

  if (WEXITSTATUS(rrc)) {
    eos_info("%s returned %d", ttybroadcastkillline.c_str(), rrc);
  }

  if (getenv("EOS_TTY_BROADCAST_LISTEN_LOGFILE") &&
      getenv("EOS_TTY_BROADCAST_EGREP")) {
    XrdOucString ttybroadcastline = "eos-tty-broadcast ";
    ttybroadcastline += getenv("EOS_TTY_BROADCAST_LISTEN_LOGFILE");
    ttybroadcastline += " ";
    ttybroadcastline += getenv("EOS_TTY_BROADCAST_EGREP");
    ttybroadcastline += " >& /dev/null &";
    eos_info("%s\n", ttybroadcastline.c_str());
    rrc = system(ttybroadcastline.c_str());

    if (WEXITSTATUS(rrc)) {
      eos_info("%s returned %d", ttybroadcastline.c_str(), rrc);
    }
  }

  int pos1 = MgmDefaultReceiverQueue.find("//");
  int pos2 = MgmDefaultReceiverQueue.find("//", pos1 + 2);

  if (pos2 != STR_NPOS) {
    MgmDefaultReceiverQueue.erase(0, pos2 + 1);
  }

  Eroute.Say("=====> mgmofs.defaultreceiverqueue : ",
             MgmDefaultReceiverQueue.c_str(), "");
  // set our Eroute for XrdMqMessage
  XrdMqMessage::Eroute = *eDest;

  // check if mgmofsfs has been set

  if (!MgmOfsName.length())
    Eroute.Say("Config error: no mgmofs fs has been defined (mgmofs.fs /...)", "",
               "");
  else {
    Eroute.Say("=====> mgmofs.fs: ", MgmOfsName.c_str(), "");
  }

  if (ErrorLog) {
    Eroute.Say("=====> mgmofs.errorlog : enabled");
  } else {
    Eroute.Say("=====> mgmofs.errorlog : disabled");
  }

  // we need to specify this if the server was not started with the explicit manager option ... e.g. see XrdOfs
  Eroute.Say("=====> all.role: ", role.c_str(), "");

  if (role == "manager") {
    putenv((char*) "XRDREDIRECT=R");
  }

  if ((AuthLib != "") && (authorize)) {
    // load the authorization plugin
    XrdSysPlugin* myLib;
    XrdAccAuthorize * (*ep)(XrdSysLogger*, const char*, const char*);
    // Authorization comes from the library or we use the default
    //
    Authorization = XrdAccAuthorizeObject(Eroute.logger(), ConfigFN, 0);

    if (!(myLib = new XrdSysPlugin(&Eroute, AuthLib.c_str()))) {
      Eroute.Emsg("Config", "Failed to load authorization library!");
      NoGo = 1;
    } else {
      ep = (XrdAccAuthorize * (*)(XrdSysLogger*, const char*, const char*))
           (myLib->getPlugin("XrdAccAuthorizeObject"));

      if (!ep) {
        Eroute.Emsg("Config", "Failed to get authorization library plugin!");
        NoGo = 1;
      } else {
        Authorization = ep(Eroute.logger(), ConfigFN, 0);
      }
    }
  }

  if ((retc = Config.LastError())) {
    NoGo = Eroute.Emsg("Config", -retc, "read config file", ConfigFN);
  }

  Config.Close();
  XrdOucString unit = "mgm@";
  unit += ManagerId;
  g_logging.SetLogPriority(LOG_INFO);
  g_logging.SetUnit(unit.c_str());
  std::string filter =
    "Process,AddQuota,UpdateHint,Update,UpdateQuotaStatus,SetConfigValue,"
    "Deletion,GetQuota,PrintOut,RegisterNode,SharedHash,"
    "placeNewReplicas,accessReplicas,placeNewReplicasOneGroup,accessReplicasOneGroup,accessHeadReplicaMultipleGroup,listenFsChange,updateTreeInfo,updateAtomicPenalties,updateFastStructures";
  g_logging.SetFilter(filter.c_str());
  Eroute.Say("=====> setting message filter: Process,AddQuota,UpdateHint,Update"
             "UpdateQuotaStatus,SetConfigValue,Deletion,GetQuota,PrintOut,"
             "RegisterNode,SharedHash,"
             "placeNewReplicas,accessReplicas,placeNewReplicasOneGroup,accessReplicasOneGroup,accessHeadReplicaMultipleGroup,listenFsChange,updateTreeInfo,updateAtomicPenalties,updateFastStructures");
  // we automatically append the host name to the config dir
  MgmConfigDir += HostName;
  MgmConfigDir += "/";
  XrdOucString makeit = "mkdir -p ";
  makeit += MgmConfigDir;
  int src = system(makeit.c_str());

  if (src) {
    eos_err("%s returned %d", makeit.c_str(), src);
  }

  XrdOucString chownit = "chown -R daemon ";
  chownit += MgmConfigDir;
  src = system(chownit.c_str());

  if (src) {
    eos_err("%s returned %d", chownit.c_str(), src);
  }

  // check config directory access
  if (::access(MgmConfigDir.c_str(), W_OK | R_OK | X_OK)) {
    Eroute.Emsg("Config", "Cannot acccess the configuration directory for r/w!",
                MgmConfigDir.c_str());
    NoGo = 1;
  } else {
    Eroute.Say("=====> mgmofs.configdir: ", MgmConfigDir.c_str(), "");
  }

  // Start the config engine
  if (MgmOfsConfigEngineType == "file") {
    ConfEngine = new FileConfigEngine(MgmConfigDir.c_str());
  }

#ifdef HAVE_QCLIENT
  else if (MgmOfsConfigEngineType == "redis") {
    ConfEngine = new RedisConfigEngine(MgmConfigDir.c_str(),
                                       MgmOfsConfigEngineRedisHost.c_str(),
                                       MgmOfsConfigEngineRedisPort);
  }

#endif
  else {
    Eroute.Emsg("Config", "Invalid Config Engine Type!",
                MgmOfsConfigEngineType.c_str());
    NoGo = 1;
  }

  commentLog = new eos::common::CommentLog("/var/log/eos/mgm/logbook.log");

  // Create comment log
  if (commentLog && commentLog->IsValid()) {
    Eroute.Say("=====> comment log in /var/log/eos/mgm/logbook.log");
  } else {
    Eroute.Emsg("Config", "Cannot create/open the comment log file "
                "/var/log/eos/mgm/logbook.log");
    NoGo = 1;
  }

  if (ConfigAutoSave && (!getenv("EOS_AUTOSAVE_CONFIG"))) {
    Eroute.Say("=====> mgmofs.autosaveconfig: true", "");
    ConfEngine->SetAutoSave(true);
  } else {
    if (getenv("EOS_AUTOSAVE_CONFIG")) {
      eos_info("autosave config=%s", getenv("EOS_AUTOSAVE_CONFIG"));
      XrdOucString autosave = getenv("EOS_AUTOSAVE_CONFIG");

      if ((autosave == "1") || (autosave == "true")) {
        Eroute.Say("=====> mgmofs.autosaveconfig: true", "");
        ConfEngine->SetAutoSave(true);
      } else {
        Eroute.Say("=====> mgmofs.autosaveconfig: false", "");
        ConfEngine->SetAutoSave(false);
      }
    } else {
      Eroute.Say("=====> mgmofs.autosaveconfig: false", "");
    }
  }

  if (getenv("EOS_MGM_ALIAS")) {
    MgmOfsAlias = getenv("EOS_MGM_ALIAS");
  }

  // we don't put the alias we need call-back's to appear on our node
  if (MgmOfsAlias.length()) {
    Eroute.Say("=====> mgmofs.alias: ", MgmOfsAlias.c_str());
  }

  XrdOucString keytabcks = "unaccessible";
  // ----------------------------------------------------------
  // build the adler & sha1 checksum of the default keytab file
  // ----------------------------------------------------------
  int fd = ::open("/etc/eos.keytab", O_RDONLY);
  XrdOucString symkey = "";

  if (fd >= 0) {
    char buffer[65535];
    char keydigest[SHA_DIGEST_LENGTH + 1];
    SHA_CTX sha1;
    SHA1_Init(&sha1);
    size_t nread = ::read(fd, buffer, sizeof(buffer));

    if (nread > 0) {
      unsigned int adler;
      SHA1_Update(&sha1, (const char*) buffer, nread);
      adler = adler32(0L, Z_NULL, 0);
      adler = adler32(adler, (const Bytef*) buffer, nread);
      char sadler[1024];
      snprintf(sadler, sizeof(sadler) - 1, "%08x", adler);
      keytabcks = sadler;
    }

    SHA1_Final((unsigned char*) keydigest, &sha1);
    eos::common::SymKey::Base64Encode(keydigest, SHA_DIGEST_LENGTH, symkey);
    close(fd);
  }

  eos_notice("MGM_HOST=%s MGM_PORT=%ld VERSION=%s RELEASE=%s KEYTABADLER=%s "
             "SYMKEY=%s", HostName, myPort, VERSION, RELEASE, keytabcks.c_str(),
             symkey.c_str());

  if (!eos::common::gSymKeyStore.SetKey64(symkey.c_str(), 0)) {
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
  FsView::gFsView.SetConfigQueues(MgmConfigQueue.c_str(),
                                  NodeConfigQueuePrefix.c_str(),
                                  GroupConfigQueuePrefix.c_str(),
                                  SpaceConfigQueuePrefix.c_str());
  FsView::gFsView.SetConfigEngine(ConfEngine);
  ObjectNotifier.SetShareObjectManager(&ObjectManager);
  // we need to set the shared object manager to be used
  eos::common::GlobalConfig::gConfig.SetSOM(&ObjectManager);
  // set the object manager to listener only
  ObjectManager.EnableBroadCast(false);
  // setup the modifications which the fs listener thread is waiting for
  ObjectManager.SetDebug(false);

  if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(MgmConfigQueue.c_str(),
      "/eos/*/mgm")) {
    eos_crit("Cannot add global config queue %s\n", MgmConfigQueue.c_str());
  }

  if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(AllConfigQueue.c_str(),
      "/eos/*")) {
    eos_crit("Cannot add global config queue %s\n", AllConfigQueue.c_str());
  }

  if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(FstConfigQueue.c_str(),
      "/eos/*/fst")) {
    eos_crit("Cannot add global config queue %s\n", FstConfigQueue.c_str());
  }

  std::string out = "";
  eos::common::GlobalConfig::gConfig.PrintBroadCastMap(out);
  fprintf(stderr, "%s", out.c_str());

  // eventuall autoload a configuration
  if (getenv("EOS_AUTOLOAD_CONFIG")) {
    MgmConfigAutoLoad = getenv("EOS_AUTOLOAD_CONFIG");
  }

  XrdOucString instancepath = "/eos/";
  MgmProcPath = "/eos/";
  XrdOucString subpath = MgmOfsInstanceName;

  // Remove leading "eos" from the instance name when building the proc path for
  // "aesthetic" reasons.
  if (subpath.beginswith("eos")) {
    subpath.erase(0, 3);
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
  MgmProcWorkflowPath = MgmProcPath;
  MgmProcWorkflowPath += "/workflow";
  MgmProcLockPath = MgmProcPath;
  MgmProcLockPath += "/lock";
  MgmProcDelegationPath = MgmProcPath;
  MgmProcDelegationPath += "/delegation";
  Recycle::gRecyclingPrefix.insert(0, MgmProcPath.c_str());
  instancepath += subpath;
  // Initialize user mapping
  eos::common::Mapping::Init();

  // Initialize the master/slave class
  if (!MgmMaster.Init()) {
    return 1;
  }

  // Configure the meta data catalog
  eosViewRWMutex.SetBlocking(true);

  if (!MgmMaster.BootNamespace()) {
    return 1;
  }

  // Check the '/' directory
  std::shared_ptr<eos::IContainerMD> rootmd;

  try {
    rootmd = eosView->getContainer("/");
  } catch (const eos::MDException& e) {
    Eroute.Emsg("Config", "cannot get the / directory meta data");
    eos_crit("eos view cannot retrieve the / directory");
    return 1;
  }

  // Check the '/' directory permissions
  if (!rootmd->getMode()) {
    if (MgmMaster.IsMaster()) {
      // no permissions set yet
      try {
        rootmd->setMode(S_IFDIR | S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP |
                        S_IWGRP | S_IXGRP);
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the / directory mode to inital mode");
        eos_crit("cannot set the / directory mode to 755");
        return 1;
      }
    } else {
      Eroute.Emsg("Config", "/ directory has no 755 permissions set");
      eos_crit("cannot see / directory with mode to 755");
      return 1;
    }
  }

  eos_info("/ permissions are %o", rootmd->getMode());

  if (MgmMaster.IsMaster()) {
    // Create /eos/ and /eos/<instance>/ directories
    std::shared_ptr<eos::IContainerMD> eosmd;

    try {
      eosmd = eosView->getContainer("/eos/");
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = eosView->createContainer("/eos/", true);
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP |
                       S_IWGRP | S_IXGRP);
        eosmd->setAttribute("sys.forced.checksum", "adler");
        eosView->updateContainerStore(eosmd.get());
        eos_info("/eos permissions are %o checksum is set <adler>", eosmd->getMode());
        eosmd = eosView->createContainer(instancepath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP |
                       S_IWGRP | S_IXGRP);
        eosmd->setAttribute("sys.forced.checksum", "adler");
        eosView->updateContainerStore(eosmd.get());
        eos_info("%s permissions are %o checksum is set <adler>", instancepath.c_str(),
                 eosmd->getMode());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the /eos/ directory mode to inital mode");
        eos_crit("cannot set the /eos/ directory mode to 755");
        return 1;
      }
    }

    // Create /eos/<instance>/proc/ directory
    try {
      eosmd = eosView->getContainer(MgmProcPath.c_str());
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = eosView->createContainer(MgmProcPath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP | S_IXGRP);
        eosView->updateContainerStore(eosmd.get());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the /eos/<instance>/proc/ "
                    "directory mode to inital mode");
        eos_crit("cannot set the /eos/proc directory mode to 755");
        return 1;
      }
    }

    // Create recycle directory
    try {
      eosmd = eosView->getContainer(Recycle::gRecyclingPrefix.c_str());
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = eosView->createContainer(Recycle::gRecyclingPrefix.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU);
        eosView->updateContainerStore(eosmd.get());
        eos_info("%s permissions are %o", Recycle::gRecyclingPrefix.c_str(),
                 eosmd->getMode());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the recycle directory mode to inital mode");
        eos_crit("cannot set the %s directory mode to 700",
                 Recycle::gRecyclingPrefix.c_str());
        eos_crit("%s", e.what());
        return 1;
      }
    }

    // Create output directory layout conversions
    try {
      eosmd = gOFS->eosView->getContainer(MgmProcConversionPath.c_str());
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = gOFS->eosView->createContainer(MgmProcConversionPath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IRWXG);
        eosmd->setCUid(2); // conversion directory is owned by daemon
        eosmd->setCGid(2);
        gOFS->eosView->updateContainerStore(eosmd.get());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the /eos/../proc/conversion directory"
                    " mode to inital mode");
        eos_crit("cannot set the /eos/../proc/conversion directory mode to 770");
        return 1;
      }
    }

    // Create directory for fast find functionality of archived dirs
    try {
      eosmd = gOFS->eosView->getContainer(MgmProcArchivePath.c_str());
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = gOFS->eosView->createContainer(MgmProcArchivePath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IRWXG);
        eosmd->setCUid(2); // archive directory is owned by daemon
        eosmd->setCGid(2);
        gOFS->eosView->updateContainerStore(eosmd.get());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the /eos/../proc/archive directory "
                    "mode to inital mode");
        eos_crit("cannot set the /eos/../proc/archive directory mode to 770");
        return 1;
      }
    }

    // Create workflow directory
    try {
      eosmd = gOFS->eosView->getContainer(MgmProcWorkflowPath.c_str());
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = gOFS->eosView->createContainer(MgmProcWorkflowPath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU);
        eosmd->setCUid(2); // workflow directory is owned by daemon
        gOFS->eosView->updateContainerStore(eosmd.get());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config",
                    "cannot set the /eos/../proc/workflow directory mode to inital mode");
        eos_crit("cannot set the /eos/../proc/workflow directory mode to 700");
        return 1;
      }
    }

    // Create lock directory
    try {
      eosmd = gOFS->eosView->getContainer(MgmProcLockPath.c_str());
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = gOFS->eosView->createContainer(MgmProcLockPath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU);
        eosmd->setCUid(2); // lock directory is owned by daemon
        gOFS->eosView->updateContainerStore(eosmd.get());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the /eos/../proc/lock directory mode "
                    "to inital mode");
        eos_crit("cannot set the /eos/../proc/lock directory mode to 700");
        return 1;
      }
    }

    // Create delegation directory
    try {
      eosmd = gOFS->eosView->getContainer(MgmProcDelegationPath.c_str());
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = gOFS->eosView->createContainer(MgmProcDelegationPath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU);
        eosmd->setCUid(2); // delegation directory is owned by daemon
        gOFS->eosView->updateContainerStore(eosmd.get());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the /eos/../proc/delegation directory"
                    " mode to inital mode");
        eos_crit("cannot set the /eos/../proc/delegation directory mode to 700");
        return 1;
      }
    }
  }

  // Set also the archiverd ZMQ endpoint were client requests are sent
  std::ostringstream oss;
  oss << "ipc://" << MgmArchiveDir.c_str() << "archive_frontend.ipc";
  mArchiveEndpoint = oss.str();
  XrdMqSharedHash* hash = 0;

  // Disable some features if we are only a redirector
  if (!MgmRedirector) {
    // create the specific listener class
    MgmOfsMessaging = new Messaging(MgmOfsBrokerUrl.c_str(),
                                    MgmDefaultReceiverQueue.c_str(),
                                    true, true, &ObjectManager);

    if (!MgmOfsMessaging->StartListenerThread()) {
      NoGo = 1;
    }

    MgmOfsMessaging->SetLogId("MgmOfsMessaging");

    if (MgmOfsMessaging->IsZombie()) {
      Eroute.Emsg("Config", "cannot create messaging object(thread)");
      return NoGo;
    }

    if (MgmOfsVstBrokerUrl.length() &&
        (!getenv("EOS_VST_BROKER_DISABLE") ||
         (strcmp(getenv("EOS_VST_BROKER_DISABLE"), "1")))) {
      MgmOfsVstMessaging = new VstMessaging(MgmOfsVstBrokerUrl.c_str(),
                                            "/eos/*/vst", true, true, 0);

      if (!MgmOfsVstMessaging->StartListenerThread()) {
        NoGo = 1;
      }

      MgmOfsMessaging->SetLogId("MgmOfsVstMessaging");

      if (MgmOfsVstMessaging->IsZombie()) {
        Eroute.Emsg("Config", "cannot create vst messaging object(thread)");
        return NoGo;
      }
    }

    // Create the ZMQ processor used especially for fuse
    zMQ = new ZMQ("tcp://*:1100");

    if (!zMQ) {
      Eroute.Emsg("Config", "cannto start ZMQ processor");
      return 1;
    }

    zMQ->ServeFuse();
    ObjectManager.CreateSharedHash("/eos/*", "/eos/*/fst");
    ObjectManager.HashMutex.LockRead();
    hash = ObjectManager.GetHash("/eos/*");
    ObjectManager.HashMutex.UnLockRead();
    XrdOucString dumperfile = MgmMetaLogDir;
    dumperfile += "/so.mgm.dump";
    ObjectManager.StartDumper(dumperfile.c_str());
    ObjectManager.SetAutoReplyQueueDerive(true);
  }

  // hook to the appropiate config file
  XrdOucString stdOut;
  XrdOucString stdErr;

  if (!MgmMaster.ApplyMasterConfig(stdOut, stdErr,
                                   Master::Transition::Type::kMasterToMaster)) {
    Eroute.Emsg("Config", "failed to apply master configuration");
    return 1;
  }

  if (!MgmRedirector) {
    if (ErrorLog) {
      // run the error log console
      XrdOucString errorlogkillline = "pkill -9 -f \"eos -b console log _MGMID_\"";
      int rrc = system(errorlogkillline.c_str());

      if (WEXITSTATUS(rrc)) {
        eos_info("%s returned %d", errorlogkillline.c_str(), rrc);
      }

      XrdOucString errorlogline = "eos -b console log _MGMID_ >& /dev/null &";
      rrc = system(errorlogline.c_str());

      if (WEXITSTATUS(rrc)) {
        eos_info("%s returned %d", errorlogline.c_str(), rrc);
      }
    }

    eos_info("starting file view loader thread");

    if ((XrdSysThread::Run(&tid, XrdMgmOfs::StaticInitializeFileView,
                           static_cast<void*>(this), 0, "File View Loader"))) {
      eos_crit("cannot start file view loader");
      NoGo = 1;
    }
  }

#ifdef EOS_INSTRUMENTED_RWMUTEX
  eos::common::RWMutex::EstimateLatenciesAndCompensation();
  FsView::gFsView.ViewMutex.SetDebugName("FsView");
  FsView::gFsView.ViewMutex.SetTiming(false);
  FsView::gFsView.ViewMutex.SetSampling(true, 0.01);
  Quota::pMapMutex.SetDebugName("QuotaView");
  Quota::pMapMutex.SetTiming(false);
  Quota::pMapMutex.SetSampling(true, 0.01);
  eosViewRWMutex.SetDebugName("eosView");
  eosViewRWMutex.SetTiming(false);
  eosViewRWMutex.SetSampling(true, 0.01);
  std::vector<eos::common::RWMutex*> order;
  order.push_back(&FsView::gFsView.ViewMutex);
  order.push_back(&eosViewRWMutex);
  order.push_back(&Quota::pMapMutex);
  eos::common::RWMutex::AddOrderRule("Eos Mgm Mutexes", order);
#endif
  eos_info("starting statistics thread");

  if ((XrdSysThread::Run(&stats_tid, XrdMgmOfs::StartMgmStats,
                         static_cast<void*>(this), 0, "Statistics Thread"))) {
    eos_crit("cannot start statistics thread");
    NoGo = 1;
  }

  eos_info("starting archive submitter thread");

  if (XrdSysThread::Run(&mSubmitterTid, XrdMgmOfs::StartArchiveSubmitter,
                        static_cast<void*>(this), XRDSYSTHREAD_HOLD,
                        "Archive/backup submitter thread")) {
    eos_crit("cannot start archive/backup submitter thread");
    NoGo = 1;
  }

  if (!MgmRedirector) {
    eos_info("starting fs listener thread");

    if ((XrdSysThread::Run(&fsconfiglistener_tid,
                           XrdMgmOfs::StartMgmFsConfigListener,
                           static_cast<void*>(this), 0, "FsListener Thread"))) {
      eos_crit("cannot start fs listener thread");
      NoGo = 1;
    }
  }

  // Start drainer engine
  if (MgmOfsCentralDraining) {
    DrainerEngine = new Drainer();
  }

  gGeoTreeEngine.StartUpdater();
  XrdSysTimer sleeper;
  sleeper.Snooze(1);

  if (!ObjectNotifier.Start()) {
    eos_crit("error starting the shared object change notifier");
  }

  // initialize the transfer database
  if (!gTransferEngine.Init("/var/eos/tx")) {
    eos_crit("cannot intialize transfer database");
    NoGo = 1;
  }

  // create the 'default' quota space which is needed if quota is disabled!
  if (!Httpd->Start()) {
    eos_warning("msg=\"cannot start httpd daemon\"");
  }

  // start the Egroup fetching
  if (!gOFS->EgroupRefresh->Start()) {
    eos_warning("msg=\"cannot start egroup thread\"");
  }

  // start the LRU daemon
  if (!LRUd.Start()) {
    eos_warning("msg=\"cannot start LRU thread\"");
  }

  // start the WFE daemon
  if (!gOFS->WFEd.Start()) {
    eos_warning("msg=\"cannot start WFE thread\"");
  }

  // start the recycler garbage collection thread on a master machine
  if ((MgmMaster.IsMaster()) && (!Recycler->Start())) {
    eos_warning("msg=\"cannot start recycle thread\"");
  }

  // add all stat entries with 0
  InitStats();
  // set IO accounting file
  XrdOucString ioaccounting = MgmMetaLogDir;
  ioaccounting += "/iostat.";
  ioaccounting += HostName;
  ioaccounting += ".dump";
  eos_notice("Setting IO dump store file to %s", ioaccounting.c_str());

  if (!IoStats->SetStoreFileName(ioaccounting.c_str()))
    eos_warning("couldn't load anything from the io stat dump file %s",
                ioaccounting.c_str());
  else {
    eos_notice("loaded io stat dump file %s", ioaccounting.c_str());
  }

  // start IO ciruclate thread
  IoStats->StartCirculate();

  if (!MgmRedirector) {
    if (hash) {
      // ask for a broadcast from fst's
      hash->BroadcastRequest("/eos/*/fst");
    }
  }

  if (!getenv("EOS_NO_SHUTDOWN")) {
    // add shutdown handler
    (void) signal(SIGINT, xrdmgmofs_shutdown);
    (void) signal(SIGTERM, xrdmgmofs_shutdown);
    (void) signal(SIGQUIT, xrdmgmofs_shutdown);

    // add SEGV handler
    if (!getenv("EOS_NO_STACKTRACE")) {
      (void) signal(SIGSEGV, xrdmgmofs_stacktrace);
      (void) signal(SIGABRT, xrdmgmofs_stacktrace);
      (void) signal(SIGBUS, xrdmgmofs_stacktrace);
    }
  }

  if (mNumAuthThreads && mFrontendPort) {
    eos_info("starting the authentication master thread");

    if ((XrdSysThread::Run(&auth_tid, XrdMgmOfs::StartAuthMasterThread,
                           static_cast<void*>(this), 0, "Auth Master Thread"))) {
      eos_crit("cannot start the authentication master thread");
      NoGo = 1;
    }

    eos_info("starting the authentication worker threads");

    for (unsigned int i = 0; i < mNumAuthThreads; i++) {
      pthread_t worker_tid;

      if ((XrdSysThread::Run(&worker_tid, XrdMgmOfs::StartAuthWorkerThread,
                             static_cast<void*>(this), 0, "Auth Worker Thread"))) {
        eos_crit("cannot start the authentication thread %i", i);
        NoGo = 1;
      }

      mVectTid.push_back(worker_tid);
    }
  }

  sleeper.Wait(200);
  // to be sure not to miss any notification while everything is starting up
  // we don't check if it succeeds because we might fail because we timeout
  // if there is no FST sending update
  gGeoTreeEngine.forceRefresh();
  return NoGo;
}
/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::InitStats()
{
  MgmStats.Add("HashSet", 0, 0, 0);
  MgmStats.Add("HashSetNoLock", 0, 0, 0);
  MgmStats.Add("HashGet", 0, 0, 0);
  MgmStats.Add("ViewLockR", 0, 0, 0);
  MgmStats.Add("ViewLockW", 0, 0, 0);
  MgmStats.Add("NsLockR", 0, 0, 0);
  MgmStats.Add("NsLockW", 0, 0, 0);
  MgmStats.Add("QuotaLockR", 0, 0, 0);
  MgmStats.Add("QuotaLockW", 0, 0, 0);
  MgmStats.Add("Access", 0, 0, 0);
  MgmStats.Add("AdjustReplica", 0, 0, 0);
  MgmStats.Add("AttrGet", 0, 0, 0);
  MgmStats.Add("AttrLs", 0, 0, 0);
  MgmStats.Add("AttrRm", 0, 0, 0);
  MgmStats.Add("AttrSet", 0, 0, 0);
  MgmStats.Add("Cd", 0, 0, 0);
  MgmStats.Add("Checksum", 0, 0, 0);
  MgmStats.Add("Chmod", 0, 0, 0);
  MgmStats.Add("Chown", 0, 0, 0);
  MgmStats.Add("Commit", 0, 0, 0);
  MgmStats.Add("CommitFailedFid", 0, 0, 0);
  MgmStats.Add("CommitFailedNamespace", 0, 0, 0);
  MgmStats.Add("CommitFailedParameters", 0, 0, 0);
  MgmStats.Add("CommitFailedUnlinked", 0, 0, 0);
  MgmStats.Add("ConversionDone", 0, 0, 0);
  MgmStats.Add("ConversionFailed", 0, 0, 0);
  MgmStats.Add("CopyStripe", 0, 0, 0);
  MgmStats.Add("DumpMd", 0, 0, 0);
  MgmStats.Add("Drop", 0, 0, 0);
  MgmStats.Add("DropStripe", 0, 0, 0);
  MgmStats.Add("Exists", 0, 0, 0);
  MgmStats.Add("Exists", 0, 0, 0);
  MgmStats.Add("FileInfo", 0, 0, 0);
  MgmStats.Add("FindEntries", 0, 0, 0);
  MgmStats.Add("Find", 0, 0, 0);
  MgmStats.Add("Fuse", 0, 0, 0);
  MgmStats.Add("Fuse-Statvfs", 0, 0, 0);
  MgmStats.Add("Fuse-Mkdir", 0, 0, 0);
  MgmStats.Add("Fuse-Stat", 0, 0, 0);
  MgmStats.Add("Fuse-Chmod", 0, 0, 0);
  MgmStats.Add("Fuse-Chown", 0, 0, 0);
  MgmStats.Add("Fuse-Access", 0, 0, 0);
  MgmStats.Add("Fuse-Access", 0, 0, 0);
  MgmStats.Add("Fuse-Checksum", 0, 0, 0);
  MgmStats.Add("Fuse-XAttr", 0, 0, 0);
  MgmStats.Add("Fuse-Utimes", 0, 0, 0);
  MgmStats.Add("GetMdLocation", 0, 0, 0);
  MgmStats.Add("GetMd", 0, 0, 0);
  MgmStats.Add("Http-COPY", 0, 0, 0);
  MgmStats.Add("Http-DELETE", 0, 0, 0);
  MgmStats.Add("Http-GET", 0, 0, 0);
  MgmStats.Add("Http-HEAD", 0, 0, 0);
  MgmStats.Add("Http-LOCK", 0, 0, 0);
  MgmStats.Add("Http-MKCOL", 0, 0, 0);
  MgmStats.Add("Http-MOVE", 0, 0, 0);
  MgmStats.Add("Http-OPTIONS", 0, 0, 0);
  MgmStats.Add("Http-POST", 0, 0, 0);
  MgmStats.Add("Http-PROPFIND", 0, 0, 0);
  MgmStats.Add("Http-PROPPATCH", 0, 0, 0);
  MgmStats.Add("Http-PUT", 0, 0, 0);
  MgmStats.Add("Http-TRACE", 0, 0, 0);
  MgmStats.Add("Http-UNLOCK", 0, 0, 0);
  MgmStats.Add("IdMap", 0, 0, 0);
  MgmStats.Add("Ls", 0, 0, 0);
  MgmStats.Add("LRUFind", 0, 0, 0);
  MgmStats.Add("MarkDirty", 0, 0, 0);
  MgmStats.Add("MarkClean", 0, 0, 0);
  MgmStats.Add("Mkdir", 0, 0, 0);
  MgmStats.Add("Motd", 0, 0, 0);
  MgmStats.Add("MoveStripe", 0, 0, 0);
  MgmStats.Add("OpenDir", 0, 0, 0);
  MgmStats.Add("OpenDir-Entry", 0, 0, 0);
  MgmStats.Add("OpenFailedCreate", 0, 0, 0);
  MgmStats.Add("OpenFailedENOENT", 0, 0, 0);
  MgmStats.Add("OpenFailedExists", 0, 0, 0);
  MgmStats.Add("OpenFailedHeal", 0, 0, 0);
  MgmStats.Add("OpenFailedPermission", 0, 0, 0);
  MgmStats.Add("OpenFailedQuota", 0, 0, 0);
  MgmStats.Add("OpenFailedNoUpdate", 0, 0, 0);
  MgmStats.Add("OpenFailedReconstruct", 0, 0, 0);
  MgmStats.Add("OpenFileOffline", 0, 0, 0);
  MgmStats.Add("OpenProc", 0, 0, 0);
  MgmStats.Add("OpenRead", 0, 0, 0);
  MgmStats.Add("OpenShared", 0, 0, 0);
  MgmStats.Add("OpenStalledHeal", 0, 0, 0);
  MgmStats.Add("OpenStalled", 0, 0, 0);
  MgmStats.Add("OpenStalled", 0, 0, 0);
  MgmStats.Add("Open", 0, 0, 0);
  MgmStats.Add("OpenWriteCreate", 0, 0, 0);
  MgmStats.Add("OpenWriteTruncate", 0, 0, 0);
  MgmStats.Add("OpenWrite", 0, 0, 0);
  MgmStats.Add("ReadLink", 0, 0, 0);
  MgmStats.Add("Recycle", 0, 0, 0);
  MgmStats.Add("ReplicaFailedSize", 0, 0, 0);
  MgmStats.Add("ReplicaFailedChecksum", 0, 0, 0);
  MgmStats.Add("Redirect", 0, 0, 0);
  MgmStats.Add("RedirectR", 0, 0, 0);
  MgmStats.Add("RedirectW", 0, 0, 0);
  MgmStats.Add("RedirectR-Master", 0, 0, 0);
  MgmStats.Add("RedirectENOENT", 0, 0, 0);
  MgmStats.Add("RedirectENONET", 0, 0, 0);
  MgmStats.Add("Rename", 0, 0, 0);
  MgmStats.Add("RmDir", 0, 0, 0);
  MgmStats.Add("Rm", 0, 0, 0);
  MgmStats.Add("Schedule2Drain", 0, 0, 0);
  MgmStats.Add("Schedule2Balance", 0, 0, 0);
  MgmStats.Add("SchedulingFailedBalance", 0, 0, 0);
  MgmStats.Add("SchedulingFailedDrain", 0, 0, 0);
  MgmStats.Add("Scheduled2Balance", 0, 0, 0);
  MgmStats.Add("Scheduled2Drain", 0, 0, 0);
  MgmStats.Add("Schedule2Delete", 0, 0, 0);
  MgmStats.Add("Scheduled2Delete", 0, 0, 0);
  MgmStats.Add("SendResync", 0, 0, 0);
  MgmStats.Add("Stall", 0, 0, 0);
  MgmStats.Add("Stat", 0, 0, 0);
  MgmStats.Add("Symlink", 0, 0, 0);
  MgmStats.Add("Touch", 0, 0, 0);
  MgmStats.Add("TxState", 0, 0, 0);
  MgmStats.Add("Truncate", 0, 0, 0);
  MgmStats.Add("VerifyStripe", 0, 0, 0);
  MgmStats.Add("Version", 0, 0, 0);
  MgmStats.Add("Versioning", 0, 0, 0);
  MgmStats.Add("WhoAmI", 0, 0, 0);
}
