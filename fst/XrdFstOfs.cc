// ----------------------------------------------------------------------
// File: XrdFstOfs.cc
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

#include "authz/XrdCapability.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/XrdFstOss.hh"
#include "fst/FmdDbMap.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/http/HttpServer.hh"
#include "fst/storage/FileSystem.hh"
#include "fst/storage/Storage.hh"
#include "fst/Messaging.hh"
#include "common/PasswordHandler.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/Path.hh"
#include "common/Statfs.hh"
#include "common/SyncAll.hh"
#include "common/StackTrace.hh"
#include "common/eos_cta_pb/EosCtaAlertHandler.hh"
#include "common/Constants.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "common/XattrCompat.hh"
#include "mq/SharedHashWrapper.hh"
#include "XrdNet/XrdNetOpts.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysDNS.hh"
#include "Xrd/XrdScheduler.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdVersion.hh"
#include "qclient/Members.hh"
#include "qclient/shared/SharedManager.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <math.h>
#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <sstream>
#include <thread>

// The global OFS handle
eos::fst::XrdFstOfs eos::fst::gOFS;

extern XrdSysError OfsEroute;
extern XrdOss* XrdOfsOss;
extern XrdOfs* XrdOfsFS;
extern XrdOucTrace OfsTrace;

// Set the version information
XrdVERSIONINFO(XrdSfsGetFileSystem2, FstOfs);

#ifdef COVERAGE_BUILD
// Forward declaration of gcov flush API
extern "C" void __gcov_flush();
#endif

//------------------------------------------------------------------------------
// XRootD OFS interface implementation
//------------------------------------------------------------------------------
extern "C"
{
  XrdSfsFileSystem* XrdSfsGetFileSystem2(XrdSfsFileSystem* nativeFS,
                                         XrdSysLogger*     Logger,
                                         const char*       configFn,
                                         XrdOucEnv*        envP)
  {
    if (XrdOfsFS) {
      return XrdOfsFS;
    }

    OfsEroute.SetPrefix("FstOfs_");
    OfsEroute.logger(Logger);
    // Disable XRootD log rotation
    Logger->setRotate(0);
    std::ostringstream oss;
    oss << "FstOfs (Object Storage File System) " << VERSION;
    XrdOucString version = "FstOfs (Object Storage File System) ";
    OfsEroute.Say("++++++ (c) 2010 CERN/IT-DSS ", oss.str().c_str());
    // Initialize the subsystems
    eos::fst::gOFS.ConfigFN = (configFn && *configFn ? strdup(configFn) : 0);

    if (eos::fst::gOFS.Configure(OfsEroute, envP)) {
      return 0;
    }

    XrdOfsFS = &eos::fst::gOFS;
    return &eos::fst::gOFS;
  }
}

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Get stacktrace from crashing process
//------------------------------------------------------------------------------
void
XrdFstOfs::xrdfstofs_stacktrace(int sig)
{
  (void) signal(SIGINT, SIG_IGN);
  (void) signal(SIGTERM, SIG_IGN);
  (void) signal(SIGQUIT, SIG_IGN);
  void* array[10];
  size_t size;
  // Get void*'s for all entries on the stack
  size = backtrace(array, 10);
  // Print out all the frames to stderr
  fprintf(stderr, "error: received signal %d:\n", sig);
  backtrace_symbols_fd(array, size, 2);
  eos::common::StackTrace::GdbTrace(0 , getpid(), "thread apply all bt");

  if (getenv("EOS_CORE_DUMP")) {
    eos::common::StackTrace::GdbTrace(0 , getpid(), "generate-core-file");
  }

  // Now we put back the initial handler and send the signal again
  signal(sig, SIG_DFL);
  kill(getpid(), sig);
  int wstatus = 0;
  wait(&wstatus);
}

//------------------------------------------------------------------------------
// Print coverage data
//------------------------------------------------------------------------------
void
XrdFstOfs::xrdfstofs_coverage(int sig)
{
#ifdef COVERAGE_BUILD
  eos_static_notice("msg=\"printing coverage data\"");
  __gcov_flush();
  return;
#endif
  eos_static_notice("msg=\"compiled without coverage support\"");
}

//------------------------------------------------------------------------------
// FST shutdown procedure
//------------------------------------------------------------------------------
void
XrdFstOfs::xrdfstofs_shutdown(int sig)
{
  static XrdSysMutex ShutDownMutex;
  ShutDownMutex.Lock(); // this handler goes only one-shot .. sorry !
  gOFS.sShutdown = true;
  pid_t watchdog;
  pid_t ppid = getpid();

  if (!(watchdog = fork())) {
    eos::common::SyncAll::AllandClose();
    // Sleep for an amount of time proportional to the number of filesystems
    // on the current machine
    std::chrono::seconds timeout(gFmdDbMapHandler.GetNumFileSystems() * 5);
    std::this_thread::sleep_for(timeout);
    fprintf(stderr, "@@@@@@ 00:00:00 op=shutdown msg=\"shutdown timedout after "
            "%li seconds, signal=%i\n", timeout.count(), sig);

    if (ppid > 1) {
      kill(ppid, 9);
    }

    fprintf(stderr, "@@@@@@ 00:00:00 %s", "op=shutdown status=forced-complete\n");
    kill(getpid(), 9);
  }

  // Handler to shutdown the daemon for valgrinding and clean server stop
  // (e.g. let time to finish write operations)
  if (gOFS.Messaging) {
    gOFS.Messaging->StopListener();  // stop any communication
    delete gOFS.Messaging;
  }

  std::this_thread::sleep_for(std::chrono::seconds(1));
  gOFS.Storage->ShutdownThreads();
  eos_static_warning("%s", "op=shutdown msg=\"stop messaging\"");
  eos_static_warning("%s", "op=shutdown msg=\"shutdown fmddbmap handler\"");
  gFmdDbMapHandler.Shutdown();

  if (watchdog > 1) {
    kill(watchdog, 9);
  }

  int wstatus = 0;
  wait(&wstatus);
  eos_static_warning("%s", "op=shutdown status=dbmapclosed");
  // Sync & close all file descriptors
  eos::common::SyncAll::AllandClose();
  eos_static_warning("%s", "op=shutdown status=completed");
  // harakiri - yes!
  (void) signal(SIGABRT, SIG_IGN);
  (void) signal(SIGINT,  SIG_IGN);
  (void) signal(SIGTERM, SIG_IGN);
  (void) signal(SIGQUIT, SIG_IGN);
  kill(getpid(), 9);
}

//------------------------------------------------------------------------------
// FST "graceful" shutdown procedure
//------------------------------------------------------------------------------
void
XrdFstOfs::xrdfstofs_graceful_shutdown(int sig)
{
  using namespace eos::common;
  eos_static_info("entering the \"graceful\" shutdown procedure");
  pid_t watchdog;
  static XrdSysMutex grace_shutdown_mtx;
  grace_shutdown_mtx.Lock();
  gOFS.sShutdown = true;
  const char* swait = getenv("EOS_GRACEFUL_SHUTDOWN_TIMEOUT");
  std::int64_t wait = (swait ? std::strtol(swait, nullptr, 10) : 390);
  pid_t ppid = getpid();

  if (!(watchdog = fork())) {
    std::this_thread::sleep_for(std::chrono::seconds(wait));
    SyncAll::AllandClose();
    std::this_thread::sleep_for(std::chrono::seconds(15));
    fprintf(stderr, "@@@@@@ 00:00:00 %s %li seconds\"\n",
            "op=shutdown msg=\"shutdown timedout after ", wait);

    if (ppid > 1) {
      kill(ppid, 9);
    }

    fprintf(stderr, "@@@@@@ 00:00:00 %s", "op=shutdown status=forced-complete");
    kill(getpid(), 9);
  }

  // Stop any communication - this will also stop scheduling to this node
  eos_static_warning("op=shutdown msg=\"stop messaging\"");

  if (gOFS.Messaging) {
    gOFS.Messaging->StopListener();
    delete gOFS.Messaging;
  }

  // Wait for 60 seconds heartbeat timeout (see mgm/FsView) + 30 seconds
  // for in-flight redirections
  eos_static_warning("op=shutdown msg=\"wait 90 seconds for configuration "
                     "propagation\"");
  std::chrono::seconds config_timeout(60 + 30);
  std::this_thread::sleep_for(config_timeout);
  std::chrono::seconds io_timeout((std::int64_t)(wait * 0.9));

  if (gOFS.WaitForOngoingIO(io_timeout)) {
    eos_static_warning("op=shutdown msg=\"successful graceful IO shutdown\"");
  } else {
    eos_static_err("op=shutdown msg=\"failed graceful IO shutdown\"");
  }

  std::this_thread::sleep_for(std::chrono::seconds(1));
  gOFS.Storage->ShutdownThreads();
  eos_static_warning("op=shutdown msg=\"shutdown fmddbmap handler\"");
  gFmdDbMapHandler.Shutdown();

  if (watchdog > 1) {
    kill(watchdog, 9);
  }

  int wstatus = 0;
  ::wait(&wstatus);
  eos_static_warning("op=shutdown status=dbmapclosed");
  // Sync & close all file descriptors
  SyncAll::AllandClose();
  eos_static_warning("op=shutdown status=completed");
  // harakiri - yes!
  (void) signal(SIGABRT, SIG_IGN);
  (void) signal(SIGINT,  SIG_IGN);
  (void) signal(SIGTERM, SIG_IGN);
  (void) signal(SIGQUIT, SIG_IGN);
  (void) signal(SIGUSR1, SIG_IGN);
  kill(getpid(), 9);
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdFstOfs::XrdFstOfs() :
  eos::common::LogId(), mHostName(NULL), mMqOnQdb(false), mHttpd(nullptr),
  mGeoTag("nogeotag"), mCloseThreadPool(8, 64, 5, 6, 5, "async_close"),
  mMgmXrdPool(nullptr), mSimIoReadErr(false), mSimIoWriteErr(false),
  mSimXsReadErr(false), mSimXsWriteErr(false), mSimFmdOpenErr(false),
  mSimErrIoReadOff(0ull), mSimErrIoWriteOff(0ull)
{
  Eroute = 0;
  Messaging = 0;
  Storage = 0;
  TransferScheduler = 0;
  TpcMap.resize(2);
  TpcMap[0].set_deleted_key(""); // readers
  TpcMap[1].set_deleted_key(""); // writers

  if (!getenv("EOS_NO_SHUTDOWN")) {
    // Add shutdown handler
    (void) signal(SIGINT, xrdfstofs_shutdown);
    (void) signal(SIGTERM, xrdfstofs_shutdown);
    (void) signal(SIGQUIT, xrdfstofs_shutdown);
    // Add graceful shutdown handler
    (void) signal(SIGUSR1, xrdfstofs_graceful_shutdown);
  }

  if (getenv("EOS_COVERAGE_REPORT")) {
    // Add coverage report handler
    (void) signal(SIGPROF, xrdfstofs_coverage);
  }

  // Initialize the google sparse hash maps
  gOFS.WNoDeleteOnCloseFid.clear_deleted_key();
  gOFS.WNoDeleteOnCloseFid.set_deleted_key(0);
  setenv("EOSFSTOFS", std::to_string((unsigned long long)this).c_str(), 1);

  if (getenv("EOS_FST_CALL_MANAGER_XRD_POOL")) {
    int max_size = 10;
    const char* csize {nullptr};

    if ((csize = getenv("EOS_FST_CALL_MANAGER_XRD_POOL_SIZE"))) {
      try {
        max_size = std::stoi(csize);

        if (max_size < 1) {
          max_size = 1;
        }

        if (max_size > 32) {
          max_size = 32;
        }
      } catch (...) {
        // ignore
      }
    }

    mMgmXrdPool.reset(new eos::common::XrdConnPool(true, max_size));
    fprintf(stderr, "Config Enabled CallManager xrootd connection pool with "
            "size=%i\n", max_size);
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFstOfs::~XrdFstOfs()
{
}

//------------------------------------------------------------------------------
// Get a new OFS directory object - not implemented
//------------------------------------------------------------------------------
XrdSfsDirectory*
XrdFstOfs::newDir(char* user, int MonID)
{
  return (XrdSfsDirectory*)(0);
}

//------------------------------------------------------------------------------
// Get a new OFS file object
//-----------------------------------------------------------------------------
XrdSfsFile*
XrdFstOfs::newFile(char* user, int MonID)
{
  return static_cast<XrdSfsFile*>(new XrdFstOfsFile(user, MonID));
}

//------------------------------------------------------------------------------
// OFS layer configuration
//------------------------------------------------------------------------------
int
XrdFstOfs::Configure(XrdSysError& Eroute, XrdOucEnv* envP)
{
  char* var;
  const char* val;
  int cfgFD;
  int NoGo = 0;
  eos::common::StringConversion::InitLookupTables();

  if (XrdOfs::Configure(Eroute, envP)) {
    Eroute.Emsg("Config", "default OFS configuration failed");
    return SFS_ERROR;
  }

  // Enforcing 'sss' authentication for all communications
  if (!getenv("EOS_FST_NO_SSS_ENFORCEMENT")) {
    setenv("XrdSecPROTOCOL", "sss", 1);
    Eroute.Say("=====> fstofs enforces SSS authentication for XROOT clients");
  } else {
    Eroute.Say("=====> fstofs does not enforce SSS authentication for XROOT"
               " clients - make sure MGM enforces sss for this FST!");
  }

  // Get the hostname
  char* errtext = 0;
  mHostName = XrdSysDNS::getHostName(0, &errtext);

  if (!mHostName || std::string(mHostName) == "0.0.0.0") {
    Eroute.Emsg("Config", "hostname is invalid : %s", mHostName);
    return 1;
  }

  TransferScheduler = new XrdScheduler(&Eroute, &OfsTrace, 8, 128, 60);
  TransferScheduler->Start();
  eos::fst::Config::gConfig.autoBoot = false;
  eos::fst::Config::gConfig.FstOfsBrokerUrl = "root://localhost:1097//eos/";

  if (getenv("EOS_BROKER_URL")) {
    eos::fst::Config::gConfig.FstOfsBrokerUrl = getenv("EOS_BROKER_URL");
  }

  {
    // set the start date as string
    XrdOucString out = "";
    time_t t = time(NULL);
    struct tm* timeinfo;
    timeinfo = localtime(&t);
    out = asctime(timeinfo);
    out.erase(out.length() - 1);
    eos::fst::Config::gConfig.StartDate = out.c_str();
  }

  // Check for EOS_GEOTAG limits
  if (getenv("EOS_GEOTAG")) {
    const int max_tag_size = 8;
    char* node_geotag_tmp = getenv("EOS_GEOTAG");
    // Copy to a different string as strtok is modifying the pointed string
    char node_geotag [strlen(node_geotag_tmp)+1];
    strcpy(node_geotag, node_geotag_tmp);
    char* gtag = strtok(node_geotag, "::");

    while (gtag != NULL) {
      if (strlen(gtag) > max_tag_size) {
        NoGo = 1;
        Eroute.Emsg("Config", "EOS_GEOTAG var contains a tag longer than "
                    "the 8 chars maximum allowed: ", gtag);
        break;
      }

      gtag = strtok(NULL, "::");
    }
  }

  eos::fst::Config::gConfig.FstMetaLogDir = "/var/tmp/eos/md/";
  eos::fst::Config::gConfig.FstAuthDir = "/var/eos/auth/";
  setenv("XrdClientEUSER", "daemon", 1);
  // Set short timeout resolution, connection window, connection retry and
  // stream error window
  XrdCl::DefaultEnv::GetEnv()->PutInt("TimeoutResolution", 1);
  XrdCl::DefaultEnv::GetEnv()->PutInt("ConnectionWindow", 5);
  XrdCl::DefaultEnv::GetEnv()->PutInt("ConnectionRetry", 1);
  XrdCl::DefaultEnv::GetEnv()->PutInt("StreamErrorWindow", 0);
  XrdCl::DefaultEnv::GetEnv()->PutInt("MetalinkProcessing", 0);
  // Extract the manager from the config file
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"));

  if (!ConfigFN || !*ConfigFN) {
    // this error will be reported by XrdOfsFS.Configure
  } else {
    // Try to open the configuration file.
    if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0) {
      return Eroute.Emsg("Config", errno, "open config file fn=", ConfigFN);
    }

    Config.Attach(cfgFD);

    // Now start reading records until eof.
    while ((var = Config.GetMyFirstWord())) {
      if (!strncmp(var, "fstofs.", 7)) {
        var += 7;

        // we parse config variables here
        if (!strcmp("broker", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument 2 for broker missing. Should be "
                        "URL like root://<host>/<queue>/");
            NoGo = 1;
          } else {
            if (getenv("EOS_BROKER_URL")) {
              eos::fst::Config::gConfig.FstOfsBrokerUrl = getenv("EOS_BROKER_URL");
            } else {
              eos::fst::Config::gConfig.FstOfsBrokerUrl = val;
            }
          }
        }

        if (!strcmp("trace", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument 2 for trace missing. Can be 'client'");
            NoGo = 1;
          } else {
            //EnvPutInt( NAME_DEBUG, 3);
          }
        }

        if (!strcmp("autoboot", var)) {
          if ((!(val = Config.GetWord())) ||
              (strcmp("true", val) && strcmp("false", val) &&
               strcmp("1", val) && strcmp("0", val))) {
            Eroute.Emsg("Config", "argument 2 for autobootillegal or missing. "
                        "Must be <true>,<false>,<1> or <0>!");
            NoGo = 1;
          } else {
            if ((!strcmp("true", val) || (!strcmp("1", val)))) {
              eos::fst::Config::gConfig.autoBoot = true;
            }
          }
        }

        if (!strcmp("metalog", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument 2 for metalog missing");
            NoGo = 1;
          } else {
            if (strlen(val)) {
              eos::fst::Config::gConfig.FstMetaLogDir = val;

              if (val[strlen(val) - 1] != '/') {
                eos::fst::Config::gConfig.FstMetaLogDir += '/';
              }
            }
          }
        }

        if (!strcmp("authdir", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument 2 for authdir missing");
            NoGo = 1;
          } else {
            if (strlen(val)) {
              eos::fst::Config::gConfig.FstAuthDir = val;

              if (val[strlen(val) - 1] != '/') {
                eos::fst::Config::gConfig.FstAuthDir += '/';
              }
            }
          }
        }

        if (!strcmp("protowfendpoint", var)) {
          if ((val = Config.GetWord())) {
            eos::fst::Config::gConfig.ProtoWFEndpoint = val;
          }
        }

        if (!strcmp("protowfresource", var)) {
          if ((val = Config.GetWord())) {
            eos::fst::Config::gConfig.ProtoWFResource = val;
          }
        }

        if (!strcmp("qdbcluster", var)) {
          std::string qdb_cluster;

          while ((val = Config.GetWord())) {
            qdb_cluster += val;
            qdb_cluster += " ";
          }

          Eroute.Say("=====> fstofs.qdbcluster : ", qdb_cluster.c_str());

          if (!qdb_cluster.empty()) {
            if (!mQdbContactDetails.members.parse(qdb_cluster)) {
              Eroute.Emsg("Config", "failed to parse qdbcluster members");
              NoGo = 1;
            }
          }
        }

        if (!strcmp("qdbpassword", var)) {
          while ((val = Config.GetWord())) {
            mQdbContactDetails.password += val;
          }

          // Trim whitespace at the end
          common::PasswordHandler::rightTrimWhitespace(mQdbContactDetails.password);
          std::string pwlen = std::to_string(mQdbContactDetails.password.size());
          Eroute.Say("=====> fstofs.qdbpassword length : ", pwlen.c_str());
        }

        if (!strcmp("qdbpassword_file", var)) {
          std::string path;

          while ((val = Config.GetWord())) {
            path += val;
          }

          if (!common::PasswordHandler::readPasswordFile(path,
              mQdbContactDetails.password)) {
            Eroute.Emsg("Config", "failed to open path pointed to by qdbpassword_file");
            NoGo = 1;
          }

          std::string pwlen = std::to_string(mQdbContactDetails.password.size());
          Eroute.Say("=====> fstofs.qdbpassword length : ", pwlen.c_str());
        }

        if (!strcmp("mq_implementation", var)) {
          std::string value;

          while ((val = Config.GetWord())) {
            value += val;
          }

          if (value == "qdb") {
            mMqOnQdb = true;
          } else {
            Eroute.Emsg("Config", "unrecognized value for mq_implementation");
            NoGo = 1;
          }

          Eroute.Say("=====> fstofs.mq_implementation : ", value.c_str());
        }
      }
    }

    Config.Close();
    close(cfgFD);
  }

  if (NoGo) {
    return 1;
  }

  if (eos::fst::Config::gConfig.autoBoot) {
    Eroute.Say("=====> fstofs.autoboot : true");
  } else {
    Eroute.Say("=====> fstofs.autoboot : false");
  }

  // Create the qclient shared by all file systems for doing the ns scan for
  // the fsck consistency checks
  if (!mQdbContactDetails.empty()) {
    mFsckQcl.reset(new qclient::QClient(mQdbContactDetails.members,
                                        mQdbContactDetails.constructOptions()));
  }

  if (!eos::fst::Config::gConfig.FstOfsBrokerUrl.endswith("/")) {
    eos::fst::Config::gConfig.FstOfsBrokerUrl += "/";
  }

  eos::fst::Config::gConfig.FstDefaultReceiverQueue =
    eos::fst::Config::gConfig.FstOfsBrokerUrl;
  eos::fst::Config::gConfig.FstOfsBrokerUrl += mHostName;
  eos::fst::Config::gConfig.FstOfsBrokerUrl += ":";
  eos::fst::Config::gConfig.FstOfsBrokerUrl += myPort;
  eos::fst::Config::gConfig.FstOfsBrokerUrl += "/fst";
  eos::fst::Config::gConfig.FstHostPort = mHostName;
  eos::fst::Config::gConfig.FstHostPort += ":";
  eos::fst::Config::gConfig.FstHostPort += myPort;
  eos::fst::Config::gConfig.KernelVersion =
    eos::common::StringConversion::StringFromShellCmd("uname -r | tr -d \"\n\"").c_str();
  Eroute.Say("=====> fstofs.broker : ",
             eos::fst::Config::gConfig.FstOfsBrokerUrl.c_str(), "");
  // Extract our queue name
  eos::fst::Config::gConfig.FstQueue = eos::fst::Config::gConfig.FstOfsBrokerUrl;
  {
    int pos1 = eos::fst::Config::gConfig.FstQueue.find("//");
    int pos2 = eos::fst::Config::gConfig.FstQueue.find("//", pos1 + 2);

    if (pos2 != STR_NPOS) {
      eos::fst::Config::gConfig.FstQueue.erase(0, pos2 + 1);
    } else {
      Eroute.Emsg("Config", "cannot determine my queue name: ",
                  eos::fst::Config::gConfig.FstQueue.c_str());
      return 1;
    }
  }
  // Create our wildcard broadcast name
  eos::fst::Config::gConfig.FstQueueWildcard = eos::fst::Config::gConfig.FstQueue;
  eos::fst::Config::gConfig.FstQueueWildcard += "/*";
  // Create our wildcard config broadcast name
  eos::fst::Config::gConfig.FstConfigQueueWildcard = "*/";
  eos::fst::Config::gConfig.FstConfigQueueWildcard += mHostName;
  eos::fst::Config::gConfig.FstConfigQueueWildcard += ":";
  eos::fst::Config::gConfig.FstConfigQueueWildcard += myPort;
  // Create our wildcard gw broadcast name
  eos::fst::Config::gConfig.FstGwQueueWildcard = "*/";
  eos::fst::Config::gConfig.FstGwQueueWildcard += mHostName;
  eos::fst::Config::gConfig.FstGwQueueWildcard += ":";
  eos::fst::Config::gConfig.FstGwQueueWildcard += myPort;
  eos::fst::Config::gConfig.FstGwQueueWildcard += "/fst/gw/txqueue/txq";
  // Set logging parameters
  XrdOucString unit = "fst@";
  unit += mHostName;
  unit += ":";
  unit += myPort;
  // Setup the circular in-memory log buffer
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  g_logging.SetLogPriority(LOG_INFO);
  g_logging.SetUnit(unit.c_str());
  // Get the XRootD log directory
  char* logdir = 0;
  XrdOucEnv::Import("XRDLOGDIR", logdir);

  if (logdir) {
    eoscpTransferLog = logdir;
    eoscpTransferLog += "eoscp.log";
  }

  Eroute.Say("=====> eoscp-log : ", eoscpTransferLog.c_str());

  if (!UpdateSanitizedGeoTag()) {
    eos_static_err("%s", "msg=\"failed to update geotag, wrongly formatted\"");
    return SFS_ERROR;
  }

  // Compute checksum of the keytab file
  std::string kt_cks = GetKeytabChecksum("/etc/eos.keytab");
  eos::fst::Config::gConfig.KeyTabAdler = kt_cks.c_str();
  // Create the messaging object(recv thread)
  eos::fst::Config::gConfig.FstDefaultReceiverQueue += "*/mgm";
  int pos1 = eos::fst::Config::gConfig.FstDefaultReceiverQueue.find("//");
  int pos2 = eos::fst::Config::gConfig.FstDefaultReceiverQueue.find("//",
             pos1 + 2);

  if (pos2 != STR_NPOS) {
    eos::fst::Config::gConfig.FstDefaultReceiverQueue.erase(0, pos2 + 1);
  }

  Eroute.Say("=====> fstofs.defaultreceiverqueue : ",
             eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str(), "");
  // Set our Eroute for XrdMqMessage
  XrdMqMessage::Eroute = OfsEroute;
  // Enable the shared object notification queue
  ObjectManager.mEnableQueue = true;
  ObjectManager.SetAutoReplyQueue("/eos/*/mgm");
  ObjectManager.SetDebug(false);

  // Enable experimental MQ on QDB? Note that any functionality not supported
  // will fallback to regular MQ, which is still required.
  if (getenv("EOS_USE_MQ_ON_QDB")) {
    eos_static_info("MQ on QDB - setting up SharedManager..");
    mQSOM.reset(new qclient::SharedManager(mQdbContactDetails.members,
                                           mQdbContactDetails.constructSubscriptionOptions()));
  }

  // Setup auth dir
  {
    XrdOucString scmd = "mkdir -p ";
    scmd += eos::fst::Config::gConfig.FstAuthDir;
    scmd += " ; chown -R daemon ";
    scmd += eos::fst::Config::gConfig.FstAuthDir;
    scmd += " ; chmod 700 ";
    scmd += eos::fst::Config::gConfig.FstAuthDir;
    int src = system(scmd.c_str());

    if (src) {
      eos_err("%s returned %d", scmd.c_str(), src);
    }

    if (access(eos::fst::Config::gConfig.FstAuthDir.c_str(),
               R_OK | W_OK | X_OK)) {
      Eroute.Emsg("Config", "cannot access the auth directory for r/w: ",
                  eos::fst::Config::gConfig.FstAuthDir.c_str());
      return 1;
    }

    Eroute.Say("=====> fstofs.authdir : ",
               eos::fst::Config::gConfig.FstAuthDir.c_str());
  }
  // Attach Storage to the meta log dir
  Storage = eos::fst::Storage::Create(
              eos::fst::Config::gConfig.FstMetaLogDir.c_str());
  Eroute.Say("=====> fstofs.metalogdir : ",
             eos::fst::Config::gConfig.FstMetaLogDir.c_str());

  if (!Storage) {
    Eroute.Emsg("Config", "cannot setup meta data storage using directory: ",
                eos::fst::Config::gConfig.FstMetaLogDir.c_str());
    return 1;
  }

  ObjectNotifier.SetShareObjectManager(&ObjectManager);

  if (!ObjectNotifier.Start()) {
    eos_crit("error starting the shared object change notifier");
  }

// Create the specific listener class
  Messaging = new eos::fst::Messaging(
    eos::fst::Config::gConfig.FstOfsBrokerUrl.c_str(),
    eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str(),
    false, false, &ObjectManager);

  if (!Messaging) {
    Eroute.Emsg("Config", "cannot allocate messaging object");
    NoGo = 1;
    return NoGo;
  }

  Messaging->SetLogId("FstOfsMessaging", "<service>");

  if (!Messaging->StartListenerThread() || Messaging->IsZombie()) {
    Eroute.Emsg("Config", "cannot create messaging object(thread)");
    NoGo = 1;
    return NoGo;
  }

  RequestBroadcasts();
  // Start dumper thread
  XrdOucString dumperfile = eos::fst::Config::gConfig.FstMetaLogDir;
  dumperfile += "so.fst.dump.";
  dumperfile += eos::fst::Config::gConfig.FstHostPort;
  ObjectManager.StartDumper(dumperfile.c_str());
  // Start the embedded HTTP server
  mHttpdPort = 8001;

  if (getenv("EOS_FST_HTTP_PORT")) {
    mHttpdPort = strtol(getenv("EOS_FST_HTTP_PORT"), 0, 10);
  }

  mHttpd.reset(new eos::fst::HttpServer(mHttpdPort));

  if (mHttpdPort) {
    mHttpd->Start();
  }

  eos_notice("FST_HOST=%s FST_PORT=%ld FST_HTTP_PORT=%d VERSION=%s RELEASE=%s "
             "KEYTABADLER=%s", mHostName, myPort, mHttpdPort, VERSION, RELEASE,
             kt_cks.c_str());
  eos::mq::SharedHashWrapper::initialize(&ObjectManager);
  return 0;
}

//------------------------------------------------------------------------------
// Define error bool variables to en-/disable error simulation in the OFS layer
//------------------------------------------------------------------------------
void
XrdFstOfs::SetSimulationError(const std::string& input)
{
  mSimIoReadErr = mSimIoWriteErr = mSimXsReadErr =
                                     mSimXsWriteErr = mSimFmdOpenErr = false;
  mSimErrIoReadOff = mSimErrIoWriteOff = 0ull;

  if (input.find("io_read") == 0) {
    mSimIoReadErr = true;
    mSimErrIoReadOff = GetSimulationErrorOffset(input);
  } else if (input.find("io_write") == 0) {
    mSimIoWriteErr = true;
    mSimErrIoWriteOff = GetSimulationErrorOffset(input);
  } else if (input.find("xs_read") == 0) {
    mSimXsReadErr = true;
  } else if (input.find("xs_write") == 0) {
    mSimXsWriteErr = true;
  } else if (input.find("fmd_open") == 0) {
    mSimFmdOpenErr = true;
  }
}

//------------------------------------------------------------------------------
// Update geotag value from the EOS_GEOTAG env variable and sanitize the
// input
//------------------------------------------------------------------------------
bool
XrdFstOfs::UpdateSanitizedGeoTag()
{
  const char* ptr = getenv("EOS_GEOTAG");

  if (ptr == nullptr) {
    return true;
  }

  std::string tmp_tag(ptr);
  // Make sure the new geotag is properly formatted and respects the contraints
  auto tokens = eos::common::StringTokenizer::split<std::vector<std::string>>
                (tmp_tag, ':');
  tmp_tag.clear();

  for (const auto& token : tokens) {
    if (token.empty()) {
      continue;
    }

    if (token.length() > 8) {
      eos_static_err("msg=\"token in geotag longer than 8 chars\" geotag=\"%s\"",
                     ptr);
      return false;
    }

    tmp_tag += token;
    tmp_tag += "::";
  }

  if (tmp_tag.length() <= 2) {
    eos_static_err("%s", "msg=\"empty geotag\"");
    return false;
  }

  tmp_tag.erase(tmp_tag.length() - 2);
  mGeoTag = tmp_tag;
  return true;
}

//------------------------------------------------------------------------------
// Get simulation error offset. Parse the last characters and return the
// desired offset e.g. io_read_8M should return 8MB
//-----------------------------------------------------------------------------
uint64_t
XrdFstOfs::GetSimulationErrorOffset(const std::string& input) const
{
  uint64_t offset {0ull};
  size_t num = std::count(input.begin(), input.end(), '_');

  if ((num < 2) || (*input.rbegin() == '_')) {
    return offset;
  }

  size_t pos = input.rfind('_');
  std::string soff = input.substr(pos + 1);
  offset = eos::common::StringConversion::GetDataSizeFromString(soff.c_str());
  return offset;
}

//------------------------------------------------------------------------------
// Stat path
//------------------------------------------------------------------------------
int
XrdFstOfs::stat(const char* path,
                struct stat* buf,
                XrdOucErrInfo& out_error,
                const XrdSecEntity* client,
                const char* opaque)
{
  EPNAME("stat");
  memset(buf, 0, sizeof(struct stat));
  XrdOucString url = path;

  if (url.beginswith("/#/")) {
    url.replace("/#/", "");
    XrdOucString url64;
    eos::common::SymKey::DeBase64(url, url64);
    fprintf(stderr, "doing stat for %s\n", url64.c_str());
    // use an IO object to stat this ...
    std::unique_ptr<FileIo> io(eos::fst::FileIoPlugin::GetIoObject(url64.c_str()));

    if (io) {
      if (io->fileStat(buf)) {
        return gOFS.Emsg(epname, out_error, errno, "stat file", url64.c_str());
      } else {
        return SFS_OK;
      }
    } else {
      return gOFS.Emsg(epname, out_error, EINVAL,
                       "stat file - IO object not supported", url64.c_str());
    }
  }

  if (!XrdOfsOss->Stat(path, buf)) {
    // we store the mtime.ns time in st_dev ... sigh@Xrootd ...
#ifdef __APPLE__
    unsigned long nsec = buf->st_mtimespec.tv_nsec;
#else
    unsigned long nsec = buf->st_mtim.tv_nsec;
#endif
    // mask for 10^9
    nsec &= 0x7fffffff;
    // enable bit 32 as indicator
    nsec |= 0x80000000;
    // overwrite st_dev
    buf->st_dev = nsec;
    return SFS_OK;
  } else {
    return gOFS.Emsg(epname, out_error, errno, "stat file", path);
  }
}

//------------------------------------------------------------------------------
// CallManager function
//------------------------------------------------------------------------------
int
XrdFstOfs::CallManager(XrdOucErrInfo* error, const char* path,
                       const char* manager, XrdOucString& capOpaqueFile,
                       XrdOucString* return_result, unsigned short timeout,
                       bool use_xrd_conn_pool, bool retry)
{
  EPNAME("CallManager");
  int rc = SFS_OK;
  XrdOucString msg = "";
  XrdCl::Buffer arg;
  XrdCl::XRootDStatus status;
  XrdOucString address = "root://";
  XrdOucString lManager;
  size_t tried = 0;

  if (!manager) {
    // use the broadcasted manager name
    XrdSysMutexHelper lock(Config::gConfig.Mutex);
    lManager = Config::gConfig.Manager.c_str();
    address += lManager.c_str();
  } else {
    address += manager;
  }

  address += "//dummy?xrd.wantprot=sss";
  XrdCl::URL url(address.c_str());

  if (!url.IsValid()) {
    eos_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

  // Use xrd connection pool if is requested by the caller and this is
  // is allowed globally.
  std::unique_ptr<eos::common::XrdConnIdHelper> conn_helper;

  if (use_xrd_conn_pool) {
    if (getenv("EOS_FST_CALL_MANAGER_XRD_POOL")) {
      conn_helper.reset(new eos::common::XrdConnIdHelper(*mMgmXrdPool, url));

      if (conn_helper->HasNewConnection()) {
        eos_info("msg=\"using url=%s\"", url.GetURL().c_str());
      }
    }
  }

  // Request sss authentication on the MGM side
  std::string opaque = capOpaqueFile.c_str();
  // Get XrdCl::FileSystem object
  // !!! WATCH OUT: GOTO ANCHOR !!!
  std::unique_ptr<XrdCl::FileSystem> fs;
  std::unique_ptr<XrdCl::Buffer> response;
  XrdCl::Buffer* responseRaw = nullptr;
again:
  fs.reset(new XrdCl::FileSystem(url));

  if (!fs) {
    eos_err("error=failed to get new FS object");

    if (error) {
      gOFS.Emsg(epname, *error, ENOMEM,
                "allocate FS object calling the manager node for fn=", path);
    }

    return EINVAL;
  }

  arg.FromString(opaque);
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, responseRaw, timeout);
  response.reset(responseRaw);
  responseRaw = nullptr;

  if (status.IsOK()) {
    eos_static_debug("msg=\"MGM query succeeded\" opaque=\"%s\"", opaque.c_str());
    rc = SFS_OK;
  } else {
    eos_static_err("msg=\"MGM query failed\" opaque=\"%s\"", opaque.c_str());
    msg = (status.GetErrorMessage().c_str());
    rc = SFS_ERROR;

    if (msg.find("[EIDRM]") != STR_NPOS) {
      rc = EIDRM;
    }

    if (msg.find("[EBADE]") != STR_NPOS) {
      rc = EBADE;
    }

    if (msg.find("[EBADR]") != STR_NPOS) {
      rc = EBADR;
    }

    if (msg.find("[EINVAL]") != STR_NPOS) {
      rc = EINVAL;
    }

    if (msg.find("[EADV]") != STR_NPOS) {
      rc = EADV;
    }

    if (msg.find("[EAGAIN]") != STR_NPOS) {
      rc = EAGAIN;
    }

    if (msg.find("[ENOTCONN]") != STR_NPOS) {
      rc = ENOTCONN;
    }

    if (msg.find("[EPROTO]") != STR_NPOS) {
      rc = EPROTO;
    }

    if (msg.find("[EREMCHG]") != STR_NPOS) {
      rc = EREMCHG;
    }

    if (rc != SFS_ERROR) {
      return gOFS.Emsg(epname, *error, rc, msg.c_str(), path);
    } else {
      eos_static_err("msg=\"query error\" status=%d code=%d", status.status,
                     status.code);

      if (retry && (status.code >= 100) && (status.code <= 300) && (!timeout)) {
        // implement automatic retry - network errors will be cured at some point
        std::this_thread::sleep_for(std::chrono::seconds(1));
        tried++;
        eos_static_info("msg=\"retry query\" query=\"%s\"", opaque.c_str());

        if (!manager || (tried > 60)) {
          // use the broadcasted manager name in the repeated try
          XrdSysMutexHelper lock(Config::gConfig.Mutex);
          lManager = Config::gConfig.Manager.c_str();
          address = "root://";
          address += lManager.c_str();
          address += "//dummy";
          url.Clear();
          url.FromString((address.c_str()));
        }

        goto again;
      }

      return gOFS.Emsg(epname, *error, ECOMM, msg.c_str(), path);
    }
  }

  if (response && return_result) {
    *return_result = response->GetBuffer();
  }

  return rc;
}

//------------------------------------------------------------------------------
// Set debug level based on the env info
//------------------------------------------------------------------------------
void
XrdFstOfs::SetDebug(XrdOucEnv& env)
{
  XrdOucString debugnode = env.Get("mgm.nodename");
  XrdOucString debuglevel = env.Get("mgm.debuglevel");
  XrdOucString filterlist = env.Get("mgm.filter");
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  int debugval = g_logging.GetPriorityByString(debuglevel.c_str());

  if (debugval < 0) {
    eos_err("debug level %s is not known!", debuglevel.c_str());
  } else {
    // We set the shared hash debug for the lowest 'debug' level
    if (debuglevel == "debug") {
      ObjectManager.SetDebug(true);
    } else {
      ObjectManager.SetDebug(false);
    }

    g_logging.SetLogPriority(debugval);
    eos_notice("setting debug level to <%s>", debuglevel.c_str());

    if (filterlist.length()) {
      g_logging.SetFilter(filterlist.c_str());
      eos_notice("setting message logid filter to <%s>", filterlist.c_str());
    }
  }
}

//------------------------------------------------------------------------------
// Set real time log level
//------------------------------------------------------------------------------
void
XrdFstOfs::SendRtLog(XrdMqMessage* message)
{
  XrdOucEnv opaque(message->GetBody());
  XrdOucString queue = opaque.Get("mgm.rtlog.queue");
  XrdOucString lines = opaque.Get("mgm.rtlog.lines");
  XrdOucString tag = opaque.Get("mgm.rtlog.tag");
  XrdOucString filter = opaque.Get("mgm.rtlog.filter");
  XrdOucString stdOut = "";

  if (!filter.length()) {
    filter = " ";
  }

  if ((!queue.length()) || (!lines.length()) || (!tag.length())) {
    eos_err("illegal parameter queue=%s lines=%s tag=%s", queue.c_str(),
            lines.c_str(), tag.c_str());
  } else {
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

    if ((g_logging.GetPriorityByString(tag.c_str())) == -1) {
      eos_err("mgm.rtlog.tag must be info,debug,err,emerg,alert,crit,warning or notice");
    } else {
      int logtagindex = g_logging.GetPriorityByString(tag.c_str());

      for (int j = 0; j <= logtagindex; j++) {
        for (int i = 1; i <= atoi(lines.c_str()); i++) {
          g_logging.gMutex.Lock();
          XrdOucString logline = g_logging.gLogMemory[j][
                                   (g_logging.gLogCircularIndex[j] - i +
                                    g_logging.gCircularIndexSize) %
                                   g_logging.gCircularIndexSize].c_str();
          g_logging.gMutex.UnLock();

          if (logline.length() && ((logline.find(filter.c_str())) != STR_NPOS)) {
            stdOut += logline;
            stdOut += "\n";
          }

          if (stdOut.length() > (4 * 1024)) {
            XrdMqMessage repmessage("rtlog reply message");
            repmessage.SetBody(stdOut.c_str());

            if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message)) {
              eos_err("unable to send rtlog reply message to %s",
                      message->kMessageHeader.kSenderId.c_str());
            }

            stdOut = "";
          }

          if (!logline.length()) {
            break;
          }
        }
      }
    }
  }

  if (stdOut.length()) {
    XrdMqMessage repmessage("rtlog reply message");
    repmessage.SetBody(stdOut.c_str());

    if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message)) {
      eos_err("unable to send rtlog reply message to %s",
              message->kMessageHeader.kSenderId.c_str());
    }
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdFstOfs::SendFsck(XrdMqMessage* message)
{
  XrdOucString stdOut = "";
  eos::common::RWMutexReadLock fs_rd_lock(gOFS.Storage->mFsMutex);

  for (const auto& elem : gOFS.Storage->mFsMap) {
    auto fs = elem.second;

    // Don't report filesystems which are not booted!
    if (fs->GetStatus() != eos::common::BootStatus::kBooted) {
      continue;
    }

    eos::common::FileSystem::fsid_t fsid = fs->GetLocalId();
    eos::common::RWMutexReadLock rd_lock(fs->mInconsistencyMutex);
    const auto& icset = fs->GetInconsistencySets();

    for (auto icit = icset.cbegin(); icit != icset.cend(); ++icit) {
      char stag[4096];
      snprintf(stag, sizeof(stag) - 1, "%s@%lu", icit->first.c_str(),
               (unsigned long) fsid);
      stdOut += stag;

      for (auto fit = icit->second.cbegin(); fit != icit->second.cend(); ++fit) {
        // Don't report files which are currently write-open
        if (gOFS.openedForWriting.isOpen(fsid, *fit)) {
          continue;
        }

        char sfid[4096];
        snprintf(sfid, sizeof(sfid) - 1, ":%08llx", *fit);
        stdOut += sfid;

        if (stdOut.length() > (64 * 1024)) {
          stdOut += "\n";
          XrdMqMessage repmessage("fsck reply message");
          repmessage.SetBody(stdOut.c_str());
          repmessage.MarkAsMonitor();

          if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message)) {
            eos_static_err("msg=\"unable to send fsck reply message\" dst=%s",
                           message->kMessageHeader.kSenderId.c_str());
          }

          stdOut = stag;
        }
      }

      stdOut += "\n";
    }
  }

  eos_static_debug("msg=\"fsck reply\" data=\"%s\"", stdOut.c_str());

  if (stdOut.length()) {
    XrdMqMessage repmessage("fsck reply message");
    repmessage.SetBody(stdOut.c_str());
    repmessage.MarkAsMonitor();

    if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message)) {
      eos_static_err("msg=\"unable to send fsck reply message\" dst=%s",
                     message->kMessageHeader.kSenderId.c_str());
    }
  }
}

//------------------------------------------------------------------------------
// Remove entry - interface function
//------------------------------------------------------------------------------
int
XrdFstOfs::rem(const char* path,
               XrdOucErrInfo& error,
               const XrdSecEntity* client,
               const char* opaque)
{
  EPNAME("rem");
  XrdOucString stringOpaque = opaque;
  stringOpaque.replace("?", "&");
  stringOpaque.replace("&&", "&");
  XrdOucEnv openOpaque(stringOpaque.c_str());
  XrdOucEnv* capOpaque = 0;
  int caprc = 0;

  if ((caprc = gCapabilityEngine.Extract(&openOpaque, capOpaque))) {
    // No capability - go away!
    if (capOpaque) {
      delete capOpaque;
      capOpaque = 0;
    }

    return gOFS.Emsg(epname, error, caprc, "remove - capability illegal", path);
  }

  int envlen;

  if (capOpaque) {
    eos_info("path=%s info=%s capability=%s", path, opaque,
             capOpaque->Env(envlen));
  } else {
    eos_info("path=%s info=%s", path, opaque);
    return gOFS.Emsg(epname, error, caprc, "remove - empty capability", path);
  }

  int rc = _rem(path, error, client, capOpaque);

  if (capOpaque) {
    delete capOpaque;
    capOpaque = 0;
  }

  return rc;
}

//------------------------------------------------------------------------------
// Remove entry - low level function
//------------------------------------------------------------------------------
int
XrdFstOfs::_rem(const char* path, XrdOucErrInfo& error,
                const XrdSecEntity* client, XrdOucEnv* capOpaque,
                const char* fstpath, unsigned long long fid,
                unsigned long fsid, bool ignoreifnotexist)
{
  EPNAME("rem");
  std::string fstPath = "";
  const char* localprefix = 0;
  const char* hexfid = 0;
  const char* sfsid = 0;
  eos_debug("");

  if ((!fstpath) && (!fsid) && (!fid)) {
    // Standard deletion brings all information via the opaque info
    if (!(localprefix = capOpaque->Get("mgm.localprefix"))) {
      return gOFS.Emsg(epname, error, EINVAL, "open - no local prefix in capability",
                       path);
    }

    if (!(hexfid = capOpaque->Get("mgm.fid"))) {
      return gOFS.Emsg(epname, error, EINVAL, "open - no file id in capability",
                       path);
    }

    if (!(sfsid = capOpaque->Get("mgm.fsid"))) {
      return gOFS.Emsg(epname, error, EINVAL,
                       "open - no file system id in capability", path);
    }

    fstPath = eos::common::FileId::FidPrefix2FullPath(hexfid, localprefix);
    fid = eos::common::FileId::Hex2Fid(hexfid);
    fsid = atoi(sfsid);
  } else {
    // Deletion during close provides the local storage path, fid & fsid
    fstPath = fstpath;
  }

  eos_info("fstpath=%s", fstPath.c_str());
  int rc = 0;
  errno = 0; // If file not found this will be ENOENT
  struct stat sbd;
  sbd.st_size = 0;

  // Unlink file and possible blockxs file - for local files we need to go
  // through XrdOfs::rem to also clean up any potential blockxs files
  if (eos::common::LayoutId::GetIoType(fstPath.c_str()) ==
      eos::common::LayoutId::kLocal) {
    // get the size before deletion
    XrdOfs::stat(fstPath.c_str(), &sbd, error, client, 0);
    rc = XrdOfs::rem(fstPath.c_str(), error, client, 0);

    if (rc) {
      eos_info("rc=%i, errno=%i", rc, errno);
    }
  } else {
    // Check for additional opaque info to create remote IO object
    std::string sFstPath = fstPath.c_str();
    std::string s3credentials =
      gOFS.Storage->GetFileSystemById(fsid)->GetString("s3credentials");

    if (!s3credentials.empty()) {
      sFstPath += "?s3credentials=" + s3credentials;
    }

    std::unique_ptr<FileIo> io(eos::fst::FileIoPlugin::GetIoObject(
                                 sFstPath.c_str()));

    if (!io) {
      return gOFS.Emsg(epname, error, EINVAL, "open - no IO plug-in avaialble",
                       sFstPath.c_str());
    }

    // get the size before deletion
    io->fileStat(&sbd);
    rc = io->fileRemove();
  }

  // Cleanup possible transactions - there should be no open transaction for this
  // file, in any case there is nothing to do here.
  (void) gOFS.Storage->CloseTransaction(fsid, fid);

  if (rc) {
    if (errno == ENOENT) {
      // Ignore error if a file to be deleted doesn't exist
      if (ignoreifnotexist) {
        rc = 0;
      } else {
        eos_notice("unable to delete file - file does not exist (anymore): %s "
                   "fstpath=%s fsid=%lu id=%llu", path, fstPath.c_str(), fsid, fid);
      }
    }

    if (rc) {
      return gOFS.Emsg(epname, error, errno, "delete file", fstPath.c_str());
    }
  } else {
    // make a deletion report entry
    MakeDeletionReport(fsid, fid, sbd);
  }

  gFmdDbMapHandler.LocalDeleteFmd(fid, fsid);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Query file system information
//------------------------------------------------------------------------------
int
XrdFstOfs::fsctl(const int cmd, const char* args, XrdOucErrInfo& error,
                 const XrdSecEntity* client)
{
  static const char* epname = "fsctl";
  const char* tident = error.getErrUser();

  if ((cmd == SFS_FSCTL_LOCATE)) {
    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    rType[1] = 'r'; //(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp, "[::%s:%d] ", mHostName, myPort);
    error.setErrInfo(strlen(locResp) + 3, (const char**) Resp, 2);
    ZTRACE(fsctl, "located at headnode: " << locResp);
    return SFS_DATA;
  }

  return gOFS.Emsg(epname, error, EPERM, "execute fsctl function", "");
}

//------------------------------------------------------------------------------
// Function dealing with plugin calls
//------------------------------------------------------------------------------
int
XrdFstOfs::FSctl(const int cmd, XrdSfsFSctl& args, XrdOucErrInfo& error,
                 const XrdSecEntity* client)
{
  char ipath[16384];
  char iopaque[16384];
  static const char* epname = "FSctl";
  const char* tident = error.getErrUser();

  if ((cmd == SFS_FSCTL_LOCATE)) {
    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    rType[1] = 'r'; //(fstat.st_mode & S_IWUSR ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp, "[::%s:%d] ", mHostName, myPort);
    error.setErrInfo(strlen(locResp) + 3, (const char**) Resp, 2);
    ZTRACE(fsctl, "located at headnode: " << locResp);
    return SFS_DATA;
  }

  // Accept only plugin calls!
  if (cmd != SFS_FSCTL_PLUGIN) {
    return gOFS.Emsg(epname, error, EPERM, "execute non-plugin function", "");
  }

  if (args.Arg1Len) {
    if (args.Arg1Len < 16384) {
      strncpy(ipath, args.Arg1, args.Arg1Len);
      ipath[args.Arg1Len] = 0;
    } else {
      return gOFS.Emsg(epname, error, EINVAL,
                       "convert path argument - string too long", "");
    }
  } else {
    ipath[0] = 0;
  }

  if (args.Arg2Len) {
    if (args.Arg2Len < 16384) {
      strncpy(iopaque, args.Arg2, args.Arg2Len);
      iopaque[args.Arg2Len] = 0;
    } else {
      return gOFS.Emsg(epname, error, EINVAL,
                       "convert opaque argument - string too long", "");
    }
  } else {
    iopaque[0] = 0;
  }

  // From here on we can deal with XrdOucString which is more 'comfortable'
  XrdOucString path = ipath;
  XrdOucString opaque = iopaque;
  XrdOucString result = "";
  XrdOucEnv env(opaque.c_str());
  eos_debug("tident=%s path=%s opaque=%s", tident, path.c_str(), opaque.c_str());
  const char* scmd;

  if ((scmd = env.Get("fst.pcmd"))) {
    XrdOucString execmd = scmd;

    if (execmd == "getfmd") {
      char* afid = env.Get("fst.getfmd.fid");
      char* afsid = env.Get("fst.getfmd.fsid");

      if ((!afid) || (!afsid)) {
        return Emsg(epname, error, EINVAL, "execute FSctl command", path.c_str());
      }

      unsigned long long fileid = eos::common::FileId::Hex2Fid(afid);
      unsigned long fsid = atoi(afsid);
      auto fmd = gFmdDbMapHandler.LocalGetFmd(fileid, fsid, true);

      if (!fmd) {
        eos_static_err("msg=\"no FMD record found\" fxid=%08llx fsid=%lu", fileid,
                       fsid);
        const char* err = "ERROR";
        error.setErrInfo(strlen(err) + 1, err);
        return SFS_DATA;
      }

      auto fmdenv = fmd->FmdToEnv();
      int envlen;
      XrdOucString fmdenvstring = fmdenv->Env(envlen);
      error.setErrInfo(fmdenvstring.length() + 1, fmdenvstring.c_str());
      return SFS_DATA;
    }

    if (execmd == "getxattr") {
      char* key = env.Get("fst.getxattr.key");
      char* path = env.Get("fst.getxattr.path");

      if (!key) {
        eos_static_err("no key specified as attribute name");
        const char* err = "ERROR";
        error.setErrInfo(strlen(err) + 1, err);
        return SFS_DATA;
      }

      if (!path) {
        eos_static_err("no path specified to get the attribute from");
        const char* err = "ERROR";
        error.setErrInfo(strlen(err) + 1, err);
        return SFS_DATA;
      }

      char value[1024];
#ifdef __APPLE__
      ssize_t attr_length = getxattr(path, key, value, sizeof(value), 0, 0);
#else
      ssize_t attr_length = getxattr(path, key, value, sizeof(value));
#endif

      if (attr_length > 0) {
        value[1023] = 0;
        XrdOucString skey = key;
        XrdOucString attr = "";

        if (skey == "user.eos.checksum") {
          // Checksum's are binary and need special reformatting (we swap the
          // byte order if they are 4 bytes long )
          if (attr_length == 4) {
            for (ssize_t k = 0; k < 4; k++) {
              char hex[4];
              snprintf(hex, sizeof(hex) - 1, "%02x", (unsigned char) value[3 - k]);
              attr += hex;
            }
          } else {
            for (ssize_t k = 0; k < attr_length; k++) {
              char hex[4];
              snprintf(hex, sizeof(hex) - 1, "%02x", (unsigned char) value[k]);
              attr += hex;
            }
          }
        } else {
          attr = value;
        }

        error.setErrInfo(attr.length() + 1, attr.c_str());
        return SFS_DATA;
      } else {
        eos_static_err("getxattr failed for path=%s", path);
        const char* err = "ERROR";
        error.setErrInfo(strlen(err) + 1, err);
        return SFS_DATA;
      }
    }
  }

  return Emsg(epname, error, EINVAL, "execute FSctl command", path.c_str());
}

//------------------------------------------------------------------------------
// Stall message for the client
//------------------------------------------------------------------------------
int
XrdFstOfs::Stall(XrdOucErrInfo& error,  // Error text & code
                 int stime, // Seconds to stall
                 const char* msg) // Message to give
{
  XrdOucString smessage = msg;
  smessage += "; come back in ";
  smessage += stime;
  smessage += " seconds!";
  EPNAME("Stall");
  const char* tident = error.getErrUser();
  ZTRACE(delay, "Stall " << stime << ": " << smessage.c_str());
  // Place the error message in the error object and return
  error.setErrInfo(0, smessage.c_str());
  // All done
  return stime;
}

//------------------------------------------------------------------------------
// Redirect message for the client
//------------------------------------------------------------------------------
int
XrdFstOfs::Redirect(XrdOucErrInfo& error,  // Error text & code
                    const char* host,
                    int& port)
{
  EPNAME("Redirect");
  const char* tident = error.getErrUser();
  ZTRACE(delay, "Redirect " << host << ":" << port);
  // Place the error message in the error object and return
  error.setErrInfo(port, host);
  // All done
  return SFS_REDIRECT;
}

//------------------------------------------------------------------------------
// When getting queried for checksum at the diskserver redirect to the MGM
//------------------------------------------------------------------------------
int
XrdFstOfs::chksum(XrdSfsFileSystem::csFunc Func, const char* csName,
                  const char* inpath, XrdOucErrInfo& error,
                  const XrdSecEntity* client, const char* ininfo)
{
  int ecode = 1094;
  XrdOucString RedirectManager;
  {
    XrdSysMutexHelper lock(eos::fst::Config::gConfig.Mutex);
    RedirectManager = eos::fst::Config::gConfig.Manager;
  }
  int pos = RedirectManager.find(":");

  if (pos != STR_NPOS) {
    RedirectManager.erase(pos);
  }

  return gOFS.Redirect(error, RedirectManager.c_str(), ecode);
}

//------------------------------------------------------------------------------
// Wait for ongoing IO operations to finish
//------------------------------------------------------------------------------
bool
XrdFstOfs::WaitForOngoingIO(std::chrono::seconds timeout)
{
  bool all_done = true;
  std::chrono::seconds check_interval(5);
  auto deadline = std::chrono::steady_clock::now() + timeout;

  while (std::chrono::steady_clock::now() <= deadline) {
    all_done = true;
    {
      XrdSysMutexHelper scope_lock(OpenFidMutex);
      all_done = ! openedForWriting.isAnyOpen();

      if (all_done) {
        all_done = ! openedForReading.isAnyOpen();
      }

      if (all_done) {
        break;
      }
    }
    std::this_thread::sleep_for(check_interval);
  }

  return all_done;
}

//------------------------------------------------------------------------------
// Report file deletion
//------------------------------------------------------------------------------
void
XrdFstOfs::MakeDeletionReport(eos::common::FileSystem::fsid_t fsid,
                              unsigned long long fid,
                              struct stat& deletion_stat)
{
  XrdOucString reportString;
  char report[16384];
  snprintf(report, sizeof(report) - 1,
           "log=%s&"
           "host=%s&fid=%llu&fxid=%08llx&fsid=%u&"
           "dc_ts=%lu&dc_tns=%lu&"
           "dm_ts=%lu&dm_tns=%lu&"
           "da_ts=%lu&da_tns=%lu&"
           "dsize=%li&sec.app=deletion"
           , this->logId, gOFS.mHostName, fid, fid, fsid
#ifdef __APPLE__
           , deletion_stat.st_ctimespec.tv_sec
           , deletion_stat.st_ctimespec.tv_nsec
           , deletion_stat.st_mtimespec.tv_sec
           , deletion_stat.st_mtimespec.tv_nsec
           , deletion_stat.st_atimespec.tv_sec
           , deletion_stat.st_atimespec.tv_nsec
#else
           , deletion_stat.st_ctim.tv_sec
           , deletion_stat.st_ctim.tv_nsec
           , deletion_stat.st_mtim.tv_sec
           , deletion_stat.st_mtim.tv_nsec
           , deletion_stat.st_atim.tv_sec
           , deletion_stat.st_atim.tv_nsec
#endif
           , deletion_stat.st_size);
  reportString = report;
  gOFS.ReportQueueMutex.Lock();
  gOFS.ReportQueue.push(reportString);
  gOFS.ReportQueueMutex.UnLock();
}

//------------------------------------------------------------------------------
// Compute adler checksum of given keytab file
//------------------------------------------------------------------------------
std::string
XrdFstOfs::GetKeytabChecksum(const std::string& kt_path) const
{
  std::string kt_cks = "unaccessible";
  int fd = ::open(kt_path.c_str(), O_RDONLY);

  if (fd >= 0) {
    char buffer[65535];
    size_t nread = ::read(fd, buffer, sizeof(buffer));

    if (nread > 0) {
      std::unique_ptr<CheckSum> KeyCKS =
        ChecksumPlugins::GetChecksumObject(eos::common::LayoutId::kAdler);

      if (KeyCKS) {
        KeyCKS->Add(buffer, nread, 0);
        kt_cks = KeyCKS->GetHexChecksum();
      }
    }

    close(fd);
  }

  return kt_cks;
}

//------------------------------------------------------------------------------
// Request broadcasts from all the registered queues
//------------------------------------------------------------------------------
void
XrdFstOfs::RequestBroadcasts()
{
  using eos::fst::Config;
  eos_notice("%s", "msg=\"requesting broadcasts\"");
  // Create a wildcard broadcast
  XrdMqSharedHash* hash = 0;
  XrdMqSharedQueue* queue = 0;
  // Create a node broadcast
  ObjectManager.CreateSharedHash(Config::gConfig.FstConfigQueueWildcard.c_str(),
                                 Config::gConfig.FstDefaultReceiverQueue.c_str());
  {
    eos::common::RWMutexReadLock rd_lock(ObjectManager.HashMutex);
    hash = ObjectManager.GetHash(Config::gConfig.FstConfigQueueWildcard.c_str());
    hash->BroadcastRequest(Config::gConfig.FstDefaultReceiverQueue.c_str());
  }
  // Create a node gateway broadcast
  ObjectManager.CreateSharedQueue(Config::gConfig.FstGwQueueWildcard.c_str(),
                                  Config::gConfig.FstDefaultReceiverQueue.c_str());
  {
    eos::common::RWMutexReadLock rd_lock(ObjectManager.HashMutex);
    queue = ObjectManager.GetQueue(Config::gConfig.FstGwQueueWildcard.c_str());
    queue->BroadcastRequest(Config::gConfig.FstDefaultReceiverQueue.c_str());
  }
  // Create a filesystem broadcast
  ObjectManager.CreateSharedHash(Config::gConfig.FstQueueWildcard.c_str(),
                                 Config::gConfig.FstDefaultReceiverQueue.c_str());
  {
    eos::common::RWMutexReadLock rd_lock(ObjectManager.HashMutex);
    hash = ObjectManager.GetHash(Config::gConfig.FstQueueWildcard.c_str());
    hash->BroadcastRequest(Config::gConfig.FstDefaultReceiverQueue.c_str());
  }
}

EOSFSTNAMESPACE_END
