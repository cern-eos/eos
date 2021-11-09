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

#include "fst/XrdFstOfs.hh"
#include "fst/XrdFstOss.hh"
#include "fst/Config.hh"
#include "fst/filemd/FmdDbMap.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/http/HttpServer.hh"
#include "fst/storage/FileSystem.hh"
#include "fst/storage/Storage.hh"
#include "fst/Messaging.hh"
#include "fst/Deletion.hh"
#include "fst/Verify.hh"
#include "common/PasswordHandler.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/Path.hh"
#include "common/Statfs.hh"
#include "common/SyncAll.hh"
#include "common/StackTrace.hh"
#include "common/Timing.hh"
#include "common/eos_cta_pb/EosCtaAlertHandler.hh"
#include "common/Constants.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "common/SymKeys.hh"
#include "common/XattrCompat.hh"
#include "common/ParseUtils.hh"
#include "common/ShellCmd.hh"
#include "common/BufferManager.hh"
#include "mq/SharedHashWrapper.hh"
#include "XrdNet/XrdNetOpts.hh"
#include "XrdNet/XrdNetUtils.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "Xrd/XrdScheduler.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdVersion.hh"
#include "qclient/Members.hh"
#include "qclient/shared/SharedManager.hh"
#include "proto/Delete.pb.h"
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
#include <cctype>
#include <algorithm>
#include <sys/prctl.h>

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

    fprintf(stderr,"pcrctl= %d\n", prctl(PR_SET_KEEPCAPS, 1, 0, 0, 0));

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

  eos_static_warning("%s", "op=shutdown msg=\"stopped messaging\"");
  gOFS.Storage->Shutdown();
  eos_static_warning("%s", "op=shutdown msg=\"stopped storage activities\"");
  gFmdDbMapHandler.Shutdown();
  eos_static_warning("%s", "op=shutdown msg=\"stopped FmdDbMap handler\"");

  if (watchdog > 1) {
    kill(watchdog, 9);
  }

  int wstatus = 0;
  wait(&wstatus);
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

  gOFS.Storage->Shutdown();
  eos_static_warning("op=shutdown msg=\"shutdown fmddbmap handler\"");
  gFmdDbMapHandler.Shutdown();
  eos_static_warning("op=shutdown status=dbmapclosed");

  if (watchdog > 1) {
    kill(watchdog, 9);
  }

  int wstatus = 0;
  ::wait(&wstatus);
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
  mGeoTag("nogeotag"),
  mXrdBuffPool(eos::common::KB, 32 * eos::common::MB),
  mCloseThreadPool(8, 64, 5, 6, 5, "async_close"),
  mMgmXrdPool(nullptr), mSimIoReadErr(false), mSimIoWriteErr(false),
  mSimXsReadErr(false), mSimXsWriteErr(false), mSimFmdOpenErr(false),
  mSimErrIoReadOff(0ull), mSimErrIoWriteOff(0ull), mSimDiskWriting(false)
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

  if (getenv("EOS_FST_ENABLE_STACKTRACE")) {
    // Add stacktrace handler - this is useful for crashes inside containers
    // where abrtd is not configured
    (void) signal(SIGSEGV, xrdfstofs_stacktrace);
    (void) signal(SIGABRT, xrdfstofs_stacktrace);
    (void) signal(SIGBUS, xrdfstofs_stacktrace);
  }

  if (getenv("EOS_MGM_ALIAS")) {
    // Use MGM alias if available
    mMgmAlias = getenv("EOS_MGM_ALIAS");
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

  UpdateTpcKeyValidity();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFstOfs::~XrdFstOfs()
{
  if (mHostName) {
    free(const_cast<char*>(mHostName));
  }
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
  {
    // Run a dummy command so that the ShellExecutor is forked before any XrdCl
    // is initialized. Otherwise it might segv due to the following bug:
    // https://github.com/xrootd/xrootd/issues/1515
    eos::common::ShellCmd dummy_cmd("uname -a");
  }

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
  const char* errtext = 0;
  mHostName = XrdNetUtils::MyHostName(0, &errtext);

  if (!mHostName) {
    Eroute.Emsg("Config", "hostname is invalid : %s", mHostName);
    return 1;
  }

  TransferScheduler = new XrdScheduler(&Eroute, &OfsTrace, 8, 128, 60);
  TransferScheduler->Start();
  gConfig.autoBoot = false;
  gConfig.FstOfsBrokerUrl = "root://localhost:1097//eos/";

  if (getenv("EOS_BROKER_URL")) {
    gConfig.FstOfsBrokerUrl = getenv("EOS_BROKER_URL");
  }

  // Handle geotag configuration
  char* ptr_geotag = getenv("EOS_GEOTAG");

  if (ptr_geotag) {
    mGeoTag = eos::common::SanitizeGeoTag(ptr_geotag);

    if (mGeoTag.empty()) {
      eos_static_err("%s", "msg=\"failed to update geotag, wrongly formatted\" "
                     "geotag=\"%s\"", ptr_geotag);
      return 1;
    }
  }

  {
    // set the start date as string
    XrdOucString out = "";
    time_t t = time(NULL);
    struct tm* timeinfo;
    timeinfo = localtime(&t);
    out = asctime(timeinfo);
    out.erase(out.length() - 1);
    gConfig.StartDate = out.c_str();
  }

  gConfig.FstMetaLogDir = "/var/tmp/eos/md/";
  gConfig.FstAuthDir = "/var/eos/auth/";
  setenv("XrdClientEUSER", "daemon", 1);
  SetXrdClTimeouts();
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
              gConfig.FstOfsBrokerUrl = getenv("EOS_BROKER_URL");
            } else {
              gConfig.FstOfsBrokerUrl = val;
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
              gConfig.autoBoot = true;
            }
          }
        }

        if (!strcmp("metalog", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument 2 for metalog missing");
            NoGo = 1;
          } else {
            if (strlen(val)) {
              gConfig.FstMetaLogDir = val;

              if (val[strlen(val) - 1] != '/') {
                gConfig.FstMetaLogDir += '/';
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
              gConfig.FstAuthDir = val;

              if (val[strlen(val) - 1] != '/') {
                gConfig.FstAuthDir += '/';
              }
            }
          }
        }

        if (!strcmp("protowfendpoint", var)) {
          if ((val = Config.GetWord())) {
            gConfig.ProtoWFEndpoint = val;
          }
        }

        if (!strcmp("protowfresource", var)) {
          if ((val = Config.GetWord())) {
            gConfig.ProtoWFResource = val;
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

  if (!mQdbContactDetails.members.empty() &&
      mQdbContactDetails.password.empty()) {
    Eroute.Say("=====> Configuration error: Found QDB cluster members, but no password."
               " EOS will only connect to password-protected QDB instances. (fstofs.qdbpassword / fstofs.qdbpassword_file missing)");
    return 1;
  }

  if (gConfig.autoBoot) {
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

  if (!gConfig.FstOfsBrokerUrl.endswith("/")) {
    gConfig.FstOfsBrokerUrl += "/";
  }

  gConfig.FstDefaultReceiverQueue =
    gConfig.FstOfsBrokerUrl;
  gConfig.FstOfsBrokerUrl += mHostName;
  gConfig.FstOfsBrokerUrl += ":";
  gConfig.FstOfsBrokerUrl += myPort;
  gConfig.FstOfsBrokerUrl += "/fst";
  gConfig.FstHostPort = mHostName;
  gConfig.FstHostPort += ":";
  gConfig.FstHostPort += myPort;
  gConfig.KernelVersion =
    eos::common::StringConversion::StringFromShellCmd("uname -r | tr -d \"\n\"").c_str();
  Eroute.Say("=====> fstofs.broker : ",
             gConfig.FstOfsBrokerUrl.c_str(), "");
  // Extract our queue name
  gConfig.FstQueue = gConfig.FstOfsBrokerUrl;
  {
    int pos1 = gConfig.FstQueue.find("//");
    int pos2 = gConfig.FstQueue.find("//", pos1 + 2);

    if (pos2 != STR_NPOS) {
      gConfig.FstQueue.erase(0, pos2 + 1);
    } else {
      Eroute.Emsg("Config", "cannot determine my queue name: ",
                  gConfig.FstQueue.c_str());
      return 1;
    }
  }
  // Create our wildcard broadcast name
  gConfig.FstQueueWildcard = gConfig.FstQueue;
  gConfig.FstQueueWildcard += "/*";
  // Create our wildcard config broadcast name
  gConfig.FstConfigQueueWildcard = "*/";
  gConfig.FstConfigQueueWildcard += mHostName;
  gConfig.FstConfigQueueWildcard += ":";
  gConfig.FstConfigQueueWildcard += myPort;
  // Create our wildcard gw broadcast name
  gConfig.FstGwQueueWildcard = "*/";
  gConfig.FstGwQueueWildcard += mHostName;
  gConfig.FstGwQueueWildcard += ":";
  gConfig.FstGwQueueWildcard += myPort;
  gConfig.FstGwQueueWildcard += "/fst/gw/txqueue/txq";
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
  // Compute checksum of the keytab file
  std::string kt_cks = GetKeytabChecksum("/etc/eos.keytab");
  gConfig.KeyTabAdler = kt_cks.c_str();
  // Create the messaging object(recv thread)
  gConfig.FstDefaultReceiverQueue += "*/mgm";
  int pos1 = gConfig.FstDefaultReceiverQueue.find("//");
  int pos2 = gConfig.FstDefaultReceiverQueue.find("//",
             pos1 + 2);

  if (pos2 != STR_NPOS) {
    gConfig.FstDefaultReceiverQueue.erase(0, pos2 + 1);
  }

  Eroute.Say("=====> fstofs.defaultreceiverqueue : ",
             gConfig.FstDefaultReceiverQueue.c_str(), "");
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

  mMessagingRealm.reset(new mq::MessagingRealm(&ObjectManager, &ObjectNotifier,
                        &XrdMqMessaging::gMessageClient, mQSOM.get()));
  // Setup auth dir
  {
    XrdOucString scmd = "mkdir -p ";
    scmd += gConfig.FstAuthDir;
    scmd += " ; chown -R daemon ";
    scmd += gConfig.FstAuthDir;
    scmd += " ; chmod 700 ";
    scmd += gConfig.FstAuthDir;
    int src = system(scmd.c_str());

    if (src) {
      eos_err("%s returned %d", scmd.c_str(), src);
    }

    if (access(gConfig.FstAuthDir.c_str(),
               R_OK | W_OK | X_OK)) {
      Eroute.Emsg("Config", "cannot access the auth directory for r/w: ",
                  gConfig.FstAuthDir.c_str());
      return 1;
    }

    Eroute.Say("=====> fstofs.authdir : ",
               gConfig.FstAuthDir.c_str());
  }
  // Attach Storage to the meta log dir
  Storage = eos::fst::Storage::Create(
              gConfig.FstMetaLogDir.c_str());
  Eroute.Say("=====> fstofs.metalogdir : ",
             gConfig.FstMetaLogDir.c_str());

  if (!Storage) {
    Eroute.Emsg("Config", "cannot setup meta data storage using directory: ",
                gConfig.FstMetaLogDir.c_str());
    return 1;
  }

  ObjectNotifier.SetShareObjectManager(&ObjectManager);

  if (!ObjectNotifier.Start()) {
    eos_crit("error starting the shared object change notifier");
  }

  // Create the specific listener class
  Messaging = new eos::fst::Messaging(
    gConfig.FstOfsBrokerUrl.c_str(),
    gConfig.FstDefaultReceiverQueue.c_str(),
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
  XrdOucString dumperfile = gConfig.FstMetaLogDir;
  dumperfile += "so.fst.dump.";
  dumperfile += gConfig.FstHostPort;
  ObjectManager.StartDumper(dumperfile.c_str());
  // Start the embedded HTTP server
  mHttpdPort = 8001;

  if (getenv("EOS_FST_HTTP_PORT")) {
    try {
      mHttpdPort = std::stol(getenv("EOS_FST_HTTP_PORT"));
    } catch (...) {
      // no change
    }
  }

  mHttpd.reset(new eos::fst::HttpServer(mHttpdPort));

  if (mHttpdPort && mHttpd) {
    mHttpd->Start();
  }

  eos_notice("FST_HOST=%s FST_PORT=%ld FST_HTTP_PORT=%d VERSION=%s RELEASE=%s "
             "KEYTABADLER=%s", mHostName, myPort, mHttpdPort, VERSION, RELEASE,
             kt_cks.c_str());
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
  mSimDiskWriting = false;

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
  } else if (input.find("fake_write") == 0) {
    mSimDiskWriting = true;
  }
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
    XrdSysMutexHelper lock(gConfig.Mutex);
    lManager = gConfig.Manager.c_str();
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
  // allowed globally.
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
          XrdSysMutexHelper lock(gConfig.Mutex);
          lManager = gConfig.Manager.c_str();
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

  if ((caprc = eos::common::SymKey::ExtractCapability(&openOpaque, capOpaque))) {
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
                unsigned long fsid, bool ignoreifnotexist,
                std::string* deletion_report)
{
  EPNAME("rem");
  std::string fstPath = "";
  const char* localprefix = 0;
  const char* hexfid = 0;
  const char* sfsid = 0;

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
    std::string s3credentials = gOFS.Storage->GetFileSystemConfig(fsid,
                                "s3credentials");

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
    if (deletion_report) {
      // just return the report to the caller e.g. storage::Remover
      *deletion_report = MakeDeletionReport(fsid, fid, sbd, false);
    } else {
      // send the report via MQ
      MakeDeletionReport(fsid, fid, sbd, true);
    }
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
  using eos::common::FileId;
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
  eos_static_debug("msg=\"handle query\" tident=%s path=\"%s\" opaque=\"%s\" "
                   "prot=\"%s\"", tident, path.c_str(), opaque.c_str(), client->prot);
  const char* scmd {nullptr};

  if ((scmd = env.Get("fst.pcmd"))) {
    XrdOucString execmd = scmd;

    if (execmd == "debug") {
      return HandleDebug(env, error);
    }

    if (execmd == "fsck") {
      return HandleFsck(env, error);
    }

    if (execmd == "resync") {
      return HandleResync(env, error);
    }

    if (execmd == "rtlog") {
      return HandleRtlog(env, error);
    }

    if (execmd == "verify") {
      return HandleVerify(env, error);
    }

    if (execmd == "drop") {
      return HandleDropFile(env, error);
    }

    if (execmd == "clean_orphans") {
      return HandleCleanOrphans(env, error);
    }

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

    if (execmd == "local_rename") {
      if (strncmp(client->prot, "sss", 3) != 0) {
        eos_static_err("%s", "msg=\"only sss authenticated clients can trigger"
                       " a local rename");
        return gOFS.Emsg(epname, error, EPERM, "do local rename",
                         "- needs sss authentication");
      }

      std::string sfsid = (env.Get("fst.rename.fsid") ?
                           env.Get("fst.rename.fsid") : "");
      std::string sold_fid = (env.Get("fst.rename.ofid") ?
                              env.Get("fst.rename.ofid") : "");
      std::string snew_fid = (env.Get("fst.rename.nfid") ?
                              env.Get("fst.rename.nfid") : "");
      std::string ns_path = (env.Get("fst.nspath") ?
                             env.Get("fst.nspath") : "");
      unsigned long fsid {0ul};
      eos::IFileMD::id_t old_fid {0ull};
      eos::IFileMD::id_t new_fid {0ull};

      try {
        fsid = std::stoul(sfsid);
        // File identifier are in hex!
        old_fid = std::stoull(sold_fid, 0, 16);
        new_fid = std::stoull(snew_fid, 0, 16);
      } catch (...) {}

      if (!fsid || !old_fid || !new_fid) {
        eos_static_err("msg=\"failed local rename, unexpected input \" "
                       "fsid=\"%s\" old_fid=\"%s\" new_fid=\"%s\"",
                       sfsid.c_str(), sold_fid.c_str(), snew_fid.c_str());
        return gOFS.Emsg(epname, error, EINVAL, "do local rename", "");
      }

      // Get the local mount point for the given file system id
      std::string fs_prefix;
      {
        eos::common::RWMutexReadLock lock(gOFS.Storage->mFsMutex);

        if (fsid && gOFS.Storage->mFsMap.count(fsid)) {
          fs_prefix = gOFS.Storage->mFsMap[fsid]->GetPath().c_str();
        }
      }

      if (fs_prefix.empty()) {
        eos_static_err("msg=\"failed to get local prefix for file system\" "
                       "fsid=%08llx", fsid);
        return gOFS.Emsg(epname, error, EINVAL, "do local rename", "");
      }

      std::string old_path =
        FileId::FidPrefix2FullPath(FileId::Fid2Hex(old_fid).c_str(),
                                   fs_prefix.c_str());
      std::string new_path =
        FileId::FidPrefix2FullPath(FileId::Fid2Hex(new_fid).c_str(),
                                   fs_prefix.c_str());
      // Check that new path doesn't exist already
      struct stat info;

      if (::stat(new_path.c_str(), &info) == 0) {
        eos_static_err("msg=\"new path already exists on filesystem\" "
                       "fsid=%08llx new_path=%s", fsid, new_path.c_str());
        return gOFS.Emsg(epname, error, EEXIST, "do local rename", "");
      }

      // Make sure the directory component of the new location exists
      mode_t mode = 0755;
      std::string dirs {new_path};
      size_t pos = dirs.rfind('/');

      if (pos != std::string::npos) {
        dirs.erase(dirs.rfind('/'));
      }

      if (!CreateDirHierarchy(dirs, mode)) {
        eos_static_err("msg=\"failed creating directory hierarchy\" "
                       "fsid=%08llx new_path=%s", fsid, new_path.c_str());
        return gOFS.Emsg(epname, error, EEXIST, "do local rename", "");
      }

      if (::rename(old_path.c_str(), new_path.c_str())) {
        eos_static_err("msg=\"rename failed\" old_path=%s new_path=%s errno=%d",
                       old_path.c_str(), new_path.c_str(), errno);
        return gOFS.Emsg(epname, error, EEXIST, "do local rename", "");
      } else {
        if (!ns_path.empty()) {
          // Update the user.eos.lfn attribute to point to the original
          // namespace file name
          if (setxattr(new_path.c_str(), "user.eos.lfn", ns_path.c_str(),
                       ns_path.length(), 0)) {
            eos_static_warning("msg=\"failed to update the user.eos.lfn xattr\""
                               " local_path=\"%s\" ns_path=\"%s\"",
                               new_path.c_str(), ns_path.c_str());
          }

          // Rename any potential block xs files
          std::string old_xs_path = old_path + ".xsmap";
          std::string new_xs_path = new_path + ".xsmap";

          if (::stat(old_xs_path.c_str(), &info) == 0) {
            if (::rename(old_xs_path.c_str(), new_xs_path.c_str())) {
              eos_static_err("msg=\"block xs file rename failed\" "
                             "old_xs_path=%s new_xs_path=%s errno=%d",
                             old_xs_path.c_str(), new_xs_path.c_str(), errno);
              return gOFS.Emsg(epname, error, EEXIST, "do local rename", "");
            }
          }
        }
      }

      const char* done = "OK";
      error.setErrInfo(strlen(done) + 1, done);
      return SFS_DATA;
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
    XrdSysMutexHelper lock(gConfig.Mutex);
    RedirectManager = gConfig.Manager;
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
std::string
XrdFstOfs::MakeDeletionReport(eos::common::FileSystem::fsid_t fsid,
                              unsigned long long fid,
                              struct stat& deletion_stat,
                              bool viamq)
{
  struct timespec ts_now;
  char report[16384];
  eos::common::Timing::GetTimeSpec(ts_now);
  snprintf(report, sizeof(report) - 1,
           "log=%s&"
           "host=%s&fid=%llu&fxid=%08llx&fsid=%u&"
           "del_ts=%lu&del_tns=%lu&"
           "dc_ts=%lu&dc_tns=%lu&"
           "dm_ts=%lu&dm_tns=%lu&"
           "da_ts=%lu&da_tns=%lu&"
           "dsize=%li&sec.app=deletion"
           , this->logId, gOFS.mHostName, fid, fid, fsid
           , ts_now.tv_sec, ts_now.tv_nsec
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

  if (viamq) {
    XrdOucString reportString = report;
    gOFS.ReportQueueMutex.Lock();
    gOFS.ReportQueue.push(reportString);
    gOFS.ReportQueueMutex.UnLock();
  }

  return report;
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
  ObjectManager.CreateSharedHash(gConfig.FstConfigQueueWildcard.c_str(),
                                 gConfig.FstDefaultReceiverQueue.c_str());
  {
    eos::common::RWMutexReadLock rd_lock(ObjectManager.HashMutex);
    hash = ObjectManager.GetHash(gConfig.FstConfigQueueWildcard.c_str());

    while (!hash->BroadcastRequest(
             gConfig.FstDefaultReceiverQueue.c_str())) {
      eos_static_notice("msg=\"retry broadcast request in 1 second\" hash=\"%s\"",
                        gConfig.FstConfigQueueWildcard.c_str());
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  // Create a node gateway broadcast
  ObjectManager.CreateSharedQueue(gConfig.FstGwQueueWildcard.c_str(),
                                  gConfig.FstDefaultReceiverQueue.c_str());
  {
    eos::common::RWMutexReadLock rd_lock(ObjectManager.HashMutex);
    queue = ObjectManager.GetQueue(gConfig.FstGwQueueWildcard.c_str());

    while (!queue->BroadcastRequest(
             gConfig.FstDefaultReceiverQueue.c_str())) {
      eos_static_notice("msg=\"retry broadcast request in 1 second\" hash=\"%s\"",
                        gConfig.FstGwQueueWildcard.c_str());
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  // Create a filesystem broadcast
  ObjectManager.CreateSharedHash(gConfig.FstQueueWildcard.c_str(),
                                 gConfig.FstDefaultReceiverQueue.c_str());
  {
    eos::common::RWMutexReadLock rd_lock(ObjectManager.HashMutex);
    hash = ObjectManager.GetHash(gConfig.FstQueueWildcard.c_str());

    while (!hash->BroadcastRequest(
             gConfig.FstDefaultReceiverQueue.c_str())) {
      eos_static_notice("msg=\"retry broadcast request in 1 second\" hash=\"%s\"",
                        gConfig.FstQueueWildcard.c_str());
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

//----------------------------------------------------------------------------
//! Update the TPC key validity value (default 120)
//----------------------------------------------------------------------------
void
XrdFstOfs::UpdateTpcKeyValidity()
{
  const char* ptr = getenv("EOS_FST_TPC_KEY_VALIDITY_SEC");

  if (ptr && strlen(ptr)) {
    std::string str(ptr);

    try {
      int validity_sec = std::stoi(str);

      if (validity_sec < 60) {
        validity_sec = 60;
      }

      mTpcKeyValidity = std::chrono::seconds(validity_sec);
      fprintf(stderr, "=====> Update TPC key validity to %li seconds\n",
              mTpcKeyValidity.count());
    } catch (...) {
      // no change
    }
  }
}

//------------------------------------------------------------------------------
// Create directory hierarchy
//------------------------------------------------------------------------------
bool
XrdFstOfs::CreateDirHierarchy(const std::string& dir_hierarchy,
                              mode_t mode) const
{
  struct stat info;
  std::string path = "/";
  auto lst_dirs = eos::common::StringTokenizer::split<std::list<std::string>>
                  (dir_hierarchy, '/');

  for (const auto& dir : lst_dirs) {
    path += dir;
    path += "/";

    if (::stat(path.c_str(), &info) == 0) {
      continue;
    }

    if (::mkdir(path.c_str(), mode) != 0) {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Handle debug query
//------------------------------------------------------------------------------
int
XrdFstOfs::HandleDebug(XrdOucEnv& env, XrdOucErrInfo& err_obj)
{
  std::string dbg_level = (env.Get("fst.debug.level") ?
                           env.Get("fst.debug.level") : "");
  std::string dbg_filter = (env.Get("fst.debug.filter") ?
                            env.Get("fst.debug.filter") : "");
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  int dbg_val = g_logging.GetPriorityByString(dbg_level.c_str());

  if (dbg_val < 0) {
    std::string msg = SSTR("unknown debug level <" << dbg_level << ">");
    eos_err("msg=\"%s\"", msg.c_str());
    err_obj.setErrInfo(EINVAL, msg.c_str());
    return SFS_ERROR;
  }

  // We set the shared hash debug for the lowest 'debug' level
  if (dbg_level == "debug") {
    //ObjectManager.SetDebug(true);
  } else {
    ObjectManager.SetDebug(false);
  }

  g_logging.SetLogPriority(dbg_val);
  eos_notice("msg=\"setting debug level to <%s>\"", dbg_level.c_str());

  if (dbg_filter.length()) {
    g_logging.SetFilter(dbg_filter.c_str());
    eos_notice("setting message logid filter to <%s>", dbg_filter.c_str());
  }

  // @todo(esindril) once xrootd bug regarding handling of SFS_OK response
  // in XrdXrootdXeq is fixed we can just return SFS_OK (>= XRootD 5)
  // return SFS_OK;
  const char* done = "OK";
  err_obj.setErrInfo(strlen(done) + 1, done);
  return SFS_DATA;
}

//------------------------------------------------------------------------------
// Handle fsck query
//------------------------------------------------------------------------------
int
XrdFstOfs::HandleFsck(XrdOucEnv& env, XrdOucErrInfo& err_obj)
{
  std::string response;
  response.reserve(4 * eos::common::MB);
  size_t max_sz = mXrdBuffPool.MaxSize() - 2 * eos::common::MB;
  {
    eos::common::RWMutexReadLock fs_rd_lock(Storage->mFsMutex);

    for (const auto& elem : Storage->mFsMap) {
      auto fs = elem.second;

      // Don't report filesystems which are not booted!
      if (fs->GetStatus() != eos::common::BootStatus::kBooted) {
        continue;
      }

      eos::common::FileSystem::fsid_t fsid = fs->GetLocalId();
      eos::common::RWMutexReadLock rd_lock(fs->mInconsistencyMutex);
      const auto& icset = fs->GetInconsistencySets();

      for (auto icit = icset.cbegin(); icit != icset.cend(); ++icit) {
        // Construct the tag
        response += SSTR(icit->first << "@" << fsid);

        for (auto fit = icit->second.cbegin(); fit != icit->second.cend(); ++fit) {
          // Don't report files which are currently write-open
          if (openedForWriting.isOpen(fsid, *fit)) {
            continue;
          }

          response += ':';
          response += eos::common::FileId::Fid2Hex(*fit);
        }

        response += '\n';

        if (response.length() >= max_sz) {
          eos_static_warning("%s", "msg=\"reached max fsck size limit\"");
          break;
        }
      }
    }
  }
  // Use XrdOucBuffPool to manage XrdOucBuffer objects that can hold redirection
  // info >= 2kb but not bigger than MaxSize
  const uint32_t aligned_sz = eos::common::GetPowerCeil(response.length() + 1);
  XrdOucBuffer* buff = mXrdBuffPool.Alloc(aligned_sz);

  if (buff == nullptr) {
    eos_static_err("msg=\"requested fsck result buffer too big\" req_sz=%llu "
                   "max_sz=%i", response.length(), mXrdBuffPool.MaxSize());
    err_obj.setErrInfo(ENOMEM, "fsck result buffer too big");
    return SFS_ERROR;
  }

  eos_static_debug("msg=\"fsck reply\" data=\"%s\"", response.c_str());
  (void) strcpy(buff->Buffer(), response.c_str());
  buff->SetLen(response.length() + 1);
  err_obj.setErrInfo(buff->DataLen(), buff);
  return SFS_DATA;
}

//------------------------------------------------------------------------------
// Handle resync query
//------------------------------------------------------------------------------
int
XrdFstOfs::HandleResync(XrdOucEnv& env, XrdOucErrInfo& err_obj)
{
  using eos::common::FileId;

  if ((env.Get("fst.resync.fsid") == nullptr) ||
      (env.Get("fst.resync.fxid") == nullptr) ||
      (env.Get("fst.resync.force") == nullptr)) {
    eos_static_err("%s", "msg=\"discard resync with missing arguments\"");
    err_obj.setErrInfo(EINVAL, "resync missing arguments");
    return SFS_ERROR;
  }

  bool force {false};
  FileId::fileid_t fid = FileId::Hex2Fid(env.Get("fst.resync.fxid"));
  FileSystem::fsid_t fsid = strtoul(env.Get("fst.resync.fsid"), 0,
                                    10);
  char* ptr = env.Get("fst.resync.force");

  if (ptr && (strncmp(ptr, "1", 1) == 0)) {
    force = true;
  }

  if ((ptr == nullptr) || (fsid == 0ul)) {
    eos_static_err("msg=\"resync with invalid args\" fsid=%lu fxid=%08llx",
                   (unsigned long) fsid, fid);
    err_obj.setErrInfo(EINVAL, "resync with invalid args");
    return SFS_ERROR;
  }

  if (!fid) {
    eos_static_warning("msg=\"deleting fmd\" fsid=%lu fxid=%08llx", fsid, fid);
    gFmdDbMapHandler.LocalDeleteFmd(fid, fsid);
  } else {
    auto fmd = gFmdDbMapHandler.LocalGetFmd(fid, fsid, true, force);

    if (fmd) {
      if (force) {
        eos_static_info("msg=\"force resync\" fxid=%08llx fsid=%lu", fid, fsid);
        std::string fpath = eos::common::FileId::FidPrefix2FullPath
                            (eos::common::FileId::Fid2Hex(fid).c_str(),
                             gOFS.Storage->GetStoragePath(fsid).c_str());

        if (gFmdDbMapHandler.ResyncDisk(fpath.c_str(), fsid, false) == 0) {
          if (gFmdDbMapHandler.ResyncFileFromQdb(fid, fsid, fpath, gOFS.mFsckQcl)) {
            eos_static_err("msg=\"resync qdb failed\" fid=%08llx fsid=%lu",
                           fid, fsid);
          }
        } else {
          eos_static_err("msg=\"resync disk failed\" fid=%08llx fsid=%lu",
                         fid, fsid);
        }
      } else {
        // Resync of meta data from the MGM by storing the FmdHelper in the
        // WrittenFilesQueue to have it done asynchronously
        gOFS.WrittenFilesQueueMutex.Lock();
        gOFS.WrittenFilesQueue.push(*fmd.get());
        gOFS.WrittenFilesQueueMutex.UnLock();
      }
    }
  }

  // @todo(esindril) once xrootd bug regarding handling of SFS_OK response
  // in XrdXrootdXeq is fixed we can just return SFS_OK (>= XRootD 5)
  // return SFS_OK;
  const char* done = "OK";
  err_obj.setErrInfo(strlen(done) + 1, done);
  return SFS_DATA;
}

//------------------------------------------------------------------------------
// Handle rtlog query
//------------------------------------------------------------------------------
int
XrdFstOfs::HandleRtlog(XrdOucEnv& env, XrdOucErrInfo& err_obj)
{
  XrdOucString tag = env.Get("mgm.rtlog.tag");
  XrdOucString lines = env.Get("mgm.rtlog.lines");
  XrdOucString filter = env.Get("mgm.rtlog.filter");
  XrdOucString response = "";

  if (!filter.length()) {
    filter = " ";
  }

  if ((!lines.length()) || (!tag.length())) {
    eos_static_err("msg=\"rtlog illegal parameter\" lines=%s tag=%s",
                   lines.c_str(), tag.c_str());
    err_obj.setErrInfo(EINVAL, "rtlog illegal parameter");
    return SFS_ERROR;
  }

  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

  if (g_logging.GetPriorityByString(tag.c_str()) == -1) {
    eos_static_err("%s", "msg=\"unknown rtlog tag\"");
    err_obj.setErrInfo(EINVAL, "rtlog unknown tag");
    return SFS_ERROR;
  }

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
        response += logline;
        response += "\n";
      }

      if (!logline.length()) {
        break;
      }
    }
  }

  // Use XrdOucBuffPool to manage XrdOucBuffer objects that can hold redirection
  // info >= 2kb but not bigger than MaxSize
  const uint32_t aligned_sz = eos::common::GetPowerCeil(response.length() + 1);
  XrdOucBuffer* buff = mXrdBuffPool.Alloc(aligned_sz);

  if (buff == nullptr) {
    eos_static_err("msg=\"requested rtlog result buffer too big\" req_sz=%llu "
                   "max_sz=%i", response.length(), mXrdBuffPool.MaxSize());
    err_obj.setErrInfo(ENOMEM, "rtlog result buffer too big");
    return SFS_ERROR;
  }

  eos_static_debug("msg=\"rtlog reply\" data=\"%s\"", response.c_str());
  (void) strcpy(buff->Buffer(), response.c_str());
  buff->SetLen(response.length() + 1);
  err_obj.setErrInfo(buff->DataLen(), buff);
  return SFS_DATA;
}

//------------------------------------------------------------------------------
// Handle verify query
//------------------------------------------------------------------------------
int
XrdFstOfs::HandleVerify(XrdOucEnv& env, XrdOucErrInfo& err_obj)
{
  int envlen = 0;
  eos_static_info("ms=\"verify opaque\" data=\%s\"", env.Env(envlen));
  Verify* new_verify = Verify::Create(&env);

  if (new_verify) {
    Storage->PushVerification(new_verify);
  } else {
    eos_static_err("%s", "msg=\"failed verify, illegal opaque info\"");
  }

  // @todo(esindril) once xrootd bug regarding handling of SFS_OK response
  // in XrdXrootdXeq is fixed we can just return SFS_OK (>= XRootD 5)
  // return SFS_OK;
  const char* done = "OK";
  err_obj.setErrInfo(strlen(done) + 1, done);
  return SFS_DATA;
}

//------------------------------------------------------------------------------
// Handle drop file query
//------------------------------------------------------------------------------
int
XrdFstOfs::HandleDropFile(XrdOucEnv& env, XrdOucErrInfo& err_obj)
{
  int caprc = 0;
  XrdOucEnv* capOpaque {nullptr};

  if ((caprc = eos::common::SymKey::ExtractCapability(&env, capOpaque))) {
    eos_static_err("msg=\"extract capability failed for deletion\" errno=%d",
                   caprc);
    return SFS_ERROR;
  } else {
    int envlen = 0;
    eos_static_debug("opaque=\"%s\"", capOpaque->Env(envlen));
    std::unique_ptr<Deletion> new_del = Deletion::Create(capOpaque);

    if (new_del) {
      gOFS.Storage->AddDeletion(std::move(new_del));
    } else {
      eos_static_err("%s", "msg=\"illegal drop opaque information\"");
      return SFS_ERROR;
    }
  }

  delete capOpaque;
  // @todo(esindril) once xrootd bug regarding handling of SFS_OK response
  // in XrdXrootdXeq is fixed we can just return SFS_OK (>= XRootD 5)
  // return SFS_OK;
  const char* done = "OK";
  err_obj.setErrInfo(strlen(done) + 1, done);
  return SFS_DATA;
}

//------------------------------------------------------------------------------
// Handle clean orphans query
//------------------------------------------------------------------------------
int
XrdFstOfs::HandleCleanOrphans(XrdOucEnv& env, XrdOucErrInfo& err_obj)
{
  const char* ptr = env.Get("fst.fsid");
  eos::common::FileSystem::fsid_t fsid = 0ul;

  if (ptr) {
    std::string sfsid {ptr};

    try {
      size_t pos = 0;
      fsid = std::stoul(sfsid, &pos);

      if (pos != sfsid.length()) {
        throw std::invalid_argument("fsid conversion failed");
      }
    } catch (...) {
      err_obj.setErrInfo(EINVAL, "fsid is not numeric");
      return SFS_ERROR;
    }
  } else {
    err_obj.setErrInfo(EINVAL, "query missing fst.fsid key ");
    return SFS_ERROR;
  }

  std::ostringstream err_msg;

  if (!Storage->CleanupOrphans(fsid, err_msg)) {
    err_obj.setErrInfo(EINVAL, err_msg.str().c_str());
    return SFS_ERROR;
  }

  // @todo(esindril) once xrootd bug regarding handling of SFS_OK response
  // in XrdXrootdXeq is fixed we can just return SFS_OK (>= XRootD 5)
  // return SFS_OK;
  const char* done = "OK";
  err_obj.setErrInfo(strlen(done) + 1, done);
  return SFS_DATA;
}

//------------------------------------------------------------------------------
// Query MGM for the deletion list
//------------------------------------------------------------------------------
int
XrdFstOfs::Query2Delete()
{
  const std::string mgm_endpoint = gConfig.GetManager();

  if (mgm_endpoint.empty()) {
    eos_static_err("%s", "msg=\"no MGM endpoint available\"");
    return SFS_ERROR;
  }

  std::ostringstream oss;
  oss << "root://" << mgm_endpoint << "//dummy?xrd.wantprot=sss";
  XrdCl::URL url(oss.str());

  if (!url.IsValid()) {
    eos_static_err("msg=\"invalid url\" url=\"%s\"", oss.str().c_str());
    return SFS_ERROR;
  }

  std::string request = "/?mgm.pcmd=query2delete&mgm.target.nodename=";
  request += gConfig.FstQueue.c_str();
  XrdCl::Buffer arg;
  XrdCl::Buffer* raw_resp {nullptr};
  XrdCl::FileSystem fs {url};
  arg.FromString(request);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg,
                                        raw_resp);
  std::unique_ptr<XrdCl::Buffer> resp(raw_resp);
  raw_resp = nullptr;

  if (!status.IsOK()) {
    eos_static_err("msg=\"failed query request\" request=\"%s\" status=\"%s\"",
                   request.c_str(), status.ToStr().c_str());
    return SFS_ERROR;
  }

  if (resp && resp->GetBuffer()) {
    eos::fst::DeletionsProto del_fst;

    if (!del_fst.ParseFromArray(resp->GetBuffer(), resp->GetSize())) {
      eos_static_err("%s", "msg=\"query2delete failed to parse protobuf\"");
      return SFS_ERROR;
    }

    // Submit deletions
    for (const auto& del : del_fst.fs()) {
      std::vector<unsigned long long> fids;
      fids.reserve(del.fids().size());
      fids.insert(fids.cend(), del.fids().cbegin(), del.fids().cend());

      try {
        gOFS.Storage->AddDeletion(std::make_unique<Deletion>
                                  (std::move(fids), del.fsid(), del.path().c_str()));
      } catch (const std::bad_alloc& e) {
        eos_static_err("%s", "msg=\"failed to alloc deletion object\"");
        continue;
      }
    }
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// *********** NOTE: THE FOLLOWING METHODS ARE TO BE DEPRECATED ***************
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Set debug level based on the env info
//------------------------------------------------------------------------------
void
XrdFstOfs::SetDebug(XrdOucEnv& env)
{
  XrdOucString debuglevel = env.Get("mgm.debuglevel");
  XrdOucString filterlist = env.Get("mgm.filter");
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  int debugval = g_logging.GetPriorityByString(debuglevel.c_str());

  if (debugval < 0) {
    eos_err("debug level %s is not known!", debuglevel.c_str());
  } else {
    // We set the shared hash debug for the lowest 'debug' level
    if (debuglevel == "debug") {
      //ObjectManager.SetDebug(true);
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
// Send real time log through MQ
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
// Send reply to FSCK query through MQ
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
// Handle resync query coming through MQ
//------------------------------------------------------------------------------
void
XrdFstOfs::DoResync(XrdOucEnv& env)
{
  eos::common::FileId::fileid_t fid {0ull};
  eos::common::FileSystem::fsid_t fsid =
    (env.Get("mgm.fsid") ? strtoul(env.Get("mgm.fsid"), 0, 10) : 0);
  bool force {false};
  char* ptr = env.Get("mgm.resync_force");

  if (ptr && (strncmp(ptr, "1", 1) == 0)) {
    force = true;
  }

  if (env.Get("mgm.fxid")) {
    fid = strtoull(env.Get("mgm.fxid"), 0, 16);  // transition
  } else if (env.Get("mgm.fid")) {
    fid = strtoull(env.Get("mgm.fid"), 0, 10);   // eventually should be HEX
  }

  if (!fsid) {
    eos_err("msg=\"dropping resync\" fsid=%lu fxid=%08llx",
            (unsigned long) fsid, fid);
  } else {
    if (!fid) {
      eos_warning("msg=\"deleting fmd\" fsid=%lu fxid=%08llx",
                  (unsigned long) fsid, fid);
      gFmdDbMapHandler.LocalDeleteFmd(fid, fsid);
    } else {
      auto fMd = gFmdDbMapHandler.LocalGetFmd(fid, fsid, true, force);

      if (fMd) {
        if (force) {
          eos_static_info("msg=\"force resync\" fid=%08llx fsid=%lu",
                          fid, fsid);
          std::string fpath = eos::common::FileId::FidPrefix2FullPath
                              (eos::common::FileId::Fid2Hex(fid).c_str(),
                               gOFS.Storage->GetStoragePath(fsid).c_str());

          if (gFmdDbMapHandler.ResyncDisk(fpath.c_str(), fsid, false) == 0) {
            if (gFmdDbMapHandler.ResyncFileFromQdb(fid, fsid, fpath,
                                                   gOFS.mFsckQcl) == 0) {
              return;
            } else {
              eos_static_err("msg=\"resync qdb failed\" fid=%08llx fsid=%lu",
                             fid, fsid);
            }
          } else {
            eos_static_err("msg=\"resync disk failed\" fid=%08llx fsid=%lu",
                           fid, fsid);
          }
        } else {
          // force a resync of meta data from the MGM
          // e.g. store in the WrittenFilesQueue to have it done asynchronous
          gOFS.WrittenFilesQueueMutex.Lock();
          gOFS.WrittenFilesQueue.push(*fMd.get());
          gOFS.WrittenFilesQueueMutex.UnLock();
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
// Handle drop query coming through MQ
//------------------------------------------------------------------------------
void
XrdFstOfs::DoDrop(XrdOucEnv& env)
{
  int caprc = 0;
  XrdOucEnv* capOpaque {nullptr};

  if ((caprc = eos::common::SymKey::ExtractCapability(&env, capOpaque))) {
    eos_static_err("msg=\"extract capability failed for deletion\" errno=%d",
                   caprc);
  } else {
    int envlen = 0;
    eos_static_debug("opaque=\"%s\"", capOpaque->Env(envlen));
    std::unique_ptr<Deletion> new_del = Deletion::Create(capOpaque);

    if (new_del) {
      gOFS.Storage->AddDeletion(std::move(new_del));
    } else {
      eos_static_err("%s", "msg=\"illegal drop opaque information\"");
    }
  }

  delete capOpaque;
}

//------------------------------------------------------------------------------
// Handle verify query coming through MQ
//------------------------------------------------------------------------------
void
XrdFstOfs::DoVerify(XrdOucEnv& env)
{
  int envlen = 0;
  eos_static_debug("ms=\"verify opaque\" data=\"%s\"", env.Env(envlen));
  Verify* new_verify = Verify::Create(&env);

  if (new_verify) {
    gOFS.Storage->PushVerification(new_verify);
  } else {
    eos_static_err("%s", "msg=\"failed verify, illegal opaque info\"");
  }
}

//------------------------------------------------------------------------------
// Set various XrdCl timeouts more appropriate for the EOS use-case but still
// allow the env variables to override them
//------------------------------------------------------------------------------
void
XrdFstOfs::SetXrdClTimeouts()
{
  char* ptr {nullptr};
  int env_value {0};
  std::map<std::string, int> map_timeouts {
    {"TimeoutResolution", 1}, {"ConnectionWindow", 5}, {"ConnectionRetry", 1},
    {"StreamErrorWindow", 0}, {"MetalinkProcessing", 0}};

  for (auto& elem : map_timeouts) {
    std::string env_name = "XRD_" + elem.first;
    std::transform(env_name.begin(), env_name.end(), env_name.begin(),
                   ::toupper);
    ptr = getenv(env_name.c_str());
    env_value = elem.second;

    // Env variable overrides default values
    if (ptr) {
      try {
        env_value = std::stoi(std::string(ptr));
      } catch (...) {
        eos_static_err("msg=\"invalid value for env var %s keeping the defaule\" "
                       "default_value=1", env_value);
      }
    }

    XrdCl::DefaultEnv::GetEnv()->PutInt(elem.first, env_value);
    eos_static_info("msg=\"update xrootd client timeouts\" name=%s value=%i",
                    elem.first.c_str(), env_value);
  }
}

EOSFSTNAMESPACE_END
