// ----------------------------------------------------------------------
// File: XrdMgmOfsConfigure.cc
// Authors: Andreas-Joachim Peters - CERN
//          Jaroslav Guenther      - CERN
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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <cstring>
#include <sstream>
#include <regex>
#include "grpc/GrpcServer.hh"
#include "grpc/GrpcWncServer.hh"
#include "grpc/GrpcRestGwServer.hh"
#include "mgm/AdminSocket.hh"
#include "mgm/Stat.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsTrace.hh"
#include "mgm/Quota.hh"
#include "mgm/Access.hh"
#include "mgm/Devices.hh"
#include "mgm/Recycle.hh"
#include "mgm/drain/Drainer.hh"
#include "mgm/config/QuarkDBConfigEngine.hh"
#include "mgm/Egroup.hh"
#include "mgm/GeoTreeEngine.hh"
#include "mgm/http/HttpServer.hh"
#include "mgm/ZMQ.hh"
#include "mgm/Iostat.hh"
#include "mgm/LRU.hh"
#include "mgm/WFE.hh"
#include "mgm/QdbMaster.hh"
#include "mgm/Messaging.hh"
#include "mgm/convert/ConverterDriver.hh"
#include "mgm/tgc/MultiSpaceTapeGc.hh"
#include "mgm/tracker/ReplicationTracker.hh"
#include "mgm/inspector/FileInspector.hh"
#include "mgm/qos/QoSClass.hh"
#include "mgm/qos/QoSConfig.hh"
#include "common/RWMutex.hh"
#include "common/Utils.hh"
#include "common/StacktraceHere.hh"
#include "common/plugin_manager/PluginManager.hh"
#include "common/CommentLog.hh"
#include "common/Path.hh"
#include "common/JeMallocHandler.hh"
#include "common/PasswordHandler.hh"
#include "common/ShellCmd.hh"
#include "common/InstanceName.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "mq/SharedHashWrapper.hh"
#include "mq/MessagingRealm.hh"
#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdNet/XrdNetUtils.hh"
#include "XrdNet/XrdNetAddr.hh"
#include "XrdSys/XrdSysPlugin.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "qclient/shared/SharedManager.hh"
#include "mgm/bulk-request/dao/proc/ProcDirectoryBulkRequestLocations.hh"
#include "mgm/bulk-request/dao/proc/cleaner/BulkRequestProcCleaner.hh"
#include "mgm/bulk-request/dao/proc/cleaner/BulkRequestProcCleanerConfig.hh"
#include "mgm/http/rest-api/manager/RestApiManager.hh"
#include "mgm/http/rest-api/Constants.hh"
#include "mgm/placement/FsScheduler.hh"

extern XrdOucTrace gMgmOfsTrace;
extern void xrdmgmofs_shutdown(int sig);
extern void xrdmgmofs_stacktrace(int sig);
extern void xrdmgmofs_coverage(int sig);

USE_EOSMGMNAMESPACE


void xrdmgmofs_stack(int sig)
{
  static time_t stacktime = 0;
  std::ostringstream out;

  if (sig == SIGUSR2) {
    out << "# ___ thread:" << syscall(SYS_gettid) << " ";
    eos::common::RWMutex::PrintMutexOps(out);
    out << std::endl;
    out << "# ................ " << eos::common::getStacktrace();
    std::string stackdump = "/var/eos/md/stacktrace.";
    stackdump += std::to_string((unsigned long long)stacktime);
    std::ofstream outf(stackdump, std::ofstream::app);
    outf << out.str() << std::endl;
    std::cerr << out.str() << std::endl;
    outf.close();
  }

  if (sig == SIGUSR1) {
    stacktime = time(NULL);
    std::string stackdump = "/var/eos/md/stacktrace.";
    stackdump += std::to_string((unsigned long long)stacktime);
    std::ofstream outf(stackdump);
    outf << "# eos mgm stack mutex states" << std::endl;
    std::set<pthread_t> tosignal = gOFS->mTracker.getInFlightThreads();
    outf << "# " << tosignal.size() << " threads in tracking" << std::endl;
    outf.close();

    for (auto it = tosignal.begin(); it != tosignal.end(); ++it) {
      pthread_kill(*it, SIGUSR2);
    }
  }
}

//------------------------------------------------------------------------------
// Start jemalloc heap profiling
//------------------------------------------------------------------------------
void XrdMgmOfs::StartHeapProfiling(int sig)
{
  if (!gOFS->mJeMallocHandler->CanProfile()) {
    eos_static_crit("cannot run heap profiling");
    return;
  }

  if (gOFS->mJeMallocHandler->StartProfiling()) {
    eos_static_warning("started jemalloc heap profiling");
  } else {
    eos_static_warning("failed to start jemalloc heap profiling");
  }
}

//------------------------------------------------------------------------------
// Stop jemalloc heap profiling
//------------------------------------------------------------------------------
void XrdMgmOfs::StopHeapProfiling(int sig)
{
  if (!gOFS->mJeMallocHandler->CanProfile()) {
    eos_static_crit("cannot run heap profiling");
    return;
  }

  if (gOFS->mJeMallocHandler->StopProfiling()) {
    eos_static_warning("stopped jemalloc heap profiling");
  } else {
    eos_static_warning("failed to stop jemalloc heap profiling");
  }
}

//------------------------------------------------------------------------------
// Dump jemalloc heap profiling info
//------------------------------------------------------------------------------
void XrdMgmOfs::DumpHeapProfile(int sig)
{
  if (!gOFS->mJeMallocHandler->ProfRunning()) {
    eos_static_crit("profiling is not running");
    return;
  }

  if (gOFS->mJeMallocHandler->DumpProfile()) {
    eos_static_warning("dumped heap profile");
  } else {
    eos_static_warning("failed to sum heap profile");
  }
}

//------------------------------------------------------------------------------
// Configure the MGM daemon by parsing info in xrd.cf.mgm
//------------------------------------------------------------------------------
int
XrdMgmOfs::Configure(XrdSysError& Eroute)
{
  // the process run's as root, but acts on the filesystem as daemon
  char* var;
  const char* val;
  int cfgFD, retc, NoGo = 0;
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"));
  XrdOucString role = "server";
  // set short timeouts in the new XrdCl class
  XrdCl::DefaultEnv::GetEnv()->PutInt("TimeoutResolution", 1);
  // set connection window short
  XrdCl::DefaultEnv::GetEnv()->PutInt("ConnectionWindow", 5);
  // set connection retry to one
  XrdCl::DefaultEnv::GetEnv()->PutInt("ConnectionRetry", 1);
  // set stream error window
  XrdCl::DefaultEnv::GetEnv()->PutInt("StreamErrorWindow", 0);
  UTF8 = getenv("EOS_UTF8") != nullptr;
  MgmQoSEnabled = getenv("EOS_ENABLE_QOS") != nullptr;
  Shutdown = false;
  setenv("XrdSecPROTOCOL", "sss", 1);
  Eroute.Say("=====> mgmofs enforces SSS authentication for XROOT clients");
  MgmOfsTargetPort = "1094";
  MgmOfsName = "";
  MgmOfsAlias = "";
  MgmOfsBrokerUrl = "root://localhost:1097//eos/";
  MgmOfsInstanceName = "testinstance";
  MgmMetaLogDir = "";
  MgmAuthDir = "";
  MgmArchiveDir = "";
  MgmQoSDir = "";
  MgmQoSConfigFile = "";
  IoReportStorePath = "/var/tmp/eos/report";
  TmpStorePath = "/var/tmp/eos/mgm";
  mQClientDir = "/var/eos/ns-queue/";
  MgmArchiveDstUrl = "";
  MgmArchiveSvcClass = "default";
  mPrepareDestSpace = "default";
  eos::common::StringConversion::InitLookupTables();
  // Enable TPC on the MGM for 0-size files
  XrdOucEnv::Export("XRDTPC", "1");

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

  // Configure heap profiling if any
  if (mJeMallocHandler->JeMallocLoaded()) {
    eos_warning("jemalloc is loaded!");
    Eroute.Say("jemalloc is loaded!");

    if (mJeMallocHandler->CanProfile()) {
      Eroute.Say(mJeMallocHandler->ProfRunning() ?
                 "jemalloc heap profiling enabled and running" :
                 "jemalloc heap profiling enabled and NOT running");
      eos_warning("jemalloc heap profiling enabled and %srunning. will start "
                  "running on signal 40 and will stop running on signal 41",
                  mJeMallocHandler->ProfRunning() ? "" : "NOT ");
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
  // or archive
  struct stat dir_stat;

  if (!::stat(TmpStorePath.c_str(), &dir_stat) && S_ISDIR(dir_stat.st_mode)) {
    XrdOucString systemline = "rm -rf ";
    systemline += TmpStorePath.c_str();
    systemline += "/* >& /dev/null &";
    int rrc = system(systemline.c_str());

    if (WEXITSTATUS(rrc)) {
      eos_err("%s returned %d", systemline.c_str(), rrc);
    }
  } else {
    eos::common::Path out_dir(std::string(std::string(TmpStorePath.c_str()) +
                                          "/dummy").c_str());

    if (!out_dir.MakeParentPath(S_IRWXU)) {
      eos_err("Unable to create temporary output file directory %s",
              TmpStorePath.c_str());
      Eroute.Emsg("Config", errno, "create temporary output file"
                  " directory", TmpStorePath.c_str());
      NoGo = 1;
      return NoGo;
    }

    // Own the directory by daemon
    if (::chown(out_dir.GetParentPath(), 2, 2)) {
      eos_err("Unable to own temporary output file directory %s",
              out_dir.GetParentPath());
      Eroute.Emsg("Config", errno, "own output file directory",
                  out_dir.GetParentPath());
      NoGo = 1;
      return NoGo;
    }
  }

  MgmConfigAutoLoad = "";
  long myPort = 0;
  std::string ns_lib_path;

  if (getenv("XRDDEBUG")) {
    gMgmOfsTrace.What = TRACE_MOST | TRACE_debug;
  }

  {
    // borrowed from XrdOfs
    char buff[256], *bp;
    int i;
    // Obtain port number we will be using
    //
    myPort = (bp = getenv("XRDPORT")) ? strtol(bp, (char**) 0, 10) : 0;
    // Establish our hostname and IPV4 address
    //
    const char* errtext = 0;
    HostName = XrdNetUtils::MyHostName(0, &errtext);

    if (!HostName) {
      return Eroute.Emsg("Config", errno, "cannot get hostname : %s", errtext);
    }

    XrdNetAddr* addrs  = 0;
    int         nAddrs = 0;
    const char* err    = XrdNetUtils::GetAddrs(HostName, &addrs, nAddrs,
                         XrdNetUtils::allIPv64,
                         XrdNetUtils::NoPortRaw);

    if (err || nAddrs == 0) {
      sprintf(buff, "[::127.0.0.1]:%ld", myPort);
    } else {
      int len = XrdNetUtils::IPFormat(addrs[0].SockAddr(), buff, sizeof(buff),
                                      XrdNetUtils::noPort | XrdNetUtils::oldFmt);
      delete [] addrs;

      if (len == 0) {
        sprintf(buff, "[::127.0.0.1]:%ld", myPort);
      } else {
        sprintf(buff + len, ":%ld", myPort);
      }
    }

    for (i = 0; HostName[i] && HostName[i] != '.'; i++);

    HostName[i] = '\0';
    HostPref = strdup(HostName);
    HostName[i] = '.';
    Eroute.Say("=====> mgmofs.hostname: ", HostName, "");
    Eroute.Say("=====> mgmofs.hostpref: ", HostPref, "");
    ManagerId = HostName;
    ManagerId += ":";
    ManagerId += (int) myPort;
    addrs  = 0;
    nAddrs = 0;
    err    = XrdNetUtils::GetAddrs(HostName, &addrs, nAddrs, XrdNetUtils::allIPv64,
                                   XrdNetUtils::NoPortRaw);

    if (err) {
      return Eroute.Emsg("Config", errno, "convert hostname to IP address: ", err);
    }

    if (nAddrs == 0) {
      return Eroute.Emsg("Config", errno, "convert hostname to IP address",
                         HostName);
    }

    int len = addrs[0].Format(buff, sizeof(buff), XrdNetAddrInfo::fmtAddr,
                              XrdNetAddrInfo::noPortRaw);
    delete [] addrs;

    if (len == 0) {
      return Eroute.Emsg("Config", errno, "convert hostname to IP address",
                         HostName);
    }

    ManagerIp = XrdOucString(buff, len);
    ManagerPort = myPort;
    Eroute.Say("=====> mgmofs.managerid: ", ManagerId.c_str(), "");
  }

  std::string tapeRestApiSitename;
  std::map<std::string, std::string> tapeRestApiEndpointUrlMap;
  XrdHttpPort = ManagerPort;

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

      // Handle TPC redirection for delegation
      // ofs.tpc redirect [delegated|undelegated] <host>[:<port>]
      if (strncmp(var, "ofs.tpc", 7) == 0) {
        var += 7;
        char c_line[4096];

        if (Config.GetRest(c_line, 4096) == 0) {
          Eroute.Emsg("Config", "argument for ofs.tpc is missing");
          NoGo = 1;
        } else {
          std::string line {c_line};
          eos::common::trim(line);
          auto tokens = eos::common::StringTokenizer::split<std::list<std::string>>(line,
                        ' ');

          // We're only interested in the redirect directive since we anyway
          // enable TPC support by default by setting the XRDTPC env variable
          if (tokens.front() == "redirect") {
            mTpcRedirect = true;
            tokens.pop_front();

            if (tokens.empty()) {
              Eroute.Emsg("Config", "argument for ofs.tpc is missing");
              NoGo = 1;
            } else {
              bool rdr_delegated = (tokens.front() == "delegated");
              tokens.pop_front();

              if (tokens.empty()) {
                Eroute.Emsg("Config", "argument for ofs.tpc redirect is missing");
                NoGo = 1;
              } else {
                std::string rdr_info = tokens.front();
                size_t pos = rdr_info.find(':');
                int rdr_port {1094};
                std::string rdr_host {rdr_info.substr(0, pos)};

                if (pos != std::string::npos) {
                  try {
                    rdr_port = std::stoul(rdr_info.substr(pos + 1));
                  } catch (...) {
                    Eroute.Emsg("Config", "ofs.tpc redirect failed to convert port,"
                                "use default 1094");
                    NoGo = 1;
                  }
                }

                Eroute.Say("=====> ofs.tpc redirect to: ", rdr_host.c_str(),
                           std::to_string(rdr_port).c_str());
                mTpcRdrInfo.emplace(rdr_delegated, std::make_pair(rdr_host, rdr_port));

                // We only accept forwarding credentials for gsi
                if (rdr_delegated) {
                  XrdOucEnv::Export("XRDTPCDLG", "gsi");
                }
              }
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

        if (!strcmp("targetport", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for fs invalid.");
            NoGo = 1;
          } else {
            Eroute.Say("=====> mgmofs.targetport: ", val, "");
            MgmOfsTargetPort = val;
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
          mQdbContactDetails.members.parse(mQdbCluster);
        }

        if (!strcmp("qdbpassword", var)) {
          while ((val = Config.GetWord())) {
            mQdbPassword += val;
          }

          // Trim whitespace at the end
          mQdbPassword.erase(mQdbPassword.find_last_not_of(" \t\n\r\f\v") + 1);
          std::string pwlen = std::to_string(mQdbPassword.size());
          Eroute.Say("=====> mgmofs.qdbpassword length : ", pwlen.c_str());
          mQdbContactDetails.password = mQdbPassword;
        }

        if (!strcmp("qdbpassword_file", var)) {
          std::string path;

          while ((val = Config.GetWord())) {
            path += val;
          }

          if (!eos::common::PasswordHandler::readPasswordFile(path, mQdbPassword)) {
            Eroute.Emsg("Config", "failed to open path pointed to by qdbpassword_file");
            NoGo = 1;
          }

          std::string pwlen = std::to_string(mQdbPassword.size());
          Eroute.Say("=====> mgmofs.qdbpassword length : ", pwlen.c_str());
          mQdbContactDetails.password = mQdbPassword;
        }

        if (!strcmp("qclientdir", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for qclientdir is invalid");
            NoGo = 1;
          } else {
            mQClientDir = val;

            if (!eos::common::endsWith(mQClientDir, "/")) {
              mQClientDir += "/";
            }

            Eroute.Say("=====> mgmofs.qclientdir : ", mQClientDir.c_str());
          }
        }

        if (!strcmp("authlib", var)) {
          if ((!(val = Config.GetWord())) || (::access(val, R_OK))) {
            Eroute.Emsg("Config", "I cannot access the authorization library!");
            NoGo = 1;
          } else {
            mAuthLib = val;
          }

          Eroute.Say("=====> mgmofs.authlib : ", mAuthLib.c_str());
        }

        if (!strcmp("tapeenabled", var)) {
          if ((!(val = Config.GetWord())) ||
              (strcmp("true", val) && strcmp("false", val) &&
               strcmp("1", val) && strcmp("0", val))) {
            Eroute.Emsg("Config", "argument for tapeenabled is invalid. "
                        "Must be <true>, <false>, <1> or <0>!");
          } else {
            if ((!strcmp("true", val) || (!strcmp("1", val)))) {
              mTapeEnabled = true;
            }

            Eroute.Say("=====> mgmofs.tapeenabled : ", val);
          }
        }

        if (!strcmp("prepare.dest.space", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for prepare.dest.space is missing.");
            NoGo = 1;
          } else {
            mPrepareDestSpace = val;
            Eroute.Say("=====> mgmofs.prepare.dest.space : ", mPrepareDestSpace.c_str());
          }
        }

        if (!strcmp("tgc.enablespace", var)) {
          std::ostringstream tapeGcSpacesStream;

          while ((val = Config.GetWord())) {
            mTapeGcSpaces.insert(val);
            tapeGcSpacesStream << " " << val;
          }

          Eroute.Say("=====> mgmofs.tgc.enablespace :", tapeGcSpacesStream.str().c_str());
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
              mAuthorize = true;
            }
          }

          if (mAuthorize) {
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
              mErrLogEnabled = true;
            } else {
              mErrLogEnabled = false;
            }
          }

          if (mErrLogEnabled) {
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

        if (!strcmp("autoloadconfig", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for autoloadconfig invalid.");
            NoGo = 1;
          } else {
            MgmConfigAutoLoad = val;
          }
        }

        if (!strcmp("qoscfg", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for qoscfg invalid.");
            NoGo = 1;
          } else {
            MgmQoSConfigFile = val;
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
              Eroute.Emsg("Config", "cannot access the meta data changelog "
                          "directory for r/w!", MgmMetaLogDir.c_str());
              NoGo = 1;
            } else {
              Eroute.Say("=====> mgmofs.metalog: ", MgmMetaLogDir.c_str(), "");
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
              Eroute.Emsg("Config", "cannot access the authentication directory "
                          "for r/w:", MgmAuthDir.c_str());
              NoGo = 1;
            } else {
              Eroute.Say("=====> mgmofs.authdir:   ", MgmAuthDir.c_str(), "");
            }
          }
        }

        if (!strcmp("qosdir", var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config", "argument for qosdir invalid.");
            NoGo = 1;
          } else {
            MgmQoSDir = val;

            if (!MgmQoSDir.endswith("/")) {
              MgmQoSDir += "/";
            }

            // attempt to change ownership
            XrdOucString chownit = "chown -R daemon ";
            chownit += MgmQoSDir;
            int src = system(chownit.c_str());

            if (src) {
              eos_err("%s returned %d", chownit.c_str(), src);
            }

            if (::access(MgmQoSDir.c_str(), W_OK | R_OK | X_OK)) {
              Eroute.Emsg("Config", "cannot access the QoS directory for r/w!",
                          MgmQoSDir.c_str());
              NoGo = 1;
            } else {
              Eroute.Say("=====> mgmofs.qosdir:   ", MgmQoSDir.c_str(), "");
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
              Eroute.Emsg("Config", "cannot access the reportstore directory "
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

        if (!strcmp("protowfendpoint", var)) {
          val = Config.GetWord();

          if (val != nullptr) {
            ProtoWFEndPoint = val;
          }
        }

        if (!strcmp("protowfresource", var)) {
          val = Config.GetWord();

          if (val != nullptr) {
            ProtoWFResource = val;
          }
        }
      }

      //Get the XrdHttp server port number
      if (!strcmp("xrd.protocol", var)) {
        val = Config.GetWord();

        if (val != nullptr) {
          std::vector<std::string> xrdProtocolValues;
          eos::common::StringConversion::Tokenize(val, xrdProtocolValues);
          auto xrdHttpProtocolItor = std::find_if(xrdProtocolValues.begin(),
          xrdProtocolValues.end(), [](const std::string & str) {
            return str.find("XrdHttp") != string::npos;
          });

          if (xrdHttpProtocolItor != xrdProtocolValues.end()) {
            //We have the XrdHttp protocol set
            const std::string& xrdHttpProtocol = *xrdHttpProtocolItor;
            size_t posPort = xrdHttpProtocol.find(':');

            if (posPort != std::string::npos && posPort < xrdHttpProtocol.size()) {
              try {
                XrdHttpPort = std::stoi(xrdHttpProtocol.substr(posPort + 1));
              } catch (const std::exception& ex) {
                // The port is not a number, don't set it
              }
            }
          }
        }
      }

      {
        if (!strcmp("taperestapi.sitename", var)) {
          val = Config.GetWord();

          if (val != nullptr) {
            tapeRestApiSitename = val;
            Eroute.Say("=====> taperestapi.sitename: ", val, "");
          } else {
            Eroute.Say("Config warning: REST API sitename not specified, disabling tape REST API.");
          }
        }

        const char* endpointsStr = "taperestapi.endpoints.";
        int endpointsStrLen = strlen(endpointsStr);

        if (!strncmp(endpointsStr, var, endpointsStrLen)) {
          char* version_ptr_begin = var + endpointsStrLen;
          char* version_ptr_end = strstr(var + endpointsStrLen, ".uri");

          if (!version_ptr_end || strcmp(version_ptr_end, ".uri")) {
            auto err_msg = std::string("command ") + var + " is invalid";
            Eroute.Emsg("Config", err_msg.c_str());
            NoGo = 1;
          } else {
            std::string version(version_ptr_begin, version_ptr_end);

            if (!std::regex_match(version, std::regex("v[0-9]+(\\.[0-9]+)?"))) {
              auto err_msg = std::string("version ") + version +
                             " in command " + var + " is invalid";
              Eroute.Emsg("Config", err_msg.c_str());
              NoGo = 1;
            } else {
              val = Config.GetWord();
              tapeRestApiEndpointUrlMap[version] = val;
              Eroute.Say("=====> ", var, ": ", val);
            }
          }
        }
      }
    }

    if ((retc = Config.LastError())) {
      NoGo = Eroute.Emsg("Config", -retc, "read config file", ConfigFN);
    }

    Config.Close();
  }

  if (!mQdbContactDetails.members.empty() &&
      mQdbContactDetails.password.empty()) {
    Eroute.Say("=====> Configuration error: Found QDB cluster members, but no password."
               " EOS will only connect to password-protected QDB instances. (mgmofs.qdbpassword / mgmofs.qdbpassword_file missing)");
    return 1;
  }

  if (NoGo) {
    return NoGo;
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
    Eroute.Say("Config error: the broker url has to be of the form "
               "<root://<hostname>[:<port>]//");
    return 1;
  }

  if (!MgmMetaLogDir.length()) {
    Eroute.Say("Config error: meta data log directory is not defined : "
               "mgm.metalog=</var/eos/md/>");
    return 1;
  }

  if (!MgmAuthDir.length()) {
    Eroute.Say("Config error: auth directory is not defined: "
               "mgm.authdir=</var/eos/auth/>");
    return 1;
  }

  if (MgmQoSEnabled && !MgmQoSDir.length()) {
    Eroute.Say("Config error: QoS is enabled but QoS directory not defined: "
               "mgm.qosdir=</var/eos/qos/>");
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
  MgmOfsQueue = "/eos/";
  MgmOfsQueue += ManagerId;
  MgmOfsQueue += "/mgm";
  // Setup the circular in-memory logging buffer
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  // Configure log-file fan out
  std::vector<std::string> lFanOutTags {
    "Grpc", "Balancer", "Converter", "DrainJob", "ZMQ", "MetadataFlusher", "Http",
    "Master", "Recycle", "LRU", "WFE", "Wnc", "WFE::Job", "GroupBalancer", "GroupDrainer",
    "GeoBalancer", "GeoTreeEngine", "ReplicationTracker", "FileInspector", "Mounts", "OAuth", "TokenCmd", "#"};
  // Get the XRootD log directory
  char* logdir = 0;
  XrdOucEnv::Import("XRDLOGDIR", logdir);

  if (logdir) {
    for (size_t i = 0; i < lFanOutTags.size(); ++i) {
      std::string log_path = logdir;
      log_path += "/";

      if (lFanOutTags[i] == "#") {
        log_path += "Clients";
      } else {
        log_path += lFanOutTags[i];
      }

      log_path += ".log";
      FILE* fp = fopen(log_path.c_str(), "a+");

      if (fp) {
        g_logging.AddFanOut(lFanOutTags[i].c_str(), fp);
      } else {
        fprintf(stderr, "error: failed to open sub-logfile=%s", log_path.c_str());
      }
    }

    // Add some alias for the logging
    g_logging.AddFanOutAlias("HttpHandler", "Http");
    g_logging.AddFanOutAlias("HttpServer", "Http");
    g_logging.AddFanOutAlias("GrpcServer", "Grpc");
    g_logging.AddFanOutAlias("GrpcWncServer", "Wnc");
    g_logging.AddFanOutAlias("ProtocolHandler", "Http");
    g_logging.AddFanOutAlias("PropFindResponse", "Http");
    g_logging.AddFanOutAlias("WebDAV", "Http");
    g_logging.AddFanOutAlias("WebDAVHandler", "Http");
    g_logging.AddFanOutAlias("WebDAVReponse", "Http");
    g_logging.AddFanOutAlias("S3", "Http");
    g_logging.AddFanOutAlias("S3Store", "Http");
    g_logging.AddFanOutAlias("S3Handler", "Http");
    g_logging.AddFanOutAlias("DrainTransferJob", "DrainJob");
    g_logging.AddFanOutAlias("DrainFs", "DrainJob");
    g_logging.AddFanOutAlias("Drainer", "DrainJob");
    g_logging.AddFanOutAlias("Clients", "Mounts");
    g_logging.AddFanOutAlias("ConversionInfo", "Converter");
    g_logging.AddFanOutAlias("ConversionJob", "Converter");
    g_logging.AddFanOutAlias("ConverterDriver", "Converter");
  }

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

  if (!MgmOfsName.length()) {
    Eroute.Say("Config error: no mgmofs fs has been defined (mgmofs.fs /...)",
               "", "");
  } else {
    Eroute.Say("=====> mgmofs.fs: ", MgmOfsName.c_str(), "");
  }

  if (mErrLogEnabled) {
    Eroute.Say("=====> mgmofs.errorlog : enabled");
  } else {
    Eroute.Say("=====> mgmofs.errorlog : disabled");
  }

  // Load the authorization plugin if requested
  if (!mAuthLib.empty() && mAuthorize) {
    XrdSysPlugin* myLib;
    XrdAccAuthorize * (*ep)(XrdSysLogger*, const char*, const char*);
    // Authorization comes from the library or we use the default

    if (!(myLib = new XrdSysPlugin(&Eroute, mAuthLib.c_str()))) {
      Eroute.Emsg("Config", "Failed to load authorization library!");
      NoGo = 1;
    } else {
      ep = (XrdAccAuthorize * (*)(XrdSysLogger*, const char*, const char*))
           (myLib->getPlugin("XrdAccAuthorizeObject"));

      if (!ep) {
        Eroute.Emsg("Config", "Failed to get authorization library plugin!");
        NoGo = 1;
      } else {
        mExtAuthz = ep(Eroute.logger(), ConfigFN, 0);

        if (mExtAuthz == nullptr) {
          Eroute.Emsg("Config", "Failed to get external authorization "
                      "plugin object!");
          NoGo = 1;
        }
      }
    }
  }

  // We need to specify this if the server was not started with the explicit
  // manager option ... e.g. see XrdOfs
  Eroute.Say("=====> all.role: ", role.c_str(), "");

  if (role == "manager") {
    putenv((char*) "XRDREDIRECT=R");
  }

  XrdOucString unit = "mgm@";
  unit += ManagerId;
  g_logging.SetLogPriority(LOG_INFO);
  g_logging.SetUnit(unit.c_str());
  std::string filter =
    "Process,AddQuota,Update,UpdateHint,"
    "Deletion,PrintOut,SharedHash,work";
  g_logging.SetFilter(filter.c_str());
  Eroute.Say("=====> setting message filter: Process,AddQuota,Update,UpdateHint,"
             "Deletion,PrintOut,SharedHash,work");

  if (gOFS->mQdbCluster.empty()) {
    Eroute.Emsg("Config", "The QuarkDB configuration is empty!");
    NoGo = 1;
  } else {
    ConfEngine = new QuarkDBConfigEngine(gOFS->mQdbContactDetails);
  }

  ConfEngine->SetAutoSave(true);

  // Read QoS config file
  if (MgmQoSEnabled) {
    eos::mgm::QoSConfig qosConfig(MgmQoSConfigFile.c_str());

    if (qosConfig.IsValid()) {
      mQoSClassMap = qosConfig.LoadConfig();
    } else {
      Eroute.Emsg("Config", "Could not load QoS config file!",
                  MgmQoSConfigFile.c_str());
      NoGo = 1;
    }
  }

  using eos::common::CommentLog;
  // Create comment log to save all proc commands executed with a comment
  mCommentLog.reset(new CommentLog("/var/log/eos/mgm/logbook.log"));

  if (mCommentLog && mCommentLog->IsValid()) {
    Eroute.Say("=====> comment log in /var/log/eos/mgm/logbook.log");
  } else {
    Eroute.Emsg("Config", "Cannot create/open the comment log file "
                "/var/log/eos/mgm/logbook.log");
    NoGo = 1;
  }

  mFusexStackTraces.reset(new
                          CommentLog("/var/log/eos/mgm/eosxd-stacktraces.log"));

  if (mFusexStackTraces && mFusexStackTraces->IsValid()) {
    Eroute.Say("=====> eosxd stacktraces log in /var/log/eos/mgm/eosxd-stacktraces.log");
  } else {
    Eroute.Emsg("Config", "Cannot create/open the eosxd stacktraces log file "
                "/var/log/eos/mgm/eosxd-stacktraces.log");
    NoGo = 1;
  }

  mFusexLogTraces.reset(new CommentLog("/var/log/eos/mgm/eosxd-logtraces.log"));

  if (mFusexLogTraces && mFusexLogTraces->IsValid()) {
    Eroute.Say("=====> eosxd logtraces log in /var/log/eos/mgm/eosxd-logtraces.log");
  } else {
    Eroute.Emsg("Config", "Cannot create/open the eosxd logtraces log file "
                "/var/log/eos/mgm/eosxd-logtraces.log");
    NoGo = 1;
  }

  // Save MGM alias if configured
  if (getenv("EOS_MGM_ALIAS")) {
    MgmOfsAlias = getenv("EOS_MGM_ALIAS");
  }

  if (MgmOfsAlias.length()) {
    Eroute.Say("=====> mgmofs.alias: ", MgmOfsAlias.c_str());
  }

  // Build the adler & sha1 checksum of the default keytab file
  const std::string keytab_fn = "/etc/eos.keytab";
  std::string keytab_xs = "unaccessible";

  if (!eos::common::GetFileAdlerXs(keytab_xs, keytab_fn)) {
    eos_static_crit("msg=\"failed keytab checksum computation\" fn=\"%s\"",
                    keytab_fn.c_str());
    return 1;
  }

  std::string binary_sha1 = "";

  if (!eos::common::GetFileBinarySha1(binary_sha1, keytab_fn)) {
    eos_static_crit("msg=\"failed keytab sha1 computation\" fn=\"%s\"",
                    keytab_fn.c_str());
    return 1;
  }

  std::string symkey = "";
  eos::common::SymKey::Base64Encode(binary_sha1.data(), binary_sha1.size(),
                                    symkey);
  eos_static_notice("MGM_HOST=%s MGM_PORT=%ld VERSION=%s RELEASE=%s "
                    "KEYTABADLER=%s", HostName, myPort, VERSION, RELEASE,
                    keytab_xs.c_str());

  if (!eos::common::gSymKeyStore.SetKey64(symkey.c_str(), 0)) {
    eos_static_crit("msg=\"unable to store the created symmetric key\" "
                    "key=\"%s\"", symkey.c_str());
    return 1;
  }

  // If tape is enabled, must enable the tape garbage collector
  if (mTapeEnabled) {
    try {
      mTapeGc->setTapeEnabled(mTapeGcSpaces);
    } catch (std::exception& ex) {
      std::ostringstream msg;
      msg << "msg=\"failed to start tape-aware garbage collection: " << ex.what() <<
          "\"";
      eos_static_crit("%s", msg.str().c_str());
      return 1;
    } catch (...) {
      eos_static_crit("%s", "msg=\"failed to start tape-aware garbage "
                      "collection: Caught an unknown exception\"");
      return 1;
    }
  } else if (!mTapeGcSpaces.empty()) {
    std::ostringstream tapeGcSpaceWarning;
    tapeGcSpaceWarning <<
                       "msg=\"These spaces will not be garbage collected because mgmofs.tapeenabled=false:";

    for (const auto& tapeGcSpace : mTapeGcSpaces) {
      tapeGcSpaceWarning << " " << tapeGcSpace;
    }

    tapeGcSpaceWarning << "\"";
    eos_warning(tapeGcSpaceWarning.str().c_str());
  }

  // If tape garbage collector is enabled, all R/W must be redirected to the master node
  if (mTapeEnabled && !getenv("EOS_HA_REDIRECT_READS")) {
    std::ostringstream msg;
    msg << "msg="
        << "Mgm node with tape-aware garbage collection must redirect all write/read traffic to master\"";
    eos_crit(msg.str().c_str());
    return 1;
  }

  eosViewRWMutex.SetBlocking(true);
  // Configure the access mutex to be blocking
  Access::gAccessMutex.SetBlocking(true);
  // Initialize the HA setup
  mMaster.reset(new eos::mgm::QdbMaster(mQdbContactDetails, ManagerId.c_str()));

  if (!mMaster->Init()) {
    return 1;
  }

  // Create global visible configuration parameters using 3 queues
  // "/eos/<instance>/"
  XrdOucString configbasequeue = "/config/";
  configbasequeue += MgmOfsInstanceName.c_str();
  MgmConfigQueue = configbasequeue;
  MgmConfigQueue += "/mgm/";
  ObjectNotifier.SetShareObjectManager(&ObjectManager);
  // Shared object manager to be used
  qclient::SharedManager* qsm = nullptr;

  if (getenv("EOS_USE_MQ_ON_QDB")) {
    eos_static_notice("%s", "msg=\"running SharedManager via QDB i.e NO-MQ\"");
    qsm = new qclient::SharedManager(
      mQdbContactDetails.members,
      mQdbContactDetails.constructSubscriptionOptions());
    mMessagingRealm.reset(new eos::mq::MessagingRealm(nullptr,
                          nullptr, nullptr, qsm));
  } else {
    eos_static_notice("%s", "msg=\"running SharedManager via MQ\"");
    mMessagingRealm.reset(new eos::mq::MessagingRealm(&ObjectManager,
                          &ObjectNotifier, &XrdMqMessaging::gMessageClient,
                          qsm));
  }

  eos::common::InstanceName::set(MgmOfsInstanceName.c_str());

  if (!mMessagingRealm->setInstanceName(MgmOfsInstanceName.c_str())) {
    eos_static_crit("%s", "msg=\"unable to set instance name in QDB\"");
    Eroute.Emsg("Config", "cannot set instance name in QDB");
    return 1;
  }

  // set the object manager to listener only
  ObjectManager.EnableBroadCast(false);
  // setup the modifications which the fs listener thread is waiting for
  ObjectManager.SetDebug(false);

  // Disable some features if we are only a redirector
  if (!MgmRedirector) {
    // Create the specific listener class when running with MQ
    if (!mMessagingRealm->haveQDB()) {
      mMgmMessaging = new Messaging(MgmOfsBrokerUrl.c_str(),
                                    MgmDefaultReceiverQueue.c_str(),
                                    mMessagingRealm.get());
      mMgmMessaging->SetLogId("MgmMessaging");

      if (!mMgmMessaging->StartListenerThread() || mMgmMessaging->IsZombie()) {
        eos_static_crit("%s", "msg=\"failed to start messaging\"");
        Eroute.Emsg("Config", "cannot start messaging thread)");
        return 1;
      }
    }

    // Create the ZMQ processor used especially for fuse
    XrdOucString zmq_port = "tcp://*:";
    zmq_port += (int) mFusexPort;
    zMQ = new ZMQ(zmq_port.c_str());

    if (!zMQ) {
      Eroute.Emsg("Config", "cannot start ZMQ processor");
      return 1;
    }

    zMQ->ServeFuse();
    ObjectManager.SetAutoReplyQueueDerive(true);
    ObjectManager.CreateSharedHash("/eos/*", "/eos/*/fst");
    XrdOucString dumperfile = MgmMetaLogDir;
    dumperfile += "/so.mgm.dump.";
    dumperfile += ManagerId;
    char* ptr = getenv("EOS_MGM_DISABLE_FILE_DUMPER");

    if ((ptr == nullptr) || (strncmp(ptr, "1", 1) != 0)) {
      eos_static_info("%s", "msg=\"mgm file dumper enabled");
      ObjectManager.StartDumper(dumperfile.c_str());
    } else {
      eos_static_info("%s", "msg=\"mgm file dumper disabled");
    }
  }

  // @todo(esindril) decide if this is still neede in qdb pubsub mode
  SetupGlobalConfig();
  // Initialize geotree engine
  mGeoTreeEngine.reset(new eos::mgm::GeoTreeEngine(mMessagingRealm.get()));

  // Eventually autoload a configuration
  if (getenv("EOS_AUTOLOAD_CONFIG")) {
    MgmConfigAutoLoad = getenv("EOS_AUTOLOAD_CONFIG");
  }

  XrdOucString instancepath = "/eos/";
  MgmProcPath = "/eos/";
  XrdOucString subpath = MgmOfsInstanceName;

  // Remove leading "eos" from the instance name when building the proc path for
  // "aesthetic" reasons
  if (subpath.beginswith("eos")) {
    subpath.erase(0, 3);
  }

  MgmProcPath += subpath;
  MgmProcPath += "/proc";
  // This path is used for temporary output files for layout conversions
  MgmProcConversionPath = MgmProcPath;
  MgmProcConversionPath += "/conversion";
  MgmProcDevicesPath = MgmProcPath;
  MgmProcDevicesPath += "/devices";
  MgmProcMasterPath = MgmProcPath;
  MgmProcMasterPath += "/master";
  MgmProcArchivePath = MgmProcPath;
  MgmProcArchivePath += "/archive";
  MgmProcWorkflowPath = MgmProcPath;
  MgmProcWorkflowPath += "/workflow";
  MgmProcTrackerPath = MgmProcPath;
  MgmProcTrackerPath += "/tracker";
  MgmProcTokenPath = MgmProcPath;
  MgmProcTokenPath += "/token";
  MgmProcBulkRequestPath = MgmProcPath;
  MgmProcBulkRequestPath += "/bulkrequests";
  Recycle::gRecyclingPrefix.insert(0, MgmProcPath.c_str());
  instancepath += subpath;
  // Initialize user mapping
  eos::common::Mapping::Init();
#ifdef EOS_INSTRUMENTED_RWMUTEX
  eos::common::RWMutex* fs_mtx = &FsView::gFsView.ViewMutex;
  eos::common::RWMutex* quota_mtx = &Quota::pMapMutex;
  eos::common::RWMutex* ns_mtx = &eosViewRWMutex;
  eos::common::RWMutex* fusex_client_mtx = &gOFS->zMQ->gFuseServer.Client();
  //eos::common::RWMutex* fusex_cap_mtx = &gOFS->zMQ->gFuseServer.Cap();
  // eos::common::RWMutex::EstimateLatenciesAndCompensation();
  fs_mtx->SetBlocking(true);
  fs_mtx->SetDebugName("FsView");
  fs_mtx->SetTiming(false);
  fs_mtx->SetSampling(true, 0.01);
  quota_mtx->SetDebugName("QuotaView");
  quota_mtx->SetTiming(false);
  quota_mtx->SetSampling(true, 0.01);
  ns_mtx->SetDebugName("eosView");
  ns_mtx->SetTiming(false);
  ns_mtx->SetSampling(true, 0.01);
  fusex_client_mtx->SetDebugName("FusexClient");
  //fusex_cap_mtx->SetDebugName("FusexCap");
  std::vector<eos::common::RWMutex*> order;
  order.push_back(fs_mtx);
  order.push_back(ns_mtx);
  order.push_back(fusex_client_mtx);
  //order.push_back(fusex_cap_mtx);
  order.push_back(quota_mtx);
  eos::common::RWMutex::AddOrderRule("Eos Mgm Mutexes", order);
#endif
  // Configure the meta data catalog
  mViewMutexWatcher.activate(eosViewRWMutex, "eosViewRWMutex");

  if (!mMaster->BootNamespace()) {
    eos_static_crit("%s", "msg=\"namespace boot failed\"");
    return 1;
  }

  // Check the '/' directory
  std::shared_ptr<eos::IContainerMD> rootmd;

  try {
    rootmd = eosView->getContainer("/");
  } catch (const eos::MDException& e) {
    Eroute.Emsg("Config", "cannot get the / directory meta data");
    eos_static_crit("eos view cannot retrieve the / directory");
    return 1;
  }

  // Check the '/' directory permissions
  if (!rootmd->getMode()) {
    if (mMaster->IsMaster()) {
      // no permissions set yet
      try {
        rootmd->setMode(S_IFDIR | S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP |
                        S_IWGRP | S_IXGRP);
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the / directory mode to initial mode");
        eos_static_crit("cannot set the / directory mode to 755");
        return 1;
      }
    } else {
      Eroute.Emsg("Config", "/ directory has no 755 permissions set");
      eos_static_crit("cannot see / directory with mode to 755");
      return 1;
    }
  }

  eos_static_info("msg=\"/ permissions are %o\"", rootmd->getMode());
  //mProcDirectoryBulkRequestLocations.reset(new bulk::ProcDirectoryBulkRequestLocations(MgmProcPath.c_str()));
  std::string restApiProcBulkRequestPath = MgmProcPath.c_str();
  restApiProcBulkRequestPath += "/tape-rest-api";
  mProcDirectoryBulkRequestTapeRestApiLocations.reset(new
      bulk::ProcDirectoryBulkRequestLocations(restApiProcBulkRequestPath));

  if (mMaster->IsMaster()) {
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
        eos_static_info("/eos permissions are %o checksum is set <adler>",
                        eosmd->getMode());
        eosmd = eosView->createContainer(instancepath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP |
                       S_IWGRP | S_IXGRP);
        eosmd->setAttribute("sys.forced.checksum", "adler");
        eosView->updateContainerStore(eosmd.get());
        eos_static_info("%s permissions are %o checksum is set <adler>",
                        instancepath.c_str(),
                        eosmd->getMode());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the /eos/ directory mode to initial mode");
        eos_static_crit("cannot set the /eos/ directory mode to 755");
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
                    "directory mode to initial mode");
        eos_static_crit("cannot set the /eos/proc directory mode to 755");
        return 1;
      }
    }

    // Create recycle directory
    try {
      eosmd = eosView->getContainer(Recycle::gRecyclingPrefix);
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = eosView->createContainer(Recycle::gRecyclingPrefix, true);
        eosmd->setMode(S_IFDIR | S_IRWXU);
        eosView->updateContainerStore(eosmd.get());
        eos_static_info("%s permissions are %o", Recycle::gRecyclingPrefix.c_str(),
                        eosmd->getMode());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the recycle directory mode to initial mode");
        eos_static_crit("cannot set the %s directory mode to 700",
                        Recycle::gRecyclingPrefix.c_str());
        eos_static_crit("%s", e.what());
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
                    " mode to initial mode");
        eos_static_crit("cannot set the /eos/../proc/conversion directory mode to 770");
        return 1;
      }
    }

    // Create output directory with device information
    try {
      eosmd = gOFS->eosView->getContainer(MgmProcDevicesPath.c_str());
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = gOFS->eosView->createContainer(MgmProcDevicesPath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IRWXG);
        eosmd->setCUid(0);
        eosmd->setCGid(0);
        gOFS->eosView->updateContainerStore(eosmd.get());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot create the /eos/../proc/devices directory");
        eos_static_crit("cannot create the /eos/../proc/devices directory");
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
                    "mode to initial mode");
        eos_static_crit("cannot set the /eos/../proc/archive directory mode to 770");
        return 1;
      }
    }

    // Create directory for clone functionality
    XrdOucString clonePath(MgmProcPath + "/clone");

    try {
      eosmd = gOFS->eosView->getContainer(clonePath.c_str());
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = gOFS->eosView->createContainer(clonePath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP | S_IXGRP);
        eosmd->setCUid(2); // clone directory is owned by daemon
        eosmd->setCGid(2);
        gOFS->eosView->updateContainerStore(eosmd.get());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the /eos/../proc/clone directory "
                    "mode to initial mode");
        eos_static_crit("cannot set the /eos/../proc/clone directory mode to 770");
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
                    "cannot set the /eos/../proc/workflow directory mode to initial mode");
        eos_static_crit("cannot set the /eos/../proc/workflow directory mode to 700");
        return 1;
      }
    }

    // Create tracker directory
    try {
      eosmd = gOFS->eosView->getContainer(MgmProcTrackerPath.c_str());
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = gOFS->eosView->createContainer(MgmProcTrackerPath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU);
        eosmd->setCUid(2); // lock directory is owned by daemon
        gOFS->eosView->updateContainerStore(eosmd.get());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the /eos/../proc/creation directory mode "
                    "to initial mode");
        eos_static_crit("cannot set the /eos/../proc/creation directory mode to 700");
        return 1;
      }
    }

    // Create token directory
    try {
      eosmd = gOFS->eosView->getContainer(MgmProcTokenPath.c_str());
    } catch (const eos::MDException& e) {
      eosmd = nullptr;
    }

    if (!eosmd) {
      try {
        eosmd = gOFS->eosView->createContainer(MgmProcTokenPath.c_str(), true);
        eosmd->setMode(S_IFDIR | S_IRWXU);
        eosmd->setCUid(0); // token directory is owned by root
        gOFS->eosView->updateContainerStore(eosmd.get());
      } catch (const eos::MDException& e) {
        Eroute.Emsg("Config", "cannot set the /eos/../proc/token directory mode "
                    "to initial mode");
        eos_static_crit("cannot set the /eos/../proc/token directory mode to 700");
        return 1;
      }
    }

    // Create bulkrequest-related directories
    /*
    for(const std::string & bulkReqDirPath : mProcDirectoryBulkRequestLocations->getAllBulkRequestDirectoriesPath()){
      try {
        eosmd = gOFS->eosView->getContainer(bulkReqDirPath);
      } catch (const eos::MDException& e) {
        eosmd = nullptr;
      }

      if (!eosmd) {
        try {
          eosmd = gOFS->eosView->createContainer(bulkReqDirPath, true);
          eosmd->setMode(S_IFDIR | S_IRWXU);
          eosmd->setCUid(2); // bulk-request directories are owned by daemon
          eosmd->setCGid(2);
          gOFS->eosView->updateContainerStore(eosmd.get());
        } catch (const eos::MDException& e) {
          {
            std::ostringstream errorMsg;
            errorMsg << "cannot set the " << bulkReqDirPath
                     << " directory mode to initial mode";
            Eroute.Emsg("Config", errorMsg.str().c_str());
          }
          {
            std::ostringstream errorMsg;
            errorMsg << "cannot set the " << bulkReqDirPath
                     << " directory mode to 700";
            eos_static_crit(errorMsg.str().c_str());
          }
          return 1;
        }
      }
    }
     */

    for (const std::string& bulkReqDirPath :
         mProcDirectoryBulkRequestTapeRestApiLocations->getAllBulkRequestDirectoriesPath()) {
      try {
        eosmd = gOFS->eosView->getContainer(bulkReqDirPath);
      } catch (const eos::MDException& e) {
        eosmd = nullptr;
      }

      if (!eosmd) {
        try {
          eosmd = gOFS->eosView->createContainer(bulkReqDirPath, true);
          eosmd->setMode(S_IFDIR | S_IRWXU);
          eosmd->setCUid(2); // bulk-request directories are owned by daemon
          eosmd->setCGid(2);
          gOFS->eosView->updateContainerStore(eosmd.get());
        } catch (const eos::MDException& e) {
          {
            std::ostringstream errorMsg;
            errorMsg << "cannot set the " << bulkReqDirPath
                     << " directory mode to initial mode";
            Eroute.Emsg("Config", errorMsg.str().c_str());
          }
          {
            std::ostringstream errorMsg;
            errorMsg << "cannot set the " << bulkReqDirPath
                     << " directory mode to 700";
            eos_static_crit(errorMsg.str().c_str());
          }
          return 1;
        }
      }
    }

    SetupProcFiles();
  }

  /*
  // start the bulk-request proc directory cleaner
  mBulkReqProcCleaner.reset(new bulk::BulkRequestProcCleaner(*gOFS->mProcDirectoryBulkRequestLocations,bulk::BulkRequestProcCleanerConfig::getDefaultConfig()));
  mBulkReqProcCleaner->Start();
   */
  mHttpTapeRestApiBulkReqProcCleaner.reset(new bulk::BulkRequestProcCleaner(
        *gOFS->mProcDirectoryBulkRequestTapeRestApiLocations,
        bulk::BulkRequestProcCleanerConfig::getDefaultConfig()));
  mHttpTapeRestApiBulkReqProcCleaner->Start();
  // Initialize the replication tracker
  mReplicationTracker.reset(ReplicationTracker::Create(
                              MgmProcTrackerPath.c_str()));
  // Configure proc path for devices
  DeviceTracker->SetDevicesPath(MgmProcDevicesPath.c_str());
  // Set also the archiver ZMQ endpoint were client requests are sent
  std::ostringstream oss;
  oss << "ipc://" << MgmArchiveDir.c_str() << "archive_frontend.ipc";
  mArchiveEndpoint = oss.str();
  eos_static_info("%s", "msg=\"starting statistics thread\"");
  mStatsTid.reset(&Stat::Circulate, &MgmStats);
  eos_static_info("%s", "msg=\"starting archive submitter thread\"");
  mSubmitterTid.reset(&XrdMgmOfs::ArchiveSubmitterThread, this);

  if (!MgmRedirector) {
    if (mErrLogEnabled) {
      if (mMessagingRealm->haveQDB()) {
        // Start ErrorReportListener and log entries in the local file
        eos_static_info("%s", "msg=\"starting error logger thread\"");

        try {
          mErrLoggerTid.reset(&XrdMgmOfs::ErrorLogListenerThread, this);
        } catch (const std::system_error& e) {
          eos_static_err("%s", "msg=\"failed to start error logger thread\"");
        }
      } else {
        // Run the error log console
        XrdOucString errorlogkillline = "pkill -9 -f \"eos -b console log _MGMID_\"";
        int rrc = system(errorlogkillline.c_str());

        if (WEXITSTATUS(rrc)) {
          eos_static_info("%s returned %d", errorlogkillline.c_str(), rrc);
        }

        XrdOucString errorlogline = "eos -b console log _MGMID_ >& /dev/null &";
        rrc = system(errorlogline.c_str());

        if (WEXITSTATUS(rrc)) {
          eos_static_info("%s returned %d", errorlogline.c_str(), rrc);
        }
      }
    }

    eos_static_info("%s", "msg=\"starting fs listener thread\"");

    try {
      mFsConfigTid.reset(&XrdMgmOfs::FsConfigListener, this);
    } catch (const std::system_error& e) {
      eos_static_crit("cannot start fs listener thread");
      NoGo = 1;
    }

    mFsMonitorTid.reset(&XrdMgmOfs::FileSystemMonitorThread, this);
  }

  if (!mMessagingRealm->haveQDB() && !ObjectNotifier.Start()) {
    eos_static_crit("error starting the shared object change notifier");
  }

  if (mHttpd) {
    const char* ptr = getenv("EOS_MGM_ENABLE_LIBMICROHTTPD");

    if (ptr && (strncmp(ptr, "1", 1) == 0)) {
      if (!mHttpd->Start()) {
        eos_static_warning("%s", "msg=\"failed to start libmicrohttpd\"");
      } else {
        eos_static_notice("%s", "msg=\"successfully started libmicrohttpd\"");
      }
    } else {
      eos_static_notice("%s", "msg=\"libmicrohttpd is disabled\"");
    }
  } else {
    eos_static_crit("%s", "msg=\"failed to allocate HttpServer object\"");
    NoGo = 1;
  }

#ifdef EOS_GRPC

  if (GRPCd) {
    GRPCd->Start();
  }

  // Start gRPC server for EOS Windows native client
  if (WNCd) {
    WNCd->StartWnc();
  }

  // Start gRPC server for EOS HTTP REST API
  if (mRestGrpcSrv) {
#ifdef EOS_GRPC_GATEWAY
    eos_static_notice("msg=\"REST GRPC service enabled\" port=%i",
                      mRestGrpcPort);
    mRestGrpcSrv->Start();
#else
    mRestGrpcSrv.reset();
    eos_static_notice("%s", "msg=\"REST GPRC service disabled due to lack of "
                      "GRPC GATEWAY support i.e. eos-grpc-gateway\"");
#endif
  } else {
    eos_static_notice("%s", "msg=\"REST GRPC service disabled\"");
  }

#endif
  // start the Admin socket
  {
    std::string admin_socket_path = std::string(gOFS->MgmMetaLogDir.c_str()) +
                                    std::string("/.admin_socket:") + std::to_string(ManagerPort);
    AdminSocketServer.reset(new eos::mgm::AdminSocket(admin_socket_path));
  }
  bool restApiActivated = false;
  bool restApiStageEnabled = false;
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

    if (FsView::gFsView.mSpaceView.find("default") !=
        FsView::gFsView.mSpaceView.end()) {
      const std::string& restApiSwitchOnOff =
        FsView::gFsView.mSpaceView["default"]->GetConfigMember
        (eos::mgm::rest::TAPE_REST_API_SWITCH_ON_OFF);

      if (restApiSwitchOnOff == "on") {
        restApiActivated = true;
      }

      const std::string& restApiStageSwitchOnOff =
        FsView::gFsView.mSpaceView["default"]->GetConfigMember(
          eos::mgm::rest::TAPE_REST_API_STAGE_SWITCH_ON_OFF);

      if (restApiStageSwitchOnOff == "on") {
        restApiStageEnabled = true;
      }
    }
  }
  {
    //Tape REST API configuration
    auto tapeRestApiConfig = mRestApiManager->getTapeRestApiConfig();
    tapeRestApiConfig->setTapeEnabled(mTapeEnabled);
    tapeRestApiConfig->setActivated(restApiActivated);
    tapeRestApiConfig->setSiteName(tapeRestApiSitename);
    tapeRestApiConfig->setEndpointToUrlMapping(tapeRestApiEndpointUrlMap);
    tapeRestApiConfig->setHostAlias(MgmOfsAlias.c_str());
    tapeRestApiConfig->setXrdHttpPort(XrdHttpPort);
    tapeRestApiConfig->setStageEnabled(restApiStageEnabled);
  }
  // start the LRU daemon
  mLRUEngine->Start();

  // start the WFE daemon
  if (!gOFS->WFEd.Start()) {
    eos_static_warning("msg=\"cannot start WFE thread\"");
  }

  // Start the device tracking thread
  if ((mMaster->IsMaster()) && (!DeviceTracker->Start())) {
    eos_static_warning("msg=\"cannot start device tracking thread\"");
  }

  // Start the recycler garbage collection thread on a master machine
  if ((mMaster->IsMaster()) && (!Recycler->Start())) {
    eos_static_warning("msg=\"cannot start recycle thread\"");
  }

  // Print a test-stacktrace to ensure we have debugging symbols.
  std::ostringstream ss;
  ss << "Printing a test stacktrace to check for debugging symbols: "
     << eos::common::getStacktrace();
  eos_static_info("%s", ss.str().c_str());
  // add all stat entries with 0
  InitStats();
  // start the fuse server
  gOFS->zMQ->gFuseServer.start();
  const std::string iostat_file = SSTR(MgmMetaLogDir << "/iostat." << ManagerId
                                       << ".dump");

  if (!IoStats->Init(MgmOfsInstanceName.c_str(), ManagerPort, iostat_file)) {
    eos_static_warning("%s", "msg=\"failed to initialize IoStat object\"");
  } else {
    eos_static_notice("%s", "msg=\"successfully initalized IoStat object\"");
  }

  if (!MgmRedirector) {
    ObjectManager.HashMutex.LockRead();
    XrdMqSharedHash* hash = ObjectManager.GetHash("/eos/*");

    // Ask for a broadcast from FSTs
    if (hash) {
      hash->BroadcastRequest("/eos/*/fst");
    }

    ObjectManager.HashMutex.UnLockRead();
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

  if (getenv("EOS_COVERAGE_REPORT")) {
    // Add coverage report handler
    (void) signal(SIGPROF, xrdmgmofs_coverage);
  }

  (void) signal(SIGUSR2, xrdmgmofs_stack);
  (void) signal(SIGUSR1, xrdmgmofs_stack);

  if (mNumAuthThreads && mFrontendPort) {
    eos_static_info("starting the authentication master thread");

    try {
      mAuthMasterTid.reset(&XrdMgmOfs::AuthMasterThread, this);
    } catch (const std::system_error& e) {
      eos_static_crit("cannot start the authentication master thread");
      NoGo = 1;
    }

    // @todo(esindril): this should be removed and we should use a
    // pool of threads
    eos_static_info("starting the authentication worker threads");

    for (unsigned int i = 0; i < mNumAuthThreads; ++i) {
      pthread_t worker_tid;

      if ((XrdSysThread::Run(&worker_tid, XrdMgmOfs::StartAuthWorkerThread,
                             static_cast<void*>(this), XRDSYSTHREAD_HOLD,
                             "Auth Worker Thread"))) {
        eos_static_crit("msg=\"cannot start authentication thread num=%i\"", i);
        NoGo = 1;
        break;
      } else {
        mVectTid.push_back(worker_tid);
      }
    }
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  // to be sure not to miss any notification while everything is starting up
  // we don't check if it succeeds because we might fail because we timeout
  // if there is no FST sending update
  mGeoTreeEngine->forceRefresh();
  mGeoTreeEngine->StartUpdater();
  // Start the drain engine
  mDrainEngine.Start();
  // Start the Converter driver
  eos_static_info("%s", "msg=\"starting Converter Engine\"");
  mConverterDriver.reset(new eos::mgm::ConverterDriver(mQdbContactDetails));
  mConverterDriver->Start();
  mFsScheduler->updateClusterData();
  {
    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);

    for (const auto& space : FsView::gFsView.mSpaceView) {
      mFsScheduler->setPlacementStrategy(space.first,
                                         space.second->GetConfigMember("scheduler.type"));
    }
  }
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
  MgmStats.Add("BulkRequestBusiness::getBulkRequest", 0, 0, 0);
  MgmStats.Add("BulkRequestBusiness::getStageBulkRequest", 0, 0, 0);
  MgmStats.Add("BulkRequestBusiness::saveBulkRequest", 0, 0, 0);
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
  MgmStats.Add("Devices::Extract", 0, 0, 0);
  MgmStats.Add("Devices::Store", 0, 0, 0);
  MgmStats.Add("DumpMd", 0, 0, 0);
  MgmStats.Add("Drop", 0, 0, 0);
  MgmStats.Add("DropStripe", 0, 0, 0);
  MgmStats.Add("Exists", 0, 0, 0);
  MgmStats.Add("Exists", 0, 0, 0);
  MgmStats.Add("Eosxd::prot::evicted", 0, 0, 0);
  MgmStats.Add("Eosxd::prot::mount", 0, 0, 0);
  MgmStats.Add("Eosxd::prot::umount", 0, 0, 0);
  MgmStats.Add("Eosxd::prot::offline", 0, 0, 0);
  MgmStats.Add("Eosxd::prot::SET", 0, 0, 0);
  MgmStats.Add("Eosxd::prot::LS", 0, 0, 0);
  MgmStats.Add("Eosxd::prot::STAT", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::GET", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::SET", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::LS", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::LS-Entry", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::CREATE", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::UPDATE", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::MKDIR", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::RMDIR", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::RENAME", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::MV", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::DELETE", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::GETCAP", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::SETLK", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::GETLK", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::SETLKW", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::BEGINFLUSH", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::ENDFLUSH", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::CREATELNK", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::SETLNK", 0, 0, 0);
  MgmStats.Add("Eosxd::ext::DELETELNK", 0, 0, 0);
  MgmStats.Add("Eosxd::int::AuthRevocation", 0, 0, 0);
  MgmStats.Add("Eosxd::int::BcConfig", 0, 0, 0);
  MgmStats.Add("Eosxd::int::BcDropAll", 0, 0, 0);
  MgmStats.Add("Eosxd::int::BcMD", 0, 0, 0);
  MgmStats.Add("Eosxd::int::BcRefresh", 0, 0, 0);
  MgmStats.Add("Eosxd::int::BcRefreshExt", 0, 0, 0);
  MgmStats.Add("Eosxd::int::BcDeletion", 0, 0, 0);
  MgmStats.Add("Eosxd::int::BcDeletionExt", 0, 0, 0);
  MgmStats.Add("Eosxd::int::DeleteEntry", 0, 0, 0);
  MgmStats.Add("Eosxd::int::FillContainerCAP", 0, 0, 0);
  MgmStats.Add("Eosxd::int::FillContainerMD", 0, 0, 0);
  MgmStats.Add("Eosxd::int::FillFileMD", 0, 0, 0);
  MgmStats.Add("Eosxd::int::Heartbeat", 0, 0, 0);
  MgmStats.Add("Eosxd::int::MonitorCaps", 0, 0, 0);
  MgmStats.Add("Eosxd::int::MonitorHeartBeat", 0, 0, 0);
  MgmStats.Add("Eosxd::int::RefreshEntry", 0, 0, 0);
  MgmStats.Add("Eosxd::int::ReleaseCap", 0, 0, 0);
  MgmStats.Add("Eosxd::int::SendCAP", 0, 0, 0);
  MgmStats.Add("Eosxd::int::SendMD", 0, 0, 0);
  MgmStats.Add("Eosxd::int::Store", 0, 0, 0);
  MgmStats.Add("Eosxd::int::ValidatePERM", 0, 0, 0);
  MgmStats.Add("FileInfo", 0, 0, 0);
  MgmStats.Add("FindEntries", 0, 0, 0);
  MgmStats.Add("Find", 0, 0, 0);
  MgmStats.Add("FScheduler::Placement::Failed", 0, 0, 0);
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
  MgmStats.Add("GetFusex", 0, 0, 0);
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
  MgmStats.Add("OpenFailedPermission", 0, 0, 0);
  MgmStats.Add("OpenFailedQuota", 0, 0, 0);
  MgmStats.Add("OpenFailedNoUpdate", 0, 0, 0);
  MgmStats.Add("OpenFailedReconstruct", 0, 0, 0);
  MgmStats.Add("OpenFileOffline", 0, 0, 0);
  MgmStats.Add("OpenProc", 0, 0, 0);
  MgmStats.Add("OpenRead", 0, 0, 0);
  MgmStats.Add("OpenRedirectLocal", 0, 0, 0);
  MgmStats.Add("OpenFailedRedirectLocal", 0, 0, 0);
  MgmStats.Add("OpenShared", 0, 0, 0);
  MgmStats.Add("OpenStalled", 0, 0, 0);
  MgmStats.Add("OpenStalled", 0, 0, 0);
  MgmStats.Add("Open", 0, 0, 0);
  MgmStats.Add("OpenWriteCreate", 0, 0, 0);
  MgmStats.Add("OpenWriteTruncate", 0, 0, 0);
  MgmStats.Add("OpenWrite", 0, 0, 0);
  MgmStats.Add("Prepare", 0, 0, 0);
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
  MgmStats.Add("DrainCentralStarted", 0, 0, 0);
  MgmStats.Add("DrainCentralSuccessful", 0, 0, 0);
  MgmStats.Add("DrainCentralFailed", 0, 0, 0);
  MgmStats.Add("Schedule2Delete", 0, 0, 0);
  MgmStats.Add("Scheduled2Delete", 0, 0, 0);
  MgmStats.Add("QueryResync", 0, 0, 0);
  MgmStats.Add("Stall", 0, 0, 0);
  MgmStats.Add("Stat", 0, 0, 0);
  MgmStats.Add("Symlink", 0, 0, 0);
  MgmStats.Add("TapeRestApiBusiness::cancelStageBulkRequest", 0, 0, 0);
  MgmStats.Add("TapeRestApiBusiness::createStageBulkRequest", 0, 0, 0);
  MgmStats.Add("TapeRestApiBusiness::deleteStageBulkRequest", 0, 0, 0);
  MgmStats.Add("TapeRestApiBusiness::getFileInfo", 0, 0, 0);
  MgmStats.Add("TapeRestApiBusiness::getStageBulkRequest", 0, 0, 0);
  MgmStats.Add("TapeRestApiBusiness::releasePaths", 0, 0, 0);
  MgmStats.Add("Touch", 0, 0, 0);
  MgmStats.Add("TxState", 0, 0, 0);
  MgmStats.Add("Truncate", 0, 0, 0);
  MgmStats.Add("VerifyStripe", 0, 0, 0);
  MgmStats.Add("Version", 0, 0, 0);
  MgmStats.Add("Versioning", 0, 0, 0);
  MgmStats.Add("WhoAmI", 0, 0, 0);
}

//------------------------------------------------------------------------------
// Setup /eos/<instance>/proc files
//------------------------------------------------------------------------------
void
XrdMgmOfs::SetupProcFiles()
{
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
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
  std::shared_ptr<eos::IFileMD> fmd;
  std::shared_ptr<eos::IContainerMD> cmd;

  try {
    fmd.reset();
    fmd = eosView->getFile(procpathwhoami.c_str());
  } catch (eos::MDException& e) {
    fmd = eosView->createFile(procpathwhoami.c_str(), 0, 0);
  }

  if (fmd) {
    fmd->setSize(4096);
    fmd->setAttribute("sys.proc", "mgm.cmd=whoami&mgm.format=fuse");
    eosView->updateFileStore(fmd.get());
  }

  try {
    fmd.reset();
    fmd = eosView->getFile(procpathwho.c_str());
  } catch (eos::MDException& e) {
    fmd = eosView->createFile(procpathwho.c_str(), 0, 0);
  }

  if (fmd) {
    fmd->setSize(4096);
    fmd->setAttribute("sys.proc", "mgm.cmd=who&mgm.format=fuse");
    eosView->updateFileStore(fmd.get());
  }

  try {
    fmd.reset();
    fmd = eosView->getFile(procpathquota.c_str());
  } catch (eos::MDException& e) {
    fmd = eosView->createFile(procpathquota.c_str(), 0, 0);
  }

  if (fmd) {
    fmd->setSize(4096);
    fmd->setAttribute("sys.proc",
                      "mgm.cmd=quota&mgm.subcmd=lsuser&mgm.format=fuse");
    eosView->updateFileStore(fmd.get());
  }

  try {
    fmd.reset();
    fmd = eosView->getFile(procpathreconnect.c_str());
  } catch (eos::MDException& e) {
    fmd = eosView->createFile(procpathreconnect.c_str(), 0, 0);
  }

  if (fmd) {
    fmd->setSize(4096);
    eosView->updateFileStore(fmd.get());
  }

  try {
    fmd.reset();
    fmd = eosView->getFile(procpathmaster.c_str());
  } catch (eos::MDException& e) {
    fmd = eosView->createFile(procpathmaster.c_str(), 0, 0);
  }

  if (fmd) {
    fmd->setSize(4096);
    eosView->updateFileStore(fmd.get());
  }
}

//------------------------------------------------------------------------------
// Set up global config
//------------------------------------------------------------------------------
void
XrdMgmOfs::SetupGlobalConfig()
{
  if (!mMessagingRealm->haveQDB()) {
    std::string configQueue = SSTR("/config/"
                                   << eos::common::InstanceName::get()
                                   << "/mgm/");

    if (!ObjectManager.CreateSharedHash(configQueue.c_str(), "/eos/*/mgm")) {
      eos_static_crit("msg=\"cannot add global config queue\" qpath=\"%s\"",
                      configQueue.c_str());
    }

    configQueue = SSTR("/config/" << eos::common::InstanceName::get()
                       << "/all/");

    if (!ObjectManager.CreateSharedHash(configQueue.c_str(), "/eos/*")) {
      eos_static_crit("msg=\"cannot add global config queue\" qpath=\"%s\"",
                      configQueue.c_str());
    }

    configQueue = SSTR("/config/" << eos::common::InstanceName::get()
                       << "/fst/");

    if (!ObjectManager.CreateSharedHash(configQueue.c_str(), "/eos/*/fst")) {
      eos_static_crit("msg=\"cannot add global config queue\" qpath=\"%s\"",
                      configQueue.c_str());
    }
  }
}
