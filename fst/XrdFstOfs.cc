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

/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOfs.hh"
#include "fst/XrdFstOss.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/FmdSqlite.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/Path.hh"
#include "common/Statfs.hh"
#include "common/Attr.hh"
#include "common/SyncAll.hh"
/*----------------------------------------------------------------------------*/
#include "XrdNet/XrdNetOpts.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdVersion.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>

#include <math.h>
#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
/*----------------------------------------------------------------------------*/


// The global OFS handle
eos::fst::XrdFstOfs eos::fst::gOFS;

extern XrdSysError OfsEroute;
extern XrdOss* XrdOfsOss;
extern XrdOfs* XrdOfsFS;
extern XrdOucTrace OfsTrace;

extern XrdOss*
XrdOssGetSS (XrdSysLogger*,
             const char*,
             const char*,
             const char*,
             XrdVersionInfo&);

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
extern "C"
{

  XrdSfsFileSystem*
  XrdSfsGetFileSystem (XrdSfsFileSystem* native_fs,
                       XrdSysLogger* lp,
                       const char* configfn)
  {
    // Do the herald thing
    //
    OfsEroute.SetPrefix("FstOfs_");
    OfsEroute.logger(lp);
    XrdOucString version = "FstOfs (Object Storage File System) ";
    version += VERSION;
    OfsEroute.Say("++++++ (c) 2010 CERN/IT-DSS ",
                  version.c_str());

    // Initialize the subsystems
    //
    eos::fst::gOFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

    if (eos::fst::gOFS.Configure(OfsEroute)) return 0;

    XrdOfsFS = &eos::fst::gOFS;
    return &eos::fst::gOFS;
  }
}

EOSFSTNAMESPACE_BEGIN


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdFstOfs::XrdFstOfs () :
eos::common::LogId ()
{
  Eroute = 0;
  Messaging = 0;
  Storage = 0;
  TransferScheduler = 0;
  //-------------------------------------------
  // add Shutdown handler
  //-------------------------------------------
  (void) signal(SIGINT, xrdfstofs_shutdown);
  (void) signal(SIGTERM, xrdfstofs_shutdown);
  (void) signal(SIGQUIT, xrdfstofs_shutdown);
  //-------------------------------------------
  // add SEGV handler
  //-------------------------------------------
  (void) signal(SIGSEGV, xrdfstofs_stacktrace);
  (void) signal(SIGABRT, xrdfstofs_stacktrace);
  (void) signal(SIGBUS, xrdfstofs_stacktrace);

  TpcMap.emplace(TpcMap.end());
  TpcMap.emplace(TpcMap.end());
  TpcMap[0].set_deleted_key(""); // readers
  TpcMap[1].set_deleted_key(""); // writers
}


//------------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------

XrdFstOfs::~XrdFstOfs () {
  // empty
}


//------------------------------------------------------------------------------
// Function newDir
//-----------------------------------------------------------------------------

XrdSfsDirectory*
XrdFstOfs::newDir (char* user, int MonID)
{
  return static_cast<XrdSfsDirectory*> (new XrdFstOfsDirectory(user, MonID));
}


//------------------------------------------------------------------------------
// Function newFile
//-----------------------------------------------------------------------------

XrdSfsFile*
XrdFstOfs::newFile (char* user, int MonID)
{
  return static_cast<XrdSfsFile*> (new XrdFstOfsFile(user, MonID));
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfs::xrdfstofs_stacktrace (int sig)
{
  (void) signal(SIGINT, SIG_IGN);
  (void) signal(SIGTERM, SIG_IGN);
  (void) signal(SIGQUIT, SIG_IGN);
  void* array[10];
  size_t size;
  // get void*'s for all entries on the stack
  size = backtrace(array, 10);
  // print out all the frames to stderr
  fprintf(stderr, "error: received signal %d:\n", sig);
  backtrace_symbols_fd(array, size, 2);
  // now we put back the initial handler and send the signal again
  signal(sig, SIG_DFL);
  kill(getpid(), sig);
#ifdef __APPLE__
  int wstatus = 0;
  wait(&wstatus);
#else
  wait();
#endif
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfs::xrdfstofs_shutdown (int sig)
{
  static XrdSysMutex ShutDownMutex;
  ShutDownMutex.Lock(); // this handler goes only one-shot .. sorry !

  pid_t watchdog;
  if (!(watchdog = fork()))
  {
    eos::common::SyncAll::AllandClose();
    XrdSysTimer sleeper;
    sleeper.Snooze(15);
    eos_static_warning("%s", "op=shutdown msg=\"shutdown timedout after 15 seconds\"");
    kill(getppid(), 9);
    eos_static_warning("%s", "op=shutdown status=forced-complete");
    kill(getpid(), 9);
  }

  // handler to shutdown the daemon for valgrinding and clean server stop (e.g. let's time to finish write operations

  if (gOFS.Messaging)
  {
    gOFS.Messaging->StopListener(); // stop any communication
  }

  XrdSysTimer sleeper;
  sleeper.Wait(1000);
  std::set<pthread_t>::const_iterator it;
  {
    XrdSysMutexHelper(gOFS.Storage->ThreadSetMutex);

    for (it = gOFS.Storage->ThreadSet.begin(); it != gOFS.Storage->ThreadSet.end(); it++)
    {
      eos_static_warning("op=shutdown threadid=%llx", (unsigned long long) *it);
      XrdSysThread::Cancel(*it);
      //      XrdSysThread::Join( *it, 0 );
    }
  }
  eos_static_warning("op=shutdown msg=\"stop messaging\"");

  eos_static_warning("%s", "op=shutdown msg=\"shutdown fmdsqlite handler\"");
  gFmdSqliteHandler.Shutdown();
  kill(watchdog,9);
  wait();

  eos_static_warning("%s", "op=shutdown status=sqliteclosed");

  // sync & close all file descriptors
  eos::common::SyncAll::AllandClose();

  eos_static_warning("%s", "op=shutdown status=completed");
  // harakiri - yes!
  (void) signal(SIGABRT, SIG_IGN);
  (void) signal(SIGINT, SIG_IGN);
  (void) signal(SIGTERM, SIG_IGN);
  (void) signal(SIGQUIT, SIG_IGN);
  exit(0);
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfs::Configure (XrdSysError& Eroute)
{
  char* var;
  const char* val;
  int cfgFD;
  int NoGo = 0;
  int rc = XrdOfs::Configure(Eroute);
  // enforcing 'sss' authentication for all communications
  setenv("XrdSecPROTOCOL", "sss", 1);
  Eroute.Say("=====> fstofs enforces SSS authentication for XROOT clients");

  if (rc)
    return rc;

  TransferScheduler = new XrdScheduler(&Eroute, &OfsTrace, 8, 128, 60);
  TransferScheduler->Start();
  eos::fst::Config::gConfig.autoBoot = false;
  eos::fst::Config::gConfig.FstOfsBrokerUrl = "root://localhost:1097//eos/";

  if (getenv("EOS_BROKER_URL"))
  {
    eos::fst::Config::gConfig.FstOfsBrokerUrl = getenv("EOS_BROKER_URL");
  }

  {
    // set the start date as string
    XrdOucString out="";
    time_t t= time(NULL);
    struct tm * timeinfo;
    timeinfo = localtime (&t);
    
    out = asctime(timeinfo); out.erase(out.length()-1); 
    eos::fst::Config::gConfig.StartDate = out.c_str();
  }

  eos::fst::Config::gConfig.FstMetaLogDir = "/var/tmp/eos/md/";
  setenv("XrdClientEUSER", "daemon", 1);
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // extract the manager from the config file
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"));

  if (!ConfigFN || !*ConfigFN)
  {
    // this error will be reported by XrdOfsFS.Configure
  }
  else
  {
    // Try to open the configuration file.
    //
    if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return Eroute.Emsg("Config", errno, "open config file fn=", ConfigFN);

    Config.Attach(cfgFD);
    // Now start reading records until eof.
    //

    while ((var = Config.GetMyFirstWord()))
    {
      if (!strncmp(var, "fstofs.", 7))
      {
        var += 7;

        // we parse config variables here
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
              eos::fst::Config::gConfig.FstOfsBrokerUrl = getenv("EOS_BROKER_URL");
            }
            else
            {
              eos::fst::Config::gConfig.FstOfsBrokerUrl = val;
            }
          }
        }

        if (!strcmp("trace", var))
        {
          if (!(val = Config.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for trace missing. Can be 'client'");
            NoGo = 1;
          }
          else
          {
            //EnvPutInt( NAME_DEBUG, 3);
          }
        }

        if (!strcmp("autoboot", var))
        {
          if ((!(val = Config.GetWord())) || (strcmp("true", val) && strcmp("false", val) && strcmp("1", val) && strcmp("0", val)))
          {
            Eroute.Emsg("Config", "argument 2 for autobootillegal or missing. Must be <true>,<false>,<1> or <0>!");
            NoGo = 1;
          }
          else
          {
            if ((!strcmp("true", val) || (!strcmp("1", val))))
            {
              eos::fst::Config::gConfig.autoBoot = true;
            }
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
            eos::fst::Config::gConfig.FstMetaLogDir = val;
          }
        }
      }
    }

    Config.Close();
  }

  if (eos::fst::Config::gConfig.autoBoot)
  {
    Eroute.Say("=====> fstofs.autoboot : true");
  }
  else
  {
    Eroute.Say("=====> fstofs.autoboot : false");
  }

  if (!eos::fst::Config::gConfig.FstOfsBrokerUrl.endswith("/"))
  {
    eos::fst::Config::gConfig.FstOfsBrokerUrl += "/";
  }

  eos::fst::Config::gConfig.FstDefaultReceiverQueue = eos::fst::Config::gConfig.FstOfsBrokerUrl;
  eos::fst::Config::gConfig.FstOfsBrokerUrl += HostName;
  eos::fst::Config::gConfig.FstOfsBrokerUrl += ":";
  eos::fst::Config::gConfig.FstOfsBrokerUrl += myPort;
  eos::fst::Config::gConfig.FstOfsBrokerUrl += "/fst";
  eos::fst::Config::gConfig.FstHostPort = HostName;
  eos::fst::Config::gConfig.FstHostPort += ":";
  eos::fst::Config::gConfig.FstHostPort += myPort;
  eos::fst::Config::gConfig.KernelVersion = eos::common::StringConversion::StringFromShellCmd("uname -r | tr -d \"\n\"").c_str();
  
  Eroute.Say("=====> fstofs.broker : ", eos::fst::Config::gConfig.FstOfsBrokerUrl.c_str(), "");
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // extract our queue name
  eos::fst::Config::gConfig.FstQueue = eos::fst::Config::gConfig.FstOfsBrokerUrl;
  {
    int pos1 = eos::fst::Config::gConfig.FstQueue.find("//");
    int pos2 = eos::fst::Config::gConfig.FstQueue.find("//", pos1 + 2);
    if (pos2 != STR_NPOS)
    {
      eos::fst::Config::gConfig.FstQueue.erase(0, pos2 + 1);
    }
    else
    {
      Eroute.Emsg("Config", "cannot determin my queue name: ", eos::fst::Config::gConfig.FstQueue.c_str());
      return 1;
    }
  }

  // create our wildcard broadcast name
  eos::fst::Config::gConfig.FstQueueWildcard = eos::fst::Config::gConfig.FstQueue;
  eos::fst::Config::gConfig.FstQueueWildcard += "/*";

  // create our wildcard config broadcast name
  eos::fst::Config::gConfig.FstConfigQueueWildcard = "*/";
  eos::fst::Config::gConfig.FstConfigQueueWildcard += HostName;
  eos::fst::Config::gConfig.FstConfigQueueWildcard += ":";
  eos::fst::Config::gConfig.FstConfigQueueWildcard += myPort;

  // create our wildcard gw broadcast name
  eos::fst::Config::gConfig.FstGwQueueWildcard = "*/";
  eos::fst::Config::gConfig.FstGwQueueWildcard += HostName;
  eos::fst::Config::gConfig.FstGwQueueWildcard += ":";
  eos::fst::Config::gConfig.FstGwQueueWildcard += myPort;
  eos::fst::Config::gConfig.FstGwQueueWildcard += "/fst/gw/txqueue/txq";

  // Set Logging parameters
  XrdOucString unit = "fst@";
  unit += HostName;
  unit += ":";
  unit += myPort;

  // setup the circular in-memory log buffer
  eos::common::Logging::Init();
  eos::common::Logging::SetLogPriority(LOG_INFO);
  eos::common::Logging::SetUnit(unit.c_str());

  eos_info("info=\"logging configured\"");

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // create the messaging object(recv thread)

  eos::fst::Config::gConfig.FstDefaultReceiverQueue += "*/mgm";
  int pos1 = eos::fst::Config::gConfig.FstDefaultReceiverQueue.find("//");
  int pos2 = eos::fst::Config::gConfig.FstDefaultReceiverQueue.find("//", pos1 + 2);
  if (pos2 != STR_NPOS)
  {
    eos::fst::Config::gConfig.FstDefaultReceiverQueue.erase(0, pos2 + 1);
  }

  Eroute.Say("=====> fstofs.defaultreceiverqueue : ", eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str(), "");
  // set our Eroute for XrdMqMessage
  XrdMqMessage::Eroute = OfsEroute;

  // Enable the shared object notification queue
  ObjectManager.EnableQueue = true;
  ObjectManager.SetAutoReplyQueue("/eos/*/mgm");
  ObjectManager.SetDebug(false);
  eos::common::Logging::SetLogPriority(LOG_INFO);

  // setup notification subjects
  ObjectManager.SubjectsMutex.Lock();
  std::string watch_id = "id";
  std::string watch_bootsenttime = "bootsenttime";
  std::string watch_scaninterval = "scaninterval";
  std::string watch_symkey = "symkey";
  std::string watch_manager = "manager";
  std::string watch_publishinterval = "publish.interval";
  std::string watch_debuglevel = "debug.level";
  std::string watch_gateway = "txgw";
  std::string watch_gateway_rate = "gw.rate";
  std::string watch_gateway_ntx = "gw.ntx";
  std::string watch_error_simulation = "error.simulation";

  ObjectManager.ModificationWatchKeys.insert(watch_id);
  ObjectManager.ModificationWatchKeys.insert(watch_bootsenttime);
  ObjectManager.ModificationWatchKeys.insert(watch_scaninterval);
  ObjectManager.ModificationWatchKeys.insert(watch_symkey);
  ObjectManager.ModificationWatchKeys.insert(watch_manager);
  ObjectManager.ModificationWatchKeys.insert(watch_publishinterval);
  ObjectManager.ModificationWatchKeys.insert(watch_debuglevel);
  ObjectManager.ModificationWatchKeys.insert(watch_gateway);
  ObjectManager.ModificationWatchKeys.insert(watch_gateway_rate);
  ObjectManager.ModificationWatchKeys.insert(watch_gateway_ntx);
  ObjectManager.ModificationWatchKeys.insert(watch_error_simulation);
  ObjectManager.SubjectsMutex.UnLock();




  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // create the specific listener class
  Messaging = new eos::fst::Messaging(eos::fst::Config::gConfig.FstOfsBrokerUrl.c_str(), eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str(), false, false, &ObjectManager);
  Messaging->SetLogId("FstOfsMessaging");

  if ((!Messaging) || (!Messaging->StartListenerThread())) NoGo = 1;

  if ((!Messaging) || (Messaging->IsZombie()))
  {
    Eroute.Emsg("Config", "cannot create messaging object(thread)");
    NoGo = 1;
  }
  if (NoGo)
    return NoGo;

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Attach Storage to the meta log dir

  Storage = eos::fst::Storage::Create(eos::fst::Config::gConfig.FstMetaLogDir.c_str());
  Eroute.Say("=====> fstofs.metalogdir : ", eos::fst::Config::gConfig.FstMetaLogDir.c_str());
  if (!Storage)
  {
    Eroute.Emsg("Config", "cannot setup meta data storage using directory: ", eos::fst::Config::gConfig.FstMetaLogDir.c_str());
    return 1;
  }


  XrdSysTimer sleeper;
  sleeper.Snooze(5);

  eos_notice("sending broadcast's ...");
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Create a wildcard broadcast 
  XrdMqSharedHash* hash = 0;
  XrdMqSharedQueue* queue = 0;

  // Create a node broadcast
  ObjectManager.CreateSharedHash(eos::fst::Config::gConfig.FstConfigQueueWildcard.c_str(), eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  ObjectManager.HashMutex.LockRead();

  hash = ObjectManager.GetHash(eos::fst::Config::gConfig.FstConfigQueueWildcard.c_str());

  if (hash)
  {
    // ask for a broadcast
    hash->BroadCastRequest(eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  }

  ObjectManager.HashMutex.UnLockRead();

  // Create a node gateway broadcast
  ObjectManager.CreateSharedQueue(eos::fst::Config::gConfig.FstGwQueueWildcard.c_str(), eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  ObjectManager.HashMutex.LockRead();

  queue = ObjectManager.GetQueue(eos::fst::Config::gConfig.FstGwQueueWildcard.c_str());

  if (queue)
  {
    // ask for a broadcast
    queue->BroadCastRequest(eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  }

  ObjectManager.HashMutex.UnLockRead();

  // Create a filesystem broadcast
  ObjectManager.CreateSharedHash(eos::fst::Config::gConfig.FstQueueWildcard.c_str(), eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  ObjectManager.HashMutex.LockRead();
  hash = ObjectManager.GetHash(eos::fst::Config::gConfig.FstQueueWildcard.c_str());

  if (hash)
  {
    // ask for a broadcast
    hash->BroadCastRequest(eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  }

  ObjectManager.HashMutex.UnLockRead();
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // start dumper thread
  XrdOucString dumperfile = eos::fst::Config::gConfig.FstMetaLogDir;
  dumperfile += "so.fst.dump";
  ObjectManager.StartDumper(dumperfile.c_str());

  XrdOucString keytabcks = "unaccessible";

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // start the embedded HTTP server
  httpd = new Http(8001);
  if (httpd)
  {
    httpd->Start();
  }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // build the adler checksum of the default keytab file
  int fd = ::open("/etc/eos.keytab", O_RDONLY);
  if (fd > 0)
  {
    char buffer[65535];
    size_t nread = ::read(fd, buffer, sizeof (buffer));
    if (nread > 0)
    {
      CheckSum* KeyCKS = ChecksumPlugins::GetChecksumObject(eos::common::LayoutId::kAdler);
      if (KeyCKS)
      {
        KeyCKS->Add(buffer, nread, 0);
        keytabcks = KeyCKS->GetHexChecksum();
        delete KeyCKS;
      }
    }
    close(fd);
  }

  eos_notice("FST_HOST=%s FST_PORT=%ld VERSION=%s RELEASE=%s KEYTABADLER=%s", HostName, myPort, VERSION, RELEASE, keytabcks.c_str());

  eos::fst::Config::gConfig.KeyTabAdler = keytabcks.c_str();

  sleeper.Snooze(5);

  return 0;
}

/*----------------------------------------------------------------------------*/

void
XrdFstOfs::SetSimulationError (const char* tag)
{
  // -----------------------------------------------
  // define error bool variables to en-/disable error simulation in the OFS layer

  XrdOucString stag = tag;
  gOFS.Simulate_IO_read_error = gOFS.Simulate_IO_write_error = gOFS.Simulate_XS_read_error = gOFS.Simulate_XS_write_error = false;

  if (stag == "io_read")
  {
    gOFS.Simulate_IO_read_error = true;
  }
  if (stag == "io_write")
  {
    gOFS.Simulate_IO_write_error = true;
  }
  if (stag == "xs_read")
  {
    gOFS.Simulate_XS_read_error = true;
  }
  if (stag == "xs_write")
  {
    gOFS.Simulate_XS_write_error = true;
  }
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfs::stat (const char* path,
                 struct stat* buf,
                 XrdOucErrInfo& out_error,
                 const XrdSecEntity* client,
                 const char* opaque)
{
  EPNAME("stat");
  memset(buf, 0, sizeof ( struct stat));

  if (!XrdOfsOss->Stat(path, buf))
  {
    return SFS_OK;
  }
  else
  {
    return gOFS.Emsg(epname, out_error, errno, "stat file", path);
  }
}


//------------------------------------------------------------------------------
// CallManager function
//------------------------------------------------------------------------------

int
XrdFstOfs::CallManager (XrdOucErrInfo* error,
                        const char* path,
                        const char* manager,
                        XrdOucString& capOpaqueFile,
                        XrdOucString* return_result)
{
  EPNAME("CallManager");
  int rc = SFS_OK;
  XrdOucString msg = "";
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";
  XrdCl::URL url(address.c_str());

  if (!url.IsValid())
  {
    eos_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

  //.............................................................................
  // Get XrdCl::FileSystem object
  //.............................................................................
  XrdCl::FileSystem* fs = new XrdCl::FileSystem(url);

  if (!fs)
  {
    eos_err("error=failed to get new FS object");

    if (error)
    {
      gOFS.Emsg(epname, *error, ENOMEM, "allocate FS object calling the manager node for fn=", path);
    }

    return EINVAL;
  }

  arg.FromString(capOpaqueFile.c_str());
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK())
  {
    eos_debug("called MGM cache - %s", capOpaqueFile.c_str());
    rc = SFS_OK;
  }
  else
  {
    msg = (status.GetErrorMessage().c_str());
    rc = SFS_ERROR;

    if (msg.find("[EIDRM]") != STR_NPOS) 
      rc = -EIDRM;

    if (msg.find("[EBADE]") != STR_NPOS) 
      rc = -EBADE;

    if (msg.find("[EBADR]") != STR_NPOS)
      rc = -EBADR;

    if (msg.find("[EINVAL]") != STR_NPOS)
      rc = -EINVAL;

    if (msg.find("[EADV]") != STR_NPOS)
      rc = -EADV;

    if (error && (rc != SFS_ERROR) )
      gOFS.Emsg(epname, *error, -rc, msg.c_str(), path);
    else
      gOFS.Emsg(epname, *error, ECOMM, msg.c_str(), path);
  }

  if ( response && return_result )
  {
    *return_result = response->GetBuffer();
  }

  delete fs;
  if (response) delete response;
  return rc;
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfs::SetDebug (XrdOucEnv& env)
{
  XrdOucString debugnode = env.Get("mgm.nodename");
  XrdOucString debuglevel = env.Get("mgm.debuglevel");
  XrdOucString filterlist = env.Get("mgm.filter");
  int debugval = eos::common::Logging::GetPriorityByString(debuglevel.c_str());

  if (debugval < 0)
  {
    eos_err("debug level %s is not known!", debuglevel.c_str());
  }
  else
  {
    // we set the shared hash debug for the lowest 'debug' level
    if (debuglevel == "debug")
    {
      ObjectManager.SetDebug(true);
    }
    else
    {
      ObjectManager.SetDebug(false);
    }

    eos::common::Logging::SetLogPriority(debugval);
    eos_notice("setting debug level to <%s>", debuglevel.c_str());

    if (filterlist.length())
    {
      eos::common::Logging::SetFilter(filterlist.c_str());
      eos_notice("setting message logid filter to <%s>", filterlist.c_str());
    }
  }

  fprintf(stderr, "Setting debug to %s\n", debuglevel.c_str());
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfs::SendRtLog (XrdMqMessage* message)
{
  XrdOucEnv opaque(message->GetBody());
  XrdOucString queue = opaque.Get("mgm.rtlog.queue");
  XrdOucString lines = opaque.Get("mgm.rtlog.lines");
  XrdOucString tag = opaque.Get("mgm.rtlog.tag");
  XrdOucString filter = opaque.Get("mgm.rtlog.filter");
  XrdOucString stdOut = "";

  if (!filter.length()) filter = " ";

  if ((!queue.length()) || (!lines.length()) || (!tag.length()))
  {
    eos_err("illegal parameter queue=%s lines=%s tag=%s", queue.c_str(), lines.c_str(), tag.c_str());
  }
  else
  {
    if ((eos::common::Logging::GetPriorityByString(tag.c_str())) == -1)
    {
      eos_err("mgm.rtlog.tag must be info,debug,err,emerg,alert,crit,warning or notice");
    }
    else
    {
      int logtagindex = eos::common::Logging::GetPriorityByString(tag.c_str());

      for (int j = 0; j <= logtagindex; j++)
      {
        for (int i = 1; i <= atoi(lines.c_str()); i++)
        {
          eos::common::Logging::gMutex.Lock();
          XrdOucString logline = eos::common::Logging::gLogMemory[j][(eos::common::Logging::gLogCircularIndex[j] - i + eos::common::Logging::gCircularIndexSize) % eos::common::Logging::gCircularIndexSize].c_str();
          eos::common::Logging::gMutex.UnLock();

          if (logline.length() && ((logline.find(filter.c_str())) != STR_NPOS))
          {
            stdOut += logline;
            stdOut += "\n";
          }

          if (stdOut.length() > (4 * 1024))
          {
            XrdMqMessage repmessage("rtlog reply message");
            repmessage.SetBody(stdOut.c_str());

            if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message))
            {
              eos_err("unable to send rtlog reply message to %s", message->kMessageHeader.kSenderId.c_str());
            }

            stdOut = "";
          }

          if (!logline.length())
            break;
        }
      }
    }
  }

  if (stdOut.length())
  {
    XrdMqMessage repmessage("rtlog reply message");
    repmessage.SetBody(stdOut.c_str());

    if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message))
    {
      eos_err("unable to send rtlog reply message to %s", message->kMessageHeader.kSenderId.c_str());
    }
  }
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfs::SendFsck (XrdMqMessage* message)
{
  XrdOucEnv opaque(message->GetBody());
  XrdOucString stdOut = "";
  XrdOucString tag = opaque.Get("mgm.fsck.tags"); // the tag is either '*' for all, or a , seperated list of tag names

  if ((!tag.length()))
  {
    eos_err("parameter tag missing");
  }
  else
  {
    stdOut = "";
    // loop over filesystems
    eos::common::RWMutexReadLock(gOFS.Storage->fsMutex);
    std::vector <eos::fst::FileSystem*>::const_iterator it;

    for (unsigned int i = 0; i < gOFS.Storage->fileSystemsVector.size(); i++)
    {
      std::map<std::string, std::set<eos::common::FileId::fileid_t> >* icset = gOFS.Storage->fileSystemsVector[i]->GetInconsistencySets();
      std::map<std::string, std::set<eos::common::FileId::fileid_t> >::const_iterator icit;

      for (icit = icset->begin(); icit != icset->end(); icit++)
      {
        // loop over all tags
        if (((icit->first != "mem_n") && (icit->first != "d_sync_n") && (icit->first != "m_sync_n")) &&
            ((tag == "*") || ((tag.find(icit->first.c_str()) != STR_NPOS))))
        {
          char stag[4096];
          eos::common::FileSystem::fsid_t fsid = gOFS.Storage->fileSystemsVector[i]->GetId();
          snprintf(stag, sizeof ( stag) - 1, "%s@%lu", icit->first.c_str(), (unsigned long) fsid);
          stdOut += stag;
          std::set<eos::common::FileId::fileid_t>::const_iterator fit;

          if (gOFS.Storage->fileSystemsVector[i]->GetStatus() != eos::common::FileSystem::kBooted)
          {
            // we don't report filesystems which are not booted!
            continue;
          }

          for (fit = icit->second.begin(); fit != icit->second.end(); fit++)
          {
            // don't report files which are currently write-open
            XrdSysMutexHelper wLock(gOFS.OpenFidMutex);

            if (gOFS.WOpenFid[fsid].count(*fit))
            {
              if (gOFS.WOpenFid[fsid][*fit] > 0)
              {
                continue;
              }
            }

            // loop over all fids
            char sfid[4096];
            snprintf(sfid, sizeof ( sfid) - 1, ":%08llx", *fit);
            stdOut += sfid;

            if (stdOut.length() > (64 * 1024))
            {
              stdOut += "\n";
              XrdMqMessage repmessage("fsck reply message");
              repmessage.SetBody(stdOut.c_str());
              fprintf(stderr, "Sending %s\n", stdOut.c_str());

              if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message))
              {
                eos_err("unable to send fsck reply message to %s", message->kMessageHeader.kSenderId.c_str());
              }

              stdOut = stag;
            }
          }

          stdOut += "\n";
        }
      }
    }
  }

  if (stdOut.length())
  {
    XrdMqMessage repmessage("fsck reply message");
    repmessage.SetBody(stdOut.c_str());
    fprintf(stderr, "Sending %s\n", stdOut.c_str());

    if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message))
    {
      eos_err("unable to send fsck reply message to %s", message->kMessageHeader.kSenderId.c_str());
    }
  }
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfs::rem (const char* path,
                XrdOucErrInfo& error,
                const XrdSecEntity* client,
                const char* opaque)
{
  EPNAME("rem");
  XrdOucString stringOpaque = opaque;
  stringOpaque.replace("?", "&");
  stringOpaque.replace("&&", "&");
  XrdOucEnv openOpaque(stringOpaque.c_str());
  XrdOucEnv* capOpaque;
  int caprc = 0;

  if ((caprc = gCapabilityEngine.Extract(&openOpaque, capOpaque)))
  {
    // no capability - go away!
    if (capOpaque) delete capOpaque;

    return gOFS.Emsg(epname, error, caprc, "open - capability illegal", path);
  }

  int envlen;

  if (capOpaque)
  {
    eos_info("path=%s info=%s capability=%s", path, opaque, capOpaque->Env(envlen));
  }
  else
  {
    eos_info("path=%s info=%s", path, opaque);
  }

  int rc = _rem(path, error, client, capOpaque);

  if (capOpaque)
  {
    delete capOpaque;
    capOpaque = 0;
  }

  return rc;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfs::_rem (const char* path,
                 XrdOucErrInfo& error,
                 const XrdSecEntity* client,
                 XrdOucEnv* capOpaque,
                 const char* fstpath,
                 unsigned long long fid,
                 unsigned long fsid,
                 bool ignoreifnotexist)
{
  EPNAME("rem");
  int retc = SFS_OK;
  XrdOucString fstPath = "";
  const char* localprefix = 0;
  const char* hexfid = 0;
  const char* sfsid = 0;
  eos_debug("");

  if ((!fstpath) && (!fsid) && (!fid))
  {
    // standard deletion brings all information via the opaque info
    if (!(localprefix = capOpaque->Get("mgm.localprefix")))
    {
      return gOFS.Emsg(epname, error, EINVAL, "open - no local prefix in capability", path);
    }

    if (!(hexfid = capOpaque->Get("mgm.fid")))
    {
      return gOFS.Emsg(epname, error, EINVAL, "open - no file id in capability", path);
    }

    if (!(sfsid = capOpaque->Get("mgm.fsid")))
    {
      return gOFS.Emsg(epname, error, EINVAL, "open - no file system id in capability", path);
    }

    eos::common::FileId::FidPrefix2FullPath(hexfid, localprefix, fstPath);
    fid = eos::common::FileId::Hex2Fid(hexfid);
    fsid = atoi(sfsid);
  }
  else
  {
    // deletion during close provides the local storage path, fid & fsid
    fstPath = fstpath;
  }

  struct stat statinfo;

  if ((retc = XrdOfsOss->Stat(fstPath.c_str(), &statinfo)))
  {
    if (!ignoreifnotexist)
    {
      eos_notice("unable to delete file - file does not exist (anymore): %s fstpath=%s fsid=%lu id=%llu", path, fstPath.c_str(), fsid, fid);
      return gOFS.Emsg(epname, error, ENOENT, "delete file - file does not exist", fstPath.c_str());
    }
  }

  eos_info("fstpath=%s", fstPath.c_str());
  int rc = 0;

  if (!retc)
  {
    // unlink file
    errno = 0;
    rc = XrdOfs::rem(fstPath.c_str(), error, client, 0);

    if (rc)
    {
      eos_info("rc=%d errno=%d", rc, errno);
    }
  }

  if (ignoreifnotexist)
  {
    // hide error if a deleted file is deleted
    rc = 0;
  }

  // unlink block checksum files
  {
    // this is not the 'best' solution, but we don't have any info about block checksums
    Adler xs; // the type does not matter here
    const char* path = xs.MakeBlockXSPath(fstPath.c_str());

    if (!xs.UnlinkXSPath())
    {
      eos_info("info=\"removed block-xs\" path=%s", path);
    }
  }

  // cleanup eventual transactions
  if (!gOFS.Storage->CloseTransaction(fsid, fid))
  {
    // it should be the normal case that there is no open transaction for that file, in any case there is nothing to do here
  }

  if (rc)
  {
    return rc;
  }

  if (!gFmdSqliteHandler.DeleteFmd(fid, fsid))
  {
    eos_notice("unable to delete fmd for fid %llu on filesystem %lu", fid, fsid);
    return gOFS.Emsg(epname, error, EIO, "delete file meta data ", fstPath.c_str());
  }

  return SFS_OK;
}

int
XrdFstOfs::fsctl (const int cmd,
                  const char* args,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client)

{
  static const char* epname = "fsctl";
  const char* tident = error.getErrUser();

  if ((cmd == SFS_FSCTL_LOCATE))
  {
    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    rType[1] = 'r'; //(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp, "[::%s:%d] ", (char*) HostName, myPort);
    error.setErrInfo(strlen(locResp) + 3, (const char**) Resp, 2);
    ZTRACE(fsctl, "located at headnode: " << locResp);
    return SFS_DATA;
  }

  return gOFS.Emsg(epname, error, EPERM, "execute fsctl function", "");
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfs::FSctl (const int cmd,
                  XrdSfsFSctl& args,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client)
{
  char ipath[16384];
  char iopaque[16384];
  static const char* epname = "FSctl";
  const char* tident = error.getErrUser();

  if ((cmd == SFS_FSCTL_LOCATE))
  {
    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    rType[1] = 'r'; //(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp, "[::%s:%d] ", (char*) HostName, myPort);
    error.setErrInfo(strlen(locResp) + 3, (const char**) Resp, 2);
    ZTRACE(fsctl, "located at headnode: " << locResp);
    return SFS_DATA;
  }

  // accept only plugin calls!

  if (cmd != SFS_FSCTL_PLUGIN)
  {
    return gOFS.Emsg(epname, error, EPERM, "execute non-plugin function", "");
  }

  if (args.Arg1Len)
  {
    if (args.Arg1Len < 16384)
    {
      strncpy(ipath, args.Arg1, args.Arg1Len);
      ipath[args.Arg1Len] = 0;
    }
    else
    {
      return gOFS.Emsg(epname, error, EINVAL, "convert path argument - string too long", "");
    }
  }
  else
  {
    ipath[0] = 0;
  }

  if (args.Arg2Len)
  {
    if (args.Arg2Len < 16384)
    {
      strncpy(iopaque, args.Arg2, args.Arg2Len);
      iopaque[args.Arg2Len] = 0;
    }
    else
    {
      return gOFS.Emsg(epname, error, EINVAL, "convert opaque argument - string too long", "");
    }
  }
  else
  {
    iopaque[0] = 0;
  }

  // from here on we can deal with XrdOucString which is more 'comfortable'
  XrdOucString path = ipath;
  XrdOucString opaque = iopaque;
  XrdOucString result = "";
  XrdOucEnv env(opaque.c_str());
  eos_debug("tident=%s path=%s opaque=%s", tident, path.c_str(), opaque.c_str());

  if (cmd != SFS_FSCTL_PLUGIN)
  {
    return SFS_ERROR;
  }

  const char* scmd;

  if ((scmd = env.Get("fst.pcmd")))
  {
    XrdOucString execmd = scmd;

    if (execmd == "getfmd")
    {
      char* afid = env.Get("fst.getfmd.fid");
      char* afsid = env.Get("fst.getfmd.fsid");

      if ((!afid) || (!afsid))
      {
        return Emsg(epname, error, EINVAL, "execute FSctl command", path.c_str());
      }

      unsigned long long fileid = eos::common::FileId::Hex2Fid(afid);
      unsigned long fsid = atoi(afsid);
      FmdSqlite* fmd = gFmdSqliteHandler.GetFmd(fileid, fsid, 0, 0, 0, false, true);

      if (!fmd)
      {
        eos_static_err("no fmd for fileid %llu on filesystem %lu", fileid, fsid);
        const char* err = "ERROR";
        error.setErrInfo(strlen(err) + 1, err);
        return SFS_DATA;
      }

      XrdOucEnv* fmdenv = fmd->FmdSqliteToEnv();
      int envlen;
      XrdOucString fmdenvstring = fmdenv->Env(envlen);
      delete fmdenv;
      delete fmd;
      error.setErrInfo(fmdenvstring.length() + 1, fmdenvstring.c_str());
      return SFS_DATA;
    }

    if (execmd == "getxattr")
    {
      char* key = env.Get("fst.getxattr.key");
      char* path = env.Get("fst.getxattr.path");

      if (!key)
      {
        eos_static_err("no key specified as attribute name");
        const char* err = "ERROR";
        error.setErrInfo(strlen(err) + 1, err);
        return SFS_DATA;
      }

      if (!path)
      {
        eos_static_err("no path specified to get the attribute from");
        const char* err = "ERROR";
        error.setErrInfo(strlen(err) + 1, err);
        return SFS_DATA;
      }

      char value[1024];
#ifdef __APPLE__
      ssize_t attr_length = getxattr(path, key, value, sizeof ( value), 0, 0);
#else
      ssize_t attr_length = getxattr(path, key, value, sizeof ( value));
#endif
      if (attr_length > 0)
      {
        value[1023] = 0;
        XrdOucString skey = key;
        XrdOucString attr = "";

        if (skey == "user.eos.checksum")
        {
          // checksum's are binary and need special reformatting ( we swap the byte order if they are 4 bytes long )
          if (attr_length == 4)
          {
            for (ssize_t k = 0; k < 4; k++)
            {
              char hex[4];
              snprintf(hex, sizeof ( hex) - 1, "%02x", (unsigned char) value[3 - k]);
              attr += hex;
            }
          }
          else
          {
            for (ssize_t k = 0; k < attr_length; k++)
            {
              char hex[4];
              snprintf(hex, sizeof ( hex) - 1, "%02x", (unsigned char) value[k]);
              attr += hex;
            }
          }
        }
        else
        {
          attr = value;
        }

        error.setErrInfo(attr.length() + 1, attr.c_str());
        return SFS_DATA;
      }
      else
      {
        eos_static_err("getxattr failed for path=%s", path);
        const char* err = "ERROR";
        error.setErrInfo(strlen(err) + 1, err);
        return SFS_DATA;
      }
    }
  }

  return Emsg(epname, error, EINVAL, "execute FSctl command", path.c_str());
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfs::OpenFidString (unsigned long fsid, XrdOucString& outstring)
{
  outstring = "";
  OpenFidMutex.Lock();
  google::sparse_hash_map<unsigned long long, unsigned int>::const_iterator idit;
  int nopen = 0;

  for (idit = ROpenFid[fsid].begin(); idit != ROpenFid[fsid].end(); ++idit)
  {
    if (idit->second > 0)
      nopen += idit->second;
  }

  outstring += "&statfs.ropen=";
  outstring += nopen;
  nopen = 0;

  for (idit = WOpenFid[fsid].begin(); idit != WOpenFid[fsid].end(); ++idit)
  {
    if (idit->second > 0)
      nopen += idit->second;
  }

  outstring += "&statfs.wopen=";
  outstring += nopen;
  OpenFidMutex.UnLock();
}

int
XrdFstOfs::Stall (XrdOucErrInfo& error, // Error text & code
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
  //
  error.setErrInfo(0, smessage.c_str());
  // All done
  //
  return stime;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfs::Redirect (XrdOucErrInfo& error, // Error text & code
                     const char* host,
                     int& port)
{
  EPNAME("Redirect");
  const char* tident = error.getErrUser();
  ZTRACE(delay, "Redirect " << host << ":" << port);
  // Place the error message in the error object and return
  //
  error.setErrInfo(port, host);
  // All done
  //
  return SFS_REDIRECT;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsDirectory::open (const char* dirName,
                          const XrdSecClientName* client,
                          const char* opaque)
{
  // ----------------------------------------------------------------------------
  // dummy implementation doing nothing
  // ----------------------------------------------------------------------------
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/

const char*
XrdFstOfsDirectory::nextEntry ()
{
  return 0;
}

/*----------------------------------------------------------------------------*/

int
XrdFstOfsDirectory::close ()
{
  return SFS_OK;
}
EOSFSTNAMESPACE_END

