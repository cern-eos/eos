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
#include "XrdOss/XrdOssApi.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysTimer.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <math.h>
#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
/*----------------------------------------------------------------------------*/
// the global OFS handle
eos::fst::XrdFstOfs eos::fst::gOFS;
// the client admin table
// the capability engine

extern XrdSysError OfsEroute;
extern XrdOssSys  *XrdOfsOss;
extern XrdOfs     *XrdOfsFS;
extern XrdOss     *XrdOssGetSS(XrdSysLogger *, const char *, const char *);
extern XrdOucTrace OfsTrace;


/*----------------------------------------------------------------------------*/
extern "C"
{
  XrdSfsFileSystem *XrdSfsGetFileSystem(XrdSfsFileSystem *native_fs, 
                                        XrdSysLogger     *lp,
                                        const char       *configfn)
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

    if ( eos::fst::gOFS.Configure(OfsEroute) ) return 0;
    // Initialize the target storage system
    //
    if (!(XrdOfsOss = (XrdOssSys*) XrdOssGetSS(lp, configfn, eos::fst::gOFS.OssLib))) return 0;

    XrdOfsFS = &eos::fst::gOFS;
    // All done, we can return the callout vector to these routines.
    //
    return &eos::fst::gOFS;
  }
}

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
XrdFstOfs::xrdfstofs_stacktrace(int sig) {
  (void) signal(SIGINT,SIG_IGN);
  (void) signal(SIGTERM,SIG_IGN);
  (void) signal(SIGQUIT,SIG_IGN);
  void *array[10];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  // print out all the frames to stderr
  fprintf(stderr, "error: received signal %d:\n", sig);
  backtrace_symbols_fd(array, size, 2);
  // now we put back the initial handler and send the signal again
  signal(sig, SIG_DFL);
  kill(getpid(), sig);
  wait();
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfs::xrdfstofs_shutdown(int sig) {
  static XrdSysMutex ShutDownMutex;

  ShutDownMutex.Lock(); // this handler goes only one-shot .. sorry !

  pid_t watchdog;
  if (!(watchdog = fork())) {
    eos::common::SyncAll::AllandClose();
    XrdSysTimer sleeper;
    sleeper.Snooze(15);
    eos_static_warning("%s","op=shutdown msg=\"shutdown timedout after 15 seconds\"");
    kill(getppid(),9);
    eos_static_warning("%s","op=shutdown status=forced-complete");
    kill(getpid(),9);
  }

  // handler to shutdown the daemon for valgrinding and clean server stop (e.g. let's time to finish write operations

  if (gOFS.Messaging) {
    gOFS.Messaging->StopListener(); // stop any communication
  }

  XrdSysTimer sleeper;
  sleeper.Wait(1000);

  std::set<pthread_t>::const_iterator it;
  {
    XrdSysMutexHelper(gOFS.Storage->ThreadSetMutex);
    for (it= gOFS.Storage->ThreadSet.begin(); it != gOFS.Storage->ThreadSet.end(); it++) {
      eos_static_warning("op=shutdown threadid=%llx", (unsigned long long) *it);
      XrdSysThread::Cancel(*it);
      //      XrdSysThread::Join(*it,0);
    }
  }

  eos_static_warning("%s","op=shutdown msg=\"shutdown fmdsqlite handler\"");
  gFmdSqliteHandler.Shutdown();
  kill(9, watchdog);

  eos_static_warning("%s","op=shutdown status=sqliteclosed");

  // sync & close all file descriptors
  eos::common::SyncAll::AllandClose();
  
  eos_static_warning("%s","op=shutdown status=completed");
  // harakiri - yes!
  kill(getpid(),9);
}

/*----------------------------------------------------------------------------*/
int XrdFstOfs::Configure(XrdSysError& Eroute) 
{
  char *var;
  const char *val;
  int  cfgFD;
  int NoGo=0;

  int rc = XrdOfs::Configure(Eroute);

  // enforcing 'sss' authentication for all communications

  setenv("XrdSecPROTOCOL","sss",1);
  Eroute.Say("=====> fstofs enforces SSS authentication for XROOT clients");

  if (rc)
    return rc;

  TransferScheduler = new XrdScheduler(&Eroute, &OfsTrace, 8, 128, 60);

  TransferScheduler->Start();

  eos::fst::Config::gConfig.autoBoot = false;

  eos::fst::Config::gConfig.FstOfsBrokerUrl = "root://localhost:1097//eos/";
  
  if (getenv("EOS_BROKER_URL")) {
    eos::fst::Config::gConfig.FstOfsBrokerUrl = getenv("EOS_BROKER_URL");
  }

  eos::fst::Config::gConfig.FstMetaLogDir = "/var/tmp/eos/md/";

  {
    // set the start date as string
    XrdOucString out="";
    time_t t= time(NULL);
    struct tm * timeinfo;
    timeinfo = localtime (&t);
    
    out = asctime(timeinfo); out.erase(out.length()-1); 
    eos::fst::Config::gConfig.StartDate = out.c_str();
  }

  setenv("XrdClientEUSER", "daemon", 1);

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // extract the manager from the config file
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"));

  if( !ConfigFN || !*ConfigFN) {
    // this error will be reported by XrdOfsFS.Configure
  } else {
    // Try to open the configuration file.
    //
    if ( (cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return Eroute.Emsg("Config", errno, "open config file fn=", ConfigFN);

    Config.Attach(cfgFD);
    // Now start reading records until eof.
    //
    
    while((var = Config.GetMyFirstWord())) {
      if (!strncmp(var, "fstofs.",7)) {
        var += 7;
        // we parse config variables here 
        if (!strcmp("broker",var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config","argument 2 for broker missing. Should be URL like root://<host>/<queue>/"); NoGo=1;
          } else {
            if (getenv("EOS_BROKER_URL")) {
              eos::fst::Config::gConfig.FstOfsBrokerUrl = getenv("EOS_BROKER_URL");
            } else {
              eos::fst::Config::gConfig.FstOfsBrokerUrl = val;
            }

          }
        }

        if (!strcmp("trace",var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config","argument 2 for trace missing. Can be 'client'"); NoGo=1;
          } else {
            EnvPutInt( NAME_DEBUG, 3);
          }
        }
        
        if (!strcmp("autoboot",var)) {
          if ((!(val = Config.GetWord())) || (strcmp("true",val) && strcmp("false",val) && strcmp("1",val) && strcmp("0",val))) {
            Eroute.Emsg("Config","argument 2 for autobootillegal or missing. Must be <true>,<false>,<1> or <0>!"); NoGo=1;
          } else {
            if ((!strcmp("true",val) || (!strcmp("1",val)))) {
              eos::fst::Config::gConfig.autoBoot = true;
            }
          }
        }

        if (!strcmp("metalog",var)) {
          if (!(val = Config.GetWord())) {
            Eroute.Emsg("Config","argument 2 for metalog missing"); NoGo=1;
          } else {
            eos::fst::Config::gConfig.FstMetaLogDir = val;
          }
        }
      }
    }
    Config.Close();
  }

  if (eos::fst::Config::gConfig.autoBoot) {
    Eroute.Say("=====> fstofs.autoboot : true");
  } else {
    Eroute.Say("=====> fstofs.autoboot : false");
  }

  if (! eos::fst::Config::gConfig.FstOfsBrokerUrl.endswith("/")) {
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


  Eroute.Say("=====> fstofs.broker : ", eos::fst::Config::gConfig.FstOfsBrokerUrl.c_str(),"");

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // extract our queue name
  eos::fst::Config::gConfig.FstQueue = eos::fst::Config::gConfig.FstOfsBrokerUrl;
  {
    int pos1 = eos::fst::Config::gConfig.FstQueue.find("//");
    int pos2 = eos::fst::Config::gConfig.FstQueue.find("//",pos1+2);
    if (pos2 != STR_NPOS) {
      eos::fst::Config::gConfig.FstQueue.erase(0, pos2+1);
    } else {
      Eroute.Emsg("Config","cannot determin my queue name: ", eos::fst::Config::gConfig.FstQueue.c_str());
      return 1;
    }
  }

  // create our wildcard broadcast name
  eos::fst::Config::gConfig.FstQueueWildcard =  eos::fst::Config::gConfig.FstQueue;
  eos::fst::Config::gConfig.FstQueueWildcard+= "/*";

  // create our wildcard config broadcast name
  eos::fst::Config::gConfig.FstConfigQueueWildcard =  "*/";
  eos::fst::Config::gConfig.FstConfigQueueWildcard+= HostName; 
  eos::fst::Config::gConfig.FstConfigQueueWildcard+= ":";
  eos::fst::Config::gConfig.FstConfigQueueWildcard+= myPort;

  // create our wildcard gw broadcast name
  eos::fst::Config::gConfig.FstGwQueueWildcard =  "*/";
  eos::fst::Config::gConfig.FstGwQueueWildcard+= HostName; 
  eos::fst::Config::gConfig.FstGwQueueWildcard+= ":";
  eos::fst::Config::gConfig.FstGwQueueWildcard+= myPort;
  eos::fst::Config::gConfig.FstGwQueueWildcard+= "/fst/gw/txqueue/txq";

  // Set Logging parameters
  XrdOucString unit = "fst@"; unit+= HostName; unit+=":"; unit+=myPort;

  // setup the circular in-memory log buffer
  eos::common::Logging::Init();
  eos::common::Logging::SetLogPriority(LOG_INFO);
  eos::common::Logging::SetUnit(unit.c_str());

  eos_info("info=\"logging configured\"");

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // create the messaging object(recv thread)
  
  eos::fst::Config::gConfig.FstDefaultReceiverQueue += "*/mgm";
  int pos1 = eos::fst::Config::gConfig.FstDefaultReceiverQueue.find("//");
  int pos2 = eos::fst::Config::gConfig.FstDefaultReceiverQueue.find("//",pos1+2);
  if (pos2 != STR_NPOS) {
    eos::fst::Config::gConfig.FstDefaultReceiverQueue.erase(0, pos2+1);
  }

  Eroute.Say("=====> fstofs.defaultreceiverqueue : ", eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str(),"");
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
  std::string watch_symkey       = "symkey";
  std::string watch_manager      = "manager";
  std::string watch_publishinterval = "publish.interval";
  std::string watch_debuglevel   = "debug.level";
  std::string watch_gateway      = "txgw";
  std::string watch_gateway_rate = "gw.rate";
  std::string watch_gateway_ntx  = "gw.ntx";
  std::string watch_error_simulation  = "error.simulation";

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
  Messaging = new eos::fst::Messaging(eos::fst::Config::gConfig.FstOfsBrokerUrl.c_str(),eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str(),false, false, &ObjectManager);
  Messaging->SetLogId("FstOfsMessaging");

  if( (!Messaging) || (!Messaging->StartListenerThread()) ) NoGo = 1;

  if ( (!Messaging) || (Messaging->IsZombie()) ) {
    Eroute.Emsg("Config","cannot create messaging object(thread)");
    NoGo = 1;
  }
  if (NoGo) 
    return NoGo;

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Attach Storage to the meta log dir

  Storage = eos::fst::Storage::Create(eos::fst::Config::gConfig.FstMetaLogDir.c_str());
  Eroute.Say("=====> fstofs.metalogdir : ", eos::fst::Config::gConfig.FstMetaLogDir.c_str());
  if (!Storage) {
    Eroute.Emsg("Config","cannot setup meta data storage using directory: ", eos::fst::Config::gConfig.FstMetaLogDir.c_str());
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
  ObjectManager.CreateSharedHash(eos::fst::Config::gConfig.FstConfigQueueWildcard.c_str(),eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  ObjectManager.HashMutex.LockRead();
  
  hash = ObjectManager.GetHash(eos::fst::Config::gConfig.FstConfigQueueWildcard.c_str());
  
  if (hash) {
    // ask for a broadcast
    hash->BroadCastRequest(eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  }

  ObjectManager.HashMutex.UnLockRead();

  // Create a node gateway broadcast
  ObjectManager.CreateSharedQueue(eos::fst::Config::gConfig.FstGwQueueWildcard.c_str(),eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  ObjectManager.HashMutex.LockRead();
  
  queue = ObjectManager.GetQueue(eos::fst::Config::gConfig.FstGwQueueWildcard.c_str());
  
  if (queue) {
    // ask for a broadcast
    queue->BroadCastRequest(eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  }

  ObjectManager.HashMutex.UnLockRead();

  // Create a filesystem broadcast
  ObjectManager.CreateSharedHash(eos::fst::Config::gConfig.FstQueueWildcard.c_str(),eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  ObjectManager.HashMutex.LockRead();
  hash = ObjectManager.GetHash(eos::fst::Config::gConfig.FstQueueWildcard.c_str());

  if (hash) {
    // ask for a broadcast
    hash->BroadCastRequest(eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  }

  ObjectManager.HashMutex.UnLockRead();
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // start dumper thread
  XrdOucString dumperfile = eos::fst::Config::gConfig.FstMetaLogDir;
  dumperfile += "so.fst.dump";
  ObjectManager.StartDumper(dumperfile.c_str());

  XrdOucString keytabcks="unaccessible";

 
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // build the adler checksum of the default keytab file
  int fd = ::open("/etc/eos.keytab",O_RDONLY);
  if (fd>0) {
    char buffer[65535];
    size_t nread = ::read(fd, buffer, sizeof(buffer));
    if (nread>0) {
      CheckSum* KeyCKS = ChecksumPlugins::GetChecksumObject(eos::common::LayoutId::kAdler);
      if (KeyCKS) {
        KeyCKS->Add(buffer,nread,0);
        keytabcks= KeyCKS->GetHexChecksum();
        delete KeyCKS;
      }
    }
    close(fd);
  }

  eos_notice("FST_HOST=%s FST_PORT=%ld VERSION=%s RELEASE=%s KEYTABADLER=%s", HostName, myPort, VERSION,RELEASE, keytabcks.c_str());

  eos::fst::Config::gConfig.KeyTabAdler = keytabcks.c_str();

  sleeper.Snooze(5);
  
  return 0;
}

/*----------------------------------------------------------------------------*/

void 
XrdFstOfs::SetSimulationError(const char* tag) {
  // -----------------------------------------------
  // define error bool variables to en-/disable error simulation in the OFS layer

  XrdOucString stag = tag;
  gOFS.Simulate_IO_read_error = gOFS.Simulate_IO_write_error = gOFS.Simulate_XS_read_error = gOFS.Simulate_XS_write_error = false;
  
  if (stag == "io_read") {
    gOFS.Simulate_IO_read_error = true;
  }
  if (stag == "io_write") {
    gOFS.Simulate_IO_write_error = true;
  } 
  if (stag == "xs_read") {
    gOFS.Simulate_XS_read_error = true;
  }
  if (stag == "xs_write") {
    gOFS.Simulate_XS_write_error = true;
  }
}

/*----------------------------------------------------------------------------*/
void 
XrdFstOfsFile::MakeReportEnv(XrdOucString &reportString)
{
  // compute avg, min, max, sigma for read and written bytes
  unsigned long long rmin,rmax,rsum;
  unsigned long long wmin,wmax,wsum;
  double ravg,wavg;
  double rsum2,wsum2;
  double rsigma,wsigma;

  // ---------------------------------------
  // compute for read
  // ---------------------------------------
  rmax=rsum=0;
  rmin=0xffffffff;
  ravg=rsum2=rsigma=0;

  {
    XrdSysMutexHelper vecLock(vecMutex);
    for (size_t i=0; i< rvec.size(); i++) {
      if (rvec[i]>rmax) rmax = rvec[i];
      if (rvec[i]<rmin) rmin = rvec[i];
      rsum += rvec[i];
    }
    ravg = rvec.size()?(1.0*rsum/rvec.size()):0;
    
    for (size_t i=0; i< rvec.size(); i++) {
      rsum2 += ((rvec[i]-ravg)*(rvec[i]-ravg));
    }
    rsigma = rvec.size()?( sqrt(rsum2/rvec.size()) ):0;
    
    // ---------------------------------------
    // compute for write
    // ---------------------------------------
    wmax=wsum=0;
    wmin=0xffffffff;
    wavg=wsum2=wsigma=0;
    
    for (size_t i=0; i< wvec.size(); i++) {
      if (wvec[i]>wmax) wmax = wvec[i];
      if (wvec[i]<wmin) wmin = wvec[i];
      wsum += wvec[i];
    }
    wavg = wvec.size()?(1.0*wsum/wvec.size()):0;
    
    for (size_t i=0; i< wvec.size(); i++) {
      wsum2 += ((wvec[i]-wavg)*(wvec[i]-wavg));
    }
    wsigma = wvec.size()?( sqrt(wsum2/wvec.size()) ):0;
    
    char report[16384];
    snprintf(report,sizeof(report)-1, "log=%s&path=%s&ruid=%u&rgid=%u&td=%s&host=%s&lid=%lu&fid=%llu&fsid=%lu&ots=%lu&otms=%lu&cts=%lu&ctms=%lu&rb=%llu&rb_min=%llu&rb_max=%llu&rb_sigma=%.02f&wb=%llu&wb_min=%llu&wb_max=%llu&&wb_sigma=%.02f&srb=%llu&swb=%llu&nrc=%lu&nwc=%lu&rt=%.02f&wt=%.02f&osize=%llu&csize=%llu&%s",this->logId,Path.c_str(),this->vid.uid,this->vid.gid, tIdent.c_str(), hostName.c_str(),lid, fileid, fsid, openTime.tv_sec, (unsigned long)openTime.tv_usec/1000,closeTime.tv_sec,(unsigned long)closeTime.tv_usec/1000,rsum,rmin,rmax,rsigma,wsum,wmin,wmax,wsigma,srBytes,swBytes,rCalls, wCalls,((rTime.tv_sec*1000.0)+(rTime.tv_usec/1000.0)), ((wTime.tv_sec*1000.0) + (wTime.tv_usec/1000.0)), (unsigned long long) openSize, (unsigned long long) closeSize, eos::common::SecEntity::ToEnv(SecString.c_str()).c_str());
    reportString = report;
  }
}


/*----------------------------------------------------------------------------*/
int          
XrdFstOfsFile::openofs(const char                *path,
		    XrdSfsFileOpenMode       open_mode,
		    mode_t                 create_mode,
		    const XrdSecEntity         *client,
                    const char                 *opaque,
                    bool                   openBlockXS, 
                    unsigned long              lid)
{
  bool isRead = true;

  if ( (open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
		     SFS_O_CREAT  | SFS_O_TRUNC) ) != 0) {
    isRead = false;
  }
  else{
    //anyway we need to open for writing for the recovery case
    open_mode |= SFS_O_RDWR;
  }
  
  if (openBlockXS && isRead ) {
    fstBlockXS = ChecksumPlugins::GetChecksumObject(lid, true);
    fstBlockSize = eos::common::LayoutId::GetBlocksize(lid);
    XrdOucString fstXSPath = fstBlockXS->MakeBlockXSPath(path);
    struct stat buf;
    if (!XrdOfsOss->Stat(path, &buf)) {
      if (!fstBlockXS->OpenMap(fstXSPath.c_str(), buf.st_size, fstBlockSize, false)) {
        eos_err("unable to create block checksum file");
        return gOFS.Emsg("XrdFstOfsFile",error, EIO,"open - cannot get block checksum file",fstXSPath.c_str());
      }
    }
  }
  return XrdOfsFile::open(path, open_mode, create_mode, client, opaque);
}


/*----------------------------------------------------------------------------*/
int          
XrdFstOfsFile::open(const char                *path,
                    XrdSfsFileOpenMode   open_mode,
                    mode_t               create_mode,
                    const XrdSecEntity        *client,
                    const char                *opaque)
{
  EPNAME("open");

  const char *tident = error.getErrUser();
  tIdent = error.getErrUser();

  char *val=0;
  isRW = false;
  int   retc = SFS_OK;
  Path = path;
  hostName = gOFS.HostName;

  gettimeofday(&openTime,&tz);

  XrdOucString stringOpaque = opaque;
  XrdOucString opaqueBlockCheckSum="";
  XrdOucString opaqueCheckSum = "";

  while(stringOpaque.replace("?","&")){}
  while(stringOpaque.replace("&&","&")){}
  stringOpaque += "&mgm.path="; stringOpaque += path;

  openOpaque  = new XrdOucEnv(stringOpaque.c_str());
  
  if ((val = openOpaque->Get("mgm.logid"))) {
    SetLogId(val, tident);
  }

  if ((val = openOpaque->Get("mgm.blockchecksum"))) {
    opaqueBlockCheckSum = val;
  } 

  if ((val = openOpaque->Get("mgm.checksum"))) {
    opaqueCheckSum = val;
  }

  int caprc = 0;

  if ((caprc=gCapabilityEngine.Extract(openOpaque, capOpaque))) {
    if (caprc == ENOKEY) {
      // if we just miss the key, better stall the client
      eos_warning("FST still misses the required capability key");
      return gOFS.Stall(error, 10, "FST still misses the required capability key");
    }
    // no capability - go away!
    return gOFS.Emsg(epname,error,caprc,"open - capability illegal",path);
  }

  int envlen;
  //ZTRACE(open,"capability contains: " << capOpaque->Env(envlen));
  
  XrdOucString maskOpaque = opaque?opaque:"";
  // mask some opaque parameters to shorten the logging
  eos::common::StringConversion::MaskTag(maskOpaque,"cap.sym");
  eos::common::StringConversion::MaskTag(maskOpaque,"cap.msg");
  eos::common::StringConversion::MaskTag(maskOpaque,"authz");
  eos_info("path=%s info=%s capability=%s", path, maskOpaque.c_str(), capOpaque->Env(envlen));

  const char* hexfid=0;
  const char* sfsid=0;
  const char* slid=0;
  const char* scid=0;
  const char* smanager=0;
  const char* sbookingsize=0;
  const char* stargetsize=0;
  const char* secinfo=0;

  bookingsize = 0;
  targetsize = 0;

  fileid=0;
  fsid=0;
  lid=0;
  cid=0;

  if (!(hexfid=capOpaque->Get("mgm.fid"))) {
    return gOFS.Emsg(epname,error,EINVAL,"open - no file id in capability",path);
  }

  if (!(sfsid=capOpaque->Get("mgm.fsid"))) {
    return gOFS.Emsg(epname,error, EINVAL,"open - no file system id in capability",path);
  }

  if (!(secinfo=capOpaque->Get("mgm.sec"))) {
    return gOFS.Emsg(epname,error, EINVAL,"open - no security information in capability",path);
  } else {
    SecString = secinfo;
  }

  if ((val = capOpaque->Get("mgm.minsize"))) {
    errno=0;
    minsize = strtoull(val,0,10);
    if (errno) {
      eos_err("illegal minimum file size specified <%s>- restricting to 1 byte", val);
      minsize=1;
    }
  } else {
    minsize=0;
  }

  if ((val = capOpaque->Get("mgm.maxsize"))) {
    errno=0;
    maxsize = strtoull(val,0,10);
    if (errno) {
      eos_err("illegal maximum file size specified <%s>- restricting to 1 byte", val);
      maxsize=1;
    }
  } else {
    maxsize=0;
  }



  // if we open a replica we have to take the right filesystem id and filesystem prefix for that replica
  if (openOpaque->Get("mgm.replicaindex")) {
    XrdOucString replicafsidtag="mgm.fsid"; replicafsidtag += (int) atoi(openOpaque->Get("mgm.replicaindex"));
    if (capOpaque->Get(replicafsidtag.c_str())) 
      sfsid=capOpaque->Get(replicafsidtag.c_str());
  }    
   
  // extract the local path prefix from the broadcasted configuration!
  eos::common::RWMutexReadLock lock(gOFS.Storage->fsMutex);
  fsid = atoi(sfsid?sfsid:"0");
  if ( fsid && gOFS.Storage->fileSystemsMap.count(fsid) ) {
    localPrefix = gOFS.Storage->fileSystemsMap[fsid]->GetPath().c_str();
  }
  // attention: the localprefix implementation does not work for gateway machines - this needs some modifications


  if (!localPrefix.length()) {
    return gOFS.Emsg(epname,error, EINVAL,"open - cannot determine the prefix path to use for the given filesystem id",path);
  }
	
  if (!(slid=capOpaque->Get("mgm.lid"))) {
    return gOFS.Emsg(epname,error, EINVAL,"open - no layout id in capability",path);
  }

  if (!(scid=capOpaque->Get("mgm.cid"))) {
    return gOFS.Emsg(epname,error, EINVAL,"open - no container id in capability",path);
  }

  if (!(smanager=capOpaque->Get("mgm.manager"))) {
    return gOFS.Emsg(epname,error, EINVAL,"open - no manager name in capability",path);
  }

  RedirectManager = smanager;
  int dpos = RedirectManager.find(":");
  if (dpos != STR_NPOS) 
    RedirectManager.erase(dpos);

  eos::common::FileId::FidPrefix2FullPath(hexfid, localPrefix.c_str(),fstPath);

  fileid = eos::common::FileId::Hex2Fid(hexfid);

  fsid   = atoi(sfsid);
  lid = atoi(slid);
  cid = strtoull(scid,0,10);

  // extract blocksize from the layout
  fstBlockSize = eos::common::LayoutId::GetBlocksize(lid);

  eos_info("blocksize=%llu lid=%x", fstBlockSize, lid);
  // check if this is an open for replication
  
  if (Path.beginswith("/replicate:")) {
    bool isopenforwrite=false;
    gOFS.OpenFidMutex.Lock();
    if (gOFS.WOpenFid[fsid].count(fileid)) {
      if (gOFS.WOpenFid[fsid][fileid]>0) {
        isopenforwrite=true;
      }
    }
    gOFS.OpenFidMutex.UnLock();
    if (isopenforwrite) {
      eos_err("forbid to open replica - file %s is opened in RW mode", Path.c_str());
      return gOFS.Emsg(epname,error, ENOENT,"open - cannot replicate: file is opened in RW mode",path);
    }
    isReplication=true;
  }
  

  open_mode |= SFS_O_MKPTH;
  create_mode|= SFS_O_MKPTH;

  if ( (open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
                     SFS_O_CREAT  | SFS_O_TRUNC) ) != 0) 
    isRW = true;


  struct stat statinfo;

  if ((retc = XrdOfsOss->Stat(fstPath.c_str(), &statinfo))) {
    // file does not exist, keep the create lfag
    isCreation = true;
    openSize = 0;
    statinfo.st_mtime=0; // this we use to indicate if a file was written in the meanwhile by someone else
  } else {
    if (open_mode & SFS_O_CREAT) 
      open_mode -= SFS_O_CREAT;
  }


  // bookingsize is only needed for file creation
  if (isRW && isCreation) {
    if (!(sbookingsize=capOpaque->Get("mgm.bookingsize"))) {
      return gOFS.Emsg(epname,error, EINVAL,"open - no booking size in capability",path);
    } else {
      errno = 0;
      bookingsize = strtoull(capOpaque->Get("mgm.bookingsize"),0,10); 
      if (errno == ERANGE) {
	eos_err("invalid bookingsize in capability bookingsize=%s", sbookingsize);
	return gOFS.Emsg(epname, error, EINVAL, "open - invalid bookingsize in capability", path);
      }
    }
    if ((stargetsize=capOpaque->Get("mgm.targetsize"))) {
      targetsize = strtoull(capOpaque->Get("mgm.targetsize"),0,10);
    }
  }


  // ------------------------------------------------------------------------
  // Code dealing with block checksums
  // ------------------------------------------------------------------------

  eos_info("blocksize=%llu layoutid=%x oxs=<%s>", fstBlockSize,lid, opaqueBlockCheckSum.c_str());
  // create a block checksum object if blocksize is defined and the feature is not explicitly disabled by the client
  if ((opaqueBlockCheckSum != "ignore"))
    fstBlockXS = ChecksumPlugins::GetChecksumObject(lid, true);
  else
    fstBlockXS = 0;

  if (fstBlockXS) {
    eos_info("info=\"created/got blocklevel checksum\"");
    XrdOucString fstXSPath = fstBlockXS->MakeBlockXSPath(fstPath.c_str());

    if (!fstBlockXS->OpenMap(fstXSPath.c_str(), isCreation?bookingsize:statinfo.st_size,fstBlockSize, isRW)) {
      eos_err("unable to create block checksum file");

      if (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReplica) {
        // there was a blockchecksum open error
        if (!isRW) {
          int ecode=1094;
          eos_warning("rebouncing client since we failed to open the block checksum file back to MGM %s:%d",RedirectManager.c_str(), ecode);
          return gOFS.Redirect(error, RedirectManager.c_str(), ecode);
        }
      } else {
        return gOFS.Emsg(epname,error, EIO,"open - cannot create/get block checksum file",fstXSPath.c_str());
      }
    }
  } 
  
  // get the identity

  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Nobody(vid);
  

  if ((val = capOpaque->Get("mgm.ruid"))) {
    vid.uid = atoi(val);
  } else {
    return gOFS.Emsg(epname,error,EINVAL,"open - sec ruid missing",path);
  }

  if ((val = capOpaque->Get("mgm.rgid"))) {
    vid.gid = atoi(val);
  } else {
    return gOFS.Emsg(epname,error,EINVAL,"open - sec rgid missing",path);
  }

  if ((val = capOpaque->Get("mgm.uid"))) {
    vid.uid_list.clear();
    vid.uid_list.push_back(atoi(val));
  } else {
    return gOFS.Emsg(epname,error,EINVAL,"open - sec uid missing",path);
  }

  if ((val = capOpaque->Get("mgm.gid"))) {
    vid.gid_list.clear();
    vid.gid_list.push_back(atoi(val));
  } else {
    return gOFS.Emsg(epname,error,EINVAL,"open - sec gid missing",path);
  }

  if ((val = capOpaque->Get("mgm.logid"))) {
    snprintf(logId,sizeof(logId)-1,"%s", val);
  }

  SetLogId(logId, vid, tident);

  eos_info("fstpath=%s", fstPath.c_str());

  // attach meta data
  fMd = gFmdSqliteHandler.GetFmd(fileid, fsid, vid.uid, vid.gid, lid, isRW);
  if (!fMd) {
    eos_crit("no fmd for fileid %llu on filesystem %lu", fileid, (unsigned long long)fsid);
    int ecode=1094;
    eos_warning("rebouncing client since we failed to get the FMD record back to MGM %s:%d",RedirectManager.c_str(), ecode);
    return gOFS.Redirect(error, RedirectManager.c_str(), ecode);
  }

  // call the checksum factory function with the selected layout
  
  layOut = eos::fst::LayoutPlugins::GetLayoutObject(this, lid, &error);

  if( !layOut) {
    int envlen;
    eos_err("unable to handle layout for %s", capOpaque->Env(envlen));
    delete fMd;
    return gOFS.Emsg(epname,error,EINVAL,"open - illegal layout specified ",capOpaque->Env(envlen));
  }

  layOut->SetLogId(logId, vid, tident);

  if (isRW || ( ( opaqueCheckSum != "ignore") && 
		((eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReplica) || 
		 (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kPlain)))) {
    
    if ( ( (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kRaidDP) || 
	   (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReedS  ) ) && 
	 (!layOut->IsEntryServer())) {
      // this case we need to exclude!
    } else {
      // we always do checksums for writes and reads for kPlain & kReplica if it was not explicitly switched off 
      checkSum = eos::fst::ChecksumPlugins::GetChecksumObject(lid);
      eos_debug("checksum requested %d %u", checkSum, lid);
    }
  }

  if (!isCreation) {
    // get the real size of the file, not the local stripe size!
    if ((retc = layOut->stat(&statinfo))) {
      return gOFS.Emsg(epname,error, EIO, "open - cannot stat layout to determine file size",Path.c_str());
    }
    // we feed the layout size, not the physical on disk!
    openSize = statinfo.st_size;

    if (checkSum && isRW) {
      // preset with the last known checksum
      checkSum->ResetInit(0, openSize, fMd->fMd.checksum.c_str());
    }
  }


  int rc = layOut->open(fstPath.c_str(), open_mode, create_mode, client, stringOpaque.c_str());

  if ( (!rc) && isCreation && bookingsize) {
    // ----------------------------------
    // check if the file system is full
    // ----------------------------------
    XrdSysMutexHelper(gOFS.Storage->fileSystemFullMapMutex);
    if (gOFS.Storage->fileSystemFullMap[fsid]) {
      writeErrorFlag=kOfsDiskFullError;
      
      return gOFS.Emsg("writeofs", error, ENOSPC, "create file - disk space (headroom) exceeded fn=", capOpaque?(capOpaque->Get("mgm.path")?capOpaque->Get("mgm.path"):FName()):FName());
      }
    rc = layOut->fallocate(bookingsize);
    if (rc) {
      eos_crit("file allocation gave return code %d errno=%d for allocation of size=%llu" , rc, errno, bookingsize);
      if (layOut->IsEntryServer()) {
	layOut->remove();
	int ecode=1094;
	eos_warning("rebouncing client since we don't have enough space back to MGM %s:%d",RedirectManager.c_str(), ecode);
	return gOFS.Redirect(error,RedirectManager.c_str(),ecode);
      } else {
	return gOFS.Emsg(epname, error, ENOSPC, "open - cannot allocate required space", Path.c_str());
      }
    }
  }

  std::string filecxerror = "0";

  if (!rc) {
    // set the eos lfn as extended attribute
    eos::common::Attr* attr = eos::common::Attr::OpenAttr(layOut->GetLocalReplicaPath());
    if (attr && isRW) {
      if (Path.beginswith("/replicate:")) {
        if (capOpaque->Get("mgm.path")) {
          if (!attr->Set(std::string("user.eos.lfn"), std::string(capOpaque->Get("mgm.path")))) {
            eos_err("unable to set extended attribute <eos.lfn> errno=%d", errno);
          }
        } else {
          eos_err("no lfn in replication capability");
        }
      } else {
        if (!attr->Set(std::string("user.eos.lfn"), std::string(path))) {
          eos_err("unable to set extended attribute <eos.lfn> errno=%d", errno);
        }
      }
    }
    
    // try to get if the file has a scan error
    
    if (attr) {
      filecxerror = attr->Get("user.filecxerror");
      delete attr;
    }    
  }

  if ((!isRW) && (filecxerror == "1")) {
    // if we have a replica layout
    if (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReplica) {
      // there was a checksum error during the last scan
      if (layOut->IsEntryServer()) {
        int ecode=1094;
        eos_warning("rebouncing client since our replica has a wrong checksum back to MGM %s:%d",RedirectManager.c_str(), ecode);
        return gOFS.Redirect(error,RedirectManager.c_str(),ecode);
      }
    }
  }

  if (!rc) {
    opened = true;
    gOFS.OpenFidMutex.Lock();

    if (isRW) {
      gOFS.WOpenFid[fsid][fileid]++;
    }
    else
      gOFS.ROpenFid[fsid][fileid]++;

    gOFS.OpenFidMutex.UnLock();
  } else {
    // if we have local errors in open we might disable ourselfs
    if ( error.getErrInfo() != EREMOTEIO ) {
      eos::common::RWMutexReadLock(gOFS.Storage->fsMutex);
      std::vector <eos::fst::FileSystem*>::const_iterator it;
      for (unsigned int i=0; i< gOFS.Storage->fileSystemsVector.size(); i++) {
        // check if the local prefix matches a filesystem path ...
        if ( (errno != ENOENT) && (fstPath.beginswith(gOFS.Storage->fileSystemsVector[i]->GetPath().c_str()))) {
          // broadcast error for this FS
          eos_crit("disabling filesystem %u after IO error on path %s", gOFS.Storage->fileSystemsVector[i]->GetId(), gOFS.Storage->fileSystemsVector[i]->GetPath().c_str());
          XrdOucString s="local IO error";
          gOFS.Storage->fileSystemsVector[i]->BroadcastError(EIO, s.c_str());
          //      gOFS.Storage->fileSystemsVector[i]->BroadcastError(error.getErrInfo(), "local IO error");
          break;
        }
      }
    }

    // in any case we just redirect back to the manager if we are the 1st entry point of the client

    if (layOut->IsEntryServer()) {
      int ecode=1094;
      rc = SFS_REDIRECT;
      eos_warning("rebouncing client after open error back to MGM %s:%d",RedirectManager.c_str(), ecode);
      return gOFS.Redirect(error, RedirectManager.c_str(),ecode);
    }
  }

  if (rc == SFS_OK) {
    // tag this transaction as open
    if (isRW) {
      if (!gOFS.Storage->OpenTransaction(fsid, fileid)) {
        eos_crit("cannot open transaction for fsid=%u fidC=%llu", fsid, fileid);
      }
    }
  }

  eos_debug("OPEN FINISHED!");
  return rc;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsFile::closeofs()
{
  int rc = 0;

  // ------------------------------------------------------------------------
  // Code dealing with block checksums
  // ------------------------------------------------------------------------
  if (fstBlockXS) {
    struct stat statinfo;
    if ((XrdOfsOss->Stat(fstPath.c_str(), &statinfo))) {
      eos_warning("close - cannot stat closed file %s- probably already unlinked!", Path.c_str());
      return XrdOfsFile::close();
    }

    if (!rc) {
      // check if there is more than one writer in this moment or a reader , if yes, we don't recompute wholes in the checksum and we don't truncate the checksum map, the last single writer will do that
      // ---->
      gOFS.OpenFidMutex.Lock();
      eos_info("%s wopen=%d ropen=%d fsid=%ld fid=%lld",fstPath.c_str(), (gOFS.WOpenFid[fsid].count(fileid))?gOFS.WOpenFid[fsid][fileid]:0 , (gOFS.ROpenFid[fsid].count(fileid))?gOFS.ROpenFid[fsid][fileid]:0, fsid, fileid);
      if ( (gOFS.WOpenFid[fsid].count(fileid) && (gOFS.WOpenFid[fsid][fileid]==1) && ((!gOFS.ROpenFid[fsid].count(fileid)) || (gOFS.ROpenFid[fsid][fileid]==0)))) {
        XrdOucErrInfo error;
        if(!fctl(SFS_FCTL_GETFD,0,error)) {
          int fd = error.getErrInfo();
          if (!fstBlockXS->AddBlockSumHoles(fd)) {
            eos_err("unable to fill holes of block checksum map");
          }
        }       
      } else {
        eos_info("info=\"block-xs skipping hole check and changemap\" nwriter=%d nreader=%d", (gOFS.WOpenFid[fsid].count(fileid))?gOFS.WOpenFid[fsid][fileid]:0 , (gOFS.ROpenFid[fsid].count(fileid))?gOFS.ROpenFid[fsid][fileid]:0);
      }
      gOFS.OpenFidMutex.UnLock();
      // -----|

      eos_info("block-xs wblocks=%llu rblocks=%llu holes=%llu", fstBlockXS->GetXSBlocksWritten(), fstBlockXS->GetXSBlocksChecked(), fstBlockXS->GetXSBlocksWrittenHoles());
      if (!fstBlockXS->CloseMap()) {
        eos_err("unable to close block checksum map");
        rc = SFS_ERROR;
      } 
    }
  }
    
  rc |= XrdOfsFile::close();

  return rc;
}

/*----------------------------------------------------------------------------*/
bool
XrdFstOfsFile::verifychecksum()
{
  bool checksumerror = false;
  int checksumlen = 0;

  // deal with checksums
  if (checkSum) {
    checkSum->Finalize();
    if (checkSum->NeedsRecalculation()) {
      if ( (!isRW) && ( (srBytes)  || (checkSum->GetMaxOffset() != openSize) ) ) {
	// we don't rescan files if they are read non-sequential or only partially
	eos_debug("info=\"skipping checksum (re-scan) for non-sequential reading ...\"");
	// remove the checksum object
	delete checkSum;
	checkSum=0;
	return false;
      }
    } else {
      if ( ((!isRW) && (checkSum->GetMaxOffset() != openSize))) {
	eos_debug("info=\"skipping checksum (re-scan) for access without any IO or partial sequential read IO from the beginning...\"");
	delete checkSum;
	checkSum=0;
	return false;
      }
    }

    if (checkSum->NeedsRecalculation()) {
      unsigned long long scansize=0;
      float scantime = 0; // is ms

      // WARNING: currently we don't run into this code anymore - it failed with 0.2.11 and xrootd 3.2.2 - to be investigaed more
      if(!fctl(SFS_FCTL_GETFD,0,error)) {
	int fd = error.getErrInfo();
	if (checkSum->ScanFile(fd, scansize, scantime)) {
	  XrdOucString sizestring;
	  eos_info("info=\"rescanned checksum\" size=%s time=%.02f ms rate=%.02f MB/s %x", eos::common::StringConversion::GetReadableSizeString(sizestring, scansize, "B"), scantime, 1.0*scansize/1000/(scantime?scantime:99999999999999LL), checkSum->GetHexChecksum());
	} else {
	  eos_err("Rescanning of checksum failed");
	}
      } else {
	eos_err("Couldn't get file descriptor");
      }
    }

    if (isRW) {
      eos_info("(write) checksum type: %s checksum hex: %s requested-checksum hex: %s", checkSum->GetName(), checkSum->GetHexChecksum(), openOpaque->Get("mgm.checksum")?openOpaque->Get("mgm.checksum"):"-none-");

      // check if the check sum for the file was given at upload time
      if (openOpaque->Get("mgm.checksum")) {
	XrdOucString opaqueChecksum = openOpaque->Get("mgm.checksum");
	XrdOucString hexChecksum = checkSum->GetHexChecksum();
	if (opaqueChecksum != hexChecksum) {
	  eos_err("requested checksum %s does not match checksum %s of uploaded file");
	  delete checkSum;
	  checkSum=0;
	  return true;
	}
      }
      
      checkSum->GetBinChecksum(checksumlen);
      // copy checksum into meta data
      fMd->fMd.checksum = checkSum->GetHexChecksum();
      
      if (haswrite || isReplication) {
        // if we have no write, we don't set this attributes (xrd3cp!)  
        // set the eos checksum extended attributes
        eos::common::Attr* attr = eos::common::Attr::OpenAttr(fstPath.c_str());
        if (attr) {
          if (!attr->Set(std::string("user.eos.checksumtype"), std::string(checkSum->GetName()))) {
            eos_err("unable to set extended attribute <eos.checksumtype> errno=%d", errno);
          }
          if (!attr->Set("user.eos.checksum",checkSum->GetBinChecksum(checksumlen), checksumlen)) {
            eos_err("unable to set extended attribute <eos.checksum> errno=%d", errno);
          }
          // reset any tagged error
          if (!attr->Set("user.eos.filecxerror","0")) {
            eos_err("unable to set extended attribute <eos.filecxerror> errno=%d", errno);
          }
          if (!attr->Set("user.eos.blockcxerror","0")) {
            eos_err("unable to set extended attribute <eos.blockcxerror> errno=%d", errno);
          }
          delete attr;
        }
      }
    } else {
      // this is a read with checksum check, compare with fMD
      eos_info("(read)  checksum type: %s checksum hex: %s fmd-checksum: %s", checkSum->GetName(), checkSum->GetHexChecksum(), fMd->fMd.checksum.c_str());
      std::string calculatedchecksum = checkSum->GetHexChecksum();
      if (calculatedchecksum != fMd->fMd.checksum.c_str()) {
	checksumerror = true;
      }
    }
  }
  return checksumerror;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsFile::close()
{
  EPNAME("close");
  int rc = 0;  // return code
  int brc = 0; // return code before 'close' is called
  bool checksumerror=false;
  bool targetsizeerror=false;
  bool committed=false;
  bool minimumsizeerror=false;

  // -------------------------------------------------------------------------------------------------------
  // we enter the close logic only once since there can be an explicit close or a close via the destructor
  // -------------------------------------------------------------------------------------------------------
  if ( opened && (!closed) && fMd) {
    eos_info("");
    // -------------------------------------------------------------------------------------------------------  
    // check if the file close comes from a client disconnect e.g. the destructor
    // -------------------------------------------------------------------------------------------------------

    XrdOucString hexstring="";
    eos::common::FileId::Fid2Hex(fMd->fMd.fid,hexstring);
    XrdOucErrInfo error;
    
    XrdOucString capOpaqueString="/?mgm.pcmd=drop";
    XrdOucString OpaqueString = "";
    OpaqueString+="&mgm.fsid="; OpaqueString += (int)fMd->fMd.fsid;
    OpaqueString+="&mgm.fid=";  OpaqueString += hexstring;
    XrdOucEnv Opaque(OpaqueString.c_str());
    capOpaqueString += OpaqueString;
      
    if ( (viaDelete||writeDelete||remoteDelete) && isCreation) {
      // -------------------------------------------------------------------------------------------------------
      // it is closed by the constructor e.g. no proper close 
      // -------------------------------------------------------------------------------------------------------
      if (viaDelete) {
	eos_info("msg=\"(unpersist): deleting file\" reason=\"client disconnect\"  fsid=%u fxid=%08x on fsid=%u ", fMd->fMd.fsid, fMd->fMd.fid);
      }
      if (writeDelete) {
	eos_info("msg=\"(unpersist): deleting file\" reason=\"write/policy error\" fsid=%u fxid=%08x on fsid=%u ", fMd->fMd.fsid, fMd->fMd.fid);
      }
      if (remoteDelete) {
	eos_info("msg=\"(unpersist): deleting file\" reason=\"remote deletion\"    fsid=%u fxid=%08x on fsid=%u ", fMd->fMd.fsid, fMd->fMd.fid);
      }

      // delete the file

      // -------------------------------------------------------------------------------------------------------
      // set the file to be deleted
      // -------------------------------------------------------------------------------------------------------
      deleteOnClose = true;
      layOut->remove();
      
      if (fstBlockXS) {
	fstBlockXS->CloseMap();
	// delete also the block checksum file
	fstBlockXS->UnlinkXSPath();
	delete fstBlockXS;
	fstBlockXS=0;
      }

      // -------------------------------------------------------------------------------------------------------
      // delete the replica in the MGM
      // -------------------------------------------------------------------------------------------------------
      int rc = gOFS.CallManager(&error, capOpaque->Get("mgm.path"),capOpaque->Get("mgm.manager"), capOpaqueString);
      if (rc) {
	eos_warning("(unpersist): unable to drop file id %s fsid %u at manager %s",hexstring.c_str(), fMd->fMd.fid, capOpaque->Get("mgm.manager"));
      }
    } else {
      // -------------------------------------------------------------------------------------------------------
      // check if this was a newly created file
      // -------------------------------------------------------------------------------------------------------
      if (isCreation) {
	// -------------------------------------------------------------------------------------------------------
	// if we had space allocation we have to truncate the allocated space to the real size of the file
	// -------------------------------------------------------------------------------------------------------
	if ((strcmp(layOut->GetName(), "raidDP") == 0) || (strcmp(layOut->GetName(), "reedS") == 0)){
	  if (layOut->IsEntryServer())
	    layOut->truncate(maxOffsetWritten);
	} else {
	  if ( (long long)maxOffsetWritten>(long long)openSize) {
	    // -------------------------------------------------------------------------------------------------------
	    // check if we have to deallocate something for this file transaction
	    // -------------------------------------------------------------------------------------------------------
	    if ((bookingsize) && (bookingsize > (long long) maxOffsetWritten)) {
	      eos_info("deallocationg %llu bytes", bookingsize - maxOffsetWritten);
	      layOut->truncate(maxOffsetWritten);
	      // we have evt. to deallocate blocks which have not been written
	      layOut->fdeallocate(maxOffsetWritten,bookingsize);
	    }
	  }
	}
      }
      
      eos_info("calling verifychecksum");
      // -------------------------------------------------------------------------------------------------------
      // call checksum verification
      checksumerror = verifychecksum();
      targetsizeerror = (targetsize)?(targetsize!=(off_t)maxOffsetWritten):false;
      if (isCreation) {
	// check that the minimum file size policy is met!
	minimumsizeerror = (minsize)?( (off_t)maxOffsetWritten < minsize):false;
	
	if (minimumsizeerror) {
	  eos_warning("written file %s is smaller than required minimum file size=%llu written=%llu", Path.c_str(), minsize, maxOffsetWritten);
	}
      }
      // ---- add error simulation for checksum errors on read
      if ((!isRW) && gOFS.Simulate_XS_read_error) {
	checksumerror = true;
	eos_warning("simlating checksum errors on read");
      }
      
      // ---- add error simulation for checksum errors on write
      if (isRW && gOFS.Simulate_XS_write_error) {
	checksumerror = true;
	eos_warning("simlating checksum errors on write");
      }

      if (isCreation && (checksumerror||targetsizeerror||minimumsizeerror)) {
	// -------------------------------------------------------------------------------------------------------
	// we have a checksum error if the checksum was preset and does not match!
	// we have a target size error, if the target size was preset and does not match!
	// -------------------------------------------------------------------------------------------------------
	// set the file to be deleted
	// -------------------------------------------------------------------------------------------------------
	deleteOnClose = true;
	layOut->remove();
	
	if (fstBlockXS) {
	  fstBlockXS->CloseMap();
	  // delete also the block checksum file
	  fstBlockXS->UnlinkXSPath();
	  delete fstBlockXS;
	  fstBlockXS=0;
	}

	// -------------------------------------------------------------------------------------------------------
	// delete the replica in the MGM
	// -------------------------------------------------------------------------------------------------------
	int rc = gOFS.CallManager(&error, capOpaque->Get("mgm.path"),capOpaque->Get("mgm.manager"), capOpaqueString);
	if (rc) {
	  eos_warning("(unpersist): unable to drop file id %s fsid %u at manager %s",hexstring.c_str(), fMd->fMd.fid, capOpaque->Get("mgm.manager"));
	} 
      }
      
      // store the entry server information before closing the layout
      bool isEntryServer=false;
      if (layOut->IsEntryServer()) {
	isEntryServer=true;
      }
      
      // first we assume that, if we have writes, we update it
      closeSize = openSize;
      
      if ((!checksumerror) && (haswrite || isCreation) && (!minimumsizeerror)) {
	// commit meta data
	struct stat statinfo;
	if ((rc = layOut->stat(&statinfo))) {
	  rc = gOFS.Emsg(epname,error, EIO, "close - cannot stat closed layout to determine file size",Path.c_str());
	}
	if (!rc) {
	  if ( (statinfo.st_size==0) || haswrite) {
	    // update size
	    closeSize = statinfo.st_size;
	    fMd->fMd.size     = statinfo.st_size;
	    fMd->fMd.disksize = statinfo.st_size;
	    fMd->fMd.mgmsize  = 0xfffffffffff1ULL;    // now again undefined
	    fMd->fMd.mgmchecksum = "";            // now again empty
	    fMd->fMd.diskchecksum = fMd->fMd.checksum; // take the fresh checksum 
	    fMd->fMd.layouterror = 0;             // reset layout errors
	    fMd->fMd.locations   = "";            // reset locations
	    fMd->fMd.filecxerror = 0;
	    fMd->fMd.blockcxerror= 0;

	    fMd->fMd.mtime    = statinfo.st_mtime;
#ifdef __APPLE__
	    fMd->fMd.mtime_ns = 0;
#else
	    fMd->fMd.mtime_ns = statinfo.st_mtim.tv_nsec;
#endif
	    
	    // set the container id
	    fMd->fMd.cid = cid;
	    
	    // for replicat's set the original uid/gid/lid values
	    if (capOpaque->Get("mgm.source.lid")) {
	      fMd->fMd.lid = strtoul(capOpaque->Get("mgm.source.lid"),0,10);
	    }
	    
	    if (capOpaque->Get("mgm.source.ruid")) {
	      fMd->fMd.uid = atoi(capOpaque->Get("mgm.source.ruid"));
	    }
	    
	    if (capOpaque->Get("mgm.source.rgid")) {
	      fMd->fMd.uid = atoi(capOpaque->Get("mgm.source.rgid"));
	    }
	    
	    // commit local
	    if (!gFmdSqliteHandler.Commit(fMd))
	      rc = gOFS.Emsg(epname,error,EIO,"close - unable to commit meta data",Path.c_str());
	    else {
	      committed=true;
	    }
	    // commit to central mgm cache
	    int envlen=0;
	    XrdOucString capOpaqueFile="";
	    XrdOucString mTimeString="";
	    capOpaqueFile += "/?";
	    capOpaqueFile += capOpaque->Env(envlen);
	    capOpaqueFile += "&mgm.pcmd=commit";
	    capOpaqueFile += "&mgm.size=";
	    char filesize[1024]; sprintf(filesize,"%llu", fMd->fMd.size);
	    capOpaqueFile += filesize;
	    if (checkSum) {
	      capOpaqueFile += "&mgm.checksum=";
	      capOpaqueFile += checkSum->GetHexChecksum();
	    }
	    capOpaqueFile += "&mgm.mtime=";
	    capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long)fMd->fMd.mtime);
	    capOpaqueFile += "&mgm.mtime_ns=";
	    capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long)fMd->fMd.mtime_ns);
	    
	    capOpaqueFile += "&mgm.add.fsid=";
	    capOpaqueFile += (int)fMd->fMd.fsid;
	    
	    // if <drainfsid> is set, we can issue a drop replica 
	    if (capOpaque->Get("mgm.drainfsid")) {
	      capOpaqueFile += "&mgm.drop.fsid=";
	      capOpaqueFile += capOpaque->Get("mgm.drainfsid");
	    }
	    
	    if (isEntryServer && !isReplication) {
	      // the entry server commits size and checksum
	      capOpaqueFile += "&mgm.commit.size=1&mgm.commit.checksum=1";
	    } else {
	      capOpaqueFile += "&mgm.replication=1";
	    }
	    
	    // the log ID to the commit
	    capOpaqueFile += "&mgm.logid="; capOpaqueFile += logId;
	    
	    rc = gOFS.CallManager(&error, capOpaque->Get("mgm.path"),capOpaque->Get("mgm.manager"), capOpaqueFile);


	    if (  (rc == -EIO) || (rc == -EIDRM) || (rc == -EBADE) || (rc == -EBADR) ) {
	      if (rc == -EIO) {
		eos_crit("commit returned an error msg=%s", error.getErrText());
		if (isCreation) {
		  // new files we should clean-up
		  deleteOnClose=true;
		}
	      }
	      
	      if ( (rc == -EIDRM) || (rc == -EBADE) || (rc == -EBADR) ) {
		if (!gOFS.Storage->CloseTransaction(fsid, fileid)) {
		  eos_crit("cannot close transaction for fsid=%u fid=%llu", fsid, fileid);
		}
		if (rc == -EIDRM) {
		  // this file has been deleted in the meanwhile ... we can unlink that immedeatly
		  eos_info("info=\"unlinking fid=%08x path=%s - file has been already unlinked from the namespace\"", fMd->fMd.fid, Path.c_str());
		}
		if (rc == -EBADE) {
		  eos_err("info=\"unlinking fid=%08x path=%s - file size of replica does not match reference\"", fMd->fMd.fid, Path.c_str());
		}
		if (rc == -EBADR) {
		  eos_err("info=\"unlinking fid=%08x path=%s - checksum of replica does not match reference\"", fMd->fMd.fid, Path.c_str());
		}
		
		int retc =  gOFS._rem(Path.c_str(), error, 0, capOpaque, fstPath.c_str(), fileid,fsid);
		if (!retc) {
		  eos_debug("<rem> returned retc=%d", retc);
		}
		deleteOnClose=true; 
	      } else {
		eos_crit("commit returned an not catched error msg=%s", error.getErrText());
	      }
	    }
	  }    
	} 
      }
    }
    
    if (isRW) {
      if (rc==SFS_OK) {
	gOFS.Storage->CloseTransaction(fsid, fileid);
      }
    }

    int closerc=0; // return of the close
    brc = rc;  // return before the close
    
    if (layOut) {
      closerc = layOut->close();
      rc |= closerc;
    } else {
      rc |= closeofs();
    }
    
    closed = true;
    
    if (closerc) {
      // some (remote) replica didn't make it through ... trigger an auto-repair
      if (!deleteOnClose) {
	repairOnClose = true;
      }
    }
    
    gOFS.OpenFidMutex.Lock();
    if (isRW) 
      gOFS.WOpenFid[fMd->fMd.fsid][fMd->fMd.fid]--;
    else
      gOFS.ROpenFid[fMd->fMd.fsid][fMd->fMd.fid]--;
    
    if (gOFS.WOpenFid[fMd->fMd.fsid][fMd->fMd.fid] <= 0) {
      // if this was a write of the last writer we had the lock and we release it
      gOFS.WOpenFid[fMd->fMd.fsid].erase(fMd->fMd.fid);
      gOFS.WOpenFid[fMd->fMd.fsid].resize(0);
    }
    
    if (gOFS.ROpenFid[fMd->fMd.fsid][fMd->fMd.fid] <= 0) {
      gOFS.ROpenFid[fMd->fMd.fsid].erase(fMd->fMd.fid);
      gOFS.ROpenFid[fMd->fMd.fsid].resize(0);
    }
    gOFS.OpenFidMutex.UnLock();
    
    gettimeofday(&closeTime,&tz);
    
    if (!deleteOnClose) {
      // prepare a report and add to the report queue
      XrdOucString reportString="";
      MakeReportEnv(reportString);
      gOFS.ReportQueueMutex.Lock();
      gOFS.ReportQueue.push(reportString);
      gOFS.ReportQueueMutex.UnLock();

      if (isRW) {
	// store in the WrittenFilesQueue
	gOFS.WrittenFilesQueueMutex.Lock();
	gOFS.WrittenFilesQueue.push(fMd->fMd);
	gOFS.WrittenFilesQueueMutex.UnLock();
      }
    } 
  }
  
  if (deleteOnClose && isCreation) {
    int retc =  gOFS._rem(Path.c_str(), error, 0, capOpaque, fstPath.c_str(), fileid,fsid, true);
    if (retc) {
      eos_debug("<rem> returned retc=%d", retc);
    }

    if (committed) {
      // if we commit the replica and an error happened remote, we have to unlink it again
      XrdOucString hexstring="";
      eos::common::FileId::Fid2Hex(fileid,hexstring);
      XrdOucErrInfo error;
      
      XrdOucString capOpaqueString="/?mgm.pcmd=drop";
      XrdOucString OpaqueString = "";
      OpaqueString+="&mgm.fsid="; OpaqueString += (int)fsid;
      OpaqueString+="&mgm.fid=";  OpaqueString += hexstring;
      XrdOucEnv Opaque(OpaqueString.c_str());
      capOpaqueString += OpaqueString;
      
      // -------------------------------------------------------------------------------------------------------
      // delete the replica in the MGM
      // -------------------------------------------------------------------------------------------------------
      int rc = gOFS.CallManager(&error, capOpaque->Get("mgm.path"),capOpaque->Get("mgm.manager"), capOpaqueString);
      if (rc) {
	if (rc != -EIDRM) {
	  eos_warning("(unpersist): unable to drop file id %s fsid %u at manager %s",hexstring.c_str(), fileid, capOpaque->Get("mgm.manager"));
	}
      }
      eos_info("info=\"removing on manager\" manager=%s fid=%llu fsid=%d fn=%s fstpath=%s rc=%d", capOpaque->Get("mgm.manager"), (unsigned long long)fileid, (int)fsid, capOpaque->Get("mgm.path"), fstPath.c_str(),rc);
    }

    rc = SFS_ERROR;

    if (minimumsizeerror) {
      gOFS.Emsg(epname,error, EIO, "store file - file has been cleaned because it is smaller than the required minimum file size in that directory", Path.c_str());
      eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason=\"minimum file size criteria\"", capOpaque->Get("mgm.path"), fstPath.c_str());    
    } else {
      if (checksumerror) {
	gOFS.Emsg(epname,error, EIO, "store file - file has been cleaned because of a checksum error ",Path.c_str());
	eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason=\"checksum error\"", capOpaque->Get("mgm.path"), fstPath.c_str());
      } else {
	if (writeErrorFlag == kOfsSimulatedIoError) {
	  gOFS.Emsg(epname,error, EIO, "store file - file has been cleaned because of a simulated IO error ",Path.c_str());
	  eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason=\"simulated IO error\"", capOpaque->Get("mgm.path"), fstPath.c_str());
	} else {
	  if (writeErrorFlag == kOfsMaxSizeError) {
	    gOFS.Emsg(epname,error, EIO, "store file - file has been cleaned because you exceeded the maximum file size settings for this namespace branch",Path.c_str());
	    eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason=\"maximum file size criteria\"", capOpaque->Get("mgm.path"), fstPath.c_str());
	  } else {
	    if (writeErrorFlag == kOfsDiskFullError) {
	      gOFS.Emsg(epname,error, EIO, "store file - file has been cleaned because the target disk filesystem got full and you didn't use reservation",Path.c_str());
	      eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason=\"filesystem full\"", capOpaque->Get("mgm.path"), fstPath.c_str());
	    } else {
	      if (writeErrorFlag == kOfsIoError) {
		gOFS.Emsg(epname,error, EIO, "store file - file has been cleaned because of an IO error during a write operation",Path.c_str());
	      eos_crit("info=\"deleting on close\" fn=%s fstpath=%s reason=\"write IO error\"", capOpaque->Get("mgm.path"), fstPath.c_str());
	      } else {
		if (targetsizeerror) {
		  gOFS.Emsg(epname,error, EIO, "store file - file has been cleaned because the stored file does not match the provided targetsize",Path.c_str());
		  eos_crit("info=\"deleting on close\" fn=%s fstpath=%s reason=\"target size mismatch\"", capOpaque->Get("mgm.path"), fstPath.c_str());
		} else {
		  gOFS.Emsg(epname,error, EIO, "store file - file has been cleaned because of a client disconnect",Path.c_str());
		  eos_crit("info=\"deleting on close\" fn=%s fstpath=%s reason=\"client disconnect\"", capOpaque->Get("mgm.path"), fstPath.c_str());
		}
	      }		
	    }
	  }
	}
      }
    }
    
    if (fstBlockXS) {
      fstBlockXS->CloseMap();
      // delete also the block checksum file
      fstBlockXS->UnlinkXSPath();
      delete fstBlockXS;
      fstBlockXS=0;
    }
  } else {
    if (checksumerror) {
      rc = SFS_ERROR;
      gOFS.Emsg(epname, error, EIO, "verify checksum - checksum error for file fn=", capOpaque->Get("mgm.path"));   
      int envlen=0;
      eos_crit("file-xs error file=%s", capOpaque->Env(envlen));
    }
  }

  if (repairOnClose) {
    // do an upcall to the MGM and ask to adjust the replica of the uploaded file
    XrdOucString OpaqueString="/?mgm.pcmd=adjustreplica&mgm.path=";
    OpaqueString += capOpaque->Get("mgm.path");
    
    eos_info("info=\"repair on close\" path=%s",  capOpaque->Get("mgm.path"));
    if (gOFS.CallManager(&error, capOpaque->Get("mgm.path"),capOpaque->Get("mgm.manager"), OpaqueString)) {
      eos_warning("failed to execute 'adjustreplica' for path=%s", capOpaque->Get("mgm.path"));
      gOFS.Emsg(epname, error, EIO, "create all replicas - uploaded file is at risk - only one replica has been successfully stored for fn=", capOpaque->Get("mgm.path"));   
    } else {
      if (!brc) {
	// reset the return code
	rc = 0 ;
	// clean error message
	gOFS.Emsg(epname, error, 0, "no error");
      }
    }
    eos_warning("executed 'adjustreplica' for path=%s - file is at low risk due to missing replica's", capOpaque->Get("mgm.path"));
  }

  return rc;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfs::stat(  const char             *path,
                  struct stat            *buf,
                  XrdOucErrInfo          &out_error,
                  const XrdSecEntity     *client,
                  const char             *opaque)
{
  EPNAME("stat");
  memset(buf,0,sizeof(struct stat));
  if (!XrdOfsOss->Stat(path, buf))
    return SFS_OK;
  else
    return gOFS.Emsg(epname,out_error,errno,"stat file",path);
}


/*----------------------------------------------------------------------------*/
int
XrdFstOfs::CallManager(XrdOucErrInfo *error, const char* path, const char* manager, XrdOucString &capOpaqueFile, XrdOucString* return_result) {
  EPNAME("CallManager");
  int rc=SFS_OK;

  char result[8192]; result[0]=0;
  int  result_size=8192;
  
  XrdOucString url = "root://"; url += manager; url += "//dummy";
  XrdClientAdmin* admin = new XrdClientAdmin(url.c_str());
  XrdOucString msg="";
      
  if (admin) {
    admin->Connect();
    admin->GetClientConn()->ClearLastServerError();
    admin->GetClientConn()->SetOpTimeLimit(10);
    admin->Query(kXR_Qopaquf,
                 (kXR_char *) capOpaqueFile.c_str(),
                 (kXR_char *) result, result_size);
    
    if (!admin->LastServerResp()) {
      if (error)
        gOFS.Emsg(epname, *error, ECOMM, "commit changed filesize to meta data cache for fn=", path);
      rc = SFS_ERROR;
    }
    switch (admin->LastServerResp()->status) {
    case kXR_ok:
      eos_debug("called MGM cache - %s", capOpaqueFile.c_str());
      rc = SFS_OK;
      break;
      
    case kXR_error:
      if (error) {
        gOFS.Emsg(epname, *error, ECOMM, "to call manager for fn=", path);
      }
      msg = (admin->LastServerError()->errmsg);
      rc = SFS_ERROR;

      if (msg.find("[EIDRM]") !=STR_NPOS)
        rc = -EIDRM;


      if (msg.find("[EBADE]") !=STR_NPOS)
        rc = -EBADE;


      if (msg.find("[EBADR]") !=STR_NPOS)
        rc = -EBADR;

      if (msg.find("[EINVAL]") != STR_NPOS)
	rc = -EINVAL;
      
      if (msg.find("[EADV]") != STR_NPOS)
	rc = -EADV;
      
      if (msg.find("[EIO]") != STR_NPOS)
	rc = -EIO;

      break;
      
    default:
      rc = SFS_OK;
      break;
    }
  } else {
    eos_crit("cannot get client admin to execute commit");
    if (error)
      gOFS.Emsg(epname, *error, ENOMEM, "allocate client admin object during close of fn=", path);
  }
  delete admin;

  if (return_result) {
    *return_result = result;
  }
  return rc;
}

/*----------------------------------------------------------------------------*/
XrdSfsXferSize 
XrdFstOfsFile::readofs(XrdSfsFileOffset   fileOffset,
                       char              *buffer,
                       XrdSfsXferSize     buffer_size)
{
  int retc = XrdOfsFile::read(fileOffset,buffer,buffer_size);

  eos_debug("read %llu %llu %lu", this, fileOffset, buffer_size);

  if (gOFS.Simulate_IO_read_error) {
    return gOFS.Emsg("readofs", error, EIO, "read file - simulated IO error fn=", capOpaque?(capOpaque->Get("mgm.path")?capOpaque->Get("mgm.path"):FName()):FName());
  }
  
  if (fstBlockXS) {
    XrdSysMutexHelper cLock (BlockXsMutex);
    if ((retc>0) && (!fstBlockXS->CheckBlockSum(fileOffset, buffer, retc))) {
      int envlen=0;
      eos_err("block-xs error offset=%llu size=%llu file=%s",(unsigned long long)fileOffset, (unsigned long long)buffer_size, capOpaque?capOpaque->Env(envlen):FName());
      return gOFS.Emsg("readofs", error, EIO, "read file - wrong block checksum fn=", capOpaque?(capOpaque->Get("mgm.path")?capOpaque->Get("mgm.path"):FName()):FName());
    }
  }
  return retc;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsFile::read(XrdSfsFileOffset   fileOffset,   
                    XrdSfsXferSize     amount)
{
  //  EPNAME("read");

  int rc = XrdOfsFile::read(fileOffset,amount);
  
  eos_debug("rc=%d offset=%lu size=%llu",rc, fileOffset,amount);
  
  return rc;
}

/*----------------------------------------------------------------------------*/
XrdSfsXferSize 
XrdFstOfsFile::read(XrdSfsFileOffset   fileOffset,
                    char              *buffer,
                    XrdSfsXferSize     buffer_size)
{
  //  EPNAME("read");

  gettimeofday(&cTime,&tz);
  rCalls++;

  eos_debug("XrdFstOfsFile: read - fileOffset: %lli, buffer_size: %i", fileOffset, buffer_size);

  int rc = layOut->read(fileOffset,buffer,buffer_size);

  if ((rc>0) && (checkSum)) {
    XrdSysMutexHelper cLock (ChecksumMutex);
    checkSum->Add(buffer, rc, fileOffset);
  }

  if (rOffset != (unsigned long long)fileOffset) {
    srBytes += llabs(rOffset-fileOffset);
  }
  
  if (rc > 0) {
    XrdSysMutexHelper vecLock(vecMutex);
    rvec.push_back(rc);
    rOffset += rc;
  }
  
  gettimeofday(&lrTime,&tz);

  AddReadTime();

  if (rc < 0) {
    // here we might take some other action
    int envlen=0;
    eos_err("block-read error=%d offset=%llu len=%llu file=%s",error.getErrInfo(), (unsigned long long)fileOffset, (unsigned long long)buffer_size,FName(), capOpaque?capOpaque->Env(envlen):FName());      
  }

  eos_debug("rc=%d offset=%lu size=%llu",rc, fileOffset,(unsigned long long) buffer_size);

  if ( (fileOffset + buffer_size) >= openSize) {
    if (checkSum) {
      checkSum->Finalize();
      if (!checkSum->NeedsRecalculation()) {
	// if this is the last read of sequential reading, we can verify the checksum now
	if (verifychecksum()) 
	  return gOFS.Emsg("read", error, EIO, "read file - wrong file checksum fn=", FName());      
      }
    }
  }



  return rc;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsFile::read(XrdSfsAio *aioparm)
{
  //  EPNAME("read");  
  return SFS_ERROR;

  //  eos_debug("aio");
  //  int rc = XrdOfsFile::read(aioparm);

  //  return rc;
}

/*----------------------------------------------------------------------------*/
XrdSfsXferSize
XrdFstOfsFile::writeofs(XrdSfsFileOffset   fileOffset,
                        const char        *buffer,
                        XrdSfsXferSize     buffer_size)
{

  if (gOFS.Simulate_IO_write_error) {
    writeErrorFlag=kOfsSimulatedIoError;
    return gOFS.Emsg("readofs", error, EIO, "write file - simulated IO error fn=", capOpaque?(capOpaque->Get("mgm.path")?capOpaque->Get("mgm.path"):FName()):FName());
  }

  if (fsid) {
    if (targetsize && (targetsize == bookingsize) ) {
      // space has been successfully pre-allocated, let him write
    } else {
      // check if the file system is full
      XrdSysMutexHelper(gOFS.Storage->fileSystemFullMapMutex);
      if (gOFS.Storage->fileSystemFullMap[fsid]) {
	writeErrorFlag=kOfsDiskFullError;
	
	return gOFS.Emsg("writeofs", error, ENOSPC, "write file - disk space (headroom) exceeded fn=", capOpaque?(capOpaque->Get("mgm.path")?capOpaque->Get("mgm.path"):FName()):FName());
      }
    }
  }

  if (maxsize) {
    // check that the user didn't exceed the maximum file size policy
    if ( (fileOffset + buffer_size) > maxsize ) {
      writeErrorFlag=kOfsMaxSizeError;
      return gOFS.Emsg("writeofs", error, ENOSPC, "write file - your file exceeds the maximum file size setting of bytes<=", capOpaque?(capOpaque->Get("mgm.maxsize")?capOpaque->Get("mgm.maxsize"):"<undef>"):"undef");
    }
  }
  if (fstBlockXS) {
    XrdSysMutexHelper cLock(BlockXsMutex);
    fstBlockXS->AddBlockSum(fileOffset, buffer, buffer_size);
  }
  
  int rc = XrdOfsFile::write(fileOffset,buffer,buffer_size);
  if (rc!=buffer_size) {
    writeErrorFlag=kOfsIoError;
  };

  return rc;
}


/*----------------------------------------------------------------------------*/
XrdSfsXferSize
XrdFstOfsFile::write(XrdSfsFileOffset   fileOffset,
                     const char        *buffer,
                     XrdSfsXferSize     buffer_size)
{
  //  EPNAME("write");

  gettimeofday(&cTime,&tz);
  wCalls++;

  int rc = layOut->write(fileOffset,(char*)buffer,buffer_size);

  if ( (rc <0) && isCreation && (error.getErrInfo() == EREMOTEIO) ) {
    if (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReplica) {
      // if we see a remote IO error, we don't fail, we just call a repair action afterwards (only for replica layouts!)
      repairOnClose = true;
      rc = buffer_size;
    }
  }
  // evt. add checksum
  if ((rc >0) && (checkSum) ) {
    XrdSysMutexHelper cLock (ChecksumMutex);
    checkSum->Add(buffer, (size_t) rc, (off_t) fileOffset);
  }

  if (wOffset != (unsigned long long) fileOffset) {
    swBytes += llabs(wOffset-fileOffset);
  }

  if (rc > 0) {
    XrdSysMutexHelper(vecMutex);
    wvec.push_back(rc);
    wOffset += rc;

    if ( (unsigned long long)(fileOffset + buffer_size)> (unsigned long long)maxOffsetWritten)
      maxOffsetWritten = (fileOffset + buffer_size);
  }

  gettimeofday(&lwTime,&tz);

  AddWriteTime();
    
  haswrite = true;
  eos_debug("rc=%d offset=%lu size=%lu",rc, fileOffset,(unsigned long)buffer_size);

  if (rc <0) {
    int envlen=0;
    // indicate the deletion flag for write errors
    writeDelete = true;
    XrdOucString errdetail;
    if (isCreation) {
      XrdOucString newerr;
      
      // add to the error message that this file has been removed after the error - which happens for creations
      newerr = error.getErrText();
      if (writeErrorFlag == kOfsSimulatedIoError) {
	errdetail += " => file has been removed because of a simulated IO error";
      } else {
	if (writeErrorFlag == kOfsDiskFullError) {
	  errdetail += " => file has been removed because the target filesystem  was full";
	} else {
	  if (writeErrorFlag == kOfsMaxSizeError) {
	    errdetail += " => file has been removed because the maximum target filesize defined for that subtree was exceeded (maxsize=";
	    char smaxsize[16];
	    snprintf(smaxsize,sizeof(smaxsize)-1, "%llu", (unsigned long long) maxsize);
	    errdetail += smaxsize;
	    errdetail += " bytes)";
	  } else {
	    if (writeErrorFlag == kOfsIoError) {
	      errdetail += " => file has been removed due to an IO error on the target filesystem";
	    } else {
	      errdetail += " => file has been removed due to an IO error (unspecified)";
	    }
	  }
	}
      }
      newerr += errdetail.c_str();
      error.setErrInfo(error.getErrInfo(),newerr.c_str());
    }
    eos_err("block-write error=%d offset=%llu len=%llu file=%s error=\"%s\"",error.getErrInfo(), (unsigned long long)fileOffset, (unsigned long long)buffer_size,FName(), capOpaque?capOpaque->Env(envlen):FName(), errdetail.c_str());      
  }

  return rc;
}


/*----------------------------------------------------------------------------*/
int
XrdFstOfsFile::write(XrdSfsAio *aioparm)
{
  //  EPNAME("write");
  // this is not supported
  return SFS_ERROR;
}


/*----------------------------------------------------------------------------*/
int          
XrdFstOfsFile::syncofs()
{
  return XrdOfsFile::sync();
}

/*----------------------------------------------------------------------------*/
int          
XrdFstOfsFile::sync()
{
  return layOut->sync();
}

/*----------------------------------------------------------------------------*/
int          
XrdFstOfsFile::sync(XrdSfsAio *aiop)
{
  return layOut->sync();
}

/*----------------------------------------------------------------------------*/
int          
XrdFstOfsFile::truncateofs(XrdSfsFileOffset   fileOffset)
{
  // truncation moves the max offset written 
  maxOffsetWritten = fileOffset;
  return XrdOfsFile::truncate(fileOffset);
}

/*----------------------------------------------------------------------------*/
int          
XrdFstOfsFile::truncate(XrdSfsFileOffset   fileOffset)
{

  if (fileOffset == EOS_FST_DELETE_FLAG_VIA_TRUNCATE_LEN) {
    eos_info("msg=\"deletion flag indicated\" path=%s",fstPath.c_str());
    // this truncate offset indicates to delete the file during the close operation
    remoteDelete = true;
    return SFS_OK;
  }

  //  fprintf(stderr,"truncate called %llu\n", fileOffset);
  eos_info("subcmd=truncate openSize=%llu fileOffset=%llu", openSize, fileOffset);
  if (fileOffset != openSize) {
    haswrite = true;
    if (checkSum) {
      if (fileOffset != checkSum->GetMaxOffset()) {
	checkSum->Reset();
	checkSum->SetDirty();
      }
    }
  }

  return layOut->truncate(fileOffset);
}


/*----------------------------------------------------------------------------*/
int          
XrdFstOfsFile::stat(struct stat *buf)
{
  EPNAME("stat");
  int rc = SFS_OK ;

  if (layOut) {
    if ((rc = layOut->stat(buf))) {
      rc = gOFS.Emsg(epname,error, EIO, "stat - cannot stat layout to determine file size ",Path.c_str());
    }
  } else {
    rc = gOFS.Emsg(epname,error, ENXIO, "stat - no layout to determine file size ",Path.c_str());
  }
  
  return rc;
}


/*----------------------------------------------------------------------------*/
void
XrdFstOfs::SetDebug(XrdOucEnv &env) 
{
  XrdOucString debugnode =  env.Get("mgm.nodename");
  XrdOucString debuglevel = env.Get("mgm.debuglevel");
  XrdOucString filterlist = env.Get("mgm.filter");
  int debugval = eos::common::Logging::GetPriorityByString(debuglevel.c_str());
  if (debugval<0) {
    eos_err("debug level %s is not known!", debuglevel.c_str());
  } else {
    // we set the shared hash debug for the lowest 'debug' level
    if (debuglevel == "debug") {
      ObjectManager.SetDebug(true);
    } else {
      ObjectManager.SetDebug(false);
    }

    eos::common::Logging::SetLogPriority(debugval);
    eos_notice("setting debug level to <%s>", debuglevel.c_str());
    if (filterlist.length()) {
      eos::common::Logging::SetFilter(filterlist.c_str());
      eos_notice("setting message logid filter to <%s>", filterlist.c_str());
    }
  }
  fprintf(stderr,"Setting debug to %s\n", debuglevel.c_str());
}


/*----------------------------------------------------------------------------*/
void
XrdFstOfs::SendRtLog(XrdMqMessage* message) 
{
  XrdOucEnv opaque(message->GetBody());
  XrdOucString queue = opaque.Get("mgm.rtlog.queue");
  XrdOucString lines = opaque.Get("mgm.rtlog.lines");
  XrdOucString tag   = opaque.Get("mgm.rtlog.tag");
  XrdOucString filter = opaque.Get("mgm.rtlog.filter");
  XrdOucString stdOut="";

  if (!filter.length()) filter = " ";

  if ( (!queue.length()) || (!lines.length()) || (!tag.length()) ) {
    eos_err("illegal parameter queue=%s lines=%s tag=%s", queue.c_str(), lines.c_str(), tag.c_str());
  }  else {
    if ( (eos::common::Logging::GetPriorityByString(tag.c_str())) == -1) {
      eos_err("mgm.rtlog.tag must be info,debug,err,emerg,alert,crit,warning or notice");
    } else {
      int logtagindex = eos::common::Logging::GetPriorityByString(tag.c_str());
      for (int j = 0; j<= logtagindex; j++) {
        for (int i=1; i<= atoi(lines.c_str()); i++) {
          eos::common::Logging::gMutex.Lock();
          XrdOucString logline = eos::common::Logging::gLogMemory[j][(eos::common::Logging::gLogCircularIndex[j]-i+eos::common::Logging::gCircularIndexSize)%eos::common::Logging::gCircularIndexSize].c_str();
          eos::common::Logging::gMutex.UnLock();
        
          if (logline.length() && ( (logline.find(filter.c_str())) != STR_NPOS)) {
            stdOut += logline;
            stdOut += "\n";
          }
          if (stdOut.length() > (4*1024)) {
            XrdMqMessage repmessage("rtlog reply message");
            repmessage.SetBody(stdOut.c_str());
            if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message)) {
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
  if (stdOut.length()) {
    XrdMqMessage repmessage("rtlog reply message");
    repmessage.SetBody(stdOut.c_str());
    if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message)) {
      eos_err("unable to send rtlog reply message to %s", message->kMessageHeader.kSenderId.c_str());
    }
  }
}


/*----------------------------------------------------------------------------*/
void
XrdFstOfs::SendFsck(XrdMqMessage* message) 
{
  XrdOucEnv opaque(message->GetBody());
  XrdOucString stdOut="";
  XrdOucString tag   = opaque.Get("mgm.fsck.tags"); // the tag is either '*' for all, or a , seperated list of tag names
  if ( (!tag.length()) ) {
    eos_err("parameter tag missing");
  }  else {
    stdOut = "";
    // loop over filesystems
    eos::common::RWMutexReadLock(gOFS.Storage->fsMutex);
    std::vector <eos::fst::FileSystem*>::const_iterator it;
    for (unsigned int i=0; i< gOFS.Storage->fileSystemsVector.size(); i++) {
      XrdSysMutexHelper ISLock (gOFS.Storage->fileSystemsVector[i]->InconsistencyStatsMutex);
      std::map<std::string, std::set<eos::common::FileId::fileid_t> >* icset = gOFS.Storage->fileSystemsVector[i]->GetInconsistencySets();
      std::map<std::string, std::set<eos::common::FileId::fileid_t> >::const_iterator icit;
      for (icit = icset->begin(); icit != icset->end(); icit++) {
	// loop over all tags
	if ( ( (icit->first != "mem_n") && (icit->first != "d_sync_n") && (icit->first != "m_sync_n") ) &&  
	     ( (tag == "*") || ( (tag.find(icit->first.c_str()) != STR_NPOS)) ) ) {
	  char stag[4096];
	  eos::common::FileSystem::fsid_t fsid = gOFS.Storage->fileSystemsVector[i]->GetId();
	  snprintf(stag,sizeof(stag)-1,"%s@%lu", icit->first.c_str(), (unsigned long )fsid);
	  stdOut += stag;
	  std::set<eos::common::FileId::fileid_t>::const_iterator fit;

	  if (gOFS.Storage->fileSystemsVector[i]->GetStatus() != eos::common::FileSystem::kBooted) {
	    // we don't report filesystems which are not booted!
	    continue;
	  }
	  for (fit = icit->second.begin(); fit != icit->second.end(); fit ++) {
	    // don't report files which are currently write-open
	    XrdSysMutexHelper wLock(gOFS.OpenFidMutex);
	    if (gOFS.WOpenFid[fsid].count(*fit)) {
	      if (gOFS.WOpenFid[fsid][*fit]>0) {
		continue;
	      }
	    }
	    // loop over all fids
	    char sfid[4096];
	    snprintf(sfid,sizeof(sfid)-1,":%08llx", *fit);
	    stdOut += sfid;

	    if (stdOut.length() > (64*1024)) {
	      stdOut += "\n";
	      XrdMqMessage repmessage("fsck reply message");
	      repmessage.SetBody(stdOut.c_str());
	      //	      fprintf(stderr,"Sending %s\n", stdOut.c_str());
	      if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message)) {
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
  if (stdOut.length()) {
    XrdMqMessage repmessage("fsck reply message");
    repmessage.SetBody(stdOut.c_str());
    //    fprintf(stderr,"Sending %s\n", stdOut.c_str());
    if (!XrdMqMessaging::gMessageClient.ReplyMessage(repmessage, *message)) {
      eos_err("unable to send fsck reply message to %s", message->kMessageHeader.kSenderId.c_str());
    }
  }
}

/*----------------------------------------------------------------------------*/
int            
XrdFstOfs::rem(const char             *path,
               XrdOucErrInfo          &error,
               const XrdSecEntity     *client,
               const char             *opaque) 
{
  
  EPNAME("rem");
  // the OFS open is catched to set the access/modify time in the nameserver

  //  const char *tident = error.getErrUser();
  //  char *val=0;

  XrdOucString stringOpaque = opaque;
  stringOpaque.replace("?","&");
  stringOpaque.replace("&&","&");

  XrdOucEnv openOpaque(stringOpaque.c_str());
  XrdOucEnv* capOpaque;

  int caprc = 0;
  

  if ((caprc=gCapabilityEngine.Extract(&openOpaque, capOpaque))) {
    // no capability - go away!
    if (capOpaque) delete capOpaque;
    return gOFS.Emsg(epname,error,caprc,"open - capability illegal",path);
  }

  int envlen;
  //ZTRACE(open,"capability contains: " << capOpaque->Env(envlen));
  if (capOpaque) {
    eos_info("path=%s info=%s capability=%s", path, opaque, capOpaque->Env(envlen));
  } else {
    eos_info("path=%s info=%s", path, opaque);
  }
  
  int rc =  _rem(path, error, client, capOpaque);
  if (capOpaque) {
    delete capOpaque;
    capOpaque = 0;
  }

  return rc;
}



/*----------------------------------------------------------------------------*/
int            
XrdFstOfs::_rem(const char             *path,
                XrdOucErrInfo          &error,
                const XrdSecEntity     *client,
                XrdOucEnv              *capOpaque, 
                const char*            fstpath, 
                unsigned long long     fid,
                unsigned long          fsid, 
		bool                   ignoreifnotexist)
{
  EPNAME("rem");
  int   retc = SFS_OK;
  XrdOucString fstPath="";

  const char* localprefix=0;
  const char* hexfid=0;
  const char* sfsid=0;

  eos_debug("");

  if ( (!fstpath) && (!fsid) && (!fid) ) {
    // standard deletion brings all information via the opaque info
    if (!(localprefix=capOpaque->Get("mgm.localprefix"))) {
      return gOFS.Emsg(epname,error,EINVAL,"open - no local prefix in capability",path);
    }
    
    if (!(hexfid=capOpaque->Get("mgm.fid"))) {
      return gOFS.Emsg(epname,error,EINVAL,"open - no file id in capability",path);
    }
    
    if (!(sfsid=capOpaque->Get("mgm.fsid"))) {
      return gOFS.Emsg(epname,error, EINVAL,"open - no file system id in capability",path);
    }
    eos::common::FileId::FidPrefix2FullPath(hexfid, localprefix,fstPath);

    fid = eos::common::FileId::Hex2Fid(hexfid);

    fsid   = atoi(sfsid);
  } else {
    // deletion during close provides the local storage path, fid & fsid
    fstPath = fstpath;
  }

  struct stat statinfo;
  if ((retc = XrdOfsOss->Stat(fstPath.c_str(), &statinfo))) {
    if (!ignoreifnotexist) {
      eos_notice("unable to delete file - file does not exist (anymore): %s fstpath=%s fsid=%lu id=%llu", path, fstPath.c_str(),fsid, fid);
      return gOFS.Emsg(epname,error,ENOENT,"delete file - file does not exist",fstPath.c_str());    
    }
  } 
  eos_info("fstpath=%s", fstPath.c_str());

  int rc=0;
  if (!retc) {
    // unlink file
    errno = 0;
    rc = XrdOfs::rem(fstPath.c_str(),error,client,0);
    if (rc) {
      eos_info("rc=%d errno=%d", rc,errno);
    }
  }

  if (ignoreifnotexist) {
    // hide error if a deleted file is deleted
    rc = 0;
  }

  // unlink block checksum files
  {
    // this is not the 'best' solution, but we don't have any info about block checksums
    Adler xs; // the type does not matter here
    const char* path = xs.MakeBlockXSPath(fstPath.c_str());
    if (!xs.UnlinkXSPath()) {
      eos_info("info=\"removed block-xs\" path=%s", path);
    }
  }

  // cleanup eventual transactions
  if (!gOFS.Storage->CloseTransaction(fsid, fid)) {
    // it should be the normal case that there is no open transaction for that file, in any case there is nothing to do here
  }
  
  if (rc) {
    return rc;
  }

  if (!gFmdSqliteHandler.DeleteFmd(fid, fsid)) {
    if (!ignoreifnotexist) {
      eos_notice("unable to delete fmd for fid %llu on filesystem %lu",fid,fsid);
      return gOFS.Emsg(epname,error,EIO,"delete file meta data ",fstPath.c_str());
    }
  }

  return SFS_OK;
}

int       
XrdFstOfs::fsctl(const int               cmd,
                 const char             *args,
                 XrdOucErrInfo          &error,
                 const XrdSecEntity *client)

{
  static const char *epname = "fsctl";
  const char *tident = error.getErrUser();

  if ((cmd == SFS_FSCTL_LOCATE)) {
    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    rType[1] = 'r';//(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp,"[::%s:%d] ",(char*)HostName,myPort);
    error.setErrInfo(strlen(locResp)+3, (const char **)Resp, 2);
    ZTRACE(fsctl,"located at headnode: " << locResp);
    return SFS_DATA;
  }
  return gOFS.Emsg(epname, error, EPERM, "execute fsctl function", "");
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfs::FSctl(const int               cmd,
                 XrdSfsFSctl            &args,
                 XrdOucErrInfo          &error,
                 const XrdSecEntity     *client) 
{  
  char ipath[16384];
  char iopaque[16384];
  
  static const char *epname = "FSctl";
  const char *tident = error.getErrUser();

  if ((cmd == SFS_FSCTL_LOCATE)) {
    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    rType[1] = 'r';//(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp,"[::%s:%d] ",(char*)HostName,myPort);
    error.setErrInfo(strlen(locResp)+3, (const char **)Resp, 2);
    ZTRACE(fsctl,"located at headnode: " << locResp);
    return SFS_DATA;
  }
  
  // accept only plugin calls!

  if (cmd!=SFS_FSCTL_PLUGIN) {
    return gOFS.Emsg(epname, error, EPERM, "execute non-plugin function", "");
  }

  if (args.Arg1Len) {
    if (args.Arg1Len < 16384) {
      strncpy(ipath,args.Arg1,args.Arg1Len);
      ipath[args.Arg1Len] = 0;
    } else {
      return gOFS.Emsg(epname, error, EINVAL, "convert path argument - string too long", "");
    }
  } else {
    ipath[0] = 0;
  }
  
  if (args.Arg2Len) {
    if (args.Arg2Len < 16384) {
      strncpy(iopaque,args.Arg2,args.Arg2Len);
      iopaque[args.Arg2Len] = 0;
    } else {
      return gOFS.Emsg(epname, error, EINVAL, "convert opaque argument - string too long", "");
    }
  } else {
    iopaque[0] = 0;
  }
  
  // from here on we can deal with XrdOucString which is more 'comfortable'
  XrdOucString path    = ipath;
  XrdOucString opaque  = iopaque;
  XrdOucString result  = "";
  XrdOucEnv env(opaque.c_str());

  eos_debug("tident=%s path=%s opaque=%s",tident, path.c_str(), opaque.c_str());

  if (cmd!=SFS_FSCTL_PLUGIN) {
    return SFS_ERROR;
  }

  const char* scmd;

  if ((scmd = env.Get("fst.pcmd"))) {
    XrdOucString execmd = scmd;

    if (execmd == "getfmd") {
      char* afid   = env.Get("fst.getfmd.fid");
      char* afsid  = env.Get("fst.getfmd.fsid");

      if ((!afid) || (!afsid)) {
        return  Emsg(epname,error,EINVAL,"execute FSctl command",path.c_str());  
      }

      unsigned long long fileid = eos::common::FileId::Hex2Fid(afid);
      unsigned long fsid = atoi(afsid);

      FmdSqlite* fmd = gFmdSqliteHandler.GetFmd(fileid, fsid, 0, 0, 0, false, true);

      if (!fmd) {
        eos_static_err("no fmd for fileid %llu on filesystem %lu", fileid, fsid);
        const char* err = "ERROR";
        error.setErrInfo(strlen(err)+1,err);
        return SFS_DATA;
      }
      
      XrdOucEnv* fmdenv = fmd->FmdSqliteToEnv();
      int envlen;
      XrdOucString fmdenvstring = fmdenv->Env(envlen);
      delete fmdenv;
      delete fmd;
      error.setErrInfo(fmdenvstring.length()+1,fmdenvstring.c_str());
      return SFS_DATA;
    }

    if (execmd == "getxattr") {
      char* key    = env.Get("fst.getxattr.key");
      char* path   = env.Get("fst.getxattr.path");
      if (!key) {
        eos_static_err("no key specified as attribute name");
        const char* err = "ERROR";
        error.setErrInfo(strlen(err)+1,err);
        return SFS_DATA;
      }
      if (!path) {
        eos_static_err("no path specified to get the attribute from");
        const char* err = "ERROR";
        error.setErrInfo(strlen(err)+1,err);
        return SFS_DATA;
      }
      char value[1024];
      ssize_t attr_length = getxattr(path,key,value,sizeof(value));
      if (attr_length>0) {
        value[1023]=0;
        XrdOucString skey=key;
        XrdOucString attr = "";
        if (skey=="user.eos.checksum") {
          // checksum's are binary and need special reformatting ( we swap the byte order if they are 4 bytes long )
          if (attr_length==4) {
            for (ssize_t k=0; k<4; k++) {
              char hex[4];
              snprintf(hex,sizeof(hex)-1,"%02x", (unsigned char) value[3-k]);
              attr += hex;
            }
          } else {
            for (ssize_t k=0; k<attr_length; k++) {
              char hex[4];
              snprintf(hex,sizeof(hex)-1,"%02x", (unsigned char) value[k]);
              attr += hex;
            }
          }
        } else {
          attr = value;
        }
        error.setErrInfo(attr.length()+1,attr.c_str());
        return SFS_DATA;
      } else {
        eos_static_err("getxattr failed for path=%s",path);
        const char* err = "ERROR";
        error.setErrInfo(strlen(err)+1,err);
        return SFS_DATA;
      }
    }
  }

  return  Emsg(epname,error,EINVAL,"execute FSctl command",path.c_str());  
}


/*----------------------------------------------------------------------------*/
void 
XrdFstOfs::OpenFidString(unsigned long fsid, XrdOucString &outstring)
{
  outstring ="";
  OpenFidMutex.Lock();
  google::sparse_hash_map<unsigned long long, unsigned int>::const_iterator idit;
  int nopen = 0;

  for (idit = ROpenFid[fsid].begin(); idit != ROpenFid[fsid].end(); ++idit) {
    if (idit->second >0)
      nopen += idit->second;
  }
  outstring += "&statfs.ropen=";
  outstring += nopen;

  nopen = 0;
  for (idit = WOpenFid[fsid].begin(); idit != WOpenFid[fsid].end(); ++idit) {
    if (idit->second >0)
      nopen += idit->second;
  }
  outstring += "&statfs.wopen=";
  outstring += nopen;
  
  OpenFidMutex.UnLock();
}

int XrdFstOfs::Stall(XrdOucErrInfo   &error, // Error text & code
                     int              stime, // Seconds to stall
                     const char      *msg)   // Message to give
{
  XrdOucString smessage = msg;
  smessage += "; come back in ";
  smessage += stime;
  smessage += " seconds!";
  
  EPNAME("Stall");
  const char *tident = error.getErrUser();
  
  ZTRACE(delay, "Stall " <<stime <<": " << smessage.c_str());

  // Place the error message in the error object and return
  //
  error.setErrInfo(0, smessage.c_str());
  
  // All done
  //
  return stime;
}

/*----------------------------------------------------------------------------*/
int XrdFstOfs::Redirect(XrdOucErrInfo   &error, // Error text & code
                        const char* host,
                        int &port)
{
  EPNAME("Redirect");
  const char *tident = error.getErrUser();
  
  ZTRACE(delay, "Redirect " <<host <<":" << port);

  // Place the error message in the error object and return
  //
  error.setErrInfo(port,host);
  
  // All done
  //
  return SFS_REDIRECT;
}


/*----------------------------------------------------------------------------*/
int
XrdFstOfsDirectory::open(const char              *dirName,
                         const XrdSecClientName  *client,
                         const char              *opaque)
{
  /* --------------------------------------------------------------------------------- */
  /* We use opendir/readdir/closedir to send meta data information about EOS FST files */
  /* --------------------------------------------------------------------------------- */
  XrdOucEnv Opaque(opaque?opaque:"disk=1");

  eos_info("info=\"calling opendir\" dir=%s", dirName);
  dirname = dirName;
  if (!client || (strcmp(client->prot,"sss"))) {
    return gOFS.Emsg("opendir",error,EPERM, "open directory - you need to connect via sss",dirName);
  }

  if (Opaque.Get("disk")) {
    std::string dn = dirname.c_str();
    if (!gOFS.Storage->GetFsidFromPath(dn, fsid)) {
      return gOFS.Emsg("opendir",error, EINVAL,"open directory - filesystem has no fsid label ", dirName);
    }
    // here we traverse the tree of the path given by dirName
    fts_paths    = (char**) calloc(2, sizeof(char*));
    fts_paths[0] = (char*) dirName;
    fts_paths[1] = 0;
    fts_tree = fts_open(fts_paths, FTS_NOCHDIR, 0);
        
    if (fts_tree) {
      return SFS_OK;
    }
    return gOFS.Emsg("opendir",error,errno,"open directory - fts_open failed for ",dirName);
  }
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/

const char*
XrdFstOfsDirectory::nextEntry()
{
  FTSENT *node;
  size_t nfound=0;
  entry="";
  // we send the directory contents in a packed format

  while ( (node = fts_read(fts_tree)) ) {
    if (node) {
      if (node->fts_level > 0 && node->fts_name[0] == '.') {
        fts_set(fts_tree, node, FTS_SKIP);
      } else {
        if (node->fts_info && FTS_F) {
          XrdOucString sizestring;
          XrdOucString filePath = node->fts_accpath;
          XrdOucString fileId   = node->fts_accpath;
          if (!filePath.matches("*.xsmap")) {
            struct stat st_buf;
            eos::common::Attr *attr = eos::common::Attr::OpenAttr(filePath.c_str());
            int spos = filePath.rfind("/");
            if (spos >0) {
              fileId.erase(0, spos+1);
            }
            if ((fileId.length() == 8) && (!stat(filePath.c_str(),&st_buf) && S_ISREG(st_buf.st_mode))) {
              // only scan closed files !!!!
              unsigned long long fileid = eos::common::FileId::Hex2Fid(fileId.c_str());
              bool isopenforwrite=false;

              gOFS.OpenFidMutex.Lock();
              if (gOFS.WOpenFid[fsid].count(fileid)) {
                if (gOFS.WOpenFid[fsid][fileid]>0) {
                  isopenforwrite=true;
                }
              }
              gOFS.OpenFidMutex.UnLock();

              std::string val="";
              // token[0]: fxid
              entry += fileId;
              entry += ":";
              // token[1] scandir timestap
              val = attr->Get("user.eos.timestamp").c_str();
              entry += val.length()?val.c_str():"x";
              entry += ":";
              // token[2] creation checksum
              val = "";
              char checksumVal[SHA_DIGEST_LENGTH];
              size_t checksumLen;
              memset(checksumVal,0,SHA_DIGEST_LENGTH);
              if (attr->Get("user.eos.checksum", checksumVal, checksumLen)) {
                for (unsigned int i=0; i< SHA_DIGEST_LENGTH; i++) {
                  char hb[3]; sprintf(hb,"%02x", (unsigned char) (checksumVal[i]));
                  val += hb;
                }
              }

              entry += val.length()?val.c_str():"x";
              entry += ":";
              // token[3] tag for file checksum error
              val = attr->Get("user.eos.filecxerror").c_str();
              entry += val.length()?val.c_str():"x";
              entry += ":";
              // token[4] tag for block checksum error
              val = attr->Get("user.eos.blockcxerror").c_str();
              entry += val.length()?val.c_str():"x";
              entry += ":";
              // token[5] tag for physical size
              entry += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)st_buf.st_size);
              entry += ":";
              if (fsid) {
                FmdSqlite* fmd = gFmdSqliteHandler.GetFmd(eos::common::FileId::Hex2Fid(fileId.c_str()), fsid, 0,0,0,0, true);
                if (fmd) {
                  // token[6] size in changelog
                  entry += eos::common::StringConversion::GetSizeString(sizestring, fmd->fMd.size);
                  entry += ":";

		  entry += fmd->fMd.checksum.c_str();
                  delete fmd;
                } else {
                  entry += "x:x:";
                }
              } else {
                entry += "0:0:";
              }
              
              gOFS.OpenFidMutex.Lock();
              if (gOFS.WOpenFid[fsid].count(fileid)) {
                if (gOFS.WOpenFid[fsid][fileid]>0) {
                  isopenforwrite=true;
                }
              }
              gOFS.OpenFidMutex.UnLock();
              // token[8] :1 if it is write-open and :0 if not
              if (isopenforwrite) {
                entry += ":1";
              } else {
                entry += ":0";
              }
              entry += "\n";
              nfound++;
            }
            if (attr)
              delete attr;
          }
        }
      }
      if (nfound)
        break;
    }
  }

  if (nfound==0) 
    return 0;
  else
    return entry.c_str();
}

/*----------------------------------------------------------------------------*/

int
XrdFstOfsDirectory::close()
{
  if (fts_tree) {
    fts_close(fts_tree);
    fts_tree = 0;
  }
  if (fts_paths) {
    free(fts_paths);
    fts_paths=0;
  }
  return SFS_OK;
}
EOSFSTNAMESPACE_END

