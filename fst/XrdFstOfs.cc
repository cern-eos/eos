/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOfs.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "common/Fmd.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/Path.hh"
#include "common/Statfs.hh"
#include "common/Attr.hh"
/*----------------------------------------------------------------------------*/
#include "XrdNet/XrdNetOpts.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysTimer.hh"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

/*----------------------------------------------------------------------------*/
// the global OFS handle
eos::fst::XrdFstOfs eos::fst::gOFS;
// the client admin table
// the capability engine

extern XrdSysError OfsEroute;
extern XrdOssSys  *XrdOfsOss;
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

// All done, we can return the callout vector to these routines.
//
   return &eos::fst::gOFS;
}
}

EOSFSTNAMESPACE_BEGIN

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

  TransferScheduler = new XrdScheduler(8, 128, 60);

  TransferScheduler->Start();

  eos::fst::Config::gConfig.autoBoot = false;

  eos::fst::Config::gConfig.FstOfsBrokerUrl = "root://localhost:1097//eos/";

  eos::fst::Config::gConfig.FstMetaLogDir = "/var/tmp/eos/md/";

  eos::fst::Config::gConfig.FstQuotaReportInterval = 60;

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
	if (!strcmp("symkey",var)) {
	  if ((!(val = Config.GetWord())) || (strlen(val)!=28)) {
	    Eroute.Emsg("Config","argument 2 for symkey missing or length!=28");
	    NoGo=1;
	  } else {
	    if (getenv("EOS_SYM_KEY")) {
	      if (!eos::common::gSymKeyStore.SetKey64(getenv("EOS_SYM_KEY"),0)) {
		Eroute.Emsg("Config","cannot decode your (sysconfig) key and use it in the sym key store!");
		NoGo=1;
	      }
	      Eroute.Say("=====> fstofs.symkey(sysconfig) : ", getenv("EOS_SYM_KEY"));
	    } else {
	      // this key is valid forever ...
	      if (!eos::common::gSymKeyStore.SetKey64(val,0)) {
		Eroute.Emsg("Config","cannot decode your key and use it in the sym key store!");
		NoGo=1;
	      }
	      Eroute.Say("=====> fstofs.symkey : ", val);
	    }
	  }
	}

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

	if (!strcmp("quotainterval",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument 2 for quotainterval missing"); NoGo=1;
	  } else {
	    eos::fst::Config::gConfig.FstQuotaReportInterval = atoi(val);
	    if (eos::fst::Config::gConfig.FstQuotaReportInterval < 10) eos::fst::Config::gConfig.FstQuotaReportInterval = 10;
	    if (eos::fst::Config::gConfig.FstQuotaReportInterval > 3600) eos::fst::Config::gConfig.FstQuotaReportInterval = 3600;
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

  XrdOucString sayquotainterval =""; sayquotainterval += eos::fst::Config::gConfig.FstQuotaReportInterval;
  Eroute.Say("=====> fstofs.quotainterval : ",sayquotainterval.c_str());

  if (! eos::fst::Config::gConfig.FstOfsBrokerUrl.endswith("/")) {
    eos::fst::Config::gConfig.FstOfsBrokerUrl += "/";
  }

  eos::fst::Config::gConfig.FstDefaultReceiverQueue = eos::fst::Config::gConfig.FstOfsBrokerUrl;

  eos::fst::Config::gConfig.FstOfsBrokerUrl += HostName; 
  eos::fst::Config::gConfig.FstOfsBrokerUrl += ":";
  eos::fst::Config::gConfig.FstOfsBrokerUrl += myPort;
  eos::fst::Config::gConfig.FstOfsBrokerUrl += "/fst";

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

  // Set Logging parameters
  XrdOucString unit = "fst@"; unit+= HostName; unit+=":"; unit+=myPort;

  // setup the circular in-memory log buffer
  eos::common::Logging::Init();
  //eos::common::Logging::SetLogPriority(LOG_DEBUG);
  eos::common::Logging::SetLogPriority(LOG_INFO);
  eos::common::Logging::SetUnit(unit.c_str());

  eos_info("logging configured\n");

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

  // setup notification subjects
  ObjectManager.SubjectsMutex.Lock();
  std::string watch_id = "id";
  std::string watch_bootsenttime = "bootsenttime";
  std::string watch_scaninterval = "scaninterval";
  ObjectManager.ModificationWatchKeys.insert(watch_id);
  ObjectManager.ModificationWatchKeys.insert(watch_bootsenttime);
  ObjectManager.ModificationWatchKeys.insert(watch_scaninterval);
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

  // Create a wildcard broadcast 
  ObjectManager.CreateSharedHash(eos::fst::Config::gConfig.FstQueueWildcard.c_str(),eos::fst::Config::gConfig.FstDefaultReceiverQueue.c_str());
  ObjectManager.HashMutex.LockRead();
  XrdMqSharedHash* hash = ObjectManager.GetHash(eos::fst::Config::gConfig.FstQueueWildcard.c_str());

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

  return 0;
}

/*----------------------------------------------------------------------------*/
int          
XrdFstOfsFile::openofs(const char                *path,
		    XrdSfsFileOpenMode   open_mode,
		    mode_t               create_mode,
		    const XrdSecEntity        *client,
		    const char                *opaque)
{
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
    // no capability - go away!
    return gOFS.Emsg(epname,error,caprc,"open - capability illegal",path);
  }

  int envlen;
  //ZTRACE(open,"capability contains: " << capOpaque->Env(envlen));
  eos_info("path=%s info=%s capability=%s", path, opaque, capOpaque->Env(envlen));

  const char* localprefix=0;
  const char* hexfid=0;
  const char* sfsid=0;
  const char* slid=0;
  const char* scid=0;
  const char* smanager=0;
  const char* sbookingsize=0;

  off_t bookingsize = 0;

  fileid=0;
  fsid=0;
  lid=0;
  cid=0;

  if (!(localprefix=capOpaque->Get("mgm.localprefix"))) {
    return gOFS.Emsg(epname,error,EINVAL,"open - no local prefix in capability",path);
  }
  
  if (!(hexfid=capOpaque->Get("mgm.fid"))) {
    return gOFS.Emsg(epname,error,EINVAL,"open - no file id in capability",path);
  }

  if (!(sfsid=capOpaque->Get("mgm.fsid"))) {
    return gOFS.Emsg(epname,error, EINVAL,"open - no file system id in capability",path);
  }

  // if we open a replica we have to take the right filesystem id and filesystem prefix for that replica
  if (openOpaque->Get("mgm.replicaindex")) {
    XrdOucString replicafsidtag="mgm.fsid"; replicafsidtag += (int) atoi(openOpaque->Get("mgm.replicaindex"));
    if (capOpaque->Get(replicafsidtag.c_str())) 
      sfsid=capOpaque->Get(replicafsidtag.c_str());
    XrdOucString replicalocalprefixtag="mgm.localprefix"; replicalocalprefixtag += (int) atoi(openOpaque->Get("mgm.replicaindex"));
    if (capOpaque->Get(replicalocalprefixtag.c_str())) 
      localprefix=capOpaque->Get(replicalocalprefixtag.c_str());
  }
  
  // attention: the localprefix implementation does not work for gateway machines - this needs some modifications
  localPrefix = localprefix;

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

  eos::common::FileId::FidPrefix2FullPath(hexfid, localprefix,fstPath);

  fileid = eos::common::FileId::Hex2Fid(hexfid);

  fsid   = atoi(sfsid);
  lid = atoi(slid);
  cid = strtoull(scid,0,10);

  // extract blocksize from the layout
  fstBlockSize = eos::common::LayoutId::GetBlocksize(lid);

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
      eos_err("forbid to open replica - file %s is opened in RW mode");
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
    openSize = statinfo.st_size;
  }


  // bookingsize is only need for file creation
  if (isRW && isCreation) {
    if (!(sbookingsize=capOpaque->Get("mgm.bookingsize"))) {
      return gOFS.Emsg(epname,error, EINVAL,"open - no booking size in capability",path);
    } else {
      bookingsize = strtoull(capOpaque->Get("mgm.bookingsize"),0,0); 
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
    eos_info("created/got blocklevel checksum\n");
    XrdOucString fstXSPath = fstBlockXS->MakeBlockXSPath(fstPath.c_str());

    if (!fstBlockXS->OpenMap(fstXSPath.c_str(), isCreation?bookingsize:statinfo.st_size,fstBlockSize, isRW)) {
      eos_err("unable to create block checksum file");

      if (lid == eos::common::LayoutId::kReplica) {
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
  fMd = eos::common::gFmdHandler.GetFmd(fileid, fsid, vid.uid, vid.gid, lid, isRW);
  if (!fMd) {
    eos_crit("no fmd for fileid %llu on filesystem %lu", fileid, fsid);
    return gOFS.Emsg(epname,error,EINVAL,"open - unable to get file meta data",path);
  }

  // call the checksum factory function with the selected layout

  if (isRW || (opaqueCheckSum != "ignore")) {
    // we always do checksums for reads if it was not explicitly switched off
    checkSum = eos::fst::ChecksumPlugins::GetChecksumObject(lid);
    eos_debug("checksum requested %d %u", checkSum, lid);
  }

  layOut = eos::fst::LayoutPlugins::GetLayoutObject(this, lid, &error);

  if( !layOut) {
    int envlen;
    eos_err("unable to handle layout for %s", capOpaque->Env(envlen));
    delete fMd;
    return gOFS.Emsg(epname,error,EINVAL,"open - illegal layout specified ",capOpaque->Env(envlen));
  }

  layOut->SetLogId(logId, vid, tident);

  int rc = layOut->open(fstPath.c_str(), open_mode, create_mode, client, stringOpaque.c_str());

  if ( (!rc) && isCreation && bookingsize) {
    rc = layOut->fallocate(bookingsize);
    if (rc) {
      int save_errno=errno;
      eos_crit("file allocation gave return code %d errno=%d for allocation of size=%llu" , rc, errno, bookingsize);
      layOut->remove();
      return gOFS.Emsg(epname,error, save_errno,"open - file allocation failed ",path);
    }
  }

  std::string filecxerror = "0";

  if (!rc) {
    // set the eos lfn as extended attribute
    eos::common::Attr* attr = eos::common::Attr::OpenAttr(layOut->GetLocalReplicaPath());
    if (attr && isRW) {
      if (Path.beginswith("/replicate:")) {
	if (capOpaque->Get("mgm.lfn")) {
	  if (!attr->Set(std::string("user.eos.lfn"), std::string(capOpaque->Get("mgm.lfn")))) {
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
    if (lid == eos::common::LayoutId::kReplica) {
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
      if (gOFS.WOpenFid[fsid][fileid]==0) {
	// this keeps this thread busy for 10 seconds trying to lock and then rebounces if the lock couldn't be taken
	//	if (!gOFS.LockManager.LockTimeout(fileid,10)) {
	//	  // this is cryptic for the moment, we have to review this ===> TODO !!!!
	//	}
      }
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
	  //	  gOFS.Storage->fileSystemsVector[i]->BroadcastError(error.getErrInfo(), "local IO error");
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
	eos_crit("cannot open transaction for fsid=%u fid=%llu", fsid, fileid);
      }
    }
  }
  return rc;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsFile::closeofs()
{
  EPNAME("closeofs");
  int rc = 0;

  // ------------------------------------------------------------------------
  // Code dealing with block checksums
  // ------------------------------------------------------------------------
  if (fstBlockXS) {
    struct stat statinfo;
    if ((XrdOfsOss->Stat(fstPath.c_str(), &statinfo))) {
      rc = gOFS.Emsg(epname,error, EIO, "close - cannot stat closed file to determine file size",Path.c_str());
    } else {
      // check if there is more than one writer in this moment or a reader , if yes, we don't recompute wholes in the checksum and we don't truncate the checksum map, the last single writer will do that
      // ---->
      eos_info("%s wopen=%d ropen=%d fsid=%ld fid=%lld",fstPath.c_str(), (gOFS.WOpenFid[fsid].count(fileid))?gOFS.WOpenFid[fsid][fileid]:0 , (gOFS.ROpenFid[fsid].count(fileid))?gOFS.ROpenFid[fsid][fileid]:0, fsid, fileid);
      gOFS.OpenFidMutex.Lock();
      if ( (gOFS.WOpenFid[fsid].count(fileid) && (gOFS.WOpenFid[fsid][fileid]==1) && ((!gOFS.ROpenFid[fsid].count(fileid)) || (gOFS.ROpenFid[fsid][fileid]==0)))) {
        XrdOucErrInfo error;
        if(!fctl(SFS_FCTL_GETFD,0,error)) {
          int fd = error.getErrInfo();
          if (!fstBlockXS->AddBlockSumHoles(fd)) {
            eos_err("unable to fill holes of block checksum map");
          }
        }
	
	if (isRW) {
	  if (!fstBlockXS->ChangeMap(statinfo.st_size, true)) {
	    eos_err("unable to change block checksum map");
	    rc = SFS_ERROR;
	  } else {
	    eos_info("adjusting block XS map to %llu\n", statinfo.st_size);
	  }
	}
      }	else {
	eos_info("block-xs skipping hole check and changemap nwriter=%d nreader=%d", (gOFS.WOpenFid[fsid].count(fileid))?gOFS.WOpenFid[fsid][fileid]:0 , (gOFS.ROpenFid[fsid].count(fileid))?gOFS.ROpenFid[fsid][fileid]:0);
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
    if (!isRW) {
      // for read's we don't scan the whole file if the file was not read upto the end - we skip checksumming for large files > 64M
      if ( (checkSum->GetLastOffset() != openSize) && ( openSize > (1024*1024*64)) ) {
        eos_info("Skipping checksum (re-scan) for files > 64M ...");
        // remove the checksum object
        delete checkSum;
        checkSum=0;
        return false;
      }
    }
    
    if (checkSum->NeedsRecalculation()) {
      unsigned long long scansize=0;
      float scantime = 0; // is ms
      if (checkSum->ScanFile(fstPath.c_str(), scansize, scantime)) {
        XrdOucString sizestring;
        eos_info("Rescanned checksum - size=%s time=%.02fms rate=%.02f MB %x/s", eos::common::StringConversion::GetReadableSizeString(sizestring, scansize, "B"), scantime, 1.0*scansize/1000/(scantime?scantime:99999999999999LL), checkSum->GetHexChecksum());
      } else {
        eos_err("Rescanning of checksum failed");
      }
    } else {
      // this was prefect streaming I/O
      checkSum->Finalize();
    }

    if (isRW) {
      if (haswrite) {
	// if we have no write, we don't set this attributes (xrd3cp!)
	eos_info("(write) checksum type: %s checksum hex: %s", checkSum->GetName(), checkSum->GetHexChecksum());
	checkSum->GetBinChecksum(checksumlen);
	// copy checksum into meta data
	memcpy(fMd->fMd.checksum, checkSum->GetBinChecksum(checksumlen),checksumlen);
	
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
      eos_info("(read)  checksum type: %s checksum hex: %s", checkSum->GetName(), checkSum->GetHexChecksum());
      checkSum->GetBinChecksum(checksumlen);
      for (int i=0; i<checksumlen ; i++) {
        if (fMd->fMd.checksum[i] != checkSum->GetBinChecksum(checksumlen)[i]) {
          checksumerror=true;
        }
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
 int rc = 0;;
 bool checksumerror=false;
 if ( opened && (!closed) && fMd) {
   eos_info("");

   if (isCreation) {
     // if we had space allocation we have to truncate the allocated space to the real size of the file
     if (layOut) {
       if ( (long long)maxOffsetWritten>(long long)openSize)
         layOut->truncate(maxOffsetWritten);
     }
   }

   checksumerror = verifychecksum();

   // store the entry server information before closing the layout
   bool isEntryServer=false;
   if (layOut->IsEntryServer()) {
     isEntryServer=true;
   }


   if (layOut) {
     rc = layOut->close();
   } else {
     rc = closeofs();
   }

   // first we assume that, if we have writes, we update it
   closeSize = openSize;

   if (haswrite || isCreation ) {
     // commit meta data
     struct stat statinfo;
     if ((XrdOfsOss->Stat(fstPath.c_str(), &statinfo))) {
       rc = gOFS.Emsg(epname,error, EIO, "close - cannot stat closed file to determine file size",Path.c_str());
     } else {
       if ( (statinfo.st_size==0) || haswrite) {
         // update size
         closeSize = statinfo.st_size;
         fMd->fMd.size     = statinfo.st_size;
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
         
         eos::common::Path cPath(capOpaque->Get("mgm.path"));
         if (cPath.GetName())strncpy(fMd->fMd.name,cPath.GetName(),255);
         const char* val =0;
         if ((val = capOpaque->Get("container"))) {
           strncpy(fMd->fMd.container,val,255);
         }
         
         // commit local
         if (!eos::common::gFmdHandler.Commit(fMd))
           rc = gOFS.Emsg(epname,error,EIO,"close - unable to commit meta data",Path.c_str());
         
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

         rc = gOFS.CallManager(&error, capOpaque->Get("mgm.path"),capOpaque->Get("mgm.manager"), capOpaqueFile);

         if ( (rc == -EIDRM) || (rc == -EBADE) || (rc == -EBADR) ) {
           if (!gOFS.Storage->CloseTransaction(fsid, fileid)) {
             eos_crit("cannot close transaction for fsid=%u fid=%llu", fsid, fileid);
           }
	   if (rc == -EIDRM) {
	     // this file has been deleted in the meanwhile ... we can unlink that immedeatly
	     eos_info("unlinking fid=%08x path=%s - file has been already unlinked from the namespace", fMd->fMd.fid, Path.c_str());
	   }
	   if (rc == -EBADE) {
	     eos_err("unlinking fid=%08x path=%s - file size of replica does not match reference", fMd->fMd.fid, Path.c_str());
	   }
	   if (rc == -EBADR) {
	     eos_err("unlinking fid=%08x path=%s - checksum of replica does not match reference", fMd->fMd.fid, Path.c_str());
	   }

           int retc =  gOFS._rem(Path.c_str(), error, 0, capOpaque, fstPath.c_str(), fileid,fsid);
	   if (!retc) {
	     eos_debug("<rem> returned retc=%d", retc);
	   }
           rc = SFS_ERROR;
           
           if (fstBlockXS) {
             // delete also the block checksum file
             fstBlockXS->UnlinkXSPath();
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
   
   closed = true;

   gOFS.OpenFidMutex.Lock();
   if (isRW) 
     gOFS.WOpenFid[fMd->fMd.fsid][fMd->fMd.fid]--;
   else
     gOFS.ROpenFid[fMd->fMd.fsid][fMd->fMd.fid]--;

   if (gOFS.WOpenFid[fMd->fMd.fsid][fMd->fMd.fid] <= 0) {
     // if this was a write of the last writer we had the lock and we release it
     gOFS.LockManager.UnLock(fMd->fMd.fid);
     gOFS.WOpenFid[fMd->fMd.fsid].erase(fMd->fMd.fid);
     gOFS.WOpenFid[fMd->fMd.fsid].resize(0);
   }

   if (gOFS.ROpenFid[fMd->fMd.fsid][fMd->fMd.fid] <= 0) {
     gOFS.ROpenFid[fMd->fMd.fsid].erase(fMd->fMd.fid);
     gOFS.ROpenFid[fMd->fMd.fsid].resize(0);
   }
   gOFS.OpenFidMutex.UnLock();

   gettimeofday(&closeTime,&tz);

   // prepare a report and add to the report queue
   XrdOucString reportString="";
   MakeReportEnv(reportString);
   gOFS.ReportQueueMutex.Lock();
   gOFS.ReportQueue.push(reportString);
   gOFS.ReportQueueMutex.UnLock();
 } 

 if (checksumerror) {
   rc = SFS_ERROR;
   gOFS.Emsg(epname, error, EIO, "verify checksum - checksum error for file fn=", capOpaque->Get("mgm.path"));   
   int envlen=0;
   eos_crit("file-xs error file=%s", capOpaque->Env(envlen));
 }

 
 if (deleteOnClose) {
   eos_info("Deleting on close fn=%s fstpath=%s\n", capOpaque->Get("mgm.path"), fstPath.c_str());
   int retc =  gOFS._rem(Path.c_str(), error, 0, capOpaque, fstPath.c_str(), fileid,fsid);
   if (retc) {
     eos_debug("<rem> returned retc=%d", retc);
   }
   rc = SFS_ERROR;
   
   if (fstBlockXS) {
     // delete also the block checksum file
     fstBlockXS->UnlinkXSPath();
   }
 }

  return rc;
 if (fstBlockXS) {
   delete fstBlockXS;
   fstBlockXS=0;
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
XrdFstOfs::CallManager(XrdOucErrInfo *error, const char* path, const char* manager, XrdOucString &capOpaqueFile) {
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
      eos_debug("commited meta data to cache - %s", capOpaqueFile.c_str());
      rc = SFS_OK;
      break;
      
    case kXR_error:
      if (error) {
	gOFS.Emsg(epname, *error, ECOMM, "commit changed filesize to meta data cache during close of fn=", path);
      }
      msg = (admin->LastServerError()->errmsg);
      rc = SFS_ERROR;

      if (msg.find("[EIDRM]") !=STR_NPOS)
	 rc = -EIDRM;


      if (msg.find("[EBADE]") !=STR_NPOS)
	 rc = -EBADE;


      if (msg.find("[EBADR]") !=STR_NPOS)
	 rc = -EBADR;
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
  return rc;
}

/*----------------------------------------------------------------------------*/
XrdSfsXferSize 
XrdFstOfsFile::readofs(XrdSfsFileOffset   fileOffset,
		       char              *buffer,
		       XrdSfsXferSize     buffer_size)
{
  int retc = XrdOfsFile::read(fileOffset,buffer,buffer_size);

  if (fstBlockXS) {
    if ((retc>0) && (!fstBlockXS->CheckBlockSum(fileOffset, buffer, retc))) {
      int envlen=0;
      eos_crit("block-xs error offset=%llu len=%llu file=%s",(unsigned long long)fileOffset, (unsigned long long)buffer_size,FName(), capOpaque?capOpaque->Env(envlen):FName());
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

  int rc = layOut->read(fileOffset,buffer,buffer_size);

  if ((rc>0) && (checkSum)) {
    checkSum->Add(buffer, rc, fileOffset);
  }

  if (rOffset != (unsigned long long)fileOffset) {
    srBytes += llabs(rOffset-fileOffset);
  }
  
  if (rc > 0) {
    rBytes += rc;
    rOffset += rc;
  }
  
  gettimeofday(&lrTime,&tz);

  AddReadTime();

  if (rc < 0) {
    // here we might take some other action
    int envlen=0;
    eos_crit("block-read error=%d offset=%llu len=%llu file=%s",error.getErrInfo(), (unsigned long long)fileOffset, (unsigned long long)buffer_size,FName(), capOpaque?capOpaque->Env(envlen):FName());      
  }

  eos_debug("rc=%d offset=%lu size=%llu",rc, fileOffset,(unsigned long long) buffer_size);

  if ( (fileOffset + buffer_size) >= openSize) {
    // if this is a read request and it exceeds the limit, we call verify checksum
    if (verifychecksum()) 
      return gOFS.Emsg("read", error, EIO, "read file - wrong file checksum fn=", FName());      
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
  if (fstBlockXS) {
    fstBlockXS->AddBlockSum(fileOffset, buffer, buffer_size);
  }
  
  return XrdOfsFile::write(fileOffset,buffer,buffer_size);
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

  // evt. add checksum
  if ((rc >0) && (checkSum)){
    //    fprintf(stderr,"Adding %d bytes to checksum \n", rc);
    checkSum->Add(buffer, (size_t) rc, (off_t) fileOffset);
    //    fprintf(stderr,"(write) checksum type: %s checksum hex: %s\n", checkSum->GetName(), checkSum->GetHexChecksum());
  } else {
    //    fprintf(stderr,"Write without checksumming\n");
  }

  if (wOffset != (unsigned long long) fileOffset) {
    swBytes += llabs(wOffset-fileOffset);
  }

  if (rc > 0) {
    wBytes += rc;
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
    eos_crit("block-write error=%d offset=%llu len=%llu file=%s",error.getErrInfo(), (unsigned long long)fileOffset, (unsigned long long)buffer_size,FName(), capOpaque?capOpaque->Env(envlen):FName());      
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
    eos_warning("Deletion flag for file %s indicated",fstPath.c_str());
    // this truncate offset indicates to delete the file during the close operation
    deleteOnClose = true;
    return SFS_OK;
  }

  //  fprintf(stderr,"truncate called %llu\n", fileOffset);
  eos_info("(truncate)  openSize=%llu fileOffset=%llu", openSize, fileOffset);
  if (fileOffset != openSize) {
    haswrite = true;
    if (checkSum) {
      checkSum->Reset();
      checkSum->SetDirty();
    }
  }

  return layOut->truncate(fileOffset);
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
		unsigned long          fsid) 
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
    eos_notice("unable to delete file - file does not exist (anymore): %s fstpath=%s fsid=%lu id=%llu", path, fstPath.c_str(),fsid, fid);
    return gOFS.Emsg(epname,error,ENOENT,"delete file - file does not exist",fstPath.c_str());    
  } 
  eos_info("fstpath=%s", fstPath.c_str());

  // unlink file
  errno = 0;
  int rc = XrdOfs::rem(fstPath.c_str(),error,client,0);
  if (rc) {
    eos_info("rc=%d errno=%d", rc,errno);
  }

  // unlink block checksum files
  {
    // this is not the 'best' solution, but we don't have any info about block checksumsa
    Adler xs; // the type does not matter here
    const char* path = xs.MakeBlockXSPath(fstPath.c_str());
    if (!xs.UnlinkXSPath()) {
      eos_info("removed block-xs: %s", path);
    }
  }

  // cleanup eventual transactions
  if (!gOFS.Storage->CloseTransaction(fsid, fid)) {
    // it should be the normal case that there is no open transaction for that file
    int rc = 1;
    rc =1;
  }
  
  if (rc) {
    return rc;
  }

  if (!eos::common::gFmdHandler.DeleteFmd(fid, fsid)) {
    eos_notice("unable to delete fmd for fid %llu on filesystem %lu",fid,fsid);
    return gOFS.Emsg(epname,error,EIO,"delete file meta data ",fstPath.c_str());
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

      struct eos::common::Fmd* fmd = eos::common::gFmdHandler.GetFmd(fileid, fsid, 0, 0, 0, false);

      if (!fmd) {
	eos_static_err("no fmd for fileid %llu on filesystem %lu", fileid, fsid);
	const char* err = "ERROR";
	error.setErrInfo(strlen(err)+1,err);
	return SFS_DATA;
      }
      
      XrdOucEnv* fmdenv = fmd->FmdToEnv();
      int envlen;
      XrdOucString fmdenvstring = fmdenv->Env(envlen);
      delete fmdenv;
      delete fmd;
      error.setErrInfo(fmdenvstring.length()+1,fmdenvstring.c_str());
      return SFS_DATA;
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

  eos_info("calling opendir for %s\n", dirName);
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
		eos::common::Fmd* fmd = eos::common::gFmdHandler.GetFmd(eos::common::FileId::Hex2Fid(fileId.c_str()), fsid, 0,0,0,0);
		if (fmd) {
		  // token[6] size in changelog
		  entry += eos::common::StringConversion::GetSizeString(sizestring, fmd->fMd.size);
		  entry += ":";

		  // token[7] checksum in changelog
		  for (unsigned int i=0; i< SHA_DIGEST_LENGTH; i++) {
		    char hb[3]; sprintf(hb,"%02x", (unsigned char) (fmd->fMd.checksum[i]));
		    entry += hb;
		  }
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

