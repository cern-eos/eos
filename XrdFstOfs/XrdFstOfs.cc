/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonFmd.hh"
#include "XrdCommon/XrdCommonFileId.hh"
#include "XrdCommon/XrdCommonFileSystem.hh"
#include "XrdCommon/XrdCommonStatfs.hh"
#include "XrdFstOfs/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/
#include "XrdNet/XrdNetOpts.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysTimer.hh"

class XrdFstOfs;

/*----------------------------------------------------------------------------*/
// the global OFS handle
XrdFstOfs gOFS;
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
   gOFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

   if ( gOFS.Configure(OfsEroute) ) return 0;
// Initialize the target storage system
//
   if (!(XrdOfsOss = (XrdOssSys*) XrdOssGetSS(lp, configfn, gOFS.OssLib))) return 0;

// All done, we can return the callout vector to these routines.
//
   return &gOFS;
}
}

/*----------------------------------------------------------------------------*/
int XrdFstOfs::Configure(XrdSysError& Eroute) 
{
  char *var;
  const char *val;
  int  cfgFD;
  int NoGo=0;
  XrdFstOfsConfig::gConfig.autoBoot = false;

  XrdFstOfsConfig::gConfig.FstOfsBrokerUrl = "root://localhost:1097//eos/";

  XrdFstOfsConfig::gConfig.FstMetaLogDir = "/var/tmp/eos/md/";

  XrdFstOfsConfig::gConfig.FstQuotaReportInterval = 60;

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
	    // this key is valid forever ...
	    if (!gXrdCommonSymKeyStore.SetKey64(val,0)) {
	      Eroute.Emsg("Config","cannot decode your key and use it in the sym key store!");
	      NoGo=1;
	    }
	    Eroute.Say("=====> fstofs.symkey : ", val);
	  }
	}

	if (!strcmp("broker",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument 2 for broker missing. Should be URL like root://<host>/<queue>/"); NoGo=1;
	  } else {
	    XrdFstOfsConfig::gConfig.FstOfsBrokerUrl = val;
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
              XrdFstOfsConfig::gConfig.autoBoot = true;
            }
          }
	}

	if (!strcmp("metalog",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument 2 for metalog missing"); NoGo=1;
	  } else {
	    XrdFstOfsConfig::gConfig.FstMetaLogDir = val;
	  }
	}

	if (!strcmp("quotainterval",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument 2 for quotainterval missing"); NoGo=1;
	  } else {
	    XrdFstOfsConfig::gConfig.FstQuotaReportInterval = atoi(val);
	    if (XrdFstOfsConfig::gConfig.FstQuotaReportInterval < 10) XrdFstOfsConfig::gConfig.FstQuotaReportInterval = 10;
	    if (XrdFstOfsConfig::gConfig.FstQuotaReportInterval > 3600) XrdFstOfsConfig::gConfig.FstQuotaReportInterval = 3600;
	  }
	}
      }
    }
    Config.Close();
  }

  if (XrdFstOfsConfig::gConfig.autoBoot) {
    Eroute.Say("=====> fstofs.autoboot : true");
  } else {
    Eroute.Say("=====> fstofs.autoboot : false");
  }

  XrdOucString sayquotainterval =""; sayquotainterval += XrdFstOfsConfig::gConfig.FstQuotaReportInterval;
  Eroute.Say("=====> fstofs.quotainterval : ",sayquotainterval.c_str());

  if (! XrdFstOfsConfig::gConfig.FstOfsBrokerUrl.endswith("/")) {
    XrdFstOfsConfig::gConfig.FstOfsBrokerUrl += "/";
  }

  XrdFstOfsConfig::gConfig.FstDefaultReceiverQueue = XrdFstOfsConfig::gConfig.FstOfsBrokerUrl;

  XrdFstOfsConfig::gConfig.FstOfsBrokerUrl += HostName; 
  XrdFstOfsConfig::gConfig.FstOfsBrokerUrl += ":";
  XrdFstOfsConfig::gConfig.FstOfsBrokerUrl += myPort;
  XrdFstOfsConfig::gConfig.FstOfsBrokerUrl += "/fst";
  Eroute.Say("=====> fstofs.broker : ", XrdFstOfsConfig::gConfig.FstOfsBrokerUrl.c_str(),"");

  // create the messaging object(recv thread)
  
  XrdFstOfsConfig::gConfig.FstDefaultReceiverQueue += "*/mgm";
  int pos1 = XrdFstOfsConfig::gConfig.FstDefaultReceiverQueue.find("//");
  int pos2 = XrdFstOfsConfig::gConfig.FstDefaultReceiverQueue.find("//",pos1+2);
  if (pos2 != STR_NPOS) {
    XrdFstOfsConfig::gConfig.FstDefaultReceiverQueue.erase(0, pos2+1);
  }

  Eroute.Say("=====> fstofs.defaultreceiverqueue : ", XrdFstOfsConfig::gConfig.FstDefaultReceiverQueue.c_str(),"");
  // set our Eroute for XrdMqMessage
  XrdMqMessage::Eroute = OfsEroute;



  // create the specific listener class
  FstOfsMessaging = new XrdFstMessaging(XrdFstOfsConfig::gConfig.FstOfsBrokerUrl.c_str(),XrdFstOfsConfig::gConfig.FstDefaultReceiverQueue.c_str());

  if ( (!FstOfsMessaging) || (FstOfsMessaging->IsZombie()) ) {
    Eroute.Emsg("Config","cannot create messaging object(thread)");
    NoGo = 1;
  }
  if (NoGo) 
    return NoGo;


  // Set Loggin parameters
  XrdOucString unit = "fst@"; unit+= HostName; unit+=":"; unit+=myPort;

  // setup the circular in-memory log buffer
  XrdCommonLogging::Init();
  XrdCommonLogging::SetLogPriority(LOG_DEBUG);
  XrdCommonLogging::SetUnit(unit.c_str());
  FstOfsMessaging->SetLogId("FstOfsMessaging");

  eos_info("logging configured\n");

  // Attach Storage to the meta log dir
  FstOfsStorage = XrdFstOfsStorage::Create(XrdFstOfsConfig::gConfig.FstMetaLogDir.c_str());
  Eroute.Say("=====> fstofs.metalogdir : ", XrdFstOfsConfig::gConfig.FstMetaLogDir.c_str());
  if (!FstOfsStorage) {
    Eroute.Emsg("Config","cannot setup meta data storage using directory: ", XrdFstOfsConfig::gConfig.FstMetaLogDir.c_str());
    return 1;
  } 

  if (XrdFstOfsConfig::gConfig.autoBoot) {
    XrdFstOfs::AutoBoot();
  }
  
  int rc = XrdOfs::Configure(Eroute);
  return rc;
}

/*----------------------------------------------------------------------------*/
int            
XrdFstOfs::FSctl(int, XrdSfsFSctl&, XrdOucErrInfo &error, const XrdSecEntity*) 
{
  EPNAME("FSctl");
  return XrdFstOfs::Emsg(epname, error, EOPNOTSUPP, "FSctl", "");
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
  char *val=0;
  isRW = false;
  int   retc = SFS_OK;

  Path = path;

  XrdOucString stringOpaque = opaque;
  stringOpaque.replace("?","&");
  stringOpaque.replace("&&","&");

  openOpaque  = new XrdOucEnv(stringOpaque.c_str());
  int caprc = 0;
  
  if ((val = openOpaque->Get("mgm.logid"))) {
    SetLogId(val, tident);
  }

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
  const char* smanager=0;

  unsigned long long fileid=0;
  unsigned long fsid=0;
  unsigned long lid=0;

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


  if (!(slid=capOpaque->Get("mgm.lid"))) {
    return gOFS.Emsg(epname,error, EINVAL,"open - no layout id in capability",path);
  }

  if (!(smanager=capOpaque->Get("mgm.manager"))) {
    return gOFS.Emsg(epname,error, EINVAL,"open - no manager name in capability",path);
  }

  XrdCommonFileId::FidPrefix2FullPath(hexfid, localprefix,fstPath);

  fileid = XrdCommonFileId::Hex2Fid(hexfid);

  fsid   = atoi(sfsid);
  lid = atoi(slid);

  open_mode |= SFS_O_MKPTH;
  create_mode|= SFS_O_MKPTH;

  if ( (open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
		     SFS_O_CREAT  | SFS_O_TRUNC) ) != 0) 
    isRW = true;

  struct stat statinfo;
  if ((retc = XrdOfsOss->Stat(fstPath.c_str(), &statinfo))) {
    // file does not exist, keep the create lfag
    haswrite = true;
  } else {
    if (open_mode & SFS_O_CREAT) 
      open_mode -= SFS_O_CREAT;
  }

  // get the identity

  uid_t sec_uid = 0;
  gid_t sec_gid = 0;
  uid_t sec_ruid = 0;
  gid_t sec_rgid = 0;
  

  if ((val = capOpaque->Get("mgm.uid"))) {
    sec_uid = atoi(val);
  } else {
    return gOFS.Emsg(epname,error,EINVAL,"open - sec uid missing",path);
  }

  if (capOpaque->Get("mgm.gid")) {
    sec_gid = atoi(val);
  } else {
    return gOFS.Emsg(epname,error,EINVAL,"open - sec gid missing",path);
  }

  if ((val = capOpaque->Get("mgm.ruid"))) {
    sec_uid = atoi(val);
  } else {
    return gOFS.Emsg(epname,error,EINVAL,"open - sec ruid missing",path);
  }

  if (capOpaque->Get("mgm.rgid")) {
    sec_gid = atoi(val);
  } else {
    return gOFS.Emsg(epname,error,EINVAL,"open - sec rgid missing",path);
  }

  SetLogId(logId, sec_uid, sec_gid, sec_ruid, sec_rgid, tident);

  eos_info("fstpath=%s", fstPath.c_str());

  // attach meta data
  fMd = gFmdHandler.GetFmd(fileid, fsid, sec_uid, sec_gid, lid, isRW);
  if (!fMd) {
    eos_crit("no fmd for fileid %llu on filesystem %lu", fileid, fsid);
    return gOFS.Emsg(epname,error,EINVAL,"open - unable to get file meta data",path);
  }

  // call the checksum factory function with the selected layout
  if (isRW || ( (val = openOpaque->Get("verifychecksum")) && ( (!strcmp(val,"1")) || (!strcmp(val,"yes")) || (!strcmp(val,"true"))) ) )  {
    checkSum = XrdFstOfsChecksumPlugins::GetChecksumObject(lid);
    eos_debug("checksum requested %d %u", checkSum, lid);
  }

  layOut = XrdFstOfsLayoutPlugins::GetLayoutObject(this, lid, &error);

  if( !layOut) {
    int envlen;
    eos_err("unable to handle layout for %s", capOpaque->Env(envlen));
    return gOFS.Emsg(epname,error,EINVAL,"open - illegal layout specified ",capOpaque->Env(envlen));
  }

  layOut->SetLogId(logId, sec_uid, sec_gid, sec_ruid, sec_rgid, tident);

  int rc = layOut->open(fstPath.c_str(), open_mode, create_mode, client, stringOpaque.c_str());

  if (!rc) {
    return rc;
  }

  return rc;
  }

/*----------------------------------------------------------------------------*/
int
XrdFstOfsFile::closeofs()
{
  return XrdOfsFile::close();
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsFile::close()
{
 EPNAME("close");
 int rc = 0;;
 bool checksumerror=false;
 int checksumlen = 0;
       
 if (!closed && fMd) {
   eos_info("");

   // deal with checksums
   if (checkSum) {
     if (checkSum && checkSum->NeedsRecalculation()) {
       eos_debug("recalculating checksum");
       // re-scan the complete file
       char checksumbuf[128 * 1024];
       checkSum->Reset();
       XrdSfsFileOffset checkoffset=0;
       XrdSfsXferSize   checksize=0;
       XrdSfsFileOffset checklength=0;
       while ((checksize = read(checkoffset,checksumbuf,sizeof(checksumbuf)))>0) {
	 checkSum->Add(checksumbuf, checksize, checkoffset);
	 checklength+= checksize;
	 checkoffset+= checksize;
       }
     } else {
       // this was prefect streaming I/O
       checkSum->Finalize();
     }
   }

   if (checkSum) {
     if (isRW) {
       if (haswrite) {
	 eos_info("(write) checksum type: %s checksum hex: %s", checkSum->GetName(), checkSum->GetHexChecksum());
	 checkSum->GetBinChecksum(checksumlen);
	 // copy checksum into meta data
	 memcpy(fMd->fMd.checksum, checkSum->GetBinChecksum(checksumlen),checksumlen);
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

   if (layOut) {
     rc = layOut->close();
   } else {
     rc = closeofs();
   }

   if (haswrite) {
     // commit meta data
     struct stat statinfo;
     if ((XrdOfsOss->Stat(fstPath.c_str(), &statinfo))) {
       rc = gOFS.Emsg(epname,error, EIO, "close - cannot stat closed file to determine file size",Path.c_str());
     } else {
       // update size
       fMd->fMd.size = statinfo.st_size;
       fMd->fMd.mtime    = statinfo.st_mtime;
       fMd->fMd.mtime_ns = statinfo.st_mtim.tv_nsec;
     }

     // commit local
     if (!gFmdHandler.Commit(fMd))
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
     capOpaqueFile += XrdCommonFileSystem::GetSizeString(mTimeString, fMd->fMd.mtime);
     capOpaqueFile += "&mgm.mtime_ns=";
     capOpaqueFile += XrdCommonFileSystem::GetSizeString(mTimeString, fMd->fMd.mtime_ns);

     capOpaqueFile += "&mgm.add.fsid=";
     capOpaqueFile += (int)fMd->fMd.fsid;

     char result[8192]; result[0]=0;
     int  result_size=8192;
     
     XrdFstOfsClientAdmin* admin = gOFS.FstOfsClientAdminManager.GetAdmin(capOpaque->Get("mgm.manager"));
     if (admin) {
       admin->Lock();
       admin->GetAdmin()->Connect();
       admin->GetAdmin()->GetClientConn()->ClearLastServerError();
       admin->GetAdmin()->Query(kXR_Qopaquf,
		  (kXR_char *) capOpaqueFile.c_str(),
		  (kXR_char *) result, result_size);

       if (!admin->GetAdmin()->LastServerResp()) {
	 gOFS.Emsg(epname, error, ECOMM, "commit changed filesize to meta data cache for fn=", capOpaque->Get("mgm.path"));
	 rc = SFS_ERROR;
       }
       switch (admin->GetAdmin()->LastServerResp()->status) {
       case kXR_ok:
	 eos_debug("commited meta data to cache - %s", capOpaqueFile.c_str());
	 rc = SFS_OK;
	 break;
	 
       case kXR_error:
	 gOFS.Emsg(epname, error, ECOMM, "commit changed filesize to meta data cache during close of fn=", capOpaque->Get("mgm.path"));
	 rc = SFS_ERROR;
	 break;
	 
       default:
	 rc = SFS_OK;
	 break;
       }
       admin->UnLock();
     } else {
       eos_crit("cannot get client admin to execute commit");
       gOFS.Emsg(epname, error, ENOMEM, "allocate client admin object during close of fn=", capOpaque->Get("mgm.path"));
     }
   }
   closed = true;
 } 

 if (checksumerror) {
   rc = SFS_ERROR;
   gOFS.Emsg(epname, error, EIO, "verify checksum - checksum error for file fn=", capOpaque->Get("mgm.path"));   
   int envlen=0;
   eos_crit("checksum error for %s", capOpaque->Env(envlen));
 }

 return rc;
}



/*----------------------------------------------------------------------------*/
XrdSfsXferSize 
XrdFstOfsFile::readofs(XrdSfsFileOffset   fileOffset,
		       char              *buffer,
		       XrdSfsXferSize     buffer_size)
{
  return XrdOfsFile::read(fileOffset,buffer,buffer_size);
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

  int rc = layOut->read(fileOffset,buffer,buffer_size);

  if ((rc>0) && (checkSum)) {
    checkSum->Add(buffer, buffer_size, fileOffset);
  }

  eos_debug("rc=%d offset=%lu size=%llu",rc, fileOffset,buffer_size);
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
  return XrdOfsFile::write(fileOffset,buffer,buffer_size);
}


/*----------------------------------------------------------------------------*/
XrdSfsXferSize
XrdFstOfsFile::write(XrdSfsFileOffset   fileOffset,
		     const char        *buffer,
		     XrdSfsXferSize     buffer_size)
{
  //  EPNAME("write");

  
  int rc = layOut->write(fileOffset,(char*)buffer,buffer_size);

  // evt. add checksum
  if ((rc >0) && (checkSum)){
    checkSum->Add(buffer, (size_t) buffer_size, (off_t) fileOffset);
  }
    
  haswrite = true;
  eos_debug("rc=%d offset=%lu size=%llu",rc, fileOffset,buffer_size);

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
  return XrdOfsFile::truncate(fileOffset);
}

/*----------------------------------------------------------------------------*/
int          
XrdFstOfsFile::truncate(XrdSfsFileOffset   fileOffset)
{
  if (checkSum) checkSum->Reset();

  return layOut->truncate(fileOffset);
}

/*----------------------------------------------------------------------------*/
void
XrdFstMessaging::Listen() 
{
  while(1) {
    XrdMqMessage* newmessage = XrdMqMessaging::gMessageClient.RecvMessage();
    if (newmessage) newmessage->Print();
    if (newmessage) {
      Process(newmessage);
      delete newmessage;
    } else {
      sleep(1);
    }
  }
}

/*----------------------------------------------------------------------------*/
void
XrdFstMessaging::Process(XrdMqMessage* newmessage) 
{
  XrdOucString saction = newmessage->GetBody();

  XrdOucEnv action(saction.c_str());
  
  XrdOucString cmd    = action.Get("mgm.cmd");
  XrdOucString subcmd = action.Get("mgm.subcmd");

  fprintf(stderr, "process got command %s\n", cmd.c_str());
  if (cmd == "fs" && subcmd == "boot") {
    // boot request
    gOFS.Boot(action);
  }

  if (cmd == "debug") {
    gOFS.SetDebug(action);
  }

  if (cmd == "restart") {
    eos_notice("restarting service");
    system("unset XRDPROG XRDCONFIGFN XRDINSTANCE XRDEXPORTS XRDHOST XRDOFSLIB XRDPORT XRDADMINPATH XRDOFSEVENTS XRDNAME XRDREDIRECT; /etc/init.d/xrd restart fst >& /dev/null");
  }

  if (cmd == "rtlog") {
    gOFS.SendRtLog(newmessage);
  }
}


/*----------------------------------------------------------------------------*/
void
XrdFstOfs::Boot(XrdOucEnv &env) 
{
  bool booted=false;
  XrdMqMessage message("fst");
  XrdOucString msgbody="";
  // send booting message
  XrdOucString response ="";
      
  XrdCommonFileSystem::GetBootReplyString(msgbody, env, XrdCommonFileSystem::kBooting);
  message.SetBody(msgbody.c_str());
  
  if (!XrdMqMessaging::gMessageClient.SendMessage(message)) {
    // display communication error
    eos_err("cannot send booting message");
  } else {
    // do boot procedure here
    booted = BootFs(env, response);
  }

  // send boot end message
  if (booted) {
    XrdCommonFileSystem::GetBootReplyString(msgbody, env, XrdCommonFileSystem::kBooted);
    if (response.length()) msgbody+=response;
    eos_info("boot procedure successful!");
  } else {
    XrdCommonFileSystem::GetBootReplyString(msgbody, env, XrdCommonFileSystem::kBootFailure);
    if (response.length()) msgbody+=response;
    eos_err("boot procedure failed!");
  }

  message.NewId();
  message.SetBody(msgbody.c_str());

  if (!XrdMqMessaging::gMessageClient.SendMessage(message)) {
    // display communication error
    eos_err("cannot send booted message");
  }
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfs::SetDebug(XrdOucEnv &env) 
{
   XrdOucString debugnode =  env.Get("mgm.nodename");
   XrdOucString debuglevel = env.Get("mgm.debuglevel");
   XrdOucString filterlist = env.Get("mgm.filter");
   int debugval = XrdCommonLogging::GetPriorityByString(debuglevel.c_str());
   if (debugval<0) {
     eos_err("debug level %s is not known!", debuglevel.c_str());
   } else {
     XrdCommonLogging::SetLogPriority(debugval);
     eos_notice("setting debug level to <%s>", debuglevel.c_str());
     if (filterlist.length()) {
       XrdCommonLogging::SetFilter(filterlist.c_str());
       eos_notice("setting message logid filter to <%s>", filterlist.c_str());
     }
   }
   fprintf(stderr,"Setting debug to %s\n", debuglevel.c_str());
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfs::AutoBoot() 
{
  bool sent=false;
  do {
    XrdOucString msgbody=XrdCommonFileSystem::GetAutoBootRequestString();
    XrdMqMessage message("bootme");
    message.SetBody(msgbody.c_str());
    if (!(sent = XrdMqMessaging::gMessageClient.SendMessage(message))) {
      // display communication error
      eos_warning("failed to send auto boot request message - probably no master online ... retry in 5s ...");
      sleep(5);
    } 
  } while(!sent);
  eos_info("sent autoboot request to %s",  XrdFstOfsConfig::gConfig.FstDefaultReceiverQueue.c_str());
}

/*----------------------------------------------------------------------------*/
bool
XrdFstOfs::BootFs(XrdOucEnv &env, XrdOucString &response) 
{
  eos_info("booting filesystem %s id %s",env.Get("mgm.fspath"), env.Get("mgm.fsid"));

  // try to statfs the filesystem
  XrdCommonStatfs* statfs = XrdCommonStatfs::DoStatfs(env.Get("mgm.fspath"));
  if (!statfs) {
    response += "errmsg=cannot statfs "; response += env.Get("mgm.fspath"); response += " ["; response += strerror(errno); response += "]";response += "&errc="; response += errno; 
    return false;
  }

  // test if we have rw access
  if (access(env.Get("mgm.fspath"),R_OK|W_OK|X_OK)) {
    response += "errmsg=cannot access "; response += env.Get("mgm.fspath"); response += " [no rwx permissions]"; response += "&errc="; response += errno;
    return false;
  }
  response = statfs->GetEnv();

  if (!FstOfsStorage->SetFileSystem(env)) {
    response = "";
    response += "errmsg=cannot configure filesystem [check fst logfile!]"; response += "&errc="; response += EIO;
    return false;
  }
  return true;
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
    if ( (XrdCommonLogging::GetPriorityByString(tag.c_str())) == -1) {
      eos_err("mgm.rtlog.tag must be info,debug,err,emerg,alert,crit,warning or notice");
    } else {
      int logtagindex = XrdCommonLogging::GetPriorityByString(tag.c_str());
      for (int j = 0; j<= logtagindex; j++) {
	XrdCommonLogging::gMutex.Lock();
	for (int i=1; i<= atoi(lines.c_str()); i++) {
	  XrdOucString logline = XrdCommonLogging::gLogMemory[j][(XrdCommonLogging::gLogCircularIndex[j]-i+XrdCommonLogging::gCircularIndexSize)%XrdCommonLogging::gCircularIndexSize].c_str();
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
	XrdCommonLogging::gMutex.UnLock();
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
  int   retc = SFS_OK;


  XrdOucString stringOpaque = opaque;
  stringOpaque.replace("?","&");
  stringOpaque.replace("&&","&");

  XrdOucEnv openOpaque(stringOpaque.c_str());
  XrdOucEnv* capOpaque;
  XrdOucString fstPath="";

  int caprc = 0;
  

  if ((caprc=gCapabilityEngine.Extract(&openOpaque, capOpaque))) {
    // no capability - go away!
    if (capOpaque) delete capOpaque;
    return gOFS.Emsg(epname,error,caprc,"open - capability illegal",path);
  }

  int envlen;
  //ZTRACE(open,"capability contains: " << capOpaque->Env(envlen));
  eos_info("path=%s info=%s capability=%s", path, opaque, capOpaque->Env(envlen));

  const char* localprefix=0;
  const char* hexfid=0;
  const char* sfsid=0;
  const char* slid=0;
  unsigned long long fileid=0;
  unsigned long fsid=0;
  unsigned long lid=0;

  if (!(localprefix=capOpaque->Get("mgm.localprefix"))) {
    if (capOpaque) delete capOpaque;
    return gOFS.Emsg(epname,error,EINVAL,"open - no local prefix in capability",path);
  }
  
  if (!(hexfid=capOpaque->Get("mgm.fid"))) {
    if (capOpaque) delete capOpaque;
    return gOFS.Emsg(epname,error,EINVAL,"open - no file id in capability",path);
  }

  if (!(sfsid=capOpaque->Get("mgm.fsid"))) {
    if (capOpaque) delete capOpaque;
    return gOFS.Emsg(epname,error, EINVAL,"open - no file system id in capability",path);
  }


  XrdCommonFileId::FidPrefix2FullPath(hexfid, localprefix,fstPath);

  fileid = XrdCommonFileId::Hex2Fid(hexfid);

  fsid   = atoi(sfsid);
  lid = atoi(slid);

  struct stat statinfo;
  if ((retc = XrdOfsOss->Stat(fstPath.c_str(), &statinfo))) {
    eos_notice("unable to delete file - file does not exist: %s fstpath=%s fsid=%lu %id=%llu", path, fstPath.c_str(),fsid, fileid);
    if (capOpaque) delete capOpaque;
    return gOFS.Emsg(epname,error,ENOENT,"delete file - file does not exist",fstPath.c_str());    
  } 
  // get the identity

  eos_info("fstpath=%s", fstPath.c_str());

  // attach meta data
  int rc = XrdOfs::rem(fstPath.c_str(),error,client,stringOpaque.c_str());

  if (!rc) {
    if (capOpaque) delete capOpaque;
    return rc;
  }

  if (!gFmdHandler.DeleteFmd(fileid, fsid)) {
    eos_crit("unable to delete fmd for fileid %llu on filesystem %lu",fileid,fsid);
    if (capOpaque) delete capOpaque;
    return gOFS.Emsg(epname,error,EIO,"delete file meta data ",fstPath.c_str());
  }

  if (capOpaque) delete capOpaque;
  return SFS_OK;
}
