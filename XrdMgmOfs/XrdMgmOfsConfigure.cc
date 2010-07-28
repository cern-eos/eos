/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <dirent.h>
#include <string.h>
/*----------------------------------------------------------------------------*/
#include "XrdMgmOfs/XrdMgmOfs.hh"
#include "XrdMgmOfs/XrdMgmOfsTrace.hh"
#include "XrdMgmOfs/XrdMgmFstNode.hh"
#include "Namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "Namespace/persistency/ChangeLogFileMDSvc.hh"
#include "Namespace/views/HierarchicalView.hh"
/*----------------------------------------------------------------------------*/
#include "XrdNet/XrdNetDNS.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysPlugin.hh"
#include "XrdCms/XrdCmsFinder.hh"
/*----------------------------------------------------------------------------*/
extern XrdOucTrace gMgmOfsTrace;
/*----------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------*/
int XrdMgmOfs::Configure(XrdSysError &Eroute) 
{
  char *var;
  const char *val;
  int  cfgFD, retc, NoGo = 0;
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"));
  XrdOucString role="server";
  bool authorize=false;
  AuthLib = "";
  Authorization = 0;

  IssueCapability = false;

  MgmOfsTargetPort = "1094";
  MgmOfsName = "";
  MgmOfsBrokerUrl = "root://localhost:1097//eos/";

  MgmConfigDir = "/var/tmp/";
  MgmMetaLogDir = "/var/tmp/eos/md/";

  long myPort=0;

  if (getenv("XRDDEBUG")) gMgmOfsTrace.What = TRACE_MOST | TRACE_debug;

  {
    // borrowed from XrdOfs 
    unsigned int myIPaddr = 0;

    char buff[256], *bp;
    int i;
    
    // Obtain port number we will be using
    //
    myPort = (bp = getenv("XRDPORT")) ? strtol(bp, (char **)0, 10) : 0;
    
    // Establish our hostname and IPV4 address
    //
    HostName      = XrdNetDNS::getHostName();
    
    if (!XrdNetDNS::Host2IP(HostName, &myIPaddr)) myIPaddr = 0x7f000001;
    strcpy(buff, "[::"); bp = buff+3;
    bp += XrdNetDNS::IP2String(myIPaddr, 0, bp, 128);
    *bp++ = ']'; *bp++ = ':';
    sprintf(bp, "%ld", myPort);
    for (i = 0; HostName[i] && HostName[i] != '.'; i++);
    HostName[i] = '\0';
    HostPref = strdup(HostName);
    HostName[i] = '.';
    Eroute.Say("=====> mgmofs.hostname: ", HostName,"");
    Eroute.Say("=====> mgmofs.hostpref: ", HostPref,"");
    ManagerId=HostName;
    ManagerId+=":";
    ManagerId+=(int)myPort;
    Eroute.Say("=====> mgmofs.managerid: ",ManagerId.c_str(),"");
  }

  if( !ConfigFN || !*ConfigFN) {
    Eroute.Emsg("Config", "Configuration file not specified.");
  } else {
    // Try to open the configuration file.
    //
    if ( (cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return Eroute.Emsg("Config", errno, "open config file", ConfigFN);
    Config.Attach(cfgFD);
    // Now start reading records until eof.
    //
    XrdOucString nsin;
    XrdOucString nsout;

    while((var = Config.GetMyFirstWord())) {
      if (!strncmp(var, "all.",4)) {
	var += 4;
	if (!strcmp("role",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for all.role missing.");NoGo=1;
	  } else {
	    XrdOucString lrole = val;

	    if ((val = Config.GetWord())) {
	      if (!strcmp(val,"if")) {
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
	if (!strcmp("fs",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for fs invalid.");NoGo=1;
	  } else {
	    Eroute.Say("=====> mgmofs.fs: ", val,"");
	    MgmOfsName = val;
	  }
	}
	if (!strcmp("targetport",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for fs invalid.");NoGo=1;
	  } else {
	    Eroute.Say("=====> mgmofs.targetport: ", val,"");
	    MgmOfsTargetPort = val;
	  }
	}
      }
      if (!strcmp("capability",var)) {
	if (!(val = Config.GetWord())) {
	  Eroute.Emsg("Config","argument 2 for capbility missing. Can be true/lazy/1 or false/0"); NoGo=1;
	} else {
	  if ( (!(strcmp(val,"true"))) || (!(strcmp(val,"1"))) || (!(strcmp(val,"lazy")))) {
	    IssueCapability = true;
	  } else {
	    if ( (!(strcmp(val,"false"))) || (!(strcmp(val,"0")))) {
	      IssueCapability = false;
	    } else {
	      Eroute.Emsg("Config","argument 2 for capbility invalid. Can be <true>/1 or <false>/0"); NoGo=1;
	    }
	  }
	}
      }

      if (!strcmp("broker",var)) {
	if (!(val = Config.GetWord())) {
	  Eroute.Emsg("Config","argument 2 for broker missing. Should be URL like root://<host>/<queue>/"); NoGo=1;
	} else {
	  MgmOfsBrokerUrl = val;
	}
      }
	  

      if (!strcmp("authlib",var)) {
	if ((!(val = Config.GetWord())) || (::access(val,R_OK))) {
	  Eroute.Emsg("Config","I cannot acccess you authorization library!"); NoGo=1;} else {
	    AuthLib=val;
	  }
	Eroute.Say("=====> mgmofs.authlib : ", AuthLib.c_str());
      }

      if (!strcmp("authorize",var)) {
	if ((!(val = Config.GetWord())) || (strcmp("true",val) && strcmp("false",val) && strcmp("1",val) && strcmp("0",val))) {
	  Eroute.Emsg("Config","argument 2 for authorize illegal or missing. Must be <true>,<false>,<1> or <0>!"); NoGo=1;} else {
	    if ((!strcmp("true",val) || (!strcmp("1",val)))) {
	      authorize = true;
	    }
	  }
	if (authorize)
	  Eroute.Say("=====> mgmofs.authorize : true");
	else
	  Eroute.Say("=====> mgmofs.authorize : false");
      }

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
	  Eroute.Say("=====> mgmofs.symkey : ", val);
	}
      }
      if (!strcmp("configdir",var)) {
	if (!(val = Config.GetWord())) {
	  Eroute.Emsg("Config","argument for configdir invalid.");NoGo=1;
	} else {
	  MgmConfigDir = val;
	  if (!MgmConfigDir.endswith("/")) 
	    MgmConfigDir += "/";
	}
      }
      if (!strcmp("metalog",var)) {
	if (!(val = Config.GetWord())) {
	  Eroute.Emsg("Config","argument 2 for metalog missing"); NoGo=1;
	} else {
	  MgmMetaLogDir = val;
	  // just try to create it in advance
	  XrdOucString makeit="mkdir -p "; makeit+= MgmMetaLogDir; system(makeit.c_str());
	  if (::access(MgmMetaLogDir.c_str(), W_OK|R_OK|X_OK)) {
	    Eroute.Emsg("Config","I cannot acccess the meta data changelog directory for r/w!", MgmMetaLogDir.c_str()); NoGo=1;
	  } else {
	    Eroute.Say("=====> mgmofs.metalog: ", MgmMetaLogDir.c_str(),"");
	  }
	}
      }
      if (!strcmp("trace",var)) {
	static struct traceopts {const char *opname; int opval;} tropts[] =
								   {
								     {"aio",      TRACE_aio},
								     {"all",      TRACE_ALL},
								     {"chmod",    TRACE_chmod},
								     {"close",    TRACE_close},
								     {"closedir", TRACE_closedir},
								     {"debug",    TRACE_debug},
								     {"delay",    TRACE_delay},
								     {"dir",      TRACE_dir},
								     {"exists",   TRACE_exists},
								     {"getstats", TRACE_getstats},
								     {"fsctl",    TRACE_fsctl},
								     {"io",       TRACE_IO},
								     {"mkdir",    TRACE_mkdir},
								     {"most",     TRACE_MOST},
								     {"open",     TRACE_open},
								     {"opendir",  TRACE_opendir},
								     {"qscan",    TRACE_qscan},
								     {"read",     TRACE_read},
								     {"readdir",  TRACE_readdir},
								     {"redirect", TRACE_redirect},
								     {"remove",   TRACE_remove},
								     {"rename",   TRACE_rename},
								     {"sync",     TRACE_sync},
								     {"truncate", TRACE_truncate},
								     {"write",    TRACE_write},
								     {"authorize",TRACE_authorize},
								     {"map",      TRACE_map},
								     {"role",     TRACE_role},
								     {"access",   TRACE_access},
								     {"attributes",TRACE_attributes},
								     {"allows",   TRACE_allows}
								   };
	int i, neg, trval = 0, numopts = sizeof(tropts)/sizeof(struct traceopts);

	if (!(val = Config.GetWord())) {
	  Eroute.Emsg("Config", "trace option not specified"); return 1;
	}

	while (val) {
	  Eroute.Say("=====> mgmofs.trace: ", val,"");
	  if (!strcmp(val, "off")) trval = 0;
	  else {if ((neg = (val[0] == '-' && val[1]))) val++;
	    for (i = 0; i < numopts; i++)
	      {if (!strcmp(val, tropts[i].opname))
		  {if (neg) trval &= ~tropts[i].opval;
		    else  trval |=  tropts[i].opval;
		    break;
		  }
	      }
	    if (i >= numopts)
	      Eroute.Say("Config warning: ignoring invalid trace option '",val,"'.");
	  }
          val = Config.GetWord();
	}

	gMgmOfsTrace.What = trval;
      }
    }
  }

  if (! MgmOfsBrokerUrl.endswith("/")) {
    MgmOfsBrokerUrl += "/";
  }

  MgmDefaultReceiverQueue = MgmOfsBrokerUrl; MgmDefaultReceiverQueue += "*/fst";  

  MgmOfsBrokerUrl += ManagerId; MgmOfsBrokerUrl += "/mgm";

  MgmOfsQueue = "/eos/"; MgmOfsQueue += ManagerId; MgmOfsQueue += "/mgm";

  // setup the circular in-memory logging buffer
  XrdCommonLogging::Init();

  XrdCommonLogging::SetUnit(MgmOfsBrokerUrl.c_str());

  Eroute.Say("=====> mgmofs.broker : ", MgmOfsBrokerUrl.c_str(),"");

 
  int pos1 = MgmDefaultReceiverQueue.find("//");
  int pos2 = MgmDefaultReceiverQueue.find("//",pos1+2);
  if (pos2 != STR_NPOS) {
    MgmDefaultReceiverQueue.erase(0, pos2+1);
  }

  Eroute.Say("=====> mgmofs.defaultreceiverqueue : ", MgmDefaultReceiverQueue.c_str(),"");

  // set our Eroute for XrdMqMessage
  XrdMqMessage::Eroute = *eDest;
  
  // check if mgmofsfs has been set

  if (!MgmOfsName.length()) {
    Eroute.Say("Config error: no mgmofs fs has been defined (mgmofs.fs /...)","","");
  } else {
    Eroute.Say("=====> mgmofs.fs: ",MgmOfsName.c_str(),"");
  } 

  // we need to specify this if the server was not started with the explicit manager option ... e.g. see XrdOfs
  
  Eroute.Say("=====> all.role: ", role.c_str(),"");

  if (role == "manager") {
    putenv((char *)"XRDREDIRECT=R");
  }

  if (( AuthLib != "") && (authorize) ) {
    // load the authorization plugin
    XrdSysPlugin    *myLib;
    XrdAccAuthorize *(*ep)(XrdSysLogger *, const char *, const char *);
    
    // Authorization comes from the library or we use the default
    //
    Authorization = XrdAccAuthorizeObject(Eroute.logger(),ConfigFN,0);

    if (!(myLib = new XrdSysPlugin(&Eroute, AuthLib.c_str()))) {
      Eroute.Emsg("Config","Failed to load authorization library!"); NoGo=1;
    } else {      
      ep = (XrdAccAuthorize *(*)(XrdSysLogger *, const char *, const char *))
	(myLib->getPlugin("XrdAccAuthorizeObject"));
      if (!ep) {
	Eroute.Emsg("Config","Failed to get authorization library plugin!"); NoGo=1;
      } else {
	Authorization = ep(Eroute.logger(), ConfigFN,0);
      }
    }
  }

  if ((retc = Config.LastError())) 
    NoGo = Eroute.Emsg("Config", -retc,"read config file",ConfigFN);
  Config.Close();

  XrdOucString unit = "mgm@"; unit+= ManagerId;

  XrdCommonLogging::SetLogPriority(LOG_DEBUG);
  XrdCommonLogging::SetUnit(unit.c_str());

  // this global hash needs to initialize the set empty key function at first place
  XrdMgmFstNode::gFileSystemById.set_empty_key(0);

  XrdCommonLogging::gFilter = "Process,AddQuota,UpdateHint,SetQuota,UpdateQuotaStatus,SetConfigValue,Deletion";
  Eroute.Say("=====> setting message filter: Process,AddQuota,UpdateHint,SetQuota,UpdateQuotaStatus,SetConfigValue");


  // check config directory access
  if (::access(MgmConfigDir.c_str(), W_OK|R_OK|X_OK)) {
    Eroute.Emsg("Config","I cannot acccess the configuration directory for r/w!", MgmConfigDir.c_str()); NoGo=1;
  } else {
    Eroute.Say("=====> mgmofs.configdir: ", MgmConfigDir.c_str(),"");
    MgmConfigDir = MgmConfigDir.c_str();
  }

  // start the config enging
  ConfigEngine = new XrdMgmConfigEngine(MgmConfigDir.c_str());

  eos_emerg("%s",(char*)"test emerg");
  eos_alert("%s",(char*)"test alert");
  eos_crit("%s", (char*)"test crit");
  eos_err("%s",  (char*)"test err");
  eos_warning("%s",(char*)"test warning");
  eos_notice("%s",(char*)"test notice");
  eos_info("%s",(char*)"test info");
  eos_debug("%s",(char*)"test debug");

  // configure the meta data catalog
  eosDirectoryService = new eos::ChangeLogContainerMDSvc;
  eosFileService      = new eos::ChangeLogFileMDSvc;
  eosView             = new eos::HierarchicalView;
  eosFsView           = new eos::FileSystemView;

  std::map<std::string, std::string> fileSettings;
  std::map<std::string, std::string> contSettings;
  std::map<std::string, std::string> settings;
  std::map<std::string, std::string> fileFsSettings;

  contSettings["changelog_path"] = MgmMetaLogDir.c_str();
  fileSettings["changelog_path"] = MgmMetaLogDir.c_str();
  contSettings["changelog_path"] += "/directories.mdlog";
  fileSettings["changelog_path"] += "/files.mdlog";

  time_t tstart = time(0);
  
  //-------------------------------------------
  try {
    eosFileService->configure( fileSettings );
    eosDirectoryService->configure( contSettings );
    
    eosView->setContainerMDSvc( eosDirectoryService );
    eosView->setFileMDSvc ( eosFileService );
    
    eosView->configure ( settings );
    
    eos_notice("%s",(char*)"eos view configure started");

    eosFileService->addChangeListener( eosFsView );

    eosView->initialize();
    eosFsView->initialize();


    time_t tstop  = time(0);
    eos_notice("eos view configure stopped after %d seconds", (tstop-tstart));
  } catch ( eos::MDException &e ) {
    time_t tstop  = time(0);
    eos_crit("eos view initialization failed after %d seconds", (tstop-tstart));
    errno = e.getErrno();
    eos_crit("initialization returnd ec=%d %s\n", e.getErrno(),e.getMessage().str().c_str());
    return 1;
  };

  // check the '/' directory permissions
  eos::ContainerMD* rootmd;
  try {
    rootmd = eosView->getContainer("/");
  } catch ( eos::MDException &e ) {
    Eroute.Emsg("Config","cannto get the / directory meta data");
    eos_crit("eos view cannot retrieve the / directory");
    return 1;
  }

  if (!rootmd->getMode()) {
    // no permissions set yet
    try {
      rootmd->setMode(S_IFDIR| S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP| S_IWGRP| S_IXGRP | S_ISGID);
    } catch ( eos::MDException &e ) {
      Eroute.Emsg("Config","cannot set the / directory mode to inital mode");
      eos_crit("cannot set the / directory mode to 755");
      return 1;
    }
  }
  eos_info("/ permissions are %o", rootmd->getMode());
  //-------------------------------------------

  // create the specific listener class
  MgmOfsMessaging = new XrdMgmMessaging(MgmOfsBrokerUrl.c_str(),MgmDefaultReceiverQueue.c_str(), true, true);
  MgmOfsMessaging->SetLogId("MgmOfsMessaging");

  if ( (!MgmOfsMessaging) || (MgmOfsMessaging->IsZombie()) ) {
    Eroute.Emsg("Config","cannot create messaging object(thread)");
    return NoGo;
  }

  // create deletion thread
  pthread_t tid;
  eos_info("starting deletion thread");
  if ((XrdSysThread::Run(&tid, XrdMgmOfs::StartMgmDeletion, static_cast<void *>(this),
                              0, "Deletion Thread"))) {
    eos_crit("cannot start deletion thread");
    NoGo = 1;
  }

  eos_info("starting statistics thread");
  if ((XrdSysThread::Run(&tid, XrdMgmOfs::StartMgmStats, static_cast<void *>(this),
                              0, "Statistics Thread"))) {
    eos_crit("cannot start statistics thread");
    NoGo = 1;
  }

  
  return NoGo;
}
/*----------------------------------------------------------------------------*/
