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
#include "mgm/Quota.hh"
#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogFileMDSvc.hh"
#include "namespace/views/HierarchicalView.hh"
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

USE_EOSMGMNAMESPACE

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
  MgmOfsAlias = "";
  MgmOfsBrokerUrl = "root://localhost:1097//eos/";
  MgmOfsInstanceName = "testinstance";

  MgmConfigDir = "/var/tmp/";
  MgmMetaLogDir = "/var/tmp/eos/md/";
  MgmHealMap.set_deleted_key(0);

  bool ConfigAutoSave = false;
  XrdOucString ConfigAutoLoad = "";

  long myPort=0;

  setenv("XrdSecPROTOCOL","sss",1);
  Eroute.Say("=====> mgmofs enforces SSS authentication for XROOT clients");

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
	    if (getenv("EOS_BROKER_URL")) {
	      MgmOfsBrokerUrl = getenv("EOS_BROKER_URL");
	    } else {
	      MgmOfsBrokerUrl = val;
	    }
	  }
	}
	
	if (!strcmp("instance", var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument 2 for instance missing. Should be the name of the EOS cluster"); NoGo=1;
	  } else {
	    if (getenv("EOS_INSTANCE_NAME")) {
	      MgmOfsInstanceName = getenv("EOS_INSTANCE_NAME");
	    } else {
	      MgmOfsInstanceName = val;
	    }
	  }
	  Eroute.Say("=====> mgmofs.instance : ", MgmOfsInstanceName.c_str(),"");
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
	    if (getenv("EOS_SYM_KEY")) {
	      if (!eos::common::gSymKeyStore.SetKey64(getenv("EOS_SYM_KEY"),0)) {
		Eroute.Emsg("Config","cannot decode your key and use it in the sym key store!");
		NoGo=1;
	      }
	      Eroute.Say("=====> mgmofs.symkey(sysconfig) : ", getenv("EOS_SYM_KEY"));
	    } else {
	      if (!eos::common::gSymKeyStore.SetKey64(val,0)) {
		Eroute.Emsg("Config","cannot decode your key and use it in the sym key store!");
		NoGo=1;
	      }
	      Eroute.Say("=====> mgmofs.symkey : ", val);
	    }
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
	
	if (!strcmp("autosaveconfig", var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument 2 for autosaveconfig missing. Can be true/1 or false/0"); NoGo=1;
	  } else {
	    if ( (!(strcmp(val,"true"))) || (!(strcmp(val,"1")))) {
	      ConfigAutoSave = true;
	    } else {
	      if ( (!(strcmp(val,"false"))) || (!(strcmp(val,"0")))) {
		ConfigAutoSave = false;
	      } else {
		Eroute.Emsg("Config","argument 2 for autosaveconfig invalid. Can be <true>/1 or <false>/0"); NoGo=1;
	      }
	    }
	  }
	}

	if (!strcmp("autoloadconfig", var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for autoloadconfig invalid.");NoGo=1;
	  } else {
	    ConfigAutoLoad = val;
	  }
	}

	if (!strcmp("alias", var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for alias missing.");NoGo=1;
	  } else {
	    MgmOfsAlias = val;
	  }
	}


	if (!strcmp("metalog",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument 2 for metalog missing"); NoGo=1;
	  } else {
	    MgmMetaLogDir = val;
	    // just try to create it in advance
	    XrdOucString makeit="mkdir -p "; makeit+= MgmMetaLogDir; int src =system(makeit.c_str()); 
	    if (src) 
	      eos_err("%s returned %d", makeit.c_str(), src);
	    XrdOucString chownit="chown -R "; chownit += (int) geteuid(); chownit += " "; chownit += MgmMetaLogDir;
	    src = system(chownit.c_str());
	    if (src)
	      eos_err("%s returned %d", chownit.c_str(), src);
	    
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
  }

  if (! MgmOfsBrokerUrl.endswith("/")) {
    MgmOfsBrokerUrl += "/";
  }

  if (! MgmOfsBrokerUrl.endswith("//eos/")) {
    Eroute.Say("Config error: the broker url has to be of the form <rood://<hostname>[:<port>]//eos");
    return 1;
  }
  
  MgmOfsBroker = MgmOfsBrokerUrl;

  MgmDefaultReceiverQueue = MgmOfsBrokerUrl; MgmDefaultReceiverQueue += "*/fst";  

  MgmOfsBrokerUrl += ManagerId; MgmOfsBrokerUrl += "/mgm";

  MgmOfsQueue = "/eos/"; MgmOfsQueue += ManagerId; MgmOfsQueue += "/mgm";

  // setup the circular in-memory logging buffer
  eos::common::Logging::Init();

  eos::common::Logging::SetUnit(MgmOfsBrokerUrl.c_str());

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

  eos::common::Logging::SetLogPriority(LOG_INFO);
  eos::common::Logging::SetUnit(unit.c_str());

  eos::common::Logging::gFilter = "Process,AddQuota,UpdateHint,UpdateQuotaStatus,SetConfigValue,Deletion,GetQuota,PrintOut,RegisterNode,SharedHash";
  Eroute.Say("=====> setting message filter: Process,AddQuota,UpdateHint,UpdateQuotaStatus,SetConfigValue,Deletion,GetQuota,PrintOut,RegisterNode,SharedHash");

  // we automatically append the host name to the config dir now !!!
  MgmConfigDir += HostName;
  MgmConfigDir += "/";

  XrdOucString makeit="mkdir -p "; makeit+= MgmConfigDir; int src = system(makeit.c_str());
  if (src) 
    eos_err("%s returned %d", makeit.c_str(), src);

  XrdOucString chownit="chown -R "; chownit += (int) geteuid(); chownit += " "; chownit += MgmConfigDir;
  src = system(chownit.c_str());
  if (src)
    eos_err("%s returned %d", chownit.c_str(), src);
  
  // check config directory access
  if (::access(MgmConfigDir.c_str(), W_OK|R_OK|X_OK)) {
    Eroute.Emsg("Config","I cannot acccess the configuration directory for r/w!", MgmConfigDir.c_str()); NoGo=1;
  } else {
    Eroute.Say("=====> mgmofs.configdir: ", MgmConfigDir.c_str(),"");
  }

  // start the config enging
  ConfEngine = new ConfigEngine(MgmConfigDir.c_str());

  if (ConfigAutoSave && (!getenv("EOS_AUTOSAVE_CONFIG"))) {
    Eroute.Say("=====> mgmofs.autosaveconfig: true","");
    ConfEngine->SetAutoSave(true);
  } else {
    if (getenv("EOS_AUTOSAVE_CONFIG")) {
      eos_info("autosave config=%s", getenv("EOS_AUTOSAVE_CONFIG"));
      XrdOucString autosave = getenv("EOS_AUTOSAVE_CONFIG");
      if ( (autosave == "1") || (autosave == "true") ) {
	Eroute.Say("=====> mgmofs.autosaveconfig: true","");
	ConfEngine->SetAutoSave(true);
      } else {
	Eroute.Say("=====> mgmofs.autosaveconfig: false","");
	ConfEngine->SetAutoSave(false);
      }
    } else {
      Eroute.Say("=====> mgmofs.autosaveconfig: false","");
    }
  }

  if (getenv("EOS_MGM_ALIAS")) {
    MgmOfsAlias = getenv("EOS_MGM_ALIAS");
  }

  if (MgmOfsAlias.length()) {
    Eroute.Say("=====> mgmofs.alias: ",MgmOfsAlias.c_str());
    ManagerId=MgmOfsAlias;
    ManagerId+= ":";
    ManagerId+= (int)myPort;
  }

  // create global visible configuration parameters
  // we create 3 queues
  // "/eos/<instance>/
  XrdOucString configbasequeue = "/config/";
  configbasequeue += MgmOfsInstanceName.c_str();

  MgmConfigQueue = configbasequeue; MgmConfigQueue += "/mgm/";
  AllConfigQueue = configbasequeue; AllConfigQueue += "/all/";
  FstConfigQueue = configbasequeue; FstConfigQueue += "/fst/";

  SpaceConfigQueuePrefix = configbasequeue; SpaceConfigQueuePrefix += "/space/";
  NodeConfigQueuePrefix  = configbasequeue; NodeConfigQueuePrefix  += "/node/";
  GroupConfigQueuePrefix = configbasequeue; GroupConfigQueuePrefix += "/group/";

  FsView::gFsView.SetConfigQueues(MgmConfigQueue.c_str(), NodeConfigQueuePrefix.c_str(), GroupConfigQueuePrefix.c_str(), SpaceConfigQueuePrefix.c_str());
  FsView::gFsView.SetConfigEngine(ConfEngine);

  // we need to set the shared object manager to be used
  eos::common::GlobalConfig::gConfig.SetSOM(&ObjectManager);
  
  // setup the modifications which the fs listener thread is waiting for
  ObjectManager.SubjectsMutex.Lock();
  std::string watch_errc = "stat.errc";
  ObjectManager.ModificationWatchKeys.insert(watch_errc);
  ObjectManager.SubjectsMutex.UnLock();
  
  if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(MgmConfigQueue.c_str(), "/eos/*/mgm")) {
    eos_crit("Cannot add global config queue %s\n", MgmConfigQueue.c_str());
  }
  if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(AllConfigQueue.c_str(), "/eos/*")) {
    eos_crit("Cannot add global config queue %s\n", AllConfigQueue.c_str());
  }
  if (!eos::common::GlobalConfig::gConfig.AddConfigQueue(FstConfigQueue.c_str(), "/eos/*/fst")) {
    eos_crit("Cannot add global config queue %s\n", FstConfigQueue.c_str());
  }
  
  std::string out ="";
  eos::common::GlobalConfig::gConfig.PrintBroadCastMap(out);
  fprintf(stderr,"%s",out.c_str());

  // eventuall autoload a configuration 
  if (getenv("EOS_AUTOLOAD_CONFIG")) {
    ConfigAutoLoad = getenv("EOS_AUTOLOAD_CONFIG");
  }

  //  eos_emerg("%s",(char*)"test emerg");
  //  eos_alert("%s",(char*)"test alert");
  //  eos_crit("%s", (char*)"test crit");
  //  eos_err("%s",  (char*)"test err");
  //  eos_warning("%s",(char*)"test warning");
  //  eos_notice("%s",(char*)"test notice");
  //  eos_info("%s",(char*)"test info");
  //  eos_debug("%s",(char*)"test debug");

  // configure the meta data catalog
  eosDirectoryService = new eos::ChangeLogContainerMDSvc;
  eosFileService      = new eos::ChangeLogFileMDSvc;
  eosView             = new eos::HierarchicalView;
  eosFsView           = new eos::FileSystemView;

  std::map<std::string, std::string> fileSettings;
  std::map<std::string, std::string> contSettings;
  std::map<std::string, std::string> settings;

  contSettings["changelog_path"] = MgmMetaLogDir.c_str();
  fileSettings["changelog_path"] = MgmMetaLogDir.c_str();
  contSettings["changelog_path"] += "/directories.";
  fileSettings["changelog_path"] += "/files.";
  contSettings["changelog_path"] += HostName;
  fileSettings["changelog_path"] += HostName;
  contSettings["changelog_path"] += ".mdlog";
  fileSettings["changelog_path"] += ".mdlog";

  MgmNsFileChangeLogFile = fileSettings["changelog_path"].c_str();
  MgmNsDirChangeLogFile  = contSettings["changelog_path"].c_str();

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

    eosView->getQuotaStats()->registerSizeMapper( Quota::MapSizeCB );
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
    Eroute.Emsg("Config","cannot get the / directory meta data");
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

  // create /eos
  eos::ContainerMD* eosmd=0;
  try {
    eosmd = eosView->getContainer("/eos/");
  } catch ( eos::MDException &e ) {
    // nothing in this case
    eosmd = 0;
  }
  
  if (!eosmd) {
    try {
      eosmd = eosView->createContainer( "/eos/", true );
      // set attribute inheritance
      eosmd->setMode(S_IFDIR| S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP| S_IWGRP| S_IXGRP | S_ISGID);
      // set default checksum 'adler'
      eosmd->setAttribute("sys.forced.checksum", "adler");
      eosView->updateContainerStore(eosmd);
      eos_info("/eos permissions are %o checksum is set <adler>", eosmd->getMode());
    } catch ( eos::MDException &e ) {
      Eroute.Emsg("Config","cannot set the /eos/ directory mode to inital mode");
      eos_crit("cannot set the /eos/ directory mode to 755");
      return 1;
    }
  }

  // create /eos/proc
  eosmd=0;
  try {
    XrdOucErrInfo error;
    XrdSfsFileExistence file_exists;
    eosmd = eosView->getContainer("/eos/proc");
    eos::common::Mapping::VirtualIdentity vid;
    eos::common::Mapping::Root(vid);
    eos::FileMD* fmd=0;
    if ((!gOFS->_exists("/eos/proc/whoami",file_exists,error,vid,0))&&(file_exists==XrdSfsFileExistNo))
      fmd = gOFS->eosView->createFile("/eos/proc/whoami",0,0);
    if (fmd) {
      fmd->setSize(4096);
      gOFS->eosView->updateFileStore(fmd);
    }
    if ((!gOFS->_exists("/eos/proc/who",file_exists,error,vid,0))&&(file_exists==XrdSfsFileExistNo))
      fmd = gOFS->eosView->createFile("/eos/proc/who",0,0);
    if (fmd) {
      fmd->setSize(4096);
      gOFS->eosView->updateFileStore(fmd);
    }
    if ((!gOFS->_exists("/eos/proc/quota",file_exists,error,vid,0))&&(file_exists==XrdSfsFileExistNo))
      fmd = gOFS->eosView->createFile("/eos/proc/quota",0,0);
    if (fmd) {
      fmd->setSize(4096);
      gOFS->eosView->updateFileStore(fmd);
    }
    if ((!gOFS->_exists("/eos/proc/reconnect",file_exists,error,vid,0))&&(file_exists==XrdSfsFileExistNo))
      fmd = gOFS->eosView->createFile("/eos/proc/reconnect",0,0);
    if (fmd) {
      fmd->setSize(4096);
      gOFS->eosView->updateFileStore(fmd);
    }
  } catch ( eos::MDException &e ) {
    // nothing in this case
    eosmd = 0;
  }
  
  if (!eosmd) {
    try {
      eosmd = eosView->createContainer( "/eos/proc", true );
      // set attribute inheritance
      eosmd->setMode(S_IFDIR| S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP | S_IXGRP);
      eosView->updateContainerStore(eosmd);
    } catch ( eos::MDException &e ) {
      Eroute.Emsg("Config","cannot set the /eos/proc directory mode to inital mode");
      eos_crit("cannot set the /eos/proc directory mode to 755");
      return 1;
    }
  }

  //-------------------------------------------

  // create the specific listener class
  MgmOfsMessaging = new Messaging(MgmOfsBrokerUrl.c_str(),MgmDefaultReceiverQueue.c_str(), true, true, &ObjectManager);
  if( !MgmOfsMessaging->StartListenerThread() ) NoGo = 1;
  MgmOfsMessaging->SetLogId("MgmOfsMessaging");

  if ( (!MgmOfsMessaging) || (MgmOfsMessaging->IsZombie()) ) {
    Eroute.Emsg("Config","cannot create messaging object(thread)");
    return NoGo;
  }
  
  ObjectManager.CreateSharedHash("/eos/*","/eos/*/fst");
  ObjectManager.HashMutex.LockRead();
  XrdMqSharedHash* hash = ObjectManager.GetHash("/eos/*");


  ObjectManager.HashMutex.UnLockRead();

  XrdOucString dumperfile = MgmMetaLogDir ;
  dumperfile += "/so.mgm.dump";

  ObjectManager.StartDumper(dumperfile.c_str());
  ObjectManager.SetAutoReplyQueueDerive(true);

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

  eos_info("starting fs listener thread");
  if ((XrdSysThread::Run(&tid, XrdMgmOfs::StartMgmFsListener, static_cast<void *>(this),
                              0, "FsListener Thread"))) {
    eos_crit("cannot start fs listener thread");
    NoGo = 1;
  }

  if (ConfigAutoLoad.length()) {
    eos_info("autoload config=%s", ConfigAutoLoad.c_str());
    XrdOucString configloader = "mgm.config.file="; 
    configloader += ConfigAutoLoad;
    XrdOucEnv configenv(configloader.c_str());
    XrdOucString stdErr="";
    if (!ConfEngine->LoadConfig(configenv, stdErr)) {
      eos_crit("Unable to auto-load config %s", ConfigAutoLoad.c_str());
    } else {
      eos_info("Successful auto-load config %s", ConfigAutoLoad.c_str());
    }
  }

  // load all the quota nodes from the namespace
  Quota::LoadNodes();
  // fill the current accounting
  Quota:: NodesToSpaceQuota();

  // add all stat entries with 0

  gOFS->MgmStats.Add("HashSet",0,0,0);
  gOFS->MgmStats.Add("HashSetNoLock",0,0,0);
  gOFS->MgmStats.Add("HashGet",0,0,0);

  gOFS->MgmStats.Add("Access",0,0,0);
  gOFS->MgmStats.Add("AttrGet",0,0,0);
  gOFS->MgmStats.Add("AttrLs",0,0,0);
  gOFS->MgmStats.Add("AttrRm",0,0,0);
  gOFS->MgmStats.Add("AttrSet",0,0,0);
  gOFS->MgmStats.Add("Chmod",0,0,0);
  gOFS->MgmStats.Add("Chown",0,0,0);
  gOFS->MgmStats.Add("Commit",0,0,0);
  gOFS->MgmStats.Add("CommitFailedFid",0,0,0);
  gOFS->MgmStats.Add("CommitFailedNamespace",0,0,0);
  gOFS->MgmStats.Add("CommitFailedParameters",0,0,0);
  gOFS->MgmStats.Add("CommitFailedUnlinked",0,0,0);
  gOFS->MgmStats.Add("CopyStripe",0,0,0);
  gOFS->MgmStats.Add("Statvfs",0,0,0);
  gOFS->MgmStats.Add("DropStripe",0,0,0);
  gOFS->MgmStats.Add("Exists",0,0,0);
  gOFS->MgmStats.Add("Exists",0,0,0);
  gOFS->MgmStats.Add("FileInfo",0,0,0);
  gOFS->MgmStats.Add("FindEntries",0,0,0);
  gOFS->MgmStats.Add("Find",0,0,0);
  gOFS->MgmStats.Add("Fuse",0,0,0);
  gOFS->MgmStats.Add("GetMdLocation",0,0,0);
  gOFS->MgmStats.Add("Ls",0,0,0);
  gOFS->MgmStats.Add("Mkdir",0,0,0);
  gOFS->MgmStats.Add("MoveStripe",0,0,0);
  gOFS->MgmStats.Add("OpenDir",0,0,0);
  gOFS->MgmStats.Add("OpenFailedExists",0,0,0);
  gOFS->MgmStats.Add("OpenFailedHeal",0,0,0);
  gOFS->MgmStats.Add("OpenFailedPermission",0,0,0);
  gOFS->MgmStats.Add("OpenFailedQuota",0,0,0);
  gOFS->MgmStats.Add("OpenFileOffline",0,0,0);
  gOFS->MgmStats.Add("OpenProc",0,0,0);
  gOFS->MgmStats.Add("OpenRead",0,0,0);
  gOFS->MgmStats.Add("OpenStalledHeal",0,0,0);
  gOFS->MgmStats.Add("OpenStalled",0,0,0);
  gOFS->MgmStats.Add("OpenStalled",0,0,0);
  gOFS->MgmStats.Add("Open",0,0,0);
  gOFS->MgmStats.Add("OpenWriteCreate",0,0,0);
  gOFS->MgmStats.Add("OpenWriteTruncate",0,0,0);
  gOFS->MgmStats.Add("OpenWrite",0,0,0);
  gOFS->MgmStats.Add("ReadLink",0,0,0);
  gOFS->MgmStats.Add("RedirectENOENT",0,0,0);
  gOFS->MgmStats.Add("Rename",0,0,0);
  gOFS->MgmStats.Add("RmDir",0,0,0);
  gOFS->MgmStats.Add("Rm",0,0,0);
  gOFS->MgmStats.Add("Stat",0,0,0);
  gOFS->MgmStats.Add("Symlink",0,0,0);
  gOFS->MgmStats.Add("Truncate",0,0,0);
  gOFS->MgmStats.Add("Utimes",0,0,0);
  gOFS->MgmStats.Add("VerifyStripe",0,0,0);
  gOFS->MgmStats.Add("WhoAmI",0,0,0);

  // set IO accounting file
  XrdOucString ioaccounting = MgmMetaLogDir ;
  ioaccounting += "/iostat.";
  ioaccounting += HostName;
  ioaccounting += ".dump";

  eos_notice("Setting IO dump store file to %s", ioaccounting.c_str());
  if (!gOFS->IoStats.SetStoreFileName(ioaccounting.c_str())) {
    eos_warning("couldn't load anything from the io stat dump file %s", ioaccounting.c_str());
  } else {
    eos_notice("loaded io stat dump file %s", ioaccounting.c_str());
  }
  // start IO ciruclate thread
  gOFS->IoStats.StartCirculate();
  // start IO accounting
  gOFS->IoStats.Start();

  if (hash) {
    // ask for a broadcast from fst's
    hash->BroadCastRequest("/eos/*/fst");
  }

  usleep(2000000);
  
  return NoGo;
}
/*----------------------------------------------------------------------------*/
