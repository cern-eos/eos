/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonStringStore.hh"
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdCommon/XrdCommonFileId.hh"
#include "XrdMgmOfs/XrdMgmPolicy.hh"
#include "XrdMgmOfs/XrdMgmVid.hh"
#include "XrdMgmOfs/XrdMgmProcInterface.hh"
#include "XrdMgmOfs/XrdMgmFstNode.hh"
#include "XrdMgmOfs/XrdMgmOfs.hh"
#include "XrdMgmOfs/XrdMgmQuota.hh"
#include "XrdMgmOfs/XrdMgmFstFileSystem.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/

#include <vector>
#include <map>
#include <string>

/*----------------------------------------------------------------------------*/
XrdMgmProcInterface::XrdMgmProcInterface()

{  

}

/*----------------------------------------------------------------------------*/
XrdMgmProcInterface::~XrdMgmProcInterface() 
{

}


/*----------------------------------------------------------------------------*/
bool 
XrdMgmProcInterface::IsProcAccess(const char* path) 
{
  XrdOucString inpath = path;
  if (inpath.beginswith("/proc/")) {
    return true;
  }
  return false;
}

/*----------------------------------------------------------------------------*/
bool 
XrdMgmProcInterface::Authorize(const char* path, const char* info, XrdCommonMapping::VirtualIdentity &vid , const XrdSecEntity* entity) {
  XrdOucString inpath = path;

  // administrator access
  if (inpath.beginswith("/proc/admin/")) {
    // one has to be part of the virtual users 3(adm)/4(adm)
    return ( (XrdCommonMapping::HasUid(3, vid.uid_list)) || (XrdCommonMapping::HasGid(4, vid.gid_list)) );
  }

  // user access
  if (inpath.beginswith("/proc/user/")) {
    return true;
  }
 
  // fst access
  if (inpath.beginswith("/proc/fst/")) {
    return false;
  }

  return false;
}


/*----------------------------------------------------------------------------*/
XrdMgmProcCommand::XrdMgmProcCommand()
{
  stdOut = "";
  stdErr = "";
  retc = 0;
  resultStream = "";
  offset = 0;
  len = 0;
  pVid = 0;
  path = "";
  adminCmd = userCmd = 0;
}

/*----------------------------------------------------------------------------*/
XrdMgmProcCommand::~XrdMgmProcCommand()
{
}

/*----------------------------------------------------------------------------*/
int 
XrdMgmProcCommand::open(const char* inpath, const char* ininfo, XrdCommonMapping::VirtualIdentity &vid_in, XrdOucErrInfo   *error) 
{
  pVid = &vid_in;

  path = inpath;
  bool dosort = false;
  if ( (path.beginswith ("/proc/admin")) ) {
    adminCmd = true;
  } 
  if ( path.beginswith ("/proc/user")) {
    userCmd = true;
  }

  XrdOucEnv opaque(ininfo);
  
  cmd    = opaque.Get("mgm.cmd");
  subcmd = opaque.Get("mgm.subcmd");
    
  stdOut = "";
  stdErr = "";
  retc = 0;
  resultStream = "";
  offset = 0;
  len = 0;

  // admin command section
  if (adminCmd) {
    if (cmd == "config") {
      if (subcmd == "ls") {
	eos_notice("config ls");
	XrdOucString listing="";
	bool showbackup = (bool)opaque.Get("mgm.config.showbackup");
	
	if (!(gOFS->ConfigEngine->ListConfigs(listing, showbackup))) {
	  stdErr += "error: listing of existing configs failed!";
	  retc = errno;
	} else {
	  stdOut += listing;
	}
      }

      int envlen;
      if (subcmd == "load") {
	if (vid_in.uid==0) {
	  eos_notice("config load: %s", opaque.Env(envlen));
	  if (!gOFS->ConfigEngine->LoadConfig(opaque, stdErr)) {
	    retc = errno;
	  } else {
	    stdOut = "success: configuration successfully loaded!";
	  }
	} else {
	  retc = EPERM;
	  stdErr = "error: you have to take role 'root' to execute this command";
	}
      }
      
      if (subcmd == "save") {
	eos_notice("config save: %s", opaque.Env(envlen));
	if (vid_in.uid == 0) {
	  if (!gOFS->ConfigEngine->SaveConfig(opaque, stdErr)) {
	    retc = errno;
	  } else {
	    stdOut = "success: configuration successfully saved!";
	  }
	}  else {
	  retc = EPERM;
	  stdErr = "error: you have to take role 'root' to execute this command";
	}
      }      
      
      if (subcmd == "reset") {
	eos_notice("config reset");
	if (vid_in.uid == 0) {
	  gOFS->ConfigEngine->ResetConfig();
	  stdOut = "success: configuration has been reset(cleaned)!";
	} else {
	  retc = EPERM;
	  stdErr = "error: you have to take role 'root' to execute this command";
	}
      }

      if (subcmd == "dump") {
	eos_notice("config dump");
	XrdOucString dump="";
	if (!gOFS->ConfigEngine->DumpConfig(dump, opaque)) {
	  stdErr += "error: listing of existing configs failed!";
	  retc = errno;
	} else {
	  stdOut += dump;
	  dosort = true;
	}
      }

      if (subcmd == "diff") {
	eos_notice("config diff");
	gOFS->ConfigEngine->Diffs(stdOut);
      }

      if (subcmd == "changelog") {
	int nlines = 5;
	char* val;
	if ((val=opaque.Get("mgm.config.lines"))) {
	  nlines = atoi(val);
	  if (nlines <1) nlines=1;
	}
	gOFS->ConfigEngine->GetChangeLog()->Tail(nlines, stdOut);
	eos_notice("config changelog");
      }

      //      stdOut+="\n==== config done ====";
      MakeResult(dosort);
      return SFS_OK;
    }

    if (cmd == "fs") {
      XrdMgmFstNode::gMutex.Lock();

      if (subcmd == "ls") {
	stdOut += XrdMgmFstNode::GetInfoHeader();
	std::map<std::string,std::string> nodeOutput;
	XrdMgmFstNode::gFstNodes.Apply(XrdMgmFstNode::ListNodes, &nodeOutput);
	//std::sort(nodeOutput.begin(),nodeOutput.end());
	std::map<std::string,std::string>::const_iterator i;
	for (i=nodeOutput.begin(); i!=nodeOutput.end(); ++i) {
	  stdOut += i->second.c_str();
	}
      }

      if (adminCmd) {
	if (subcmd == "dumpmd") {
	  if (vid_in.uid == 0) {
	    char* fsidst = opaque.Get("mgm.fsid");
	    bool dumppath = false;
	    bool dumpfid  = false;
	    XrdOucString dp = opaque.Get("mgm.dumpmd.path");
	    XrdOucString df = opaque.Get("mgm.dumpmd.fid");
	    if (dp == "1") {
	      dumppath = true;
	    } 
	    if (df == "1") {
	      dumpfid = true;
	    }
	      
	    int fsid = 0;
	    
	    if (!fsidst) {
	      stdErr="error: illegal parameters";
	      retc = EINVAL;
	    } else {
	      fsid = atoi(fsidst);	
	      gOFS->eosViewMutex.Lock();
	      try {
		eos::FileMD* fmd = 0;
		eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(fsid);
		eos::FileSystemView::FileIterator it;
		for (it = filelist.begin(); it != filelist.end(); ++it) {
		  std::string env;
		  fmd = gOFS->eosFileService->getFileMD(*it);
		  if (fmd) {
		    if ( (!dumppath) && (!dumpfid) ) {
		      fmd->getEnv(env);
		      stdOut += env.c_str();
		      stdOut += "\n";
		    } else {
		      if (dumppath) {
			std::string fullpath = gOFS->eosView->getUri(fmd);
			stdOut += "path="; stdOut += fullpath.c_str(); 
		      }
		      if (dumpfid) {
			if (dumppath) stdOut += " ";
			char sfid[40]; snprintf(sfid,40, "fid=%llu", (unsigned long long)fmd->getId());
			stdOut += sfid;
		      }
		      stdOut += "\n";
		    }
		  }
		}
	      } catch ( eos::MDException &e ) {
		errno = e.getErrno();
		eos_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
	      }
	      gOFS->eosViewMutex.UnLock();
	      //-------------------------------------------
	    }
	  } else {
	    retc = EPERM;
	    stdErr = "error: you have to take role 'root' to execute this command";
	  }
	}

	if (subcmd == "set") {
	  if (vid_in.uid == 0) {
	    char* fsname = opaque.Get("mgm.fsname");
	    char* fsidst = opaque.Get("mgm.fsid");
	    char* fssched= opaque.Get("mgm.fsschedgroup");
	    bool  fsforce = false;
	    const char* val=0;
	    if((val = opaque.Get("mgm.fsforce"))) {
	      fsforce = atoi(val);
	    }
	    
	    unsigned int fsid = 0;
	    
	    if (!fsname || !fsidst) {
	      stdErr="error: illegal parameters";
	      retc = EINVAL;
	    } else {
	      fsid = atoi(fsidst);
	      // cross check if this is really a number
	      char cfsid[1024]; sprintf(cfsid,"%u",fsid); 
	      
	      if (strcmp(cfsid,fsidst)) {
		stdErr="error: filesystem id="; stdErr += fsidst; stdErr += " is not a positive number! "; stdErr += fsidst;
		retc = EINVAL;
	      } else {
		XrdMgmFstNode::gFstNodes.Apply(XrdMgmFstNode::ExistsNodeFileSystemId, &fsid);
		if (!fsid) {
		  stdErr="error: filesystem id="; stdErr += fsidst; stdErr += " is already in use!";
		  retc = EBUSY;
		} else {
		  if (!XrdMgmFstNode::Update(fsname, fsid, fssched, XrdCommonFileSystem::kDown, 0,0,0,true)) {
		    stdErr="error: cannot set the filesystem information to mgm.fsname="; stdErr += fsname; stdErr += " mgm.fsid=", stdErr += fsidst; stdErr += " mgm.fsschedgroup=" ; stdErr += fssched;
		    retc = EINVAL;
		  } else {
		    stdOut="success: added/set mgm.fsname="; stdOut += fsname; stdOut += " mgm.fsid=", stdOut += fsidst; stdOut += " mgm.fsschedgroup=" ; stdOut += fssched;
		  }
		}
	      }
	    }
	  } else {
	    retc = EPERM;
	    stdErr = "error: you have to take role 'root' to execute this command";
	  }
	}
      }

      if (subcmd == "rm") {
	if (vid_in.uid == 0) {
	  const char* nodename =  opaque.Get("mgm.nodename");
	  const char* fsname   =  opaque.Get("mgm.fsname");
	  const char* fsidst   =  opaque.Get("mgm.fsid");
	  
	  const char* fspath   =  0;
	  
	  XrdOucString splitpathname="";
	  XrdOucString splitnodename="";
	  
	  if (fsname) {
	    XrdOucString q = fsname;
	    int spos = q.find("/fst/");
	    if (spos != STR_NPOS) {
	      splitpathname.assign(q,spos+4);
	      splitnodename.assign(q,0,spos+3);
	      
	      if (!splitpathname.endswith("/")) {
		splitpathname+= "/";
	      }
	      
	      fspath = splitpathname.c_str();
	      nodename = splitnodename.c_str();
	    }
	  }	
	  if (nodename) {
	    // delete by node
	    XrdMgmFstNode* node = XrdMgmFstNode::gFstNodes.Find(nodename);
	    if (node) {
	      if (!fspath) {
		// delete complete node
		XrdMgmFstNode::gFstNodes.Del(nodename);
		stdOut="success: deleted node mgm.nodename="; stdOut += nodename;
	      } else {
		// delete filesystem of a certain node
		if (!node->fileSystems.Del(fspath)) {
		  // success
		  stdOut="success: deleted filesystem from node mgm.nodename=";stdOut += nodename;  stdOut += " and filesystem mgm.fsname="; stdOut += fsname;
		  gOFS->ConfigEngine->DeleteConfigValue("fs",fsname);
		} else {
		  // failed
		  stdErr="error: cannot delete filesystem - no filesystem with name mgm.fsname="; stdErr += fsname; stdErr += " at node mgm.nodename="; stdErr += nodename;	
		  retc = ENOENT;
		}
	      }
	    } else {
	      stdErr="error: cannot delete node - no node with name mgm.nodename="; stdErr += nodename;
	      retc = EINVAL;
	    }
	  } else {
	    if (fsidst) {
	      unsigned int fsid = atoi(fsidst);
	      // delete by fs id
	      XrdMgmFstNode::FindStruct fsfinder(fsid,"");
	      XrdMgmFstNode::gFstNodes.Apply(XrdMgmFstNode::FindNodeFileSystem,&fsfinder);
	      if (fsfinder.found) {
		XrdMgmFstNode* node = XrdMgmFstNode::gFstNodes.Find(fsfinder.nodename.c_str());
		if (node && (!node->fileSystems.Del(fsfinder.fsname.c_str()))) {
		  // success
		  stdOut="success: deleted filesystem from node mgm.nodename=";stdOut += nodename;  stdOut += " and filesystem id mgm.fsid="; stdOut += fsidst;
		} else {
		  // failed
		  stdErr="error: cannot delete filesystem - no filesystem with id mgm.fsid="; stdErr += fsidst; stdErr += " at node mgm.nodename="; stdErr += nodename;		
		  retc = ENOENT;
		}
	      }
	    } 
	  }
	} else {
	  retc = EPERM;
	  stdErr = "error: you have to take role 'root' to execute this command";
	}
      }

      if (subcmd == "config") {
	if (vid_in.uid == 0) {
	  const char* nodename =  opaque.Get("mgm.nodename");
	  const char* fsname   =  opaque.Get("mgm.fsname");
	  const char* fsidst   =  opaque.Get("mgm.fsid");
	  const char* fsconfig =  opaque.Get("mgm.fsconfig");
	  char* fssched= opaque.Get("mgm.fsschedgroup");

	  const char* fspath   =  0;
	  int configstatus = XrdCommonFileSystem::kUnknown;
	  if (fsconfig)
	    configstatus = XrdCommonFileSystem::GetConfigStatusFromString(fsconfig);
	  
	  if (configstatus == XrdCommonFileSystem::kUnknown) {
	    stdErr="error: cannot set the configuration status to the requested status: "; stdErr += fsconfig; ; stdErr += " - this status must be 'rw','wo', 'ro','drain','off'";
	    retc = EINVAL;
	  } else {
	    XrdOucString splitpathname="";
	    XrdOucString splitnodename="";
	    
	    if (fsname) {
	      XrdOucString q = fsname; 
	      int spos = q.find("/fst/");
	      if (spos != STR_NPOS) {
		splitpathname.assign(q,spos+4);
		splitnodename.assign(q,0,spos+3);
		
		if (!splitpathname.endswith("/")) {
		  splitpathname+= "/";
		}
		
		fspath = splitpathname.c_str();
		nodename = splitnodename.c_str();
	      }
	    }	
	    
	    if (nodename) {
	      // set by node
	      XrdMgmFstNode* node = XrdMgmFstNode::gFstNodes.Find(nodename);
	      if (node) {
		if (!fspath) {
		  // set status to all filesystems of a complete node
		  node->SetNodeConfigStatus(configstatus);
		  if (fssched)
		    node->SetNodeConfigSchedulingGroup(fssched);
		  stdOut="success: set config status "; stdOut += fsconfig; stdOut += " at node "; stdOut += nodename;
		} else {
		  // set status for one filesystem of a certain node
		  XrdMgmFstFileSystem* filesystem = node->fileSystems.Find(fspath);
		  if (filesystem) {
		    filesystem->SetConfigStatus(configstatus);
		    if (fssched)
		      filesystem->SetSchedulingGroup(fssched);
		    gOFS->ConfigEngine->SetConfigValue("fs", filesystem->GetQueuePath(), filesystem->GetBootString());
		    // success
		    stdOut="success: set config status "; stdOut += fsconfig; stdOut += " at filesystem ";stdOut += fsname;
		  } else {
		    stdErr="error: cannot set config status on node/filesystem - no filesystem on node "; stdOut += nodename; stdOut += " with path "; stdOut += fspath;
		    retc= ENOENT;
		  }
		}
	      } else {
		stdErr="error: cannot set config status on node - no node with name mgm.nodename="; stdErr += nodename;
		retc = ENOENT;
	      }
	    } else {
	      if (fsidst) {
		unsigned int fsid = atoi(fsidst);
		// set by fs id
		XrdMgmFstNode::FindStruct fsfinder(fsid,"");
		XrdMgmFstNode::gFstNodes.Apply(XrdMgmFstNode::FindNodeFileSystem,&fsfinder);
		if (fsfinder.found) {
		  XrdMgmFstNode* node = XrdMgmFstNode::gFstNodes.Find(fsfinder.nodename.c_str());
		  XrdMgmFstFileSystem* filesystem = 0;
		  if (node) filesystem  = node->fileSystems.Find(fsfinder.fsname.c_str());
		  
		  if (node && filesystem) {
		    filesystem->SetConfigStatus(configstatus);
		    if (fssched)
		      filesystem->SetSchedulingGroup(fssched);
		    gOFS->ConfigEngine->SetConfigValue("fs", filesystem->GetQueuePath(), filesystem->GetBootString());
		    // success
		    stdOut="success: set config status "; stdOut += fsconfig; stdOut += " at filesystem ";stdOut += fsname;
		  } else {
		    // failed
		    stdErr="error: cannot set config status on filesystem - no filesystem with name "; stdErr += fsidst;
		    retc = ENOENT;
		  }
		}
	      } 
	    }
	  }
	} else {
	  retc = EPERM;
	  stdErr = "error: you have to take role 'root' to execute this command";
	}
      }

      if (subcmd == "boot") {
	if (vid_in.uid == 0) {
	  const char* nodename =  opaque.Get("mgm.nodename");
	  const char* fsidst   =  opaque.Get("mgm.fsid");
	  if (nodename && (!strcmp(nodename,"*"))) {
	    XrdOucString bootfs="";
	    // boot all!
	    XrdMgmFstNode::gFstNodes.Apply(XrdMgmFstNode::BootNode, &bootfs);
	    stdOut="success: sent boot message to: \n";stdOut += bootfs;
	  } else {
	    if (nodename) {
	      // boot by node
	      XrdMgmFstNode* node = XrdMgmFstNode::gFstNodes.Find(nodename);
	      
	      if (node) {
		XrdOucString bootfs="";
		// node found
		node->fileSystems.Apply(XrdMgmFstNode::BootFileSystem, &bootfs);
		stdOut="success: sent boot message to mgm.nodename=";stdOut += nodename;  stdOut += " and filesystem mgm.fsname="; stdOut += bootfs;
	      } else {
		// not found
		stdErr="error: cannot boot node - no node with name mgm.nodename="; stdErr += nodename;
		retc= ENOENT;
	      }
	    } else {
	      if (fsidst) {
		// boot by fs id
		unsigned int fsid = atoi(fsidst);
		// delete by fs id
		XrdMgmFstNode::FindStruct fsfinder(fsid,"");
		XrdMgmFstNode::gFstNodes.Apply(XrdMgmFstNode::FindNodeFileSystem,&fsfinder);
		if (fsfinder.found) {
		  XrdMgmFstNode* node = XrdMgmFstNode::gFstNodes.Find(fsfinder.nodename.c_str());
		  XrdMgmFstFileSystem* filesystem=0;
		  if (node && (filesystem = node->fileSystems.Find(fsfinder.fsname.c_str()))) {
		    XrdOucString bootfs="";
		    XrdMgmFstNode::BootFileSystem(fsfinder.fsname.c_str(), filesystem, &bootfs);
		    stdOut="success: sent boot message to mgm.nodename="; stdOut += node->GetQueue(); stdOut+= " mgm.fsid="; stdOut += bootfs;
		  } else {
		    stdErr="error: cannot boot filesystem - no filesystem with id mgm.fsid="; stdErr += fsidst; 
		    retc = ENOENT;
		  }
		} else {
		  stdErr="error: cannot boot filesystem - no filesystem with id mgm.fsid="; stdErr += fsidst; 
		  retc = ENOENT;
		}
	      }
	    }
	  }
	}  else {
	  retc = EPERM;
	  stdErr = "error: you have to take role 'root' to execute this command";
	}
      }
      
      XrdMgmFstNode::gMutex.UnLock();
      
      //      stdOut+="\n==== fs done ====";
    }

    if (cmd == "ns") {
      if (subcmd == "stat") {
	XrdOucString option = opaque.Get("mgm.option");
	bool details=false;
	if (option == "a") 
	  details = true;

	eos_notice("ns stat");
	stdOut+="# ------------------------------------------------------------------------------------\n";
	stdOut+="# Namespace Statistic\n";
	stdOut+="# ------------------------------------------------------------------------------------\n";
	char files[1024]; sprintf(files,"%llu" ,(unsigned long long)gOFS->eosFileService->getNumFiles());
	char dirs[1024];  sprintf(dirs,"%llu"  ,(unsigned long long)gOFS->eosDirectoryService->getNumContainers());
	stdOut+="ALL      Files                            ";stdOut += files; stdOut+="\n";
	stdOut+="ALL      Directories                      ";stdOut += dirs;  stdOut+="\n";
	stdOut+="# ------------------------------------------------------------------------------------\n";

	gOFS->MgmStats.PrintOutTotal(stdOut, details);
      }
    }

    if (cmd == "quota") {
      if (subcmd == "ls") {
	eos_notice("quota ls");
	XrdOucString space = opaque.Get("mgm.quota.space");
	XrdOucString uid_sel = opaque.Get("mgm.quota.uid");
	XrdOucString gid_sel = opaque.Get("mgm.quota.gid");
	
	XrdMgmQuota::PrintOut(space.c_str(), stdOut , uid_sel.length()?atol(uid_sel.c_str()):-1, gid_sel.length()?atol(gid_sel.c_str()):-1);
      }

      if (subcmd == "set") {
	eos_notice("quota set");
	XrdOucString space = opaque.Get("mgm.quota.space");
	XrdOucString uid_sel = opaque.Get("mgm.quota.uid");
	XrdOucString gid_sel = opaque.Get("mgm.quota.gid");
	XrdOucString svolume = opaque.Get("mgm.quota.maxbytes");
	XrdOucString sinodes = opaque.Get("mgm.quota.maxinodes");

	if (uid_sel.length() && gid_sel.length()) {
	  stdErr="error: you either specify a uid or a gid - not both!";
	  retc = EINVAL;
	} else {
	  unsigned long long size   = XrdCommonFileSystem::GetSizeFromString(svolume);
	  if ((svolume.length()) && (errno == EINVAL)) {
	    stdErr="error: the size you specified is not a valid number!";
	    retc = EINVAL;
	  } else {
	    unsigned long long inodes = XrdCommonFileSystem::GetSizeFromString(sinodes);
	    if ((sinodes.length()) && (errno == EINVAL)) {
	      stdErr="error: the inodes you specified are not a valid number!";
	      retc = EINVAL;
	    } else {
	      if ( (!svolume.length())&&(!sinodes.length())  ) {
		stdErr="error: quota set - max. bytes or max. inodes have to be defined!";
		retc = EINVAL;
	      } else {
		XrdOucString msg ="";
		if (!XrdMgmQuota::SetQuota(space, uid_sel.length()?atol(uid_sel.c_str()):-1, gid_sel.length()?atol(gid_sel.c_str()):-1, svolume.length()?size:-1, sinodes.length()?inodes:-1, msg, retc)) {
		stdErr = msg;
		} else {
		  stdOut = msg;
		}
	      }
	    }
	  }
	}
      }

      if (subcmd == "rm") {
	eos_notice("quota rm");
	XrdOucString space = opaque.Get("mgm.quota.space");
	XrdOucString uid_sel = opaque.Get("mgm.quota.uid");
	XrdOucString gid_sel = opaque.Get("mgm.quota.gid");

	XrdOucString msg ="";
	if (!XrdMgmQuota::RmQuota(space, uid_sel.length()?atol(uid_sel.c_str()):-1, gid_sel.length()?atol(gid_sel.c_str()):-1, msg, retc)) {
	  stdErr = msg;
	} else {
	  stdOut = msg;
	}
      }
      //      stdOut+="\n==== quota done ====";
    }

    if (cmd == "debug") {
      if (vid_in.uid == 0) {
	XrdOucString debugnode =  opaque.Get("mgm.nodename");
	XrdOucString debuglevel = opaque.Get("mgm.debuglevel");
	XrdOucString filterlist = opaque.Get("mgm.filter");
	
	XrdMqMessage message("debug");
	int envlen;
	XrdOucString body = opaque.Env(envlen);
	message.SetBody(body.c_str());
	// filter out several *'s ...
	int nstars=0;
	int npos=0;
	while ( (npos=debugnode.find("*",npos)) != STR_NPOS) {npos++;nstars++;}
	if (nstars>1) {
	  stdErr="error: debug level node can only contain one wildcard character (*) !";
	  retc = EINVAL;
	} else {
	  if ((debugnode == "*") || (debugnode == "") || (debugnode == gOFS->MgmOfsQueue)) {
	    // this is for us!
	    int debugval = XrdCommonLogging::GetPriorityByString(debuglevel.c_str());
	    if (debugval<0) {
	      stdErr="error: debug level "; stdErr += debuglevel; stdErr+= " is not known!";
	      retc = EINVAL;
	    } else {
	      XrdCommonLogging::SetLogPriority(debugval);
	      stdOut="success: debug level is now <"; stdOut+=debuglevel.c_str();stdOut += ">";
	      eos_notice("setting debug level to <%s>", debuglevel.c_str());
	      if (filterlist.length()) {
		XrdCommonLogging::SetFilter(filterlist.c_str());
		stdOut+= " filter="; stdOut += filterlist;
		eos_notice("setting message logid filter to <%s>", filterlist.c_str());
	      }
	    }
	  }
	  if (debugnode == "*") {
	    debugnode = "/eos/*/fst";
	    if (!XrdMgmMessaging::gMessageClient.SendMessage(message, debugnode.c_str())) {
	      stdErr="error: could not send debug level to nodes mgm.nodename="; stdErr += debugnode; stdErr += "\n";
	      retc = EINVAL;
	    } else {
	      stdOut="success: switched to mgm.debuglevel="; stdOut += debuglevel; stdOut += " on nodes mgm.nodename="; stdOut += debugnode; stdOut += "\n";
	      eos_notice("forwarding debug level <%s> to nodes mgm.nodename=%s", debuglevel.c_str(), debugnode.c_str());
	    }
	    debugnode = "/eos/*/mgm";
	    if (!XrdMgmMessaging::gMessageClient.SendMessage(message, debugnode.c_str())) {
	      stdErr+="error: could not send debug level to nodes mgm.nodename="; stdErr += debugnode;
	      retc = EINVAL;
	    } else {
	      stdOut+="success: switched to mgm.debuglevel="; stdOut += debuglevel; stdOut += " on nodes mgm.nodename="; stdOut += debugnode;
	      eos_notice("forwarding debug level <%s> to nodes mgm.nodename=%s", debuglevel.c_str(), debugnode.c_str());
	    }
	  } else {
	    if (debugnode != "") {
	      // send to the specified list
	      if (!XrdMgmMessaging::gMessageClient.SendMessage(message, debugnode.c_str())) {
		stdErr="error: could not send debug level to nodes mgm.nodename="; stdErr += debugnode;
		retc = EINVAL;
	      } else {
		stdOut="success: switched to mgm.debuglevel="; stdOut += debuglevel; stdOut += " on nodes mgm.nodename="; stdOut += debugnode;
		eos_notice("forwarding debug level <%s> to nodes mgm.nodename=%s", debuglevel.c_str(), debugnode.c_str());
	      }
	    }
	  }
	}
      //      stdOut+="\n==== debug done ====";
      }  else {
	retc = EPERM;
	stdErr = "error: you have to take role 'root' to execute this command";
      }
    }
    
    if (cmd == "vid") {
      if (subcmd == "ls") {
	eos_notice("vid ls");
	XrdMgmVid::Ls(opaque, retc, stdOut, stdErr);
	dosort = true;
      } 

      if ( ( subcmd == "set" ) || (subcmd == "rm") ) {
	if (vid_in.uid == 0) {
	  if (subcmd == "set") {
	    eos_notice("vid set");
	    XrdMgmVid::Set(opaque, retc, stdOut,stdErr);
	  }
	  
	  
	  if (subcmd == "rm") {
	    eos_notice("vid rm");
	    XrdMgmVid::Rm(opaque, retc, stdOut, stdErr);
	  }
	} else {
	  retc = EPERM;
	  stdErr = "error: you have to take role 'root' to execute this command";
	}
      }
    }

    if (cmd == "restart") {
      if (vid_in.uid == 0) {
	if (subcmd == "fst") {
	  XrdOucString debugnode =  opaque.Get("mgm.nodename");
	  if (( debugnode == "") || (debugnode == "*")) {
	    XrdMqMessage message("mgm"); XrdOucString msgbody="";
	    XrdCommonFileSystem::GetRestartRequestString(msgbody);
	    message.SetBody(msgbody.c_str());
	    
	    // broadcast a global restart message
	    if (XrdMqMessaging::gMessageClient.SendMessage(message, "/eos/*/fst")) {
	      stdOut="success: sent global service restart message to all fst nodes"; 
	    } else {
	      stdErr="error: could not send global fst restart message!";
	      retc = EIO;
	    } 
	  } else {
	    stdErr="error: only global fst restart is supported yet!";
	    retc = EINVAL;
	  } 
	}
      } else {
	retc = EPERM;
	stdErr = "error: you have to take role 'root' to execute this command";
      }
    }

    if (cmd == "droptransfers") {
      if (vid_in.uid == 0) {
	if (subcmd == "fst") {
	  XrdOucString debugnode =  opaque.Get("mgm.nodename");
	  if (( debugnode == "") || (debugnode == "*")) {
	    XrdMqMessage message("mgm"); XrdOucString msgbody="";
	    XrdCommonFileSystem::GetDropTransferRequestString(msgbody);
	    message.SetBody(msgbody.c_str());
	    
	    // broadcast a global drop message
	    if (XrdMqMessaging::gMessageClient.SendMessage(message, "/eos/*/fst")) {
	      stdOut="success: sent global drop transfer message to all fst nodes"; 
	    } else {
	      stdErr="error: could not send global fst drop transfer message!";
	      retc = EIO;
	    } 
	  } else {
	    stdErr="error: only global fst drop transfer is supported yet!";
	    retc = EINVAL;
	  } 
	}
      } else {
	retc = EPERM;
	stdErr = "error: you have to take role 'root' to execute this command";
      }
    }

    if (cmd == "listtransfers") {
      if (vid_in.uid == 0) {
	if (subcmd == "fst") {
	  XrdOucString debugnode =  opaque.Get("mgm.nodename");
	  if (( debugnode == "") || (debugnode == "*")) {
	    XrdMqMessage message("mgm"); XrdOucString msgbody="";
	    XrdCommonFileSystem::GetListTransferRequestString(msgbody);
	    message.SetBody(msgbody.c_str());
	    
	    // broadcast a global list message
	    if (XrdMqMessaging::gMessageClient.SendMessage(message, "/eos/*/fst")) {
	      stdOut="success: sent global list transfer message to all fst nodes"; 
	    } else {
	      stdErr="error: could not send global fst list transfer message!";
	      retc = EIO;
	    } 
	  } else {
	    stdErr="error: only global fst list transfer is supported yet!";
	    retc = EINVAL;
	  } 
	}
      } else {
	retc = EPERM;
	stdErr = "error: you have to take role 'root' to execute this command";
      }
    }
    
    if (cmd == "rtlog") {
      if (vid_in.uid == 0) {
	dosort = 1;
	// this is just to identify a new queue for reach request
	static int bccount=0;
	bccount++;
	XrdOucString queue = opaque.Get("mgm.rtlog.queue");
	XrdOucString lines = opaque.Get("mgm.rtlog.lines");
	XrdOucString tag   = opaque.Get("mgm.rtlog.tag");
	XrdOucString filter = opaque.Get("mgm.rtlog.filter");
	if (!filter.length()) filter = " ";
	if ( (!queue.length()) || (!lines.length()) || (!tag.length()) ) {
	  stdErr = "error: mgm.rtlog.queue, mgm.rtlog.lines, mgm.rtlog.tag have to be given as input paramters!";
	  retc = EINVAL;
	}  else {
	  if ( (XrdCommonLogging::GetPriorityByString(tag.c_str())) == -1) {
	    stdErr = "error: mgm.rtlog.tag must be info,debug,err,emerg,alert,crit,warning or notice";
	    retc = EINVAL;
	  } else {
	    if ((queue==".") || (queue == "*") || (queue == gOFS->MgmOfsQueue)) {
	      int logtagindex = XrdCommonLogging::GetPriorityByString(tag.c_str());
	      for (int j = 0; j<= logtagindex; j++) {
		XrdCommonLogging::gMutex.Lock();
		for (int i=1; i<= atoi(lines.c_str()); i++) {
		  XrdOucString logline = XrdCommonLogging::gLogMemory[j][(XrdCommonLogging::gLogCircularIndex[j]-i+XrdCommonLogging::gCircularIndexSize)%XrdCommonLogging::gCircularIndexSize].c_str();
		  if (logline.length() && ( (logline.find(filter.c_str())) != STR_NPOS)) {
		    stdOut += logline;
		    stdOut += "\n";
		  }
		  if (!logline.length())
		    break;
		}
		XrdCommonLogging::gMutex.UnLock();
	      }
	    }
	    if ( (queue == "*") || ((queue != gOFS->MgmOfsQueue) && (queue != "."))) {
	      XrdOucString broadcastresponsequeue = gOFS->MgmOfsBrokerUrl;
	      broadcastresponsequeue += "-rtlog-";
	      broadcastresponsequeue += bccount;
	      XrdOucString broadcasttargetqueue = gOFS->MgmDefaultReceiverQueue;
	      if (queue != "*") 
		broadcasttargetqueue = queue;
	      
	      int envlen;
	      XrdOucString msgbody;
	      msgbody=opaque.Env(envlen);
	      
	      if (!gOFS->MgmOfsMessaging->BroadCastAndCollect(broadcastresponsequeue,broadcasttargetqueue, msgbody, stdOut, 2)) {
		eos_err("failed to broad cast and collect rtlog from [%s]:[%s]", broadcastresponsequeue.c_str(),broadcasttargetqueue.c_str());
		stdErr = "error: broadcast failed\n";
		retc = EFAULT;
	      }
	    }
	  }
	}
      }  else {
	retc = EPERM;
	stdErr = "error: you have to take role 'root' to execute this command";
      }
    }

    MakeResult(dosort);
    return SFS_OK;
  }

  if (userCmd) {
    if ( cmd == "fuse" ) {
      XrdOucString path = opaque.Get("mgm.path");
      resultStream = "inodirlist: retc=";
      if (!path.length()) {
	resultStream += EINVAL;
      } else {
	XrdMgmOfsDirectory* inodir = (XrdMgmOfsDirectory*)gOFS->newDir((char*)"");
	if (!inodir) {
	  resultStream += ENOMEM;
	  return SFS_ERROR;
	}
	
	if ((retc = inodir->open(path.c_str(),vid,0)) != SFS_OK) {
	  delete inodir;
	  return retc;
	}
	
	const char* entry;
	
	resultStream += 0;
	resultStream += " ";

	unsigned long long inode=0;

	char inodestr[256];

	while ( (entry = inodir->nextEntry() ) ) {
	  XrdOucString whitespaceentry=entry;
	  whitespaceentry.replace(" ","%20");
	  resultStream += whitespaceentry;
	  resultStream += " ";
	  XrdOucString statpath = path;
	  statpath += "/"; statpath += entry;

	  // attach MD to get inode number
	  eos::FileMD* fmd=0;
	  inode = 0;

	  //-------------------------------------------
	  gOFS->eosViewMutex.Lock();
	  try {
	    fmd = gOFS->eosView->getFile(statpath.c_str());
	    inode = fmd->getId() << 28;
	  } catch ( eos::MDException &e ) {
	    errno = e.getErrno();
	    eos_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
	  }
	  gOFS->eosViewMutex.UnLock();
	  //-------------------------------------------

	  // check if that is a directory in case
	  if (!fmd) {
	    eos::ContainerMD* dir=0;
	    //-------------------------------------------
	    gOFS->eosViewMutex.Lock();
	    try {
	      dir = gOFS->eosView->getContainer(statpath.c_str());
	      inode = dir->getId();
	    } catch( eos::MDException &e ) {
	      dir = 0;
	      eos_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
	    }
	    gOFS->eosViewMutex.UnLock();
	    //-------------------------------------------
	  }
	  sprintf(inodestr, "%lld",inode);
	  resultStream += inodestr;
	  resultStream += " ";
	}

	inodir->close();
	delete inodir;
	//	eos_debug("returning resultstream %s", resultStream.c_str());
	len = resultStream.length();
	offset = 0;
	return SFS_OK;
      }
    }


    if ( cmd == "file" ) {
      XrdOucString path = opaque.Get("mgm.path");
      if (!path.length()) {
	stdErr="error: you have to give a path name to call 'file'";
	retc = EINVAL;
      } else {
	if (subcmd == "drop") {
	  XrdOucString sfsid = opaque.Get("mgm.file.fsid");
	  unsigned long fsid = (sfsid.length())?strtoul(sfsid.c_str(),0,10):0;

	  if (gOFS->_dropstripe(path.c_str(),*error, vid, fsid)) {
	    stdErr += "error: unable to drop stripe";
	    retc = errno;
	  } else {
	    stdOut += "success: dropped stripe on fs="; stdOut += (int) fsid;
	  }
	}

	if (subcmd == "layout") {
	  XrdOucString stripes    = opaque.Get("mgm.file.layout.stripes");
	  int newstripenumber = 0;
	  if (stripes.length()) newstripenumber = atoi(stripes.c_str());
	  if (!stripes.length() || ((newstripenumber< (XrdCommonLayoutId::kOneStripe+1)) || (newstripenumber > (XrdCommonLayoutId::kSixteenStripe+1)))) {
	    stdErr="error: you have to give a valid number of stripes as an argument to call 'file layout'";
	    retc = EINVAL;
	  } else {
	    // only root can do that
	    if (vid_in.uid==0) {
	      eos::FileMD* fmd=0;
	      if (path.beginswith("fid:")) {
		path.replace("fid:","");
		unsigned long long fid = strtoull(path.c_str(),0,10);
		
		// reference by fid+fsid
		//-------------------------------------------
		gOFS->eosViewMutex.Lock();
		try {
		  fmd = gOFS->eosFileService->getFileMD(fid);
		} catch ( eos::MDException &e ) {
		  errno = e.getErrno();
		  stdErr = "error: cannot retrieve file meta data - "; stdErr += e.getMessage().str().c_str();
		  eos_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
		}
	      } else {
		// reference by path
		//-------------------------------------------
		gOFS->eosViewMutex.Lock();
		try {
		  fmd = gOFS->eosView->getFile(path.c_str());
		} catch ( eos::MDException &e ) {
		  errno = e.getErrno();
		  stdErr = "error: cannot retrieve file meta data - "; stdErr += e.getMessage().str().c_str();
		  eos_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
		}
	      }
	      
	      if (fmd) {
		if (XrdCommonLayoutId::GetLayoutType(fmd->getLayoutId()) == XrdCommonLayoutId::kReplica) {
		  unsigned long newlayout = XrdCommonLayoutId::GetId(XrdCommonLayoutId::kReplica, XrdCommonLayoutId::GetChecksum(fmd->getLayoutId()), newstripenumber, XrdCommonLayoutId::GetStripeWidth(fmd->getLayoutId()));
		  fmd->setLayoutId(newlayout);
		  stdOut += "success: setting new stripe number to "; stdOut += newstripenumber;
		  // commit new layout
		  gOFS->eosView->updateFileStore(fmd);
		} else {
		  retc = EPERM;
		  stdErr = "error: you can only change the number of stripes for files with replica layout";
		}
	      } else {
		retc = errno;
	      }

	      gOFS->eosViewMutex.UnLock();
	      //-------------------------------------------
	      
	    } else {
	      retc = EPERM;
	      stdErr = "error: you have to take role 'root' to execute this command";
	    }
	  }
	}

	if (subcmd == "move") {
	  XrdOucString sfsidsource = opaque.Get("mgm.file.sourcefsid");
	  unsigned long sourcefsid = (sfsidsource.length())?strtoul(sfsidsource.c_str(),0,10):0;
	  XrdOucString sfsidtarget = opaque.Get("mgm.file.targetfsid");
	  unsigned long targetfsid = (sfsidsource.length())?strtoul(sfsidtarget.c_str(),0,10):0;

	  if (gOFS->_movestripe(path.c_str(),*error, vid, sourcefsid, targetfsid)) {
	    stdErr += "error: unable to move stripe";
	    retc = errno;
	  } else {
	    stdOut += "success: scheduled move from source fs="; stdOut += sfsidsource; stdOut += " => target fs="; stdOut += sfsidtarget;
	  }
	}
	
	if (subcmd == "replicate") {
	  XrdOucString sfsidsource = opaque.Get("mgm.file.sourcefsid");
	  unsigned long sourcefsid = (sfsidsource.length())?strtoul(sfsidsource.c_str(),0,10):0;
	  XrdOucString sfsidtarget = opaque.Get("mgm.file.targetfsid");
	  unsigned long targetfsid = (sfsidtarget.length())?strtoul(sfsidtarget.c_str(),0,10):0;

	  if (gOFS->_copystripe(path.c_str(),*error, vid, sourcefsid, targetfsid)) {
	    stdErr += "error: unable to replicate stripe";
	    retc = errno;
	  } else {
	    stdOut += "success: scheduled replication from source fs="; stdOut += sfsidsource; stdOut += " => target fs="; stdOut += sfsidtarget;
	  }
	}

	if (subcmd == "adjustreplica") {
	  // only root can do that
	  if (vid_in.uid==0) {
	    eos::FileMD* fmd=0;

	    XrdOucString creationspace    = opaque.Get("mgm.file.desiredspace");
	    int icreationsubgroup = -1;

	    if (opaque.Get("mgm.file.desiredsubgroup")) {
	      icreationsubgroup = atoi(opaque.Get("mgm.file.desiredsubgroup"));
	    }

	    if (path.beginswith("fid:")) {
	      path.replace("fid:","");
	      unsigned long long fid = strtoull(path.c_str(),0,10);
	      
	      // reference by fid+fsid
	      //-------------------------------------------
	      gOFS->eosViewMutex.Lock();
	      try {
		fmd = gOFS->eosFileService->getFileMD(fid);
	      } catch ( eos::MDException &e ) {
		errno = e.getErrno();
		stdErr = "error: cannot retrieve file meta data - "; stdErr += e.getMessage().str().c_str();
		eos_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
	      }
	      gOFS->eosViewMutex.UnLock();
	      //-------------------------------------------
	    } else {
	      // reference by path
	      //-------------------------------------------
	      gOFS->eosViewMutex.Lock();
	      try {
		fmd = gOFS->eosView->getFile(path.c_str());
	      } catch ( eos::MDException &e ) {
		errno = e.getErrno();
		stdErr = "error: cannot retrieve file meta data - "; stdErr += e.getMessage().str().c_str();
		eos_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
	      }
	      gOFS->eosViewMutex.UnLock();
	      //-------------------------------------------
	    }
	    
	    XrdOucString space = "default";
	    unsigned int forcedsubgroup = 0;

	    if (fmd) {
	      // check if that is a replica layout at all
	      if (XrdCommonLayoutId::GetLayoutType(fmd->getLayoutId()) == XrdCommonLayoutId::kReplica) {
		// check the configured and available replicas
		
		XrdOucString sizestring;
		
		eos::FileMD::LocationVector::const_iterator lociter;
		int nreplayout = XrdCommonLayoutId::GetStripeNumber(fmd->getLayoutId()) + 1;
		int nrep = (int)fmd->getNumLocation();
		int nreponline=0;
		
		for ( lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter) {
		  XrdMgmFstNode::gMutex.Lock();
		  XrdMgmFstFileSystem* filesystem = (XrdMgmFstFileSystem*) XrdMgmFstNode::gFileSystemById[(int) *lociter];
		  if (filesystem) {
		    if (filesystem && ((filesystem->GetConfigStatus() > XrdCommonFileSystem::kDrain)) &&
			((filesystem->GetBootStatus()   == XrdCommonFileSystem::kBooted))) {
		      // this is a good accessible one
		      nreponline++;
		      // remember the spacename
		      space = filesystem->GetSpaceName();
		      forcedsubgroup = filesystem->GetSchedulingGroupIndex();
		    }
		  }
		  XrdMgmFstNode::gMutex.UnLock();
		}
		
		eos_debug("path=%s nrep=%lu nrep-layout=%lu nrep-online=%lu", path.c_str(), nrep, nreplayout, nreponline);

		if (nreplayout > nreponline) {
		  // set the desired space & subgroup if provided
		  if (creationspace.length()) {
		    space = creationspace;
		  }

		  if (icreationsubgroup) {
		    forcedsubgroup = icreationsubgroup;
		  }

		  // if the space is explicitly set, we don't force into a particular subgroup
		  if (creationspace.length()) {
		    forcedsubgroup = -1;
		  }

		  // we don't have enough replica's online, we trigger asynchronous replication
		  int nnewreplicas = nreplayout - nreponline; // we have to create that much new replica
		  
		  // get the location where we can read that file
		  XrdMgmSpaceQuota* quotaspace = XrdMgmQuota::GetSpaceQuota(space.c_str(),false);
		  eos_debug("creating %d new replicas space=%s subgroup=%d", nnewreplicas, space.c_str(), forcedsubgroup);

		  if (!quotaspace) {
		    stdErr = "error: create new replicas => cannot get space: "; stdErr += space; stdErr += "\n";
		    errno = ENOSPC;
		  } else {
		    unsigned long fsIndex; // this defines the fs to use in the selectefs vector
		    std::vector<unsigned int> selectedfs;
		    // fill the existing locations
		    for ( lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter) {
		      selectedfs.push_back(*lociter);
		    }
		    
		    if (!(errno=quotaspace->FileAccess(vid.uid, vid.gid, (unsigned long)0, space.c_str(), (unsigned long)fmd->getLayoutId(), selectedfs, fsIndex, false))) {
		      // this is now our source filesystem
		      unsigned int sourcefsid = selectedfs[fsIndex];
		      // now the just need to ask for <n> targets
		      int layoutId = XrdCommonLayoutId::GetId(XrdCommonLayoutId::kReplica, XrdCommonLayoutId::kNone, nnewreplicas);
		      
		      // we don't know the container tag here, but we don't really care since we are scheduled as root
		      if (!(errno = quotaspace->FilePlacement(vid.uid, vid.gid, 0 , layoutId, selectedfs, SFS_O_TRUNC, forcedsubgroup))) {
			// yes we got a new replication vector
			for (unsigned int i=0; i< selectedfs.size(); i++) {
			  //			  stdOut += "info: replication := "; stdOut += (int) sourcefsid; stdOut += " => "; stdOut += (int)selectedfs[i]; stdOut += "\n";
			  // add replication here 
			  if (gOFS->_replicatestripe(fmd,*error, vid, sourcefsid, selectedfs[i])) {
			    stdErr += "error: unable to replicate stripe "; stdErr += (int) sourcefsid; stdErr += " => "; stdErr += (int) selectedfs[i]; stdErr += "\n";
			    retc = errno;
			  } else {
			    stdOut += "success: scheduled replication from source fs="; stdOut += (int) sourcefsid; stdOut += " => target fs="; stdOut += (int) selectedfs[i]; stdOut +="\n";
			  }
			}
		      } else {
			stdErr = "error: create new replicas => cannot place replicas: "; stdErr += path; stdErr += "\n";
		      }
		    } else {
		      stdErr = "error: create new replicas => no source available: "; stdErr += path; stdErr += "\n";
		    }
		  }
		}

		// this is magic code to adjust the number of replicas to the desired policy ;-)
		if (nreplayout < nrep) {
		  std::vector<unsigned long> fsid2delete;
		  unsigned int n2delete = nrep-nreplayout;

		  eos::FileMD::LocationVector locvector;
		  // we build three views to sort the order of dropping

		  std::multimap <int /*configstate*/, int /*fsid*/> statemap;
		  std::multimap <std::string /*schedgroup*/, int /*fsid*/> groupmap;
		  std::multimap <std::string /*space*/, int /*fsid*/> spacemap;

		  // we have too many replica's online, we drop (nrepoonline-nreplayout) replicas starting with the lowest configuration state
		  XrdMgmFstNode::gMutex.Lock();

		  eos_debug("trying to drop %d replicas space=%s subgroup=%d", n2delete, creationspace.c_str(), icreationsubgroup);

		  // fill the views
		  for ( lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter) {
		    XrdMgmFstFileSystem* filesystem = (XrdMgmFstFileSystem*) XrdMgmFstNode::gFileSystemById[(int) *lociter];
		    if (filesystem) {
		      unsigned int fsid = filesystem->GetId();
		      statemap.insert(std::pair<int,int>(filesystem->GetConfigStatus(),fsid));
		      groupmap.insert(std::pair<std::string,int>(filesystem->GetSchedulingGroup(),fsid));
		      spacemap.insert(std::pair<std::string,int>(filesystem->GetSpaceName(),fsid));
		    }
		  }
		  XrdMgmFstNode::gMutex.UnLock();

		  
		  if (!creationspace.length()) {
		    // there is no requirement to keep a certain space
		    std::multimap <int, int>::const_iterator sit;
		    for (sit=statemap.begin(); sit!= statemap.end(); ++sit) {
		      fsid2delete.push_back(sit->second);
		      // we add to the deletion vector until we have found enough replicas
		      if (fsid2delete.size() == n2delete)
			break;
		    }
		  } else {
		    if (!icreationsubgroup) {
		      // we have only a space requirement no subgroup required
		      std::multimap <std::string, int>::const_iterator sit;
		      std::multimap <int,int> limitedstatemap;
		      
		      std::string cspace = creationspace.c_str();

		      for (sit=spacemap.begin(); sit != spacemap.end(); ++sit) {
			
			// match the space name
			if (sit->first == cspace) {
			  continue;
			}

			// we default to the highest state for safety reasons
			int state=XrdCommonFileSystem::kRW;
			
			std::multimap <int,int>::const_iterator stateit;
			
			// get the state for each fsid matching
			for (stateit=statemap.begin(); stateit != statemap.end(); stateit++) {
			  if (stateit->second == sit->second) {
			    state = stateit->first;
			    break;
			  }
			}
			
			// fill the map containing only the candidates
			limitedstatemap.insert(std::pair<int,int>(state, sit->second));
		      }

		      std::multimap <int,int>::const_iterator lit;

		      for (lit = limitedstatemap.begin(); lit != limitedstatemap.end(); ++lit) {
			fsid2delete.push_back(lit->second);
			if (fsid2delete.size() == n2delete)
			  break;
		      }
		    } else {
		      // we have a clear requirement on space/subgroup
		      std::multimap <std::string, int>::const_iterator sit;
		      std::multimap <int,int> limitedstatemap;
		      
		      std::string cspace = creationspace.c_str();
		      cspace += "."; cspace += icreationsubgroup;

		      for (sit=groupmap.begin(); sit != groupmap.end(); ++sit) {
			
			// match the space name
			if (sit->first == cspace) {
			  continue;
			}
			

			// we default to the highest state for safety reasons
			int state=XrdCommonFileSystem::kRW;

			std::multimap <int,int>::const_iterator stateit;

			// get the state for each fsid matching
			for (stateit=statemap.begin(); stateit != statemap.end(); stateit++) {
			  if (stateit->second == sit->second) {
			    state = stateit->first;
			    break;
			  }
			}
			
			// fill the map containing only the candidates
			limitedstatemap.insert(std::pair<int,int>(state, sit->second));
		      }
		      
		      std::multimap <int,int>::const_iterator lit;

		      for (lit = limitedstatemap.begin(); lit != limitedstatemap.end(); ++lit) {
			fsid2delete.push_back(lit->second);
			if (fsid2delete.size() == n2delete)
			  break;
		      }
		    }
		  }
		  
		  if (fsid2delete.size() != n2delete) {
		    // add a warning that something does not work as requested ....
		    stdErr = "warning: cannot adjust replicas according to your requirement: space="; stdErr += creationspace; stdErr += " subgroup="; stdErr += icreationsubgroup; stdErr += "\n";
		  }
		  
		  for (unsigned int i = 0 ; i< fsid2delete.size(); i++) {
		    if (fmd->hasLocation(fsid2delete[i])) {
		      //-------------------------------------------
		      gOFS->eosViewMutex.Lock();
		      try {
			fmd->unlinkLocation(fsid2delete[i]);
			gOFS->eosView->updateFileStore(fmd);
			eos_debug("removing location %u", fsid2delete[i]);
			stdOut += "success: dropping replica on fs="; stdOut += (int)fsid2delete[i]; stdOut += "\n";
		      } catch ( eos::MDException &e ) {
			errno = e.getErrno();
			stdErr = "error: drop excess replicas => cannot unlink location - "; stdErr += e.getMessage().str().c_str(); stdErr += "\n";
			eos_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
		      }
		      gOFS->eosViewMutex.UnLock();
		    }
		  }
		}
	      }
	    }
	  } else {
	    retc = EPERM;
	    stdErr = "error: you have to take role 'root' to execute this command";
	  }
	}

	if (subcmd == "place") {
	  // this returns a file system id to place a file/replica
	}

	if (subcmd == "getmdlocation") {
	  // this returns the access urls to query local metadata information
	  XrdOucString path = opaque.Get("mgm.path");
	  
	  if (!path.length()) {
	    stdErr="error: you have to give a path name to call 'fileinfo'";
	    retc = EINVAL;
	  } else {
	    eos::FileMD* fmd=0;
	    
	    //-------------------------------------------
	    gOFS->eosViewMutex.Lock();
	    try {
	      fmd = gOFS->eosView->getFile(path.c_str());
	    } catch ( eos::MDException &e ) {
	      errno = e.getErrno();
	      stdErr = "error: cannot retrieve file meta data - "; stdErr += e.getMessage().str().c_str();
	      eos_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
	    }
	    gOFS->eosViewMutex.UnLock();
	    //-------------------------------------------

	    if (!fmd) {
	      retc = errno;
	    } else {
	      XrdOucString sizestring;
	      
	      eos::FileMD::LocationVector::const_iterator lociter;
	      int i=0;
	      stdOut += "&";
	      stdOut += "mgm.nrep="; stdOut += (int)fmd->getNumLocation(); stdOut += "&";
	      stdOut += "mgm.checksumtype=";stdOut += XrdCommonLayoutId::GetChecksumString(fmd->getLayoutId()); stdOut +="&";
	      stdOut += "mgm.size="; stdOut += XrdCommonFileSystem::GetSizeString(sizestring, fmd->getSize()); stdOut+="&";
	      stdOut += "mgm.checksum="; 
	      for (unsigned int i=0; i< SHA_DIGEST_LENGTH; i++) {
		char hb[3]; sprintf(hb,"%02x", (unsigned char) (fmd->getChecksum().getDataPtr()[i]));
		stdOut += hb;
	      }
	      stdOut += "&";

	      for ( lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter) {
		XrdMgmFstNode::gMutex.Lock();
		XrdMgmFstFileSystem* filesystem = (XrdMgmFstFileSystem*) XrdMgmFstNode::gFileSystemById[(int) *lociter];
		if (filesystem) {
		  XrdOucString host; int port;
		  XrdOucString hostport="";
		  filesystem->GetHostPort(host,port);
		  hostport += host; hostport += ":"; hostport += port;
		  stdOut += "mgm.replica.url";stdOut += i; stdOut += "="; stdOut += hostport; stdOut +="&";
		  XrdOucString hexstring="";
		  XrdCommonFileId::Fid2Hex(fmd->getId(), hexstring);
		  stdOut += "mgm.fid"; stdOut += i; stdOut += "="; stdOut += hexstring;  stdOut += "&";
		  stdOut += "mgm.fsid";stdOut += i; stdOut += "="; stdOut += (int) *lociter; stdOut += "&";
		  stdOut += "mgm.fsbootstat"; stdOut += i; stdOut += "="; stdOut += filesystem->GetBootStatusString(); stdOut += "&";
		} else {
		  stdOut += "NA&";
		}
		i++;
		XrdMgmFstNode::gMutex.UnLock();
	      }
	    }						     
	  }
	}
      }
      MakeResult(dosort);
      return SFS_OK;
    }


    if ( cmd == "fileinfo" ) {
      XrdOucString path = opaque.Get("mgm.path");
      if (!path.length()) {
	stdErr="error: you have to give a path name to call 'fileinfo'";
	retc = EINVAL;
      } else {
	eos::FileMD* fmd=0;

	//-------------------------------------------
	gOFS->eosViewMutex.Lock();
	try {
	  fmd = gOFS->eosView->getFile(path.c_str());
	} catch ( eos::MDException &e ) {
	  fmd = 0;
	  errno = e.getErrno();
	  stdErr = "error: cannot retrieve file meta data - "; stdErr += e.getMessage().str().c_str();
	  eos_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
	}
	gOFS->eosViewMutex.UnLock();
	//-------------------------------------------

	if (!fmd) {
	  retc = errno;
	} else {
	  XrdOucString sizestring;
	  char ctimestring[4096];
	  char mtimestring[4096];
	  eos::FileMD::ctime_t mtime;
	  eos::FileMD::ctime_t ctime;
	  fmd->getCTime(ctime);
	  fmd->getMTime(mtime);
	  time_t filectime = (time_t) ctime.tv_sec;
	  time_t filemtime = (time_t) mtime.tv_sec;

	  stdOut  = "  File: '"; stdOut += path; stdOut += "'";
	  stdOut += "  Size: "; stdOut += XrdCommonFileSystem::GetSizeString(sizestring, fmd->getSize()); stdOut+="\n";
	  stdOut += "Modify: "; stdOut += ctime_r(&filectime, mtimestring); stdOut.erase(stdOut.length()-1); stdOut += " Timestamp: ";stdOut += XrdCommonFileSystem::GetSizeString(sizestring, mtime.tv_sec); stdOut += "."; stdOut += XrdCommonFileSystem::GetSizeString(sizestring, mtime.tv_nsec); stdOut += "\n";
	  stdOut += "Change: "; stdOut += ctime_r(&filemtime, ctimestring); stdOut.erase(stdOut.length()-1); stdOut += " Timestamp: ";stdOut += XrdCommonFileSystem::GetSizeString(sizestring, ctime.tv_sec); stdOut += "."; stdOut += XrdCommonFileSystem::GetSizeString(sizestring, ctime.tv_nsec);stdOut += "\n";
	  stdOut += "  CUid: "; stdOut += (int)fmd->getCUid(); stdOut += " CGid: "; stdOut += (int)fmd->getCGid();
	  
	  stdOut += "   Fid: "; XrdCommonFileId::Fid2Hex(fmd->getId(),sizestring); stdOut += sizestring; stdOut+=" ";
	  stdOut += "   Pid: "; stdOut += XrdCommonFileSystem::GetSizeString(sizestring, fmd->getContainerId()); stdOut+="\n";
	  stdOut += "XStype: "; stdOut += XrdCommonLayoutId::GetChecksumString(fmd->getLayoutId());
	  stdOut += "    XS: "; 
	  for (unsigned int i=0; i< SHA_DIGEST_LENGTH; i++) {
	    char hb[3]; sprintf(hb,"%02x ", (unsigned char) (fmd->getChecksum().getDataPtr()[i]));
	    stdOut += hb;
	  }
	  stdOut+="\n";
	  stdOut +  "Layout: "; stdOut += XrdCommonLayoutId::GetLayoutTypeString(fmd->getLayoutId()); stdOut += " Stripes: "; stdOut += (int)(XrdCommonLayoutId::GetStripeNumber(fmd->getLayoutId())+1);
	  stdOut += " *******\n";
	  stdOut += "  #Rep: "; stdOut += (int)fmd->getNumLocation(); stdOut+="\n";

	  stdOut += "<#> <fs-id> "; stdOut += XrdMgmFstFileSystem::GetInfoHeader();
	  stdOut += "-------\n";
	  eos::FileMD::LocationVector::const_iterator lociter;
	  int i=0;
	  for ( lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter) {
	    char fsline[4096];
	    XrdOucString location="";
	    XrdOucString si=""; si+= (int) i;
	    location += (int) *lociter;
	    sprintf(fsline,"%3s   %5s ",si.c_str(), location.c_str());
	    stdOut += fsline; 
	    XrdMgmFstNode::gMutex.Lock();
	    XrdMgmFstFileSystem* filesystem = (XrdMgmFstFileSystem*) XrdMgmFstNode::gFileSystemById[(int) *lociter];
	    if (filesystem) {
	      stdOut += filesystem->GetInfoString();
	    } else {
	      stdOut += "NA";
	    }
								     
	    
	    XrdMgmFstNode::gMutex.UnLock();
	    i++;
	  }
	  for ( lociter = fmd->unlinkedLocationsBegin(); lociter != fmd->unlinkedLocationsEnd(); ++lociter) {
	    stdOut += "(undeleted) $ "; stdOut += (int) *lociter; stdOut += "\n";
	  }
	  stdOut += "*******";

	}
      }
      MakeResult(dosort);
      return SFS_OK;
    } 
    
    if ( cmd == "mkdir" ) {
      XrdOucString path = opaque.Get("mgm.path");
      XrdOucString option = opaque.Get("mgm.option");
      
      if (!path.length()) {
	stdErr="error: you have to give a path name to call 'mkdir'";
	retc = EINVAL;
      } else {
	XrdSfsMode mode = 0;
	if (option == "p") {
	  mode |= SFS_O_MKPTH;
	}
	if (gOFS->_mkdir(path.c_str(), mode, *error, *pVid,(const char*)0)) {
	  stdErr += "error: unable to create directory";
	  retc = errno;
	}
      }
      MakeResult(dosort);
      return SFS_OK;
    }

    if ( cmd == "rmdir" ) {
      XrdOucString path = opaque.Get("mgm.path");
      if (!path.length()) {
	stdErr="error: you have to give a path name to call 'rmdir'";
	retc = EINVAL;
      } else {
	if (gOFS->_remdir(path.c_str(), *error, *pVid,(const char*)0)) {
	  stdErr += "error: unable to remove directory";
	  retc = errno;
	}
      }
      MakeResult(dosort);
      return SFS_OK;
    }

    if ( cmd == "ls" ) {
      XrdOucString path = opaque.Get("mgm.path");
      XrdOucString option = opaque.Get("mgm.option");
      if (!path.length()) {
	stdErr="error: you have to give a path name to call 'ls'";
	retc = EINVAL;
      } else {
	XrdMgmOfsDirectory dir;
	struct stat buf;
	int listrc=0;
	XrdOucString filter = "";

	if(gOFS->_stat(path.c_str(),&buf, *error,  *pVid, (const char*) 0)) {
	  stdErr = error->getErrText();
	  retc = errno;
	} else {
	  // if this is a directory open it and list
	  if (S_ISDIR(buf.st_mode)) {
	    listrc = dir.open(path.c_str(), *pVid, (const char*) 0);
	  } else {
	    // if this is a file, open the parent and set the filter
	    if (path.endswith("/")) {
	      path.erase(path.length()-1);
	    }
	    int rpos = path.rfind("/");
	    if (rpos == STR_NPOS) {
	      listrc = SFS_ERROR;
	      retc = ENOENT;
	    } else {
	      filter.assign(path,rpos+1);
	      path.erase(rpos);
	      listrc = dir.open(path.c_str(), *pVid, (const char*) 0);
	    }
	  }
	  
	  if (!listrc) {
	    const char* val;
	    while ((val=dir.nextEntry())) {
	      XrdOucString entryname = val;
	      if (((option.find("a"))==STR_NPOS) && entryname.beginswith(".")) {
		// skip over . .. and hidden files
		continue;
	      }
	      if ( (filter.length()) && (filter != entryname) ) {
		// apply filter
		continue;
	      }
	      if ((((option.find("l"))==STR_NPOS)) && ((option.find("F"))== STR_NPOS)) {
		stdOut += val ;stdOut += "\n";
	      } else {
		// yeah ... that is actually castor code ;-)
		char t_creat[14];
		char ftype[8];
		unsigned int ftype_v[7];
		char fmode[10];
		int fmode_v[9];
		char modestr[11];
		strcpy(ftype,"pcdb-ls");
		ftype_v[0] = S_IFIFO; ftype_v[1] = S_IFCHR; ftype_v[2] = S_IFDIR;
		ftype_v[3] = S_IFBLK; ftype_v[4] = S_IFREG; ftype_v[5] = S_IFLNK;
		ftype_v[6] = S_IFSOCK;
		strcpy(fmode,"rwxrwxrwx");
		fmode_v[0] = S_IRUSR; fmode_v[1] = S_IWUSR; fmode_v[2] = S_IXUSR;
		fmode_v[3] = S_IRGRP; fmode_v[4] = S_IWGRP; fmode_v[5] = S_IXGRP;
		fmode_v[6] = S_IROTH; fmode_v[7] = S_IWOTH; fmode_v[8] = S_IXOTH;
		// return full information
		XrdOucString statpath = path; statpath += "/"; statpath += val;
		while (statpath.replace("//","/")) {}
		struct stat buf;
		if (gOFS->_stat(statpath.c_str(),&buf, *error, *pVid, (const char*) 0)) {
		  stdErr += "error: unable to stat path "; stdErr += statpath; stdErr +="\n";
		retc = errno;
		} else {
		  int i=0;
		  // TODO: convert virtual IDs back
		  XrdOucString suid=""; suid += (int) buf.st_uid;
		  XrdOucString sgid=""; sgid += (int) buf.st_gid;
		  XrdOucString sizestring="";
		  struct tm *t_tm;
		  t_tm = localtime(&buf.st_mtime);
		  
		  strcpy(modestr,"----------");
		  for (i=0; i<6; i++) if ( ftype_v[i] == ( S_IFMT & buf.st_mode ) ) break;
		  modestr[0] = ftype[i];
		  for (i=0; i<9; i++) if (fmode_v[i] & buf.st_mode) modestr[i+1] = fmode[i];
		  if ( S_ISUID & buf.st_mode ) modestr[3] = 's';
		  if ( S_ISGID & buf.st_mode ) modestr[6] = 's';
		  
		  
		  strftime(t_creat,13,"%b %d %H:%M",t_tm);
		  char lsline[4096];
		  XrdOucString dirmarker="";
		  if ((option.find("F"))!=STR_NPOS) 
		    dirmarker="/";
		  if (modestr[0] != 'd') 
		    dirmarker="";

		  sprintf(lsline,"%s %3d %-8.8s %-8.8s %12s %s %s%s\n", modestr,(int)buf.st_nlink,
			  suid.c_str(),sgid.c_str(),XrdCommonFileSystem::GetSizeString(sizestring,buf.st_size),t_creat, val, dirmarker.c_str());
		  if ((option.find("l"))!=STR_NPOS) 
		    stdOut += lsline;
		  else {
		    stdOut += val;
		    stdOut += dirmarker;
		    stdOut += "\n";
		  }
		}
	      }
	    }
	    dir.close();
	  } else {
	    stdErr += "error: unable to open directory";
	    retc = errno;
	  }
	}
      }
      MakeResult(1);
      return SFS_OK;
    }

    if ( cmd == "rm" ) {
      XrdOucString path = opaque.Get("mgm.path");
      XrdOucString option = opaque.Get("mgm.option");
      if (!path.length()) {
	stdErr="error: you have to give a path name to call 'rm'";
	retc = EINVAL;
      } else {
	// find everything to be deleted
	if (option == "r") {
	  std::vector< std::vector<std::string> > found_dirs;
	  std::vector< std::vector<std::string> > found_files;
	  
	  if (gOFS->_find(path.c_str(), *error, *pVid, found_dirs , found_files)) {
	    stdErr += "error: unable to remove file/directory";
	    retc = errno;
	  } else {
	    // delete files starting at the deepest level
	    for (int i = found_files.size()-1 ; i>=0; i--) {
	      std::sort(found_files[i].begin(), found_files[i].end());
	      for (unsigned int j = 0; j< found_files[i].size(); j++) {
		if (gOFS->_rem(found_files[i][j].c_str(), *error, *pVid,(const char*)0)) {
		  stdErr += "error: unable to remove file\n";
		  retc = errno;
		} 
	      }
	    } 
	    // delete directories starting at the deepest level
	    for (int i = found_dirs.size()-1; i>=0; i--) {
	      std::sort(found_dirs[i].begin(), found_dirs[i].end());
	      for (unsigned int j = 0; j< found_dirs[i].size(); j++) {
		// don't even try to delete the root directory
		if (found_dirs[i][j] == "/")
		  continue;
		if (gOFS->_remdir(found_dirs[i][j].c_str(), *error, *pVid,(const char*)0)) {
		  stdErr += "error: unable to remove directory";
		  retc = errno;
		} 
	      }
	    }
	  }
	} else {
	  if (gOFS->_rem(path.c_str(), *error, *pVid,(const char*)0)) {
	    stdErr += "error: unable to remove file/directory";
	    retc = errno;
	  }
	}
      }
      MakeResult(dosort);
      return SFS_OK;
    }

    if (cmd == "whoami") {
      stdOut += "Virtual Identity: uid=";stdOut += (int)vid_in.uid; stdOut+= " (";
      for (unsigned int i=0; i< vid_in.uid_list.size(); i++) {stdOut += (int)vid_in.uid_list[i]; stdOut += ",";}
      stdOut.erase(stdOut.length()-1);
      stdOut += ") gid="; stdOut+= (int)vid_in.gid; stdOut += " (";
      for (unsigned int i=0; i< vid_in.gid_list.size(); i++) {stdOut += (int)vid_in.gid_list[i]; stdOut += ",";}
      stdOut.erase(stdOut.length()-1);
      stdOut += ")";
      stdOut += " [authz:"; stdOut += vid.prot; stdOut += "]";
      if (vid_in.sudoer) 
	stdOut += " sudo*";

      MakeResult(0);
      return SFS_OK;
    }

	
    if ( cmd == "find" ) {
      XrdOucString path = opaque.Get("mgm.path");
      XrdOucString option = opaque.Get("mgm.option");
      XrdOucString attribute = opaque.Get("mgm.find.attribute");
      XrdOucString key = attribute;
      XrdOucString val = attribute;
      XrdOucString printkey = opaque.Get("mgm.find.printkey");

      // this hash is used to calculate the balance of the found files over the filesystems involved

      google::dense_hash_map<unsigned long, unsigned long long> filesystembalance;
      google::dense_hash_map<std::string, unsigned long long> spacebalance;
      google::dense_hash_map<std::string, unsigned long long> schedulinggroupbalance;

      filesystembalance.set_empty_key(0);
      spacebalance.set_empty_key("");
      schedulinggroupbalance.set_empty_key("");
      
      bool calcbalance = false;

      if (option.find("b")!=STR_NPOS) {
	calcbalance=true;
      }

      if (attribute.length()) {
	key.erase(attribute.find("="));
	val.erase(0, attribute.find("=")+1);
      }

      if (!path.length()) {
	stdErr="error: you have to give a path name to call 'find'";
	retc = EINVAL;
      } else {
	std::vector< std::vector<std::string> > found_dirs;
	std::vector< std::vector<std::string> > found_files;


	if (gOFS->_find(path.c_str(), *error, *pVid, found_dirs , found_files, key.c_str(),val.c_str())) {
	  stdErr += "error: unable to remove file/directory";
	  retc = errno;
	}

	if ( ((option.find("f")) != STR_NPOS) || ((option.find("d"))==STR_NPOS)) {
	  for (unsigned int i = 0 ; i< found_files.size(); i++) {
	    std::sort(found_files[i].begin(), found_files[i].end());
	    for (unsigned int j = 0; j< found_files[i].size(); j++) {
	      if (!calcbalance) {
		stdOut += found_files[i][j].c_str();
		stdOut += "\n";
	      } else {
		// get location
		//-------------------------------------------
		gOFS->eosViewMutex.Lock();
		eos::FileMD* fmd = 0;
		try {
		  fmd = gOFS->eosView->getFile(found_files[i][j].c_str());
		} catch( eos::MDException &e ) {
		  eos_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
		}
		//-------------------------------------------
		for (unsigned int i=0; i< fmd->getNumLocation(); i++) {
		  int loc = fmd->getLocation(i);
		  size_t size = fmd->getSize();
		  filesystembalance[loc]+=size;
		  XrdMgmFstNode::gMutex.Lock();
		  XrdMgmFstFileSystem* filesystem = (XrdMgmFstFileSystem*)XrdMgmFstNode::gFileSystemById[loc];
		  if (filesystem) {
		    spacebalance[filesystem->GetSpaceName()]+=size;
		    schedulinggroupbalance[filesystem->GetSchedulingGroup()]+=size;
		  }
		  XrdMgmFstNode::gMutex.UnLock();
		}
		gOFS->eosViewMutex.UnLock();
	      }
	    }
	  } 
	}
	
	if ( ((option.find("d")) != STR_NPOS) || ((option.find("f"))==STR_NPOS)){
	  for (unsigned int i = 0; i< found_dirs.size(); i++) {
	    std::sort(found_dirs[i].begin(), found_dirs[i].end());
	    for (unsigned int j = 0; j< found_dirs[i].size(); j++) {
	      XrdOucString attr="";
	      if (printkey.length()) {
		gOFS->_attr_get(found_dirs[i][j].c_str(), *error, vid, (const char*) 0, printkey.c_str(), attr);
	      }
	      if (printkey.length()) {
		char pattr[4096];
		if (!attr.length()) {
		  attr = "undef";
		}
		sprintf(pattr,"%-32s",attr.c_str());
		stdOut += pattr;
	      }
	      stdOut += found_dirs[i][j].c_str();
	      stdOut += "\n";
	    }
	  }
	}
      }

      if (calcbalance) {
	google::dense_hash_map<unsigned long, unsigned long long>::iterator it;
	for ( it = filesystembalance.begin(); it != filesystembalance.end(); it++) {
	  char outline[1024];
	  sprintf(outline,"fsid=%lu \tnbytes=%llu\n",it->first,it->second);
	  stdOut += outline;
	}

	google::dense_hash_map<std::string, unsigned long long>::iterator its;
	for ( its= spacebalance.begin(); its != spacebalance.end(); its++) {
	  char outline[1024];
	  sprintf(outline,"space=%s \tnbytes=%llu\n",its->first.c_str(),its->second);
	  stdOut += outline;
	}

	google::dense_hash_map<std::string, unsigned long long>::iterator itg;
	for ( itg= schedulinggroupbalance.begin(); itg != schedulinggroupbalance.end(); itg++) {
	  char outline[1024];
	  sprintf(outline,"sched=%s \tnbytes=%llu\n",itg->first.c_str(),itg->second);
	  stdOut += outline;
	}


      }
      MakeResult(1);
      return SFS_OK;
    }

    
    if ( cmd == "attr" ) {
      XrdOucString path = opaque.Get("mgm.path");
      XrdOucString option = opaque.Get("mgm.option");
      if ( (!path.length()) || 
	   ( (subcmd !="set") && (subcmd != "get") && (subcmd != "ls") && (subcmd != "rm") ) ) {
	stdErr="error: you have to give a path name to call 'attr' and one of the subcommands 'ls', 'get','rm','set' !";
	retc = EINVAL;
      } else {
	if ( ( (subcmd == "set") && ((!opaque.Get("mgm.attr.key")) || ((!opaque.Get("mgm.attr.value"))))) ||
	     ( (subcmd == "get") && ((!opaque.Get("mgm.attr.key"))) ) ||
	     ( (subcmd == "rm")  && ((!opaque.Get("mgm.attr.key"))) ) ) {
	  
	  stdErr="error: you have to provide 'mgm.attr.key' for set,get,rm and 'mgm.attr.value' for set commands!";
	  retc = EINVAL;
	} else {
	  retc = 0;
	  XrdOucString key = opaque.Get("mgm.attr.key");
	  XrdOucString val = opaque.Get("mgm.attr.value");
	  
	  // find everything to be modified
	  std::vector< std::vector<std::string> > found_dirs;
	  std::vector< std::vector<std::string> > found_files;
	  if (option == "r") {
	    if (gOFS->_find(path.c_str(), *error, *pVid, found_dirs , found_files)) {
	      stdErr += "error: unable to search in path";
	      retc = errno;
	    } 
	  } else {
	    // the single dir case
	    found_dirs.resize(1);
	    found_dirs[0].push_back(path.c_str());
	  }
	  
	  if (!retc) {
	    // apply to  directories starting at the highest level
	    for (unsigned int i = 0; i< found_dirs.size(); i++) {
	      std::sort(found_dirs[i].begin(), found_dirs[i].end());
	      for (unsigned int j = 0; j< found_dirs[i].size(); j++) {
		eos::ContainerMD::XAttrMap map;
		
		if (subcmd == "ls") {
		  XrdOucString partialStdOut = "";
		  if (gOFS->_attr_ls(found_dirs[i][j].c_str(), *error, *pVid,(const char*)0, map)) {
		    stdErr += "error: unable to list attributes in directory "; stdErr += found_dirs[i][j].c_str();
		    retc = errno;
		  } else {
		    eos::ContainerMD::XAttrMap::const_iterator it;
		    if ( option == "r" ) {
		      stdOut += found_dirs[i][j].c_str();
		      stdOut += ":\n";
		    }

		    for ( it = map.begin(); it != map.end(); ++it) {
		      partialStdOut += (it->first).c_str(); partialStdOut += "="; partialStdOut += "\""; partialStdOut += (it->second).c_str(); partialStdOut += "\""; partialStdOut +="\n";
		    }
		    XrdMqMessage::Sort(partialStdOut);
		    stdOut += partialStdOut;
		    if (option == "r") 
		      stdOut += "\n";
		  }
		}
		
		if (subcmd == "set") {
		  if (gOFS->_attr_set(found_dirs[i][j].c_str(), *error, *pVid,(const char*)0, key.c_str(),val.c_str())) {
		    stdErr += "error: unable to set attribute in directory "; stdErr += found_dirs[i][j].c_str();
		    retc = errno;
		  } else {
		    stdOut += "success: set attribute '"; stdOut += key; stdOut += "'='"; stdOut += val; stdOut += "' in directory "; stdOut += found_dirs[i][j].c_str();stdOut += "\n";
		  }
		}
		
		if (subcmd == "get") {
		  if (gOFS->_attr_get(found_dirs[i][j].c_str(), *error, *pVid,(const char*)0, key.c_str(), val)) {
		    stdErr += "error: unable to get attribute '"; stdErr += key; stdErr += "' in directory "; stdErr += found_dirs[i][j].c_str();
		  } else {
		    stdOut += key; stdOut += "="; stdOut += "\""; stdOut += val; stdOut += "\""; stdOut +="\n"; 
		  }
		}
		
		if (subcmd == "rm") {
		  if (gOFS->_attr_rem(found_dirs[i][j].c_str(), *error, *pVid,(const char*)0, key.c_str())) {
		    stdErr += "error: unable to remove attribute '"; stdErr += key; stdErr += "' in directory "; stdErr += found_dirs[i][j].c_str();
		  } else {
		    stdOut += "success: removed attribute '"; stdOut += key; stdOut +="' from directory "; stdOut += found_dirs[i][j].c_str();stdOut += "\n";
		  }
		}
	      }
	    }
	  }
	}
      }
      MakeResult(dosort);
      return SFS_OK;
    }
  
    if ( cmd == "chmod" ) {
      XrdOucString path = opaque.Get("mgm.path");
      XrdOucString option = opaque.Get("mgm.option");
      XrdOucString mode   = opaque.Get("mgm.chmod.mode");
      if ( (!path.length()) || (!mode.length())) {
	stdErr = "error: you have to provide a path and the mode to set!\n";
	retc = EINVAL;
      } else {
	// find everything to be modified
	std::vector< std::vector<std::string> > found_dirs;
	std::vector< std::vector<std::string> > found_files;
	if (option == "r") {
	  if (gOFS->_find(path.c_str(), *error, *pVid, found_dirs , found_files)) {
	    stdErr += "error: unable to search in path";
	    retc = errno;
	  } 
	} else {
	  // the single dir case
	  found_dirs.resize(1);
	  found_dirs[0].push_back(path.c_str());
	}

	XrdSfsMode Mode = (XrdSfsMode) strtoul(mode.c_str(),0,8);
	 
	
	for (unsigned int i = 0; i< found_dirs.size(); i++) {
	  std::sort(found_dirs[i].begin(), found_dirs[i].end());
	  for (unsigned int j = 0; j< found_dirs[i].size(); j++) {
	    if (gOFS->_chmod(found_dirs[i][j].c_str(), Mode, *error, *pVid, (char*)0)) {
	      stdErr += "error: unable to chmod of directory "; stdErr += found_dirs[i][j].c_str();
	      retc = errno;
	    } else {
	      stdOut += "success: mode of directory "; stdOut += found_dirs[i][j].c_str(); stdOut += " is now '"; stdOut += mode; stdOut += "'";
	    }
	  }
	}
	MakeResult(dosort);
	return SFS_OK;
      }
    }

    stdErr += "errro: no such user command '"; stdErr += cmd; stdErr += "'";
    retc = EINVAL;
  
    MakeResult(dosort);
    return SFS_OK;
  }

  return gOFS->Emsg((const char*)"open", *error, EINVAL, "execute command - not implemented ",ininfo);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmProcCommand::read(XrdSfsFileOffset offset, char* buff, XrdSfsXferSize blen) 
{
  if ( ((unsigned int)blen <= (len - offset)) ) {
    memcpy(buff, resultStream.c_str() + offset, blen);
    return blen;
  } else {
    memcpy(buff, resultStream.c_str() + offset, (len - offset));
    return (len - offset);
  }
}

/*----------------------------------------------------------------------------*/
int 
XrdMgmProcCommand::stat(struct stat* buf) 
{
  memset(buf, 0, sizeof(struct stat));
  buf->st_size = len;

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmProcCommand::close() 
{
  return retc;
}

/*----------------------------------------------------------------------------*/
void
XrdMgmProcCommand::MakeResult(bool dosort) 
{
  resultStream =  "mgm.proc.stdout=";
  XrdMqMessage::Sort(stdOut,dosort);
  resultStream += XrdMqMessage::Seal(stdOut);
  resultStream += "&mgm.proc.stderr=";
  resultStream += XrdMqMessage::Seal(stdErr);
  resultStream += "&mgm.proc.retc=";
  resultStream += retc;

  //    fprintf(stderr,"%s\n",resultStream.c_str());
  if (retc) {
    eos_static_err("%s (errno=%u)", stdErr.c_str(), retc);
  }
  len = resultStream.length();
  offset = 0;
}

/*----------------------------------------------------------------------------*/
