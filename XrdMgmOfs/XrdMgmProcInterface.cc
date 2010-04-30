/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonStringStore.hh"
#include "XrdMgmOfs/XrdMgmProcInterface.hh"
#include "XrdMgmOfs/XrdMgmFstNode.hh"
#include "XrdMgmOfs/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/


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
XrdMgmProcInterface::Authorize(const char* path, const char* info, uid_t uid, gid_t gid , const XrdSecEntity* entity) {
  XrdOucString inpath = path;

  // administrator access
  if (inpath.beginswith("/proc/admin/")) {
    return true;
  }

  // projectadmin access
  if (inpath.beginswith("/proc/projectadmin/")) {
    return false;
  }

  // user access
  if (inpath.beginswith("/proc/user/")) {
    return false;
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
  uid = 0;
  gid = 0;
  path = "";
  adminCmd = userCmd = projectAdminCmd = 0;
}

/*----------------------------------------------------------------------------*/
XrdMgmProcCommand::~XrdMgmProcCommand()
{
}

/*----------------------------------------------------------------------------*/
int 
XrdMgmProcCommand::open(const char* inpath, const char* ininfo, uid_t inuid, gid_t ingid, XrdOucErrInfo   *error) 
{
  uid = inuid;
  gid = ingid;
  path = inpath;

  if ( path.beginswith ("/proc/admin")) {
    adminCmd = true;
  } 
  if ( path.beginswith ("/proc/projectadmin")) {
    projectAdminCmd = true;
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
    if (cmd == "fs") {
      if (subcmd == "ls") {
	stdOut += XrdMgmFstNode::GetInfoHeader();
	XrdMgmFstNode::gFstNodes.Apply(XrdMgmFstNode::ListNodes, &stdOut);
      }

      if (subcmd == "set") {
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
	  stdErr="usage: fs set <fs-name> <fs-id> [<fs-sched-group>]  : configure filesystem with name and id";
	  retc = EINVAL;
	} else {
	  fsid = atoi(fsidst);
	  XrdMgmFstNode::gFstNodes.Apply(XrdMgmFstNode::ExistsNodeFileSystemId, &fsid);
	  if (!fsid) {
	    stdErr="error: filesystem id="; stdErr += fsidst; stdErr += " is already in use!";
	    retc = EBUSY;
	  } else {
	    if (!XrdMgmFstNode::Update(fsname, fsid, fssched)) {
	      stdErr="error: cannot set the filesystem information to mgm.fsname="; stdErr += fsname; stdErr += " mgm.fsid=", stdErr += fsidst; stdErr += " mgm.fsschedgroup=" ; stdErr += fssched;
	      retc = EINVAL;
	    } else {
	    stdOut="success: added/set mgm.fsname="; stdOut += fsname; stdOut += " mgm.fsid=", stdOut += fsidst; stdOut += " mgm.fsschedgroup=" ; stdOut += fssched;
	    }
	  }
	}
      }

      if (subcmd == "rm") {
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
	
	printf("Deleting %s %s\n", nodename, fspath);
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
      }

      if (subcmd == "boot") {
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
      }
    }

    if (cmd == "debug") {
      XrdOucString debugnode =  opaque.Get("mgm.nodename");
      XrdOucString debuglevel = opaque.Get("mgm.debuglevel");
      XrdMqMessage message("debug");
      int envlen;
      XrdOucString body = opaque.Env(envlen);
      message.SetBody(body.c_str());
      
      if (debugnode == "") {
	// this is for us!
	int debugval = XrdCommonLogging::GetPriorityByString(debuglevel.c_str());
	if (debugval<0) {
	  stdErr="error: debug level "; stdErr += debuglevel; stdErr+= " is not known!";
	  retc = EINVAL;
	} else {
	  XrdCommonLogging::SetLogPriority(debugval);
	  stdOut="success: debug level is now <"; stdOut+=debuglevel.c_str();stdOut += ">";
	  eos_notice("setting debug level to <%s>", debuglevel.c_str());
	}
      } else {
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

    MakeResult();
    XrdMgmFstNode::gMutex.UnLock();
    return SFS_OK;
  }

  XrdMgmFstNode::gMutex.UnLock();
  return gOFS->Emsg((const char*)"open", *error, EINVAL, "execute command - not implemented ",ininfo);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmProcCommand::read(XrdSfsFileOffset offset, char* buff, XrdSfsXferSize blen) 
{
  if ( (blen <= (len - offset)) ) {
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
  memset(buf, sizeof(struct stat), 0);
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
XrdMgmProcCommand::MakeResult() 
{
  resultStream =  "mgm.proc.stdout=";
  resultStream += XrdMqMessage::Seal(stdOut);
  resultStream += "&mgm.proc.stderr=";
  resultStream += XrdMqMessage::Seal(stdErr);
  resultStream += "&mgm.proc.retc=";
  resultStream += retc;

  len = resultStream.length();
  offset = 0;
}

/*----------------------------------------------------------------------------*/
