/*----------------------------------------------------------------------------*/
#include "XrdMqOfs/XrdMqMessaging.hh"
#include "XrdMgmOfs/XrdMgmFstNode.hh"
#include "XrdMgmOfs/XrdMgmQuota.hh"
#include "XrdMgmOfs/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucTokenizer.hh"
/*----------------------------------------------------------------------------*/

XrdOucHash<XrdMgmFstNode> XrdMgmFstNode::gFstNodes;
google::dense_hash_map<unsigned int, unsigned long long> XrdMgmFstNode::gFileSystemById;

//google::dense_hash_map<long, unsigned long long> gFstIndex;

XrdSysMutex XrdMgmFstNode::gMutex;

/*----------------------------------------------------------------------------*/
bool
XrdMgmFstNode::SetNodeStatus(int status) 
{
  if (status == kOffline) {
    int fsstatus = XrdMgmFstFileSystem::kDown;
    // disable the filesystems here!
    fileSystems.Apply(XrdMgmFstNode::SetBootStatusFileSystem, &fsstatus);    
  }
  nodeStatus = status;
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmFstNode::SetNodeConfigStatus(int status) 
{
  fileSystems.Apply(XrdMgmFstNode::SetConfigStatusFileSystem, &status);    
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmFstNode::Update(XrdAdvisoryMqMessage* advmsg) 
{
  if (!advmsg)
    return false;

  gMutex.Lock();
  XrdMgmFstNode* node = gFstNodes.Find(advmsg->kQueue.c_str());
  if (!node) {
    // create one
    node = new XrdMgmFstNode(advmsg->kQueue.c_str());
    node->hostPortName = advmsg->kQueue.c_str();
    int pos = node->hostPortName.find("/",2);
    if (pos != STR_NPOS) 
      node->hostPortName.erase(0,pos-1);
    gFstNodes.Add(advmsg->kQueue.c_str(), node);
  } else {
    // update the one
    node->lastHeartBeat = advmsg->kMessageHeader.kSenderTime_sec;
    node->SetNodeStatus(advmsg->kOnline);
  }
  gMutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmFstNode::Update(XrdOucEnv &config) 
{
  XrdOucString infsname     = config.Get("mgm.fsname");
  XrdOucString sid          = config.Get("mgm.fsid");
  XrdOucString schedgroup   = config.Get("mgm.fsschedgroup");
  XrdOucString fsstatus     = config.Get("mgm.fsstatus");
  XrdOucString serrc        = config.Get("errc");
  
  int envlen=0;
  eos_static_debug("%s", config.Env(envlen));
  int errc=0;
  XrdOucString errmsg       = config.Get("errmsg");
  if (serrc.length()) 
      errc = atoi(serrc.c_str());

  int id = 0;
  if (sid.length()) 
    id = atoi(sid.c_str());
  
  if (!id) 
    return false;
  
  int statusid = XrdCommonFileSystem::GetStatusFromString(fsstatus.c_str());

  return Update(infsname.c_str(), id, schedgroup.c_str(),statusid, &config, errc, errmsg.c_str());
}


/*----------------------------------------------------------------------------*/
bool
XrdMgmFstNode::UpdateQuotaStatus(XrdOucEnv &config) 
{
  // get the quota values
  XrdOucString userbytes  = config.Get("fst.quota.userbytes");
  XrdOucString groupbytes = config.Get("fst.quota.groupbytes");
  XrdOucString userfiles  = config.Get("fst.quota.userfiles");
  XrdOucString groupfiles = config.Get("fst.quota.groupfiles");

  // decode the env strings
  while (userbytes.replace (","," ")){};
  while (groupbytes.replace(","," ")){};
  while (userfiles.replace (","," ")){};
  while (groupfiles.replace(","," ")){};

  if (userbytes.c_str()) {
    // user bytes
    XrdOucTokenizer tokenizer((char*)userbytes.c_str());
    tokenizer.GetLine();
    const char* val;
    while ( (val = tokenizer.GetToken()) ) {
      XrdOucString keyval = val;
      XrdOucString key="";
      XrdOucString value="";
      if (XrdCommonFileSystem::SplitKeyValue(keyval, key, value)) {
	long long fsiduid   = strtoll(key.c_str(),0,10);
	unsigned long long fsidquota = strtoll(value.c_str(),0,10);
	unsigned long fsid = (fsiduid>>32) & 0xffffffff;
	unsigned long uid  = fsiduid & 0xffffffff;
	eos_static_debug("decoded quota userbytes    : fsid=%lu uid=%lu bytes=%llu", fsid, uid, fsidquota);
	XrdMgmFstFileSystem* filesystem = 0;
	if ( (filesystem = (XrdMgmFstFileSystem*)XrdMgmFstNode::gFileSystemById[fsid]) ) {
	  filesystem->UserBytes[uid] = fsidquota;
	  XrdMgmQuota::UpdateHint(fsid);
	}
      } else {
	eos_static_err("key-value pair split error for %s", keyval.c_str());
      }
    }
  }

  if (groupbytes.c_str()) {
    // user bytes
    XrdOucTokenizer tokenizer((char*)groupbytes.c_str());
    tokenizer.GetLine();
    const char* val;
    while ( (val = tokenizer.GetToken()) ) {
      XrdOucString keyval = val;
      XrdOucString key="";
      XrdOucString value="";
      if (XrdCommonFileSystem::SplitKeyValue(keyval, key, value)) {
	long long fsiduid   = strtoll(key.c_str(),0,10);
	unsigned long long fsidquota = strtoll(value.c_str(),0,10);
	unsigned long fsid = (fsiduid>>32) & 0xffffffff;
	unsigned long gid  = fsiduid & 0xffffffff;
	eos_static_debug("decoded quota groupbytes  : fsid=%lu uid=%lu bytes=%llu", fsid, gid, fsidquota);
	XrdMgmFstFileSystem* filesystem = 0;
	if ( (filesystem = (XrdMgmFstFileSystem*)XrdMgmFstNode::gFileSystemById[fsid]) ) {
	  filesystem->GroupBytes[gid] = fsidquota;
	  XrdMgmQuota::UpdateHint(fsid);
	}
      } else {
	eos_static_err("key-value pair split error for %s", keyval.c_str());
      }
    }
  }

  if (userfiles.c_str()) {
    // user bytes
    XrdOucTokenizer tokenizer((char*)userfiles.c_str());
    tokenizer.GetLine();
    const char* val;
    while ( (val = tokenizer.GetToken()) ) {
      XrdOucString keyval = val;
      XrdOucString key="";
      XrdOucString value="";
      if (XrdCommonFileSystem::SplitKeyValue(keyval, key, value)) {
	long long fsiduid   = strtoll(key.c_str(),0,10);
	unsigned long long fsidquota = strtoll(value.c_str(),0,10);
	unsigned long fsid = (fsiduid>>32) & 0xffffffff;
	unsigned long uid  = fsiduid & 0xffffffff;
	eos_static_debug("decoded quota userfiles: fsid=%lu uid=%lu files=%llu", fsid, uid, fsidquota);
	XrdMgmFstFileSystem* filesystem = 0;
	if ( (filesystem = (XrdMgmFstFileSystem*)XrdMgmFstNode::gFileSystemById[fsid]) ) {
	  filesystem->UserFiles[uid] = fsidquota;
	  XrdMgmQuota::UpdateHint(fsid);
	}
      } else {
	eos_static_err("key-value pair split error for %s", keyval.c_str());
      }
    }
  }

  if (groupfiles.c_str()) {
    // user bytes
    XrdOucTokenizer tokenizer((char*)groupfiles.c_str());
    tokenizer.GetLine();
    const char* val;
    while ( (val = tokenizer.GetToken()) ) {
      XrdOucString keyval = val;
      XrdOucString key="";
      XrdOucString value="";
      if (XrdCommonFileSystem::SplitKeyValue(keyval, key, value)) {
	long long fsiduid   = strtoll(key.c_str(),0,10);
	unsigned long long fsidquota = strtoll(value.c_str(),0,10);
	unsigned long fsid = (fsiduid>>32) & 0xffffffff;
	unsigned long gid  = fsiduid & 0xffffffff;
	eos_static_debug("decoded quota groupfiles: fsid=%lu uid=%lu files=%llu", fsid, gid, fsidquota);
	XrdMgmFstFileSystem* filesystem = 0;
	if ( (filesystem = (XrdMgmFstFileSystem*) XrdMgmFstNode::gFileSystemById[fsid]) ) {
	  filesystem->GroupFiles[gid] = fsidquota;
	  XrdMgmQuota::UpdateHint(fsid);
	}
      } else {
	eos_static_err("key-value pair split error for %s", keyval.c_str());
      }
    }
  }

  return true;
}


/*----------------------------------------------------------------------------*/
bool
XrdMgmFstNode::Update(const char* infsname, int id, const char* schedgroup, int bootstatus, XrdOucEnv* env , int errc, const char* errmsg, bool configchangelog) 
{
  if (!infsname) 
    return false;

  // set the empty key for the static filesystem hash

  if (!schedgroup) {
    schedgroup = "default";
  }

  eos_static_debug("%s %d %s %d", infsname, id, schedgroup, bootstatus);
  XrdOucString fsname = infsname;
  XrdOucString lQueue="";
  // remove // from fsnames
  while (fsname.replace("//","/")) {}
  // end fsnames with a /
  if (!fsname.endswith("/")) fsname+="/";

  lQueue = fsname;

  XrdOucString nodename = fsname;
  int spos = nodename.find("/fst/");
  if (!spos)
    return false;

  nodename.erase(spos+4);
  fsname.erase(0,spos+4);

  // get the node
  XrdMgmFstNode* node = gFstNodes.Find(nodename.c_str());
  if (!node) {
    // create one
    node = new XrdMgmFstNode(nodename.c_str());
    node->hostPortName = fsname.c_str();
    int pos = node->hostPortName.find("/",2);
    if (pos != STR_NPOS) 
      node->hostPortName.erase(0,pos-1);
    gFstNodes.Add(nodename.c_str(), node);
  } 

  // get the filesystem
  XrdMgmFstFileSystem* fs = node->fileSystems.Find(fsname.c_str());
  if (!fs) {
    // create a new filesystem there
    fs = new XrdMgmFstFileSystem(id, fsname.c_str(), nodename.c_str(), schedgroup);
    node->fileSystems.Add(fsname.c_str(),fs);
    gFileSystemById[id]=(unsigned long long)fs;
    // create the quota space entry in the hash to be able to set quota on the empty space
    XrdMgmQuota::GetSpaceQuota(fs->GetSpaceName());
    XrdMgmQuota::UpdateHint(fs->GetId());
  } else {
    if (fs->GetId()>0) {
      // invalidate the old entry
      gFileSystemById[fs->GetId()] = 0;
    }
    gFileSystemById[id] = (unsigned long long)fs;
    fs->SetId(id);

    if (schedgroup && (strcmp(fs->GetSchedulingGroup(),schedgroup)) ) {
      // scheduling group changed
      // create the quota space entry in the hash to be able to set quota on the empty space
      XrdMgmQuota::GetSpaceQuota(fs->GetSpaceName());
      XrdMgmQuota::UpdateHint(fs->GetId());
    }

    
    if (fsname.length())    fs->SetPath(fsname.c_str());
    if (strlen(schedgroup)) fs->SetSchedulingGroup(schedgroup);

    if (bootstatus!= XrdCommonFileSystem::kDown) 
      fs->SetBootStatus(bootstatus);

  }

  fs->SetConfigStatusEnv(env);
  fs->SetError(errc, errmsg);
  fs->SetStatfsEnv(env);

  // change config
  gOFS->ConfigEngine->SetConfigValue("fs", fs->GetQueuePath(), fs->GetBootString(), configchangelog );

  return true;
}

/*----------------------------------------------------------------------------*/
XrdMgmFstNode*
XrdMgmFstNode::GetNode(const char* queue) 
{
  
  gMutex.Lock();
  XrdMgmFstNode* node = gFstNodes.Find(queue);
  gMutex.UnLock();
  return node;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmFstNode::ListNodes(const char* key, XrdMgmFstNode* node, void* Arg)  
{
  XrdOucString* listing = (XrdOucString*) Arg;
  *listing += node->GetInfoString();
  *listing += XrdMgmFstFileSystem::GetInfoHeader();
  node->fileSystems.Apply(XrdMgmFstNode::ListFileSystems, Arg);
  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmFstNode::ListFileSystems(const char* key, XrdMgmFstFileSystem* filesystem, void* Arg)
{
  XrdOucString* listing = (XrdOucString*) Arg;
  *listing += filesystem->GetInfoString();
  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmFstNode::ExistsNodeFileSystemId(const char* key, XrdMgmFstNode* node, void* Arg)  
{
  unsigned int* listing = (unsigned int*) Arg;

  if (*listing) {
    node->fileSystems.Apply(XrdMgmFstNode::ExistsFileSystemId, Arg);
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmFstNode::ExistsFileSystemId(const char* key, XrdMgmFstFileSystem* filesystem, void* Arg)
{
  unsigned int* listing = (unsigned int*) Arg;

  if (*listing) {
    if (filesystem->GetId() == *listing) 
      *listing = 0;
  }
      
  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmFstNode::FindNodeFileSystem(const char* key, XrdMgmFstNode* node, void* Arg)  
{
  struct FindStruct* finder = (struct FindStruct*) Arg;
  if (!finder->found) {
    node->fileSystems.Apply(XrdMgmFstNode::FindFileSystem, Arg);
    if (finder->found) {
      finder->nodename = node->GetQueue();
      return 1;
    }
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmFstNode::BootNode(const char* key, XrdMgmFstNode* node, void* Arg)  
{
  XrdOucString* bootfs = (XrdOucString*) Arg;
  (*bootfs)+="mgm.nodename=";(*bootfs)+= node->GetQueue();
  (*bootfs)+="\t";
  (*bootfs)+=" mgm.fsnames=";
  node->fileSystems.Apply(XrdMgmFstNode::BootFileSystem, Arg);
  (*bootfs)+="\n";
  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmFstNode::FindFileSystem(const char* key, XrdMgmFstFileSystem* filesystem, void* Arg)
{
  struct FindStruct* finder = (struct FindStruct*) Arg;

  // find by id
  if (finder->id) {
    if (filesystem->GetId() == finder->id) {
      finder->found = true;
      finder->fsname = filesystem->GetPath(); 
      return 1;
    }
  } else {
    // find by name
    XrdOucString path = filesystem->GetPath();
    if (path.length()) {
      if (path == finder->fsname) {
	finder->found = true;
	finder->id = filesystem->GetId();
	return 1;
      }
    }
  }
  
  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmFstNode::BootFileSystem(const char* key, XrdMgmFstFileSystem* filesystem, void *Arg)
{
  XrdOucString* bootfs = (XrdOucString*) Arg;
  XrdMqMessage message("mgm"); XrdOucString msgbody="";
  XrdOucEnv config(filesystem->GetBootString());

  XrdCommonFileSystem::GetBootRequestString(msgbody,config);

  message.SetBody(msgbody.c_str());
  XrdOucString lastchar = bootfs->c_str()+bootfs->length()-1;
  if (lastchar != "=") {
    *bootfs+=",";
  }
  if (XrdMqMessaging::gMessageClient.SendMessage(message, filesystem->GetQueue())) {
    *bootfs+= filesystem->GetPath();
    filesystem->SetBootSent();
  } else {
    filesystem->SetBootFailure("no fst listening on this queue");
  }

  // set boot status


  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmFstNode::SetBootStatusFileSystem(const char* key, XrdMgmFstFileSystem* filesystem, void *Arg)
{
  int* status = (int*) Arg;

  if (filesystem) {
    filesystem->SetBootStatus(*status);
    // add to config
    gOFS->ConfigEngine->SetConfigValue("fs", filesystem->GetQueuePath(), filesystem->GetBootString());
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmFstNode::SetConfigStatusFileSystem(const char* key, XrdMgmFstFileSystem* filesystem, void *Arg)
{
  int* status = (int*) Arg;
  if (filesystem) {
    filesystem->SetConfigStatus(*status);
    eos_static_info("%s %s", filesystem->GetQueue(), filesystem->GetConfigStatusString());
      
    gOFS->ConfigEngine->SetConfigValue("fs", filesystem->GetQueuePath(), filesystem->GetBootString());
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
