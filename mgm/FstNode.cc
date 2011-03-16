/*----------------------------------------------------------------------------*/
#include "mq/XrdMqMessaging.hh"
#include "mgm/FstNode.hh"
#include "mgm/Quota.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucTokenizer.hh"
/*----------------------------------------------------------------------------*/

XrdOucHash<FstNode> FstNode::gFstNodes;
google::dense_hash_map<unsigned int, unsigned long long> FstNode::gFileSystemById;

//google::dense_hash_map<long, unsigned long long> gFstIndex;

XrdSysMutex FstNode::gMutex;


/*----------------------------------------------------------------------------*/
void
FstNode::SetLastHeartBeat(time_t hbt) {
  lastHeartBeat = hbt;
  // distribute to filesystems attached
  fileSystems.Apply(FstNode::SetHeartBeatTimeFileSystem, &hbt);
  }


/*----------------------------------------------------------------------------*/
bool
FstNode::SetNodeStatus(int status) 
{
  if (status == kOffline) {
    int fsstatus = FstFileSystem::kDown;
    // disable the filesystems here!
    fileSystems.Apply(FstNode::SetBootStatusFileSystem, &fsstatus);    
  }
  nodeStatus = status;
  return true;
}

/*----------------------------------------------------------------------------*/
bool
FstNode::SetNodeConfigStatus(int status) 
{
  fileSystems.Apply(FstNode::SetConfigStatusFileSystem, &status);    
  return true;
}

/*----------------------------------------------------------------------------*/
bool 
FstNode::SetNodeConfigSchedulingGroup(const char* schedgroup)
{
  fileSystems.Apply(FstNode::SetConfigSchedulingGroupFileSystem, (void*)schedgroup);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
FstNode::Update(XrdAdvisoryMqMessage* advmsg) 
{
  if (!advmsg)
    return false;

  gMutex.Lock();
  FstNode* node = gFstNodes.Find(advmsg->kQueue.c_str());
  if (!node) {
    // create one
    node = new FstNode(advmsg->kQueue.c_str());
    node->hostPortName = advmsg->kQueue.c_str();
    int pos = node->hostPortName.find("/",2);
    if (pos != STR_NPOS) 
      node->hostPortName.erase(0,pos-1);
    gFstNodes.Add(advmsg->kQueue.c_str(), node);
  } else {
    // update the one
    node->SetLastHeartBeat(advmsg->kMessageHeader.kSenderTime_sec);
    node->SetNodeStatus(advmsg->kOnline);
  }
  gMutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
bool
FstNode::Update(XrdOucEnv &config) 
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
  
  int statusid = eos::common::FileSystem::GetStatusFromString(fsstatus.c_str());

  return Update(infsname.c_str(), id, schedgroup.c_str(),statusid, &config, errc, errmsg.c_str());
}


/*----------------------------------------------------------------------------*/
bool
FstNode::UpdateQuotaStatus(XrdOucEnv &config) 
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
      if (eos::common::StringConversion::SplitKeyValue(keyval, key, value)) {
	long long fsiduid   = strtoll(key.c_str(),0,10);
	long long fsidquota = strtoll(value.c_str(),0,10);
	unsigned long fsid = (fsiduid>>32) & 0xffffffff;
	unsigned long uid  = fsiduid & 0xffffffff;

	if (fsid ==0) {
	  eos_static_err("decoded quota userbytes    : fsid=%lu uid=%lu bytes=%llu", fsid, uid, fsidquota);
	  continue;
	}

	eos_static_debug("decoded quota userbytes    : fsid=%lu uid=%lu bytes=%llu", fsid, uid, fsidquota);
	FstFileSystem* filesystem = 0;
	if ( (filesystem = (FstFileSystem*)FstNode::gFileSystemById[fsid]) ) {
	  const char* spacename = filesystem->GetSpaceName();
	  SpaceQuota* spacequota = Quota::GetSpaceQuota(spacename);
	  if (spacequota) {
	    spacequota->AddQuota(SpaceQuota::kUserBytesIs, uid, fsidquota-filesystem->UserBytes[uid]);
	    spacequota->AddQuota(SpaceQuota::kAllUserBytesIs, uid, fsidquota-filesystem->UserBytes[uid]);
	  }
	  filesystem->UserBytes[uid] = fsidquota;
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
      if (eos::common::StringConversion::SplitKeyValue(keyval, key, value)) {
	long long fsiduid   = strtoll(key.c_str(),0,10);
	long long fsidquota = strtoll(value.c_str(),0,10);
	unsigned long fsid = (fsiduid>>32) & 0xffffffff;
	unsigned long gid  = fsiduid & 0xffffffff;
	if (fsid ==0) {
	  eos_static_err("decoded quota groupbytes  : fsid=%lu uid=%lu bytes=%llu", fsid, gid, fsidquota);
	  continue;
	}
	eos_static_debug("decoded quota groupbytes  : fsid=%lu uid=%lu bytes=%llu", fsid, gid, fsidquota);
	FstFileSystem* filesystem = 0;
	if ( fsid && (filesystem = (FstFileSystem*)FstNode::gFileSystemById[fsid]) ) {
	  // trying to replace the update hint with differential changes
	  const char* spacename = filesystem->GetSpaceName();
	  SpaceQuota* spacequota = Quota::GetSpaceQuota(spacename);
	  if (spacequota) {
	    spacequota->AddQuota(SpaceQuota::kGroupBytesIs, gid, fsidquota-filesystem->GroupBytes[gid]);
	    spacequota->AddQuota(SpaceQuota::kAllGroupBytesIs, gid, fsidquota-filesystem->GroupBytes[gid]);
	  }
	  filesystem->GroupBytes[gid] = fsidquota;
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
      if (eos::common::StringConversion::SplitKeyValue(keyval, key, value)) {
	long long fsiduid   = strtoll(key.c_str(),0,10);
	long long fsidquota = strtoll(value.c_str(),0,10);
	unsigned long fsid = (fsiduid>>32) & 0xffffffff;
	unsigned long uid  = fsiduid & 0xffffffff;
	if (fsid==0) {
	  eos_static_err("decoded quota userfiles: fsid=%lu uid=%lu files=%llu", fsid, uid, fsidquota);
	  continue;
	}
	eos_static_debug("decoded quota userfiles: fsid=%lu uid=%lu files=%llu", fsid, uid, fsidquota);
	FstFileSystem* filesystem = 0;
	if ( fsid && (filesystem = (FstFileSystem*)FstNode::gFileSystemById[fsid]) ) {
	  // trying to replace the update hint with differential changes
	  const char* spacename = filesystem->GetSpaceName();
	  SpaceQuota* spacequota = Quota::GetSpaceQuota(spacename);
	  if (spacequota) {
	    spacequota->AddQuota(SpaceQuota::kUserFilesIs, uid, fsidquota-filesystem->UserFiles[uid]);
	    spacequota->AddQuota(SpaceQuota::kAllUserFilesIs, uid, fsidquota-filesystem->UserFiles[uid]);
	  }
	  filesystem->UserFiles[uid] = fsidquota;
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
      if (eos::common::StringConversion::SplitKeyValue(keyval, key, value)) {
	long long fsiduid   = strtoll(key.c_str(),0,10);
	long long fsidquota = strtoll(value.c_str(),0,10);
	unsigned long fsid = (fsiduid>>32) & 0xffffffff;
	unsigned long gid  = fsiduid & 0xffffffff;
	if (fsid==0) {
	  eos_static_err("decoded quota groupfiles: fsid=%lu uid=%lu files=%llu", fsid, gid, fsidquota);
	  continue;
	}
	eos_static_debug("decoded quota groupfiles: fsid=%lu uid=%lu files=%llu", fsid, gid, fsidquota);
	FstFileSystem* filesystem = 0;
	if ( fsid && (filesystem = (FstFileSystem*) FstNode::gFileSystemById[fsid]) ) {
	  // trying to replace the update hint with differential changes
	  const char* spacename = filesystem->GetSpaceName();
	  SpaceQuota* spacequota = Quota::GetSpaceQuota(spacename);
	  if (spacequota) {
	    spacequota->AddQuota(SpaceQuota::kGroupFilesIs, gid, fsidquota-filesystem->GroupFiles[gid]);
	    spacequota->AddQuota(SpaceQuota::kAllGroupFilesIs, gid, fsidquota-filesystem->GroupFiles[gid]);
	  }
	  filesystem->GroupFiles[gid] = fsidquota;
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
FstNode::Update(const char* infsname, int id, const char* schedgroup, int bootstatus, XrdOucEnv* env , int errc, const char* errmsg, bool configchangelog) 
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
  FstNode* node = gFstNodes.Find(nodename.c_str());
  if (!node) {
    // create one
    node = new FstNode(nodename.c_str());
    node->hostPortName = fsname.c_str();
    int pos = node->hostPortName.find("/",2);
    if (pos != STR_NPOS) 
      node->hostPortName.erase(0,pos-1);
    gFstNodes.Add(nodename.c_str(), node);
  } 

  // get the filesystem
  FstFileSystem* fs = node->fileSystems.Find(fsname.c_str());
  if (!fs) {
    // create a new filesystem there
    //    fs = new FstFileSystem(id, fsname.c_str(), nodename.c_str(), schedgroup);
    fs = 0;
    if ((!id) || (!fs) ) {
      eos_static_err("unable to create filesystem object");
      return false;
    }

    node->fileSystems.Add(fsname.c_str(),fs);
    gFileSystemById[id]=(unsigned long long)fs;
    // create the quota space entry in the hash to be able to set quota on the empty space
    Quota::GetSpaceQuota(fs->GetSpaceName());
    Quota::UpdateHint(fs->GetId());
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
      Quota::GetSpaceQuota(fs->GetSpaceName());
    }

    
    if (fsname.length())    fs->SetPath(fsname.c_str());
    if (strlen(schedgroup)) fs->SetSchedulingGroup(schedgroup);

    if (bootstatus!= eos::common::FileSystem::kDown) 
      fs->SetBootStatus(bootstatus);

  }

  fs->SetConfigStatusEnv(env);
  fs->SetError(errc, errmsg);
  fs->SetStatfsEnv(env);
  
  Quota::UpdateHint(fs->GetId());

  // change config
  gOFS->ConfEngine->SetConfigValue("fs", fs->GetQueuePath(), fs->GetBootString(), configchangelog );

  return true;
}

/*----------------------------------------------------------------------------*/
FstNode*
FstNode::GetNode(const char* queue) 
{
  
  gMutex.Lock();
  FstNode* node = gFstNodes.Find(queue);
  gMutex.UnLock();
  return node;
}

/*----------------------------------------------------------------------------*/
int
FstNode::ListNodes(const char* key, FstNode* node, void* Arg)  
{
  std::map<std::string,std::string> *nodeOutput = (std::map<std::string,std::string>*) Arg;
  std::map<std::string,std::string> fileSysOutput;
  XrdOucString listing="";
  listing += node->GetInfoString();
  listing += FstFileSystem::GetInfoHeader();
  
  node->fileSystems.Apply(FstNode::ListFileSystems, &fileSysOutput);

  //  std::sort(fileSysOutput.begin(), fileSysOutput.end());
  std::map<std::string,std::string>::const_iterator i;
  for (i=fileSysOutput.begin(); i!=fileSysOutput.end(); ++i) {
    listing += (i->second).c_str();
  }
  nodeOutput->insert(std::pair<std::string,std::string>(node->GetQueue(),listing.c_str()));
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FstNode::ListFileSystems(const char* key, FstFileSystem* filesystem, void* Arg)
{
  std::map<std::string,std::string>* fileSysOutput = (std::map<std::string,std::string>*) Arg;
  XrdOucString sid ="";
  sid += (int)filesystem->GetId();
  fileSysOutput->insert(std::pair<std::string,std::string>(sid.c_str(),filesystem->GetInfoString()));
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FstNode::ExistsNodeFileSystemId(const char* key, FstNode* node, void* Arg)  
{
  unsigned int* listing = (unsigned int*) Arg;

  if (*listing) {
    node->fileSystems.Apply(FstNode::ExistsFileSystemId, Arg);
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FstNode::ExistsFileSystemId(const char* key, FstFileSystem* filesystem, void* Arg)
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
FstNode::FindNodeFileSystem(const char* key, FstNode* node, void* Arg)  
{
  struct FindStruct* finder = (struct FindStruct*) Arg;
  if (!finder->found) {
    node->fileSystems.Apply(FstNode::FindFileSystem, Arg);
    if (finder->found) {
      finder->nodename = node->GetQueue();
      return 1;
    }
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FstNode::BootNode(const char* key, FstNode* node, void* Arg)  
{
  XrdOucString* bootfs = (XrdOucString*) Arg;
  (*bootfs)+="mgm.nodename=";(*bootfs)+= node->GetQueue();
  (*bootfs)+="\t";
  (*bootfs)+=" mgm.fsnames=";
  node->fileSystems.Apply(FstNode::BootFileSystem, Arg);
  (*bootfs)+="\n";
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FstNode::FindFileSystem(const char* key, FstFileSystem* filesystem, void* Arg)
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
FstNode::BootFileSystem(const char* key, FstFileSystem* filesystem, void *Arg)
{
  XrdOucString* bootfs = (XrdOucString*) Arg;
  XrdMqMessage message("mgm"); XrdOucString msgbody="";
  XrdOucEnv config(filesystem->GetBootString());

  //  eos::common::FileSystem::GetBootRequestString(msgbody,config);

  return 0;

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
FstNode::SetBootStatusFileSystem(const char* key, FstFileSystem* filesystem, void *Arg)
{
  int* status = (int*) Arg;

  if (filesystem) {
    filesystem->SetBootStatus(*status);
    // add to config
    gOFS->ConfEngine->SetConfigValue("fs", filesystem->GetQueuePath(), filesystem->GetBootString());
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
int
FstNode::SetHeartBeatTimeFileSystem(const char* key, FstFileSystem* filesystem, void *Arg) 
{
  time_t* hbt = (time_t*) Arg;
  if (filesystem) {
    filesystem->SetHeartBeatTime(*hbt);
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FstNode::SetConfigStatusFileSystem(const char* key, FstFileSystem* filesystem, void *Arg)
{
  int* status = (int*) Arg;
  if (filesystem) {
    filesystem->SetConfigStatus(*status);
    eos_static_info("%s %s", filesystem->GetQueue(), filesystem->GetConfigStatusString());
      
    gOFS->ConfEngine->SetConfigValue("fs", filesystem->GetQueuePath(), filesystem->GetBootString());
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
int
FstNode::SetConfigSchedulingGroupFileSystem(const char* key, FstFileSystem* filesystem, void *Arg)
{
  const char* group = (const char*) Arg;
  if (filesystem) {
    filesystem->SetSchedulingGroup(group);
    eos_static_info("%s %s", filesystem->GetQueue(), filesystem->GetSchedulingGroup());
      
    gOFS->ConfEngine->SetConfigValue("fs", filesystem->GetQueuePath(), filesystem->GetBootString());
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
