/*----------------------------------------------------------------------------*/
#include "mgm/FsView.hh"
#include "common/StringConversion.hh"

/*----------------------------------------------------------------------------*/
#include <math.h>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
FsView FsView::gFsView;
std::string FsSpace::gConfigQueuePrefix;
std::string FsGroup::gConfigQueuePrefix;
std::string FsNode::gConfigQueuePrefix;


/*----------------------------------------------------------------------------*/
std::string
FsView::GetNodeFormat(std::string option) {

  if (option == "m") {
    // monitoring format
    return "member=type:width=1:format=os|sep= |member=name:width=1:format=os|sep= |member=status:width=1:format=os|sep= |member=cfg.status:width=1:format=os|sep= |member=heartbeatdelta:width=1:format=os|sep= |member=nofs:width=1:format=os";
  }

  if (option == "l") {
    // long output formag
    return "header=1:member=type:width=10:format=-s|sep=   |member=name:width=32:format=s|sep=   |member=status:width=10:format=s|sep=   |member=cfg.status:width=12:format=s|sep=   |member=heartbeatdelta:width=16:format=s|sep=   |member=nofs:width=5:format=s"; 
  }
  return "header=1:member=type:width=10:format=-s|sep=   |member=name:width=32:format=s|sep=   |member=status:width=10:format=s|sep=   |member=cfg.status:width=12:format=s|sep=   |member=heartbeatdelta:width=16:format=s|sep=   |member=nofs:width=5:format=s"; 
}

/*----------------------------------------------------------------------------*/
std::string
FsView::GetFileSystemFormat(std::string option) {
  
  if (option == "m") {
    // monitoring format
    return "key=host:width=1:format=os|sep= |key=port:width=1:format=os|sep= |key=id:width=1:format=os|sep= |key=uuid:width=1:format=os|sep= |key=path:width=1:format=os";
  }

  if (option == "l") {
    // long format
    return "header=1:key=host:width=24:format=-s|sep= |key=port:width=5:format=s|sep= |key=id:width=6:format=s|sep= |key=uuid:width=16:format=s|sep= |key=path:width=16:format=s|key=schedgroup:width=16:format=s";
  }
  
  return "header=1:key=host:width=24:format=-s|sep= |key=port:width=5:format=s|sep= |key=id:width=6:format=s|sep= |key=uuid:width=16:format=s|sep= |key=path:width=16:format=s";
}

/*----------------------------------------------------------------------------*/
std::string
FsView::GetSpaceFormat(std::string option) {
  if (option == "m") {
    // monitoring format
    return "member=type:width=1:format=os|sep= |member=name:width=1:format=os";
  }

  if (option == "l") {
    // long output formag
    return "header=1:member=type:width=10:format=-s|sep=   |member=name:width=32:format=s";
  }

  return "header=1:member=type:width=10:format=-s|sep=   |member=name:width=32:format=s";
}

/*----------------------------------------------------------------------------*/
std::string
FsView::GetGroupFormat(std::string option) {
  if (option == "m") {
    // monitoring format
    return "member=type:width=1:format=os|sep= |member=name:width=1:format=os";
  }

  if (option == "l") {
    // long output formag
    return "header=1:member=type:width=10:format=-s|sep=   |member=name:width=32:format=s";
  }

  return "header=1:member=type:width=10:format=-s|sep=   |member=name:width=32:format=s";
}

/*----------------------------------------------------------------------------*/
bool 
FsView::Register (eos::common::FileSystem* fs) 
{
  if (!fs)
    return false;

  eos::common::RWMutexWriteLock lock(ViewMutex);

  // create a snapshot of the current variables of the fs
  eos::common::FileSystem::fs_snapshot snapshot;
  
  if (fs->SnapShotFileSystem(snapshot)) {
    //----------------------------------------------------------------
    //! align view by filesystem object and filesystem id
    //----------------------------------------------------------------

    // check if there is already a filesystem with the same path on the same node

    if (mNodeView.count(snapshot.mQueue)) {
      // loop over all attached filesystems and compare the queue path
      std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
      for (it=mNodeView[snapshot.mQueue]->begin(); it != mNodeView[snapshot.mQueue]->end(); it++) {
	fprintf(stderr,"Comparing %s = %s\n", FsView::gFsView.mIdView[*it]->GetQueuePath().c_str(), snapshot.mQueuePath.c_str());
	if (FsView::gFsView.mIdView[*it]->GetQueuePath() == snapshot.mQueuePath) {
	  // this queue path was already existing, we cannot register
	  return false;
	}
      }
    }
    
    // check if this is already in the view
    if (mFileSystemView.count(fs)) {
      // this filesystem is already there, this might be an update
      eos::common::FileSystem::fsid_t fsid;
      fsid = mFileSystemView[fs];
      if (fsid != snapshot.mId) {
	// remove previous mapping
	mIdView.erase(fsid);
	// setup new two way mapping
	mFileSystemView[fs] = snapshot.mId;
	mIdView[snapshot.mId] = fs;
	eos_debug("updating mapping %u<=>%lld", snapshot.mId,fs);
      }
    } else {
      mFileSystemView[fs] = snapshot.mId;
      mIdView[snapshot.mId] = fs;
      eos_debug("registering mapping %u<=>%lld", snapshot.mId,fs);
    }
    
    //----------------------------------------------------------------
    //! align view by nodename (= MQ queue) e.g. /eos/<host>:<port>/fst
    //----------------------------------------------------------------
    
    // check if we have already a node view
    if (mNodeView.count(snapshot.mQueue)) {
      mNodeView[snapshot.mQueue]->insert(snapshot.mId);
      eos_debug("inserting into node view %s<=>%u",snapshot.mQueue.c_str(), snapshot.mId,fs);
    } else {
      FsNode* node = new FsNode(snapshot.mQueue.c_str());
      mNodeView[snapshot.mQueue] = node;
      node->insert(snapshot.mId);
      eos_debug("creating/inserting into node view %s<=>%u",snapshot.mQueue.c_str(), snapshot.mId,fs);
    }
    
    //----------------------------------------------------------------
    //! align view by groupname
    //----------------------------------------------------------------
    
    // check if we have already a group view
    if (mGroupView.count(snapshot.mGroup)) {
      mGroupView[snapshot.mGroup]->insert(snapshot.mId);
      eos_debug("inserting into group view %s<=>%u",snapshot.mGroup.c_str(), snapshot.mId,fs);
    } else {
      FsGroup* group = new FsGroup(snapshot.mGroup.c_str());
      mGroupView[snapshot.mGroup] = group;
      group->insert(snapshot.mId);
      eos_debug("creating/inserting into group view %s<=>%u",snapshot.mGroup.c_str(), snapshot.mId,fs);
    }

    //----------------------------------------------------------------
    //! align view by spacename
    //----------------------------------------------------------------

    // check if we have already a space view
    if (mSpaceView.count(snapshot.mSpace)) {
      mSpaceView[snapshot.mSpace]->insert(snapshot.mId);
      eos_debug("inserting into space view %s<=>%u %x",snapshot.mSpace.c_str(), snapshot.mId,fs);
    } else {
      FsSpace* space = new FsSpace(snapshot.mSpace.c_str());
      mSpaceView[snapshot.mSpace] = space;
      space->insert(snapshot.mId);
      eos_debug("creating/inserting into space view %s<=>%u %x",snapshot.mSpace.c_str(), snapshot.mId,fs);
    }    
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool 
FsView::UnRegister(eos::common::FileSystem* fs) 
{
  if (!fs)
    return false;
  
  eos::common::RWMutexWriteLock lock(ViewMutex);

  // create a snapshot of the current variables of the fs
  eos::common::FileSystem::fs_snapshot snapshot;

  if (fs->SnapShotFileSystem(snapshot)) {  
    //----------------------------------------------------------------
    //! remove view by filesystem object and filesystem id
    //----------------------------------------------------------------
    
    // check if this is in the view
    if (mFileSystemView.count(fs)) {
      mFileSystemView.erase(fs);
      mIdView.erase(snapshot.mId);
      eos_debug("unregister %lld from filesystem view", fs);
    }

    //----------------------------------------------------------------
    //! remove fs from node view & evt. remove node view
    //----------------------------------------------------------------
    if (mNodeView.count(snapshot.mQueue)) {
      FsNode* node = mNodeView[snapshot.mQueue];
      node->erase(snapshot.mId);
      eos_debug("unregister node %s from node view", node->GetMember("name").c_str());
      if (!node->size()) {
	mNodeView.erase(snapshot.mQueue);
	delete node;
      }
    }

    //----------------------------------------------------------------
    //! remove fs from group view & evt. remove group view
    //----------------------------------------------------------------
    if (mGroupView.count(snapshot.mGroup)) {
      FsGroup* group = mGroupView[snapshot.mGroup];
      group->erase(snapshot.mId);
      eos_debug("unregister group %s from group view", group->GetMember("name").c_str());
      if (!group->size()) {
	mGroupView.erase(snapshot.mGroup);
	delete group;
      }
    }

    //----------------------------------------------------------------
    //! remove fs from space view & evt. remove space view
    //----------------------------------------------------------------
    if (mSpaceView.count(snapshot.mSpace)) {
      FsSpace* space = mSpaceView[snapshot.mSpace];
      space->erase(snapshot.mId);
      eos_debug("unregister space %s from space view", space->GetMember("name").c_str());
      if (!space->size()) {
	mSpaceView.erase(snapshot.mSpace);
	delete space;
      }
    }

    //----------------------------------------------------------------
    //! remove mapping
    //----------------------------------------------------------------
    RemoveMapping(snapshot.mId, snapshot.mUuid);
    

    //----------------------------------------------------------------
    //! delete fs object
    //----------------------------------------------------------------
    delete fs;

    return true;
  }
  return false;
}

/*----------------------------------------------------------------------------*/
bool 
FsView::RegisterNode(const char* nodename)
{
  //----------------------------------------------------------------
  //! add view by nodename (= MQ queue) e.g. /eos/<host>:<port>/fst
  //----------------------------------------------------------------
  
  std::string nodequeue = nodename;

  // check if we have already a node view
  if (mNodeView.count(nodequeue)) {
    eos_debug("node is existing");
    return false;
  } else {
    FsNode* node = new FsNode(nodequeue.c_str());
    mNodeView[nodequeue] = node;
    eos_debug("creating node view %s",nodequeue.c_str());
    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool 
FsView::UnRegisterNode(const char* nodename)
{
  //----------------------------------------------------------------
  //! remove view by nodename (= MQ queue) e.g. /eos/<host>:<port>/fst
  //----------------------------------------------------------------

  // we have to remove all the connected filesystems via UnRegister(fs) to keep space, group, node view in sync
  std::map<std::string , FsNode* >::iterator it;
  bool retc = true;
  bool hasfs= false;
  if (mNodeView.count(nodename)) {
    while (mNodeView.count(nodename) && (mNodeView[nodename]->begin()!= mNodeView[nodename]->end())) {
      eos::common::FileSystem::fsid_t fsid = *(mNodeView[nodename]->begin());
      eos::common::FileSystem* fs = mIdView[fsid];
      if (fs) {
	hasfs = true;
	eos_static_debug("Unregister filesystem fsid=%llu node=%s queue=%s", (unsigned long long) fsid, nodename, fs->GetQueue().c_str());
	retc |= UnRegister(fs);
      }
    }
    if (!hasfs) {
      // we have to explicitly remove the node from the view here because no fs was removed
      retc = (mNodeView.erase(nodename)?true:false);
    }
  }

  return retc;
}

/*----------------------------------------------------------------------------*/
bool 
FsView::RegisterSpace(const char* spacename)
{
  //----------------------------------------------------------------
  //! add view by spacename (= MQ queue) e.g. /eos/<host>:<port>/fst
  //----------------------------------------------------------------
  
  std::string spacequeue = spacename;

  // check if we have already a space view
  if (mSpaceView.count(spacequeue)) {
    eos_debug("space is existing");
    return false;
  } else {
    FsSpace* space = new FsSpace(spacequeue.c_str());
    mSpaceView[spacequeue] = space;
    eos_debug("creating space view %s",spacequeue.c_str());
    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool 
FsView::UnRegisterSpace(const char* spacename)
{
  //----------------------------------------------------------------
  //! remove view by spacename (= MQ queue) e.g. /eos/<host>:<port>/fst
  //----------------------------------------------------------------

  // we have to remove all the connected filesystems via UnRegister(fs) to keep space, group, space view in sync
  std::map<std::string , FsSpace* >::iterator it;
  bool retc = true;
  bool hasfs= false;
  if (mSpaceView.count(spacename)) {
    while (mSpaceView.count(spacename) && (mSpaceView[spacename]->begin()!= mSpaceView[spacename]->end())) {
      eos::common::FileSystem::fsid_t fsid = *(mSpaceView[spacename]->begin());
      eos::common::FileSystem* fs = mIdView[fsid];
      if (fs) {
	hasfs = true;
	eos_static_debug("Unregister filesystem fsid=%llu space=%s queue=%s", (unsigned long long) fsid, spacename, fs->GetQueue().c_str());
	retc |= UnRegister(fs);
      }
    }
    if (!hasfs) {
      // we have to explicitly remove the space from the view here because no fs was removed
      retc = (mSpaceView.erase(spacename)?true:false);
    }
  }

  return retc;
}

/*----------------------------------------------------------------------------*/
bool 
FsView::RegisterGroup(const char* groupname)
{
  //----------------------------------------------------------------
  //! add view by groupname (= MQ queue) e.g. /eos/<host>:<port>/fst
  //----------------------------------------------------------------
  
  std::string groupqueue = groupname;

  // check if we have already a group view
  if (mGroupView.count(groupqueue)) {
    eos_debug("group is existing");
    return false;
  } else {
    FsGroup* group = new FsGroup(groupqueue.c_str());
    mGroupView[groupqueue] = group;
    eos_debug("creating group view %s",groupqueue.c_str());
    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool 
FsView::UnRegisterGroup(const char* groupname)
{
  //----------------------------------------------------------------
  //! remove view by groupname (= MQ queue) e.g. /eos/<host>:<port>/fst
  //----------------------------------------------------------------

  // we have to remove all the connected filesystems via UnRegister(fs) to keep group, group, group view in sync
  std::map<std::string , FsGroup* >::iterator it;
  bool retc = true;
  bool hasfs= false;
  if (mGroupView.count(groupname)) {
    while (mGroupView.count(groupname) && (mGroupView[groupname]->begin()!= mGroupView[groupname]->end())) {
      eos::common::FileSystem::fsid_t fsid = *(mGroupView[groupname]->begin());
      eos::common::FileSystem* fs = mIdView[fsid];
      if (fs) {
	hasfs = true;
	eos_static_debug("Unregister filesystem fsid=%llu group=%s queue=%s", (unsigned long long) fsid, groupname, fs->GetQueue().c_str());
	retc |= UnRegister(fs);
      }
    }
    if (!hasfs) {
      // we have to explicitly remove the group from the view here because no fs was removed
      retc = (mGroupView.erase(groupname)?true:false);
    }
  }

  return retc;
}

/*----------------------------------------------------------------------------*/
std::string 
BaseView::GetMember(std::string member) {
  if (member == "name")
      return mName;
  if (member == "type")
    return mType;
  if (member == "nofs") {
    char line[1024];
    snprintf(line, sizeof(line)-1, "%llu", (unsigned long long) size());
    mSize = line;
    return mSize;
  }
  
  if (member == "heartbeat") {
    char line[1024];
    snprintf(line, sizeof(line)-1, "%llu", (unsigned long long) mHeartBeat);
    mHeartBeatString = line;
    return mHeartBeatString;
  }
  
  if (member == "heartbeatdelta") {
    char line[1024];
    if ( labs(time(NULL)-mHeartBeat) > 86400) {
      snprintf(line, sizeof(line)-1, "~");
    } else {
      snprintf(line, sizeof(line)-1, "%llu", (unsigned long long) (time(NULL)-mHeartBeat));
    }
    mHeartBeatDeltaString = line;
    return mHeartBeatDeltaString;
  }
  
  if (member == "status") {
    return mStatus;
  }
  
  // return global config value
  std::string prefix = member;
  prefix.erase(4);
  if (prefix == "cfg.") {
    std::string val="???";
    member.erase(0,4);
    eos::common::GlobalConfig::gConfig.SOM()->HashMutex.LockRead();
    std::string nodeconfigname = eos::common::GlobalConfig::gConfig.QueuePrefixName(GetConfigQueuePrefix(), mName.c_str());
    XrdMqSharedHash* hash = eos::common::GlobalConfig::gConfig.Get(nodeconfigname.c_str());
    if (hash) {
      val = hash->Get(member.c_str());
    }
    eos::common::GlobalConfig::gConfig.SOM()->HashMutex.UnLockRead();
    return val;
  }
  return "";
}

/*----------------------------------------------------------------------------*/
eos::common::FileSystem::fsid_t 
FsView::CreateMapping(std::string fsuuid)
{
  eos::common::RWMutexWriteLock lock(MapMutex);
  if (Uuid2FsMap.count(fsuuid)) {
    return Uuid2FsMap[fsuuid];
  } else {
    if (!NextFsId) NextFsId++; // we don't use 0 as fsid!
    while (Fs2UuidMap.count(NextFsId)) {
      NextFsId++;
    }
    Uuid2FsMap[fsuuid]=NextFsId;
    Fs2UuidMap[NextFsId] = fsuuid;
    return NextFsId;
  }
}

/*----------------------------------------------------------------------------*/
bool 
FsView::ProvideMapping(std::string fsuuid, eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexWriteLock lock(MapMutex);  
  if (Uuid2FsMap.count(fsuuid)) {
    if (Uuid2FsMap[fsuuid] == fsid) 
      return true;  // we accept if it is consistent with the existing mapping
    else 
      return false; // we reject if it is in contradiction to an existing mapping
  } else {
    Uuid2FsMap[fsuuid] = fsid;
    Fs2UuidMap[fsid]   = fsuuid;
    return true;
  }
}

/*----------------------------------------------------------------------------*/
eos::common::FileSystem::fsid_t 
FsView::GetMapping(std::string fsuuid)
{
  eos::common::RWMutexReadLock lock(MapMutex);
  if (Uuid2FsMap.count(fsuuid)) {
    return Uuid2FsMap[fsuuid];
  } else {
    return 0; // 0 means there is no mapping
  }
}

bool        
FsView::RemoveMapping(eos::common::FileSystem::fsid_t fsid, std::string fsuuid) 
{
  eos::common::RWMutexWriteLock lock(MapMutex);
  bool removed=false;
  if (Uuid2FsMap.count(fsuuid)) {
    Uuid2FsMap.erase(fsuuid);
    removed = true;
  }
  if (Fs2UuidMap.count(fsid)) {
    Fs2UuidMap.erase(fsid);
    removed = true;
  }
  return removed;
}

/*----------------------------------------------------------------------------*/
void
FsView::PrintSpaces(std::string &out, std::string headerformat, std::string listformat)
{
  std::map<std::string , FsSpace* >::iterator it;
  bool first=true;
  for (it = mSpaceView.begin(); it != mSpaceView.end(); it++) {
    it->second->Print(out, headerformat,listformat);
    first = false;
    if ( !listformat.length() && ((headerformat.find("header=1:")) == 0)) {
      headerformat.erase(0, 9);
    }
  }
}

/*----------------------------------------------------------------------------*/
void
FsView::PrintGroups(std::string &out, std::string headerformat, std::string listformat)
{
  std::map<std::string , FsGroup* >::iterator it;
  bool first=true;
  for (it = mGroupView.begin(); it != mGroupView.end(); it++) {
    it->second->Print(out, headerformat,listformat);
    first = false;
    if ( !listformat.length() && ((headerformat.find("header=1:")) == 0)) {
      headerformat.erase(0, 9);
    }
  }
}

/*----------------------------------------------------------------------------*/
void
FsView::PrintNodes(std::string &out, std::string headerformat, std::string listformat)
{
  std::map<std::string , FsNode* >::iterator it;
  bool first=true;
  for (it = mNodeView.begin(); it != mNodeView.end(); it++) {
    it->second->Print(out, headerformat,listformat);
    first = false;
    if ( !listformat.length() && ((headerformat.find("header=1:")) == 0)) {
      headerformat.erase(0, 9);
    }
  }
}

/*----------------------------------------------------------------------------*/
long long
BaseView::SumLongLong(const char* param)
{
  eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
  
  long long sum = 0;
  std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
  for (it=begin(); it != end(); it++) {
    sum += FsView::gFsView.mIdView[*it]->GetLongLong(param);
  }
  return sum;
}

/*----------------------------------------------------------------------------*/
double 
BaseView::SumDouble(const char* param) 
{
  eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
  
  double sum = 0;
  std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
  for (it=begin(); it != end(); it++) {
    sum += FsView::gFsView.mIdView[*it]->GetDouble(param);
  }
  return sum;
}

/*----------------------------------------------------------------------------*/
double
BaseView::AverageDouble(const char* param)
{
  eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
  
  double sum = 0;
  int cnt=0;
  std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
  for (it=begin(); it != end(); it++) {
    cnt++;
    sum += FsView::gFsView.mIdView[*it]->GetDouble(param);
  }
  return (double)(1.0*sum/cnt);
}

/*----------------------------------------------------------------------------*/
double
BaseView::SigmaDouble(const char* param)

{  
  eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
  
  double avg = AverageDouble(param);;
  double sumsquare=0;
  int cnt=0;
  std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
  for (it=begin(); it != end(); it++) {
    cnt++;
    sumsquare += pow( (avg-FsView::gFsView.mIdView[*it]->GetDouble(param)),2);
  }

  sumsquare = sqrt(sumsquare/cnt);

  return sumsquare;
}

/*----------------------------------------------------------------------------*/
void
BaseView::Print(std::string &out, std::string headerformat, std::string listformat) 
{
  //-------------------------------------------------------------------------------
  // headerformat
  //-------------------------------------------------------------------------------
  // format has to be provided as a chain (separated by "|" ) of the following tags
  // "member=<key>:width=<width>:format=[+][-][so]:unit=<unit>" -> to print a member variable of the view
  // "avg=<key>:width=<width>:format=[fo]"                      -> to print the average 
  // "sum=<key>:width=<width>:format=[lo]                       -> to print a sum
  // "sig=<key>:width=<width>:format=[lo]                       -> to print the standard deviation
  // "sep=<seperator>"                                          -> to put a seperator
  // "header=1"                                                 -> put a header with description on top! This must be the first format tag!!!
  //-------------------------------------------------------------------------------
  // listformat
  //-------------------------------------------------------------------------------
  // format has to be provided as a chain (separated by "|" ) of the following tags
  // "key=<key>:width=<width>:format=[+][-][slfo]:unit=<unit>"  -> to print a key of the attached children
  // "sep=<seperator>"                        -> to put a seperator
  // "header=1"                                                 -> put a header with description on top! This must be the first format tag!!!
  // the formats are:
  // 's' : print as string
  // 'l' : print as long long
  // 'f' : print as double
  // 'o' : print as <key>=<val>
  // '-' : left align the printout
  // '+' : convert numbers into k,M,G,T,P ranges
  // the unit is appended to every number:
  // e.g. 1500 with unit=B would end up as '1.5 kB'
  // the command only appends to <out> and DOES NOT initialize it

  std::vector<std::string> formattoken;
  bool buildheader = false;

  std::string header = "";
  std::string body   = "";

  eos::common::StringConversion::Tokenize(headerformat, formattoken, "|");

  for (unsigned int i=0; i< formattoken.size(); i++) {
    std::vector<std::string> tagtoken;
    std::map<std::string, std::string> formattags;

    eos::common::StringConversion::Tokenize(formattoken[i],tagtoken,":");
    for (unsigned int j=0; j< tagtoken.size(); j++) {
      std::vector<std::string> keyval;
      eos::common::StringConversion::Tokenize(tagtoken[j], keyval,"=");
      formattags[keyval[0]] = keyval[1];
    }

    //---------------------------------------------------------------------------------------
    // "key=<key>:width=<width>:format=[slfo]"

    bool alignleft=false;
    if ( (formattags["format"].find("-") != std::string::npos) ) {
      alignleft = true;
    }
    
    if (formattags.count("header") ) {
      // add the desired seperator
      if (formattags.count("header") == 1) {
	buildheader=true;
      }
    }

    if (formattags.count("width") && formattags.count("format")) {
      unsigned int width = atoi(formattags["width"].c_str());
      // string
      char line[1024];
      char tmpline[1024];
      char lformat[1024];
      char lenformat[1024];
      line[0]=0;
      lformat[0]=0;

      if ((formattags["format"].find("s"))!= std::string::npos) 
	snprintf(lformat,sizeof(lformat)-1, "%%s");
      
      if ((formattags["format"].find("l"))!= std::string::npos)
	snprintf(lformat,sizeof(lformat)-1, "%%lld");
      
      
      if ((formattags["format"].find("f"))!= std::string::npos)
	snprintf(lformat,sizeof(lformat)-1, "%%.02f");
      
      // protect against missing format types
      if (lformat[0]== 0) 
	continue;
      
      if (alignleft) {
	snprintf(lenformat,sizeof(lenformat)-1, "%%-%ds",width);
      } else {
	snprintf(lenformat,sizeof(lenformat)-1, "%%%ds",width);
      }
      
      // normal member printout
      if (formattags.count("member")) {
	snprintf(tmpline,sizeof(tmpline)-1,lformat,GetMember(formattags["member"]).c_str());
	snprintf(line,sizeof(line)-1,lenformat,tmpline);

	if (buildheader) {
	  char headline[1024];
	  char lenformat[1024];
	  snprintf(lenformat, sizeof(lenformat)-1, "%%%ds", width-1);
	  snprintf(headline,sizeof(headline)-1, lenformat,formattags["member"].c_str());
	  std::string sline = headline;
	  if (sline.length() > (width-1)) {
	    sline.erase(0, ((sline.length()-width+1+3)>0)?(sline.length()-width+1+3):0);
	    sline.insert(0,"...");
	  }
	  header += "#";
	  header += sline;
	}
      }
      
      // sum printout
      if (formattags.count("sum")) {
	snprintf(tmpline,sizeof(tmpline)-1,lformat,SumLongLong(formattags["sum"].c_str()));
	
	if ( ((formattags["format"].find("+")) != std::string::npos) ) {
	  std::string ssize;
	  eos::common::StringConversion::GetReadableSizeString(ssize,(unsigned long long)SumLongLong(formattags["sum"].c_str()), formattags["unit"].c_str());
	  snprintf(line,sizeof(line)-1,lenformat,ssize.c_str());
	} else {
	  snprintf(line,sizeof(line)-1,lenformat,tmpline);
	}

	if (buildheader) {
	  char headline[1024];
	  char lenformat[1024];
	  snprintf(lenformat, sizeof(lenformat)-1, "%%%ds", width-1);
	  snprintf(headline,sizeof(headline)-1, lenformat,formattags["sum"].c_str());
	  std::string sline = headline;
	  if (sline.length() > (width-6)) {
	    sline.erase(0, ((sline.length()-width+6+3)>0)?(sline.length()-width+6+3):0);
	    sline.insert(0,"...");
	  }
	  header += "#";
	  header += "sum(";
	  header += sline;
	  header += ")";
	}
      }
      
      if (formattags.count("avg")) {
	snprintf(tmpline,sizeof(tmpline)-1,lformat,AverageDouble(formattags["avg"].c_str()));
	
	if ( (formattags["format"].find("+")!= std::string::npos) ) {
	  std::string ssize;
	  eos::common::StringConversion::GetReadableSizeString(ssize,(unsigned long long)AverageDouble(formattags["avg"].c_str()), formattags["unit"].c_str());
	  snprintf(line,sizeof(line)-1,lenformat,ssize.c_str());
	} else {
	  snprintf(line,sizeof(line)-1,lenformat,tmpline);
	}
	
	if (buildheader) {
	  char headline[1024];
	  char lenformat[1024];
	  snprintf(lenformat, sizeof(lenformat)-1, "%%%ds", width-1);
	  snprintf(headline,sizeof(headline)-1, lenformat,formattags["avg"].c_str());
	  std::string sline = headline;
	  if (sline.length() > (width-6)) {
	    sline.erase(0, ((sline.length()-width+6+3)>0)?(sline.length()-width+6+3):0);
	    sline.insert(0,"...");
	  }
	  header += "#";
	  header += "avg(";
	  header += sline;
	  header += ")";
	}
      }

      if (formattags.count("sig")) {
	snprintf(tmpline,sizeof(tmpline)-1,lformat,SigmaDouble(formattags["sig"].c_str()));
	
	if ( (formattags["format"].find("+")!= std::string::npos) ) {
	  std::string ssize;
	  eos::common::StringConversion::GetReadableSizeString(ssize,(unsigned long long)SigmaDouble(formattags["sig"].c_str()), formattags["unit"].c_str());
	  snprintf(line,sizeof(line)-1,lenformat,ssize.c_str());
	} else {
	  snprintf(line,sizeof(line)-1,lenformat,tmpline);
	}
	if (buildheader) {
	  char headline[1024];
	  char lenformat[1024];
	  snprintf(lenformat, sizeof(lenformat)-1, "%%%ds", width-1);
	  snprintf(headline,sizeof(headline)-1, lenformat,formattags["sig"].c_str());
	  std::string sline = headline;
	  if (sline.length() > (width-6)) {
	    sline.erase(0, ((sline.length()-width+6+3)>0)?(sline.length()-width+6+3):0);
	    sline.insert(0,"...");
	  }
	  header += "#";
	  header += "sig(";
	  header += sline;
	  header += ")";
	}
      }

      if ( (formattags["format"].find("o")!= std::string::npos) ) {
	char keyval[4096];
	buildheader = false; // auto disable header
	if (formattags.count("member")) {
	  snprintf(keyval,sizeof(keyval)-1,"%s=%s", formattags["member"].c_str(), line);
	}
	if (formattags.count("sum")) {
	  snprintf(keyval,sizeof(keyval)-1,"sum.%s=%s", formattags["sum"].c_str(), line);
	}
	if (formattags.count("avg")) {
	  snprintf(keyval,sizeof(keyval)-1,"avg.%s=%s", formattags["avg"].c_str(), line);
	}
	if (formattags.count("sig")) {
	  snprintf(keyval,sizeof(keyval)-1,"sig.%s=%s", formattags["sig"].c_str(), line);
	}
	body += keyval;
      }  else {
	std::string sline = line;
	if (sline.length() > width) {
	  sline.erase(0, ((sline.length()-width+3)>0)?(sline.length()-width+3):0);
	  sline.insert(0,"...");
	}
	body += sline;
      }
    }

    if (formattags.count("sep") ) {
      // add the desired seperator
      body += formattags["sep"];
      if (buildheader) {
	header += formattags["sep"];
      }
    }
  }

  body += "\n";
  
  //---------------------------------------------------------------------------------------
  if (listformat.length()) {
    bool first=true;
    std::set<eos::common::FileSystem::fsid_t>::const_iterator it;      
    // if a format was given for the filesystem children, forward the print to the filesystems
    for (it=begin(); it != end(); it++) {
      FsView::gFsView.mIdView[*it]->Print(body, listformat);
      if (first) {
	// put the header format only in the first node printout
	first = false;
	if ( (listformat.find("header=1:")) == 0) {
	  listformat.erase(0, 9);
	}
      }
    }      
  }

  if (buildheader) {
    std::string line ="";
    line += "#";
    for (unsigned int i=0; i< (header.length()-1); i++) {
      line += "-";
    }
    line += "\n";
    out += line;
    out += header; out += "\n";
    out += line;
    out += body;
  } else {
    out += body;
  }
}

EOSMGMNAMESPACE_END
