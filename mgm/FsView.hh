#ifndef __EOSMGM_FSVIEW__HH__
#define __EOSMGM_FSVIEW__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Balancer.hh"
#include "mgm/BalanceJob.hh"
#include "mgm/Namespace.hh"
#include "mgm/FileSystem.hh"
#include "mgm/FsView.hh"
#include "common/RWMutex.hh"
#include "common/Logging.hh"
#include "common/GlobalConfig.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#ifndef __APPLE__
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
#endif

#include <map>
#include <set>

#ifndef EOSMGMFSVIEWTEST
#include "mgm/ConfigEngine.hh"
#endif

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class BalanceJob;

//------------------------------------------------------------------------
//! Classes providing views on filesystems by space,group,node
//------------------------------------------------------------------------

class BaseView : public std::set<eos::common::FileSystem::fsid_t> {
private:
  time_t      mHeartBeat;
  std::string mHeartBeatString;
  std::string mHeartBeatDeltaString;
  std::string mStatus;
  std::string mSize;
public:
  std::string mName;
  std::string mType;
  
  BaseView(){mStatus="unknown";}
  ~BaseView(){};
  
  virtual const char* GetConfigQueuePrefix() { return "";}

  void Print(std::string &out, std::string headerformat, std::string listformat);
  
  virtual std::string GetMember(std::string member);
  virtual bool SetConfigMember(std::string key, string value, bool create=false, std::string broadcastqueue="", bool isstatus=false);
  virtual std::string GetConfigMember(std::string key);

  void SetHeartBeat(time_t hb)       { mHeartBeat = hb;       }
  void SetStatus(const char* status) { mStatus = status;      }
  const char* GetStatus()            { return mStatus.c_str();}
  time_t      GetHeartBeat()         { return mHeartBeat;     }


  long long SumLongLong(const char* param, bool lock=true); // calculates the sum of <param> as long long
  double SumDouble(const char* param);      // calculates the sum of <param> as double
  double AverageDouble(const char* param);  // calculates the average of <param> as double
  double MaxDeviation(const char* param);   // calculates the maximum deviation from the average in a group
  double SigmaDouble(const char* param);    // calculates the standard deviation of <param> as double
};

class FsSpace : public BaseView {
public:
#ifndef EOSMGMFSVIEWTEST
  Balancer* mBalancer;
#endif


  FsSpace(const char* name) {
    mName = name; mType = "spaceview"; 
#ifndef EOSMGMFSVIEWTEST
    mBalancer = new Balancer(name);
    // set default balancing variables
    if (GetConfigMember("balancer")== "")
      SetConfigMember("balancer","off"); // enable balancing by default
    if (GetConfigMember("balancer.threshold")=="") 
      SetConfigMember("balancer.threshold","50000000000"); // set deviation treshold
#endif

  }
  ~FsSpace() { 
#ifndef EOSMGMFSVIEWTEST
    if (mBalancer) delete mBalancer;
#endif
  };

  static std::string gConfigQueuePrefix;
  virtual const char* GetConfigQueuePrefix() { return gConfigQueuePrefix.c_str();}
  static const char* sGetConfigQueuePrefix() { return gConfigQueuePrefix.c_str();}
};

//------------------------------------------------------------------------
class FsGroup : public BaseView {
  friend class FsView;

#ifndef EOSMGMFSVIEWTEST
  BalanceJob* mBalanceJob;
#endif

protected:
  unsigned int mIndex;
  
public:

#ifndef EOSMGMFSVIEWTEST
  XrdSysMutex BalancerLock;
  bool StartBalancerJob();
  bool StopBalancerJob();
  void DetachBalancerJob();
#endif


  FsGroup(const char* name) {
    mName = name; 
    mType="groupview";
#ifndef EOSMGMFSVIEWTEST
    mBalanceJob=0;
#endif
}
  ~FsGroup(){};

  unsigned int GetIndex() { return mIndex; }

  static std::string gConfigQueuePrefix;
  virtual const char* GetConfigQueuePrefix() { return gConfigQueuePrefix.c_str();}
  static const char* sGetConfigQueuePrefix() { return gConfigQueuePrefix.c_str();}
};

//------------------------------------------------------------------------
class FsNode : public BaseView {
public:

  virtual std::string GetMember(std::string name);

  FsNode(const char* name) {mName = name; mType="nodesview";}
  ~FsNode(){};

  static std::string gConfigQueuePrefix;
  virtual const char* GetConfigQueuePrefix() { return gConfigQueuePrefix.c_str();}
  static const char* sGetConfigQueuePrefix() { return gConfigQueuePrefix.c_str();}
};

//------------------------------------------------------------------------
class FsView : public eos::common::LogId {
private:
  
  eos::common::FileSystem::fsid_t NextFsId;
  std::map<eos::common::FileSystem::fsid_t , std::string> Fs2UuidMap;
  std::map<std::string, eos::common::FileSystem::fsid_t>  Uuid2FsMap;
  std::string  MgmConfigQueueName;

public:

#ifndef EOSMGMFSVIEWTEST
  static ConfigEngine* ConfEngine;
#endif

  bool Register   (FileSystem* fs);  // this adds or modifies a filesystem
  bool MoveGroup(FileSystem* fs, std::string group);
  void StoreFsConfig(FileSystem* fs);// this stores the filesystem configuration into the config engine and should be called whenever a filesystem wide parameters is changed
  bool UnRegister (FileSystem* fs);  // this removes a filesystem
  bool ExistsQueue(std::string queue, std::string queuepath); // check's if a queue+path exists already
  
  bool RegisterNode   (const char* nodequeue);            // this adds or modifies an fst node
  bool UnRegisterNode (const char* nodequeue);            // this removes an fst node

  bool RegisterSpace  (const char* spacename);            // this adds or modifies a space 
  bool UnRegisterSpace(const char* spacename);            // this remove a space

  bool RegisterGroup   (const char* groupname);           // this adds or modifies a group
  bool UnRegisterGroup (const char* groupname);           // this removes a group

  eos::common::RWMutex ViewMutex;  // protecting all xxxView variables
  eos::common::RWMutex MapMutex;   // protecting all xxxMap varables

  std::map<std::string , std::set<FsGroup*> > mSpaceGroupView; // this contains a map from space name => FsGroup (list of fsid's in a subgroup)

  std::map<std::string , FsSpace* > mSpaceView;
  std::map<std::string , FsGroup* > mGroupView;
  std::map<std::string , FsNode* >  mNodeView;

  std::map<eos::common::FileSystem::fsid_t, FileSystem*> mIdView;
  std::map<FileSystem*, eos::common::FileSystem::fsid_t> mFileSystemView;

  // find filesystem
  FileSystem* FindByQueuePath(std::string &queuepath); // this requires that YOU lock the ViewMap beforehand

  // filesystem mapping functions
  eos::common::FileSystem::fsid_t CreateMapping(std::string fsuuid);
  bool                            ProvideMapping(std::string fsuuid, eos::common::FileSystem::fsid_t fsid);
  eos::common::FileSystem::fsid_t GetMapping(std::string fsuuid);
  bool        HasMapping(eos::common::FileSystem::fsid_t fsid) { return (Fs2UuidMap.count(fsid)>0)?true:false;}
  bool        RemoveMapping(eos::common::FileSystem::fsid_t fsid, std::string fsuuid);
  bool        RemoveMapping(eos::common::FileSystem::fsid_t fsid);

  void PrintSpaces(std::string &out, std::string headerformat, std::string listformat, const char* selection=0);
  void PrintGroups(std::string &out, std::string headerformat, std::string listformat, const char* selection=0);
  void PrintNodes (std::string &out, std::string headerformat, std::string listformat, const char* selection=0);
  
  static std::string GetNodeFormat       (std::string option);
  static std::string GetGroupFormat      (std::string option);
  static std::string GetSpaceFormat      (std::string option);
  static std::string GetFileSystemFormat (std::string option);

  void Reset(); // clears all mappings and filesystem objects

  pthread_t hbthread;

  static void* StaticHeartBeatCheck(void*);
  void* HeartBeatCheck();

  FsView() { 
    MgmConfigQueueName="";
    
#ifndef EOSMGMFSVIEWTEST
    ConfEngine = 0;
#endif
    XrdSysThread::Run(&hbthread, FsView::StaticHeartBeatCheck, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "HeartBeat Thread");
  }

  ~FsView() {
    // this currently never happens
    XrdSysThread::Cancel(hbthread);
    XrdSysThread::Join(hbthread,0);
  };

  void SetConfigQueues(const char* mgmconfigqueue, const char* nodeconfigqueue, const char* groupconfigqueue, const char* spaceconfigqueue) {
    FsSpace::gConfigQueuePrefix = spaceconfigqueue;
    FsGroup::gConfigQueuePrefix = groupconfigqueue;
    FsNode::gConfigQueuePrefix  = nodeconfigqueue;
    MgmConfigQueueName = mgmconfigqueue;
  }

#ifndef EOSMGMFSVIEWTEST
  void SetConfigEngine(ConfigEngine* engine) {ConfEngine = engine;}
  bool ApplyFsConfig(const char* key, std::string &val);
  bool ApplyGlobalConfig(const char* key, std::string &val);

  // set/return fields from 
  bool SetGlobalConfig(std::string key, std::string value);
  std::string GetGlobalConfig(std::string key);

#endif

  void SetNextFsId(eos::common::FileSystem::fsid_t fsid);

  static FsView gFsView; // singleton
};

EOSMGMNAMESPACE_END

#endif
