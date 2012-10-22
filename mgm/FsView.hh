// ----------------------------------------------------------------------
// File: FsView.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef __EOSMGM_FSVIEW__HH__
#define __EOSMGM_FSVIEW__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Balancer.hh"
#include "mgm/Namespace.hh"
#include "mgm/FileSystem.hh"
#include "mgm/FsView.hh"
#include "common/RWMutex.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include "common/GlobalConfig.hh"
#include "common/TransferQueue.hh"
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
  size_t      mInQueue;

public:
  std::string mName;

  std::string mType;
  
  BaseView(){mStatus="unknown";mHeartBeat=0;mInQueue=0;}
  ~BaseView(){};
  
  virtual const char* GetConfigQueuePrefix() { return "";}

  void Print(std::string &out, std::string headerformat, std::string listformat);
  
  virtual std::string GetMember(std::string member);
  virtual bool SetConfigMember(std::string key, string value, bool create=false, std::string broadcastqueue="", bool isstatus=false);
  virtual std::string GetConfigMember(std::string key);
  bool GetConfigKeys(std::vector<std::string> &keys);

  void SetHeartBeat(time_t hb)       { mHeartBeat = hb;       }
  void SetStatus(const char* status) { mStatus = status;      }
  const char* GetStatus()            { return mStatus.c_str();}
  time_t      GetHeartBeat()         { return mHeartBeat;     }
  void SetInQueue(size_t iq)         { mInQueue=iq; }

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

  // this variable is set when a configuration get's loaded to avoid overwriting of the loaded values by default values
  static bool gDisableDefaults;

  FsSpace(const char* name) {
    mName = name; mType = "spaceview"; 
#ifndef EOSMGMFSVIEWTEST
    mBalancer = new Balancer(name);

    if (!gDisableDefaults) {
      // set default balancing variables
      if (GetConfigMember("balancer")== "") 
        SetConfigMember("balancer","off",true,"/eos/*/mgm"); // disable balancing by default
      if (GetConfigMember("balancer.threshold")=="") 
        SetConfigMember("balancer.threshold","20",true,"/eos/*/mgm"); // set deviation treshold
      if (GetConfigMember("balancer.node.rate")=="") 
        SetConfigMember("balancer.node.rate","25",true,"/eos/*/mgm"); // set balancing rate per balancing stream
      if (GetConfigMember("balancer.node.ntx")=="") 
        SetConfigMember("balancer.node.ntx","2",true,"/eos/*/mgm"); // set parallel balancing streams per node
      if (GetConfigMember("drain.node.rate")=="") 
        SetConfigMember("drainer.node.rate","25",true,"/eos/*/mgm"); // set drain rate per drain stream
      if (GetConfigMember("drainer.node.ntx")=="") 
        SetConfigMember("drainer.node.ntx","2",true,"/eos/*/mgm"); // set parallel draining streams per node
      if (GetConfigMember("graceperiod")=="")
        SetConfigMember("graceperiod","86400",true,"/eos/*/mgm");
      if (GetConfigMember("drainperiod")=="")
        SetConfigMember("drainperiod","86400",true,"/eos/*/mgm");
      if (GetConfigMember("scaninterval")=="")
        SetConfigMember("scaninterval","604800",true,"/eos/*/mgm");
      if (GetConfigMember("quota")=="") 
        SetConfigMember("quota", "off",true,"/eos/*/mgm");
      if (GetConfigMember("groupmod")=="") 
        SetConfigMember("groupmod", "0",true,"/eos/*/mgm");
      if (GetConfigMember("groupsize")=="") 
        SetConfigMember("groupsize", "0",true,"/eos/*/mgm");
    }
    
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

  
  bool ApplySpaceDefaultParameters(eos::mgm::FileSystem* fs, bool force=false); //return's true if something was modified
};

//------------------------------------------------------------------------
class FsGroup : public BaseView {
  friend class FsView;

protected:
  unsigned int mIndex;
  
public:

  FsGroup(const char* name) {
    mName = name; 
    mType="groupview";
    mIndex=0;
  }

#ifdef EOSMGMFSVIEWTEST
  virtual ~FsGroup() {};
#else
  virtual ~FsGroup();
#endif

  unsigned int GetIndex() { return mIndex; }

  static std::string gConfigQueuePrefix;
  virtual const char* GetConfigQueuePrefix() { return gConfigQueuePrefix.c_str();}
  static const char* sGetConfigQueuePrefix() { return gConfigQueuePrefix.c_str();}
};

//------------------------------------------------------------------------
class FsNode : public BaseView {
public:

  static std::string gManagerId;

  virtual std::string GetMember(std::string name);
  eos::common::TransferQueue* mGwQueue;

  FsNode(const char* name) {
    mName = name; mType="nodesview";
    std::string n = mName.c_str(); n += "/gw";
    mGwQueue = new eos::common::TransferQueue(mName.c_str(), n.c_str(), "txq", (eos::common::FileSystem*)0, eos::common::GlobalConfig::gConfig.SOM(), false);
  }

  ~FsNode();


  void SetNodeConfigDefault() { 
    if (!(GetConfigMember("manager").length())) {
      SetConfigMember("manager", gManagerId, true, mName.c_str(), true); 
    }
    if (!(GetConfigMember("stat.balance.ntx").length())) {
      SetConfigMember("stat.balance.ntx", "2",true,  mName.c_str(), true); // we configure for two balancer transfers by default
    }
    if (!(GetConfigMember("stat.balance.rate").length())) {
      SetConfigMember("stat.balance.rate","25",true, mName.c_str(), true); // we configure for 25 Mb/s for each balancing transfer
    }
    eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

    if (!(GetConfigMember("symkey").length())) {
      SetConfigMember("symkey",symkey->GetKey64(),true, mName.c_str(), true); // we put the current sym key
    }

    if (!(GetConfigMember("debug.level").length())) {
      SetConfigMember("debug.level","notice",true, mName.c_str(), true); // we put to 'notice' by default
    }

    if ( (GetConfigMember("txgw") != "on") && (GetConfigMember("txgw") != "off") ) {
      SetConfigMember("txgw","off", true, mName.c_str(), true); // by default node's aren't transfer gateways
    }

    if ( (atoi(GetConfigMember("gw.ntx").c_str()) == 0) || (atoi(GetConfigMember("gw.ntx").c_str()) == LONG_MAX ) ) {
      SetConfigMember("gw.ntx","10", true, mName.c_str(), true); // by default we set 10 parallel gw transfers
    }

    if ( (atoi(GetConfigMember("gw.rate").c_str()) == 0) || (atoi(GetConfigMember("gw.rate").c_str()) == LONG_MAX ) ) {
      SetConfigMember("gw.rate","120", true, mName.c_str(), true); // by default we allow 1GBit speed per transfer
    }
  }
  
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
  void UnRegisterNodes();                                 // this removes all fst nodes

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

  eos::common::RWMutex GwMutex; // protecting the mGwNodes;
  std::set<std::string> mGwNodes;

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

  void Reset();       // clears all mappings and filesystem objects obtaining locks

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
  
  void StopHeartBeat() {
    if (hbthread) {
      XrdSysThread::Cancel(hbthread);
      XrdSysThread::Join(hbthread,0);
      hbthread=0;
    }
  }

  ~FsView() {
    StopHeartBeat();
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
