#ifndef __EOSMGM_FSTNODES__HH__
#define __EOSMGM_FSTNODES__HH__

/*----------------------------------------------------------------------------*/
// this is needed because of some openssl definition conflict!
#undef des_set_key
#include <google/dense_hash_map>
#include <google/sparsehash/densehashtable.h>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include "mgm/FstFileSystem.hh"
#include "mq/XrdMqMessage.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

class FstNode  {
  friend class ProcCommand;
  friend class Messaging;
private:
  char infoString[1024];
  XrdOucString queueName;
  
  time_t lastHeartBeat; // time when last hb was sent
  int nodeStatus;       // status of the node

protected:

  XrdOucHash<FstFileSystem> fileSystems;


public:
  XrdOucString hostPortName;

  const char* GetNodeStatusString() {if (nodeStatus==kHeartBeatLow) return "lowhb"; if (nodeStatus==kOffline) return "offline"; if (nodeStatus==kOnline) return "online"; return "";}
  enum eNodeStatus { kHeartBeatLow=-1, kOffline=0, kOnline=1}; 

  static google::dense_hash_map<unsigned int, unsigned long long> gFileSystemById;  // the value points to the address of a FstFileSystem object. Doing so saves a lot of hassle

  static bool Update(XrdAdvisoryMqMessage* advmsg);
  static bool Update(XrdOucEnv &config);
  static bool UpdateQuotaStatus(XrdOucEnv &config);
  static bool Update(const char* infsname, int id, const char* schedgroup = "default", int bootstatus=eos::common::FileSystem::kDown, XrdOucEnv* env=0, int errc=0, const char* errmsg=0, bool configchangelog=false);

  bool SetNodeStatus(int status); 
  bool SetNodeConfigStatus(int status); 
  bool SetNodeConfigSchedulingGroup(const char* schedgroup);

  static FstNode* GetNode(const char* queue);
   
  FstNode(const char* queue){queueName = queue; nodeStatus = kOffline;lastHeartBeat = 0; }
  ~FstNode(){}

  unsigned int GetNumberOfFileSystems() {return fileSystems.Num();}

  const char* GetQueue() {return queueName.c_str();}
  time_t GetLastHeartBeat() {return lastHeartBeat;}
  void   SetLastHeartBeat(time_t hbt);

  static const char* GetInfoHeader() {static char infoHeader[1024];sprintf(infoHeader,"%-36s %-4s %-10s %-s\n","QUEUE","HBT","STATUS","#FS");return infoHeader;}
  const char* GetInfoString() {
    time_t tdif = time(0)-GetLastHeartBeat();
    if ( (tdif<10000) && (tdif>=0) ) {
      sprintf(infoString,"\n%-36s %04lu %-10s %02d\n", GetQueue(), tdif, GetNodeStatusString(), GetNumberOfFileSystems()); return infoString;
    } else {
      sprintf(infoString,"\n%-36s ---- %-10s %02d\n", GetQueue(), GetNodeStatusString(), GetNumberOfFileSystems()); return infoString;}
  }
  static XrdOucHash<FstNode> gFstNodes;  
  //  static google::dense_hash_map<long, unsigned long long> gFstIndex;

  
  
  static XrdSysMutex  gMutex;  // mutex to protect node hash access


  // to do listings
  static int                    ListNodes(const char* key, FstNode* node, void* Arg);
  static int                    ListFileSystems(const char* key, FstFileSystem* filesystem, void* Arg);
  
  // to check if an id is already in use
  static int                    ExistsNodeFileSystemId(const char* key, FstNode* node, void *Arg);
  static int                    ExistsFileSystemId(const char* key, FstFileSystem* filesystem, void *Arg);
  
  // to remove by id
  struct FindStruct {
  public:
    unsigned int id;
    XrdOucString nodename;
    XrdOucString fsname;
    bool found;
    
    FindStruct(unsigned int lid = 0, XrdOucString name = "") {id = lid; nodename="";fsname=name;found=false;}
    ~FindStruct() {}
  };

  // returns the node + filesystem names of an id
  static int                    FindNodeFileSystem(const char* key, FstNode* node, void *Arg);
  static int                    BootNode(const char* key, FstNode* node, void *Arg);

  static int                    FindFileSystem(const char* key, FstFileSystem* filesystem, void *Arg);
  static int                    BootFileSystem(const char* key, FstFileSystem* filesystem, void *Arg);
  static int                    SetBootStatusFileSystem(const char* key ,FstFileSystem* filesystem, void *Arg);
  static int                    SetHeartBeatTimeFileSystem(const char* key ,FstFileSystem* filesystem, void *Arg);
  static int                    SetConfigStatusFileSystem(const char* key ,FstFileSystem* filesystem, void *Arg);
  static int                    SetConfigSchedulingGroupFileSystem(const char* key ,FstFileSystem* filesystem, void *Arg);
};

EOSMGMNAMESPACE_END

#endif

