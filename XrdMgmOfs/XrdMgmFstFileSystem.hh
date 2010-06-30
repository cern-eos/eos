#ifndef __XRDMGMOFS_FSTFILESYSTEM__HH__
#define __XRDMGMOFS_FSTFILESYSTEM__HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonFileSystem.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <sys/vfs.h>
/*----------------------------------------------------------------------------*/

class XrdMgmFstFileSystem : public XrdCommonFileSystem {
private:
  char infoString[1024];
  unsigned int Id;
  XrdOucString Path;           // this is only the path
  XrdOucString queueName;      // this does not contain the path
  XrdOucString queueNamePath;  // this contains the path

  XrdOucString schedulingGroup;
  XrdOucString spaceName;

  unsigned int schedulingGroupIndex;

  time_t bootSentTime;
  time_t bootDoneTime;
  XrdOucString bootFailureMsg;
  XrdOucString bootString;

  int bootStatus;
  int configStatus;

  int errc;
  XrdOucString errmsg;

  struct statfs statFs;
  int ropen; // files open for read
  int wopen; // files open for write

public:

  google::dense_hash_map<long, unsigned long long> UserBytes; // the key is uid
  google::dense_hash_map<long, unsigned long long> GroupBytes;// the key is gid
  google::dense_hash_map<long, unsigned long long> UserFiles; // the key is uid
  google::dense_hash_map<long, unsigned long long> GroupFiles;// the key is gid

  unsigned int GetId()    {return Id;}
  const char*  GetPath()  {return Path.c_str();}
  const char*  GetQueue() {return queueName.c_str();}
  const char*  GetQueuePath() {queueNamePath = GetQueue(); queueNamePath += GetPath(); return queueNamePath.c_str();}
  int GetBootStatus()     {return bootStatus;}
  int GetConfigStatus()   {return configStatus;}
  time_t GetBootSentTime() { return bootSentTime;}
  time_t GetBootDoneTime() { return bootDoneTime;}
  int GetErrc()            {return errc;}
  const char* GetErrMsg()  {return errmsg.c_str();}
  unsigned int GetSchedulingGroupIndex() { return schedulingGroupIndex; }

  void GetHostPort(XrdOucString &host, int &port){
    int spos,epos, dpos; 
    spos=epos=dpos=0;
    spos = queueName.find("/",1); 
    epos = queueName.find("/",spos+1);
    dpos = queueName.find(":",spos+1);
    if ( (spos != STR_NPOS) && (epos != STR_NPOS) ) {
      if ( (dpos == STR_NPOS) || (dpos > epos) ) {
	host.assign(queueName, spos+1, epos-1);
	port = 1094;
      } else {
	host.assign(queueName, spos+1, dpos-1);
	XrdOucString sport;
	sport.assign(queueName, dpos+1, epos-1);
	port = atoi(sport.c_str());
      }
    }
    return;
  }

  void ExtractSchedulinGroupIndex() {
    int ppos = schedulingGroup.find("."); 
    if (ppos != STR_NPOS) {
      XrdOucString sindex; 
      sindex.assign(schedulingGroup, ppos+1); 
      schedulingGroupIndex = atoi(sindex.c_str());
    } else {
      schedulingGroupIndex = 0;
    }
  }

  const char*  GetBootString() { bootString = "mgm.nodename="; bootString += GetQueue(); bootString += "&mgm.fsname="; bootString += GetQueue(); bootString += GetPath(); bootString += "&mgm.fspath="; bootString += GetPath();bootString += "&mgm.fsid="; bootString += (int)GetId(); bootString += "&mgm.fsschedgroup="; bootString += GetSchedulingGroup(); bootString += "&mgm.cfgstatus=";bootString += GetConfigStatusString(); return bootString.c_str();}

  const char*  GetBootFailureMsg() {return bootFailureMsg.c_str();}

  const char*  GetSchedulingGroup() {return schedulingGroup.c_str();}

  // a space is derived from a scheduling group which has to be defined as <schedgroup> = <space> or <schedgroup> = <space>.<int>
  const char*  GetSpaceName() { int ppos = schedulingGroup.find("."); if (ppos != STR_NPOS) {spaceName.assign(schedulingGroup, 0, ppos-1); return spaceName.c_str();} else return schedulingGroup.c_str();}


  const char* GetBootStatusString()   {if (bootStatus==kBootFailure) return "failed"; if (bootStatus==kDown) return "down"; if (bootStatus==kBootSent) return "sent"; if (bootStatus==kBooting) return "booting"; if (bootStatus==kBooted) return "booted"; if (bootStatus==kOpsError) return "opserror";  return "";}
  const char* GetConfigStatusString() {if (configStatus==kOff) return "off"; if (configStatus==kUnknown) return "?"; if (configStatus==kRO) return "ro"; if (configStatus==kDrain) return "drain"; if (configStatus==kWO) return "wo"; if (configStatus==kRW) return "rw"; return "unknown";}
  static const char* GetInfoHeader() {static char infoHeader[1024];sprintf(infoHeader,"%-36s %-4s %-24s %-16s %-10s %-4s %-10s %-8s %-8s %-8s %-3s %s\n","QUEUE","FSID","PATH","SCHEDGROUP","BOOTSTAT","BT", "CONFIGSTAT","BLOCKS", "FREE", "FILES", "EC ", "EMSG"); return infoHeader;}
  const char* GetInfoString()         {XrdOucString sizestring,freestring,filesstring; sprintf(infoString,"%-36s %04u %-24s %-16s %-10s %04lu %-10s %-8s %-8s %-8s %03d %s\n",GetQueue(),GetId(),GetPath(),GetSchedulingGroup(),GetBootStatusString(),GetBootDoneTime()?(GetBootDoneTime()-GetBootSentTime()):(GetBootSentTime()?(time(0)-GetBootSentTime()):0) , GetConfigStatusString(), XrdCommonFileSystem::GetReadableSizeString(sizestring,statFs.f_blocks * 4096ll,"B"), XrdCommonFileSystem::GetReadableSizeString(freestring, statFs.f_bfree * 4096ll,"B"), XrdCommonFileSystem::GetReadableSizeString(filesstring, (statFs.f_files-statFs.f_ffree) *1ll),errc, errmsg.c_str());return infoString;}

  void SetDown()    {bootStatus   = kDown;}   
  void SetBootSent(){bootStatus   = kBootSent;bootSentTime = time(0);bootDoneTime = 0;}
  void SetBooting() {bootStatus   = kBooting;}
  void SetBooted()  {bootStatus   = kBooted; bootDoneTime = time(0);if (!bootSentTime) bootSentTime = time(0);}
  void SetBootStatus(int status) {if (bootStatus != status) {bootStatus = status; if (status == kBooted) bootDoneTime = time(0); if (status == kBootSent) bootSentTime = time(0);if (!bootSentTime) bootSentTime = time(0)-9999;}}
  void SetBootFailure(const char* txt) {bootStatus = kBootFailure;bootFailureMsg = txt;}
  void SetRO()      {configStatus = kRO;}
  void SetRW()      {configStatus = kRW;}
  void SetWO()      {configStatus = kWO;}

  void SetId(unsigned int inid)  {Id   = inid;}
  void SetPath(const char* path) {Path = path;}
  void SetSchedulingGroup(const char* group="default") { schedulingGroup = group; ExtractSchedulinGroupIndex(); }
  void SetError(int inerrc, const char* inerrmsg) {errc = inerrc; if (inerrmsg) errmsg = inerrmsg; else errmsg="";}
  void SetStatfsEnv(XrdOucEnv* env) {
    const char* val;
    if (!env) return;
    if (( val = env->Get("statfs.type")))  {statFs.f_type = strtol(val,0,10);}
    if (( val = env->Get("statfs.bsize"))) {statFs.f_bsize = strtol(val,0,10);}
    if (( val = env->Get("statfs.blocks"))){statFs.f_blocks = strtol(val,0,10);}
    if (( val = env->Get("statfs.bfree"))) {statFs.f_bfree = strtol(val,0,10);}
    if (( val = env->Get("statfs.bavail"))){statFs.f_bavail = strtol(val,0,10);}
    if (( val = env->Get("statfs.files"))) {statFs.f_files = strtol(val,0,10);}
    if (( val = env->Get("statfs.ffree"))) {statFs.f_ffree = strtol(val,0,10);}
    if (( val = env->Get("statfs.namelen"))){statFs.f_namelen = strtol(val,0,10);}
    if (( val = env->Get("statfs.ropen"))) {ropen = strtol(val,0,10);}
    if (( val = env->Get("statfs.wopen"))) {wopen = strtol(val,0,10);}
  }

  void SetConfigStatusEnv(XrdOucEnv* env) {
    if (env) {
      const char* val = env->Get("mgm.cfgstatus");
      if (val) { SetConfigStatus( GetConfigStatusFromString(val));}
    }
  }

  void SetConfigStatus(int status) { if ( (status >= kUnknown) && (status <= kRW)) configStatus = status; }

  struct statfs* GetStatfs() { return &statFs;}

  XrdMgmFstFileSystem(int id, const char* path, const char* queue, const char* schedulinggroup = "default") {
    Id = id; Path = path; queueName = queue; bootStatus=kDown;configStatus = kUnknown; schedulingGroup = schedulinggroup; ExtractSchedulinGroupIndex();  bootSentTime=0; bootFailureMsg=""; bootDoneTime=0; errc=0; errmsg=""; memset(&statFs,0,sizeof(statFs)); spaceName = ""; 
    UserBytes.set_empty_key(-1);
    GroupBytes.set_empty_key(-1);
    UserFiles.set_empty_key(-1);
    GroupFiles.set_empty_key(-1);
    ropen = 0;
    wopen = 0;
  };

  ~XrdMgmFstFileSystem() {};

}; 


#endif

