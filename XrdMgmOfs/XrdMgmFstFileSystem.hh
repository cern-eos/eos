#ifndef __XRDMGMOFS_FSTFILESYSTEM__HH__
#define __XRDMGMOFS_FSTFILESYSTEM__HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonFileSystem.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

class XrdMgmFstFileSystem : public XrdCommonFileSystem {
private:
  char infoString[1024];
  unsigned int Id;
  XrdOucString Path;
  XrdOucString queueName;
  XrdOucString schedulingGroup;
  time_t bootSentTime;
  time_t bootDoneTime;
  XrdOucString bootFailureMsg;
  XrdOucString bootString;

  int bootStatus;
  int configStatus;

  int errc;
  XrdOucString errmsg;

public:
  unsigned int GetId()    {return Id;}
  const char*  GetPath()  {return Path.c_str();}
  const char*  GetQueue() {return queueName.c_str();}
  int GetBootStatus()     {return bootStatus;}
  int GetConfigStatus()   {return configStatus;}
  time_t GetBootSentTime() { return bootSentTime;}
  time_t GetBootDoneTime() { return bootDoneTime;}
  int GetErrc()            {return errc;}
  const char* GetErrMsg()  {return errmsg.c_str();}


  const char*  GetBootString() { bootString = "mgm.nodename="; bootString += GetQueue(); bootString += "&mgm.fsname="; bootString += GetQueue(); bootString += GetPath(); bootString += "&mgm.fspath="; bootString += GetPath();bootString += "&mgm.fsid="; bootString += (int)GetId(); bootString += "&mgm.fsschedgroup="; bootString += GetSchedulingGroup(); bootString += "&mgm.cfgstatus=";bootString += GetConfigStatusString(); return bootString.c_str();}

  const char*  GetBootFailureMsg() {return bootFailureMsg.c_str();}

  const char*  GetSchedulingGroup() {return schedulingGroup.c_str();}

  const char* GetBootStatusString()   {if (bootStatus==kBootFailure) return "failed"; if (bootStatus==kDown) return "down"; if (bootStatus==kBootSent) return "sent"; if (bootStatus==kBooting) return "booting"; if (bootStatus==kBooted) return "booted"; return "";}
  const char* GetConfigStatusString() {if (configStatus==kOff) return "off"; if (configStatus==kRO) return "ro"; if (configStatus=kRW) return "rw";}
  static const char* GetInfoHeader() {static char infoHeader[1024];sprintf(infoHeader,"%-36s %-04s %-24s %-16s %-10s %-04s %-10s %-3s %s\n","QUEUE","FSID","PATH","SCHEDGROUP","BOOTSTAT","BT", "CONFIGSTAT","EC ", "EMSG"); return infoHeader;}
  const char* GetInfoString()         {sprintf(infoString,"%-36s %04d %-24s %-16s %-10s %04d %-10s %03d %s\n",GetQueue(),GetId(),GetPath(),GetSchedulingGroup(),GetBootStatusString(),GetBootDoneTime()?(GetBootDoneTime()-GetBootSentTime()):(GetBootSentTime()?(time(NULL)-GetBootSentTime()):0) , GetConfigStatusString(), errc, errmsg.c_str());return infoString;}

  void SetDown()    {bootStatus   = kDown;}   
  void SetBootSent(){bootStatus   = kBootSent;bootSentTime = time(NULL);}
  void SetBooting() {bootStatus   = kBooting;}
  void SetBooted()  {bootStatus   = kBooted; bootDoneTime = time(NULL);}
  void SetBootStatus(int status) {bootStatus = status; if (status == kBooted) bootDoneTime = time(NULL); if (status == kBootSent) bootSentTime = time(NULL);}
  void SetBootFailure(const char* txt) {bootStatus = kBootFailure;bootFailureMsg = txt;}
  void SetRO()      {configStatus = kRO;}
  void SetRW()      {configStatus = kRW;}

  void SetId(unsigned int inid)  {Id   = inid;}
  void SetPath(const char* path) {Path = path;}
  void SetSchedulingGroup(const char* group="default") { schedulingGroup = group; }
  void SetError(int inerrc, const char* inerrmsg) {errc = inerrc; if (inerrmsg) errmsg = inerrmsg;}

  XrdMgmFstFileSystem(int id, const char* path, const char* queue, const char* schedulinggroup = "default") {
    Id = id; Path = path; queueName = queue; bootStatus=kDown;configStatus = kOff; schedulingGroup = schedulinggroup; bootSentTime=0; bootFailureMsg=""; bootDoneTime=0; errc=0; errmsg="";
  };
  
  ~XrdMgmFstFileSystem() {};
  
}; 


#endif

