//         $Id: XrdMqOfs.hh,v 1.00 2007/10/04 01:34:19 abh Exp $
#ifndef __XFTSOFS_NS_H__
#define __XFTSOFS_NS_H__

#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <memory.h>
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <map>
#include <string>
#include <vector>
#include <deque>

#include <utime.h>
#include <pwd.h>
#include <uuid/uuid.h>

#include "XrdClient/XrdClientAdmin.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTable.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucChain.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdOfs/XrdOfs.hh"
#include "Xrd/XrdScheduler.hh"


// if we have too many messages pending we don't take new ones for the moment
#define MQOFSMAXMESSAGEBACKLOG 100000
#define MQOFSMAXQUEUEBACKLOG 50000
#define MQOFSREJECTQUEUEBACKLOG 100000

class XrdSysError;
class XrdSysLogger;

class XrdSmartOucEnv : public XrdOucEnv {
private:
  int nref;
public:
  XrdSysMutex procmutex;
  
  int  Refs() { return nref; }
  void DecRefs() { nref--;}
  void AddRefs(int nrefs) { ;nref += nrefs;}
  XrdSmartOucEnv(const char *vardata=0, int vardlen=0) : XrdOucEnv(vardata,vardlen) {
    nref = 0;
  }

  virtual ~XrdSmartOucEnv() {}
};

class XrdMqOfsMatches {
public:
  int matches;
  int messagetype;
  bool backlog;
  bool backlogrejected;
  XrdOucString backlogqueues;
  XrdOucString sendername;
  XrdOucString queuename;
  XrdSmartOucEnv* message;
  const char* tident;
  XrdMqOfsMatches(const char* qname, XrdSmartOucEnv* msg, const char* t, int type, const char* sender="ignore") {matches=0; queuename=qname; message = msg;tident = t;messagetype = type;sendername = sender;backlog=false;backlogqueues="";backlogrejected=false;}
  ~XrdMqOfsMatches(){}
};

class XrdMqMessageOut : public XrdSysMutex {
public:
  bool AdvisoryStatus;
  bool AdvisoryQuery; 
  int  nQueued;
  int  WaitOnStat;

  XrdOucString QueueName;
  XrdSysSemWait DeletionSem; 
  XrdSysSemWait MessageSem; 
  std::deque<XrdSmartOucEnv*> MessageQueue;
  XrdMqMessageOut(const char* queuename){MessageBuffer="";AdvisoryStatus=false; AdvisoryQuery=false; nQueued=0;QueueName=queuename;MessageQueue.clear();};
  std::string MessageBuffer;
  size_t RetrieveMessages();
  virtual ~XrdMqMessageOut(){
    RetrieveMessages();
  };
};



class XrdMqOfsFile : public XrdOfsFile
{
public:

  int open(const char                *fileName,
           XrdSfsFileOpenMode   openMode,
           mode_t               createMode,
           const XrdSecEntity        *client = 0,
           const char                *opaque = 0);
  
  int close();

  
  XrdSfsXferSize read(XrdSfsFileOffset   fileOffset,
                      char              *buffer,
                      XrdSfsXferSize     buffer_size);

  int stat(struct stat *buf);
  
  XrdMqOfsFile(char *user=0) : XrdOfsFile(user) {
    QueueName = "";envOpaque=0;Out=0;IsOpen = false;}
  
  ~XrdMqOfsFile() { 
    if (envOpaque) delete envOpaque;
    close();
  }

private:
  XrdOucEnv*             envOpaque;
  XrdMqMessageOut*       Out;
  
  XrdOucString           QueueName;
  bool                   IsOpen;
  
};


class XrdMqOfsOutMutex {
public:
  XrdMqOfsOutMutex();
  ~XrdMqOfsOutMutex();
};

class XrdMqOfs : public XrdOfs
{
public:
  friend class XrdMqOfsFile;
  
  // our plugin function
  int FSctl(int, XrdSfsFSctl&, XrdOucErrInfo&, const XrdSecEntity*);
  
  XrdMqOfs(XrdSysError *lp=0);
  virtual              ~XrdMqOfs() {if (HostName) free(HostName);if (HostPref) free(HostPref);}
  virtual bool Init(XrdSysError &);
  const   char          *getVersion();
  int          Stall(XrdOucErrInfo &error, int stime, const char *msg); 
  
  int          Configure(XrdSysError& Eroute);

  XrdSysMutex     StoreMutex;          // -> protecting the string store hash
  
  static  XrdOucHash<XrdOucString> *stringstore;
  
  XrdOfsFile      *newFile(char *user=0) {return (XrdOfsFile*) new XrdMqOfsFile(user);}
  
  char*            HostName;           // -> our hostname as derived in XrdOfs
  char*            HostPref;           // -> our hostname as derived in XrdOfs without domain
  XrdOucString     ManagerId;          // -> manager id in <host>:<port> format
  XrdOucString     QueuePrefix;        // -> prefix of the accepted queues to server
  XrdOucString     QueueAdvisory;      // -> "<queueprefix>/*" for advisory message matches
  XrdOucString     BrokerId;           // -> manger id + queue name as path

  std::map<std::string, XrdMqMessageOut*> QueueOut;  // -> hash of all output's connected
  XrdSysMutex                 QueueOutMutex;  // -> mutex protecting the output hash

  bool             Deliver(XrdMqOfsMatches &Match); // -> delivers a message into matching output queues

  std::map<std::string, XrdSmartOucEnv*> Messages;  // -> hash with all messages

  XrdSysMutex                 MessagesMutex;  // -> mutex protecting the message hash
  int              stat(const char               *Name,
                        struct stat              *buf,
                        XrdOucErrInfo            &error,
                        const XrdSecEntity       *client = 0,
                        const char               *opaque = 0);

  int              stat(const char               *Name,
                        mode_t                   &mode,
                        XrdOucErrInfo            &error,
                        const XrdSecEntity       *client = 0,
                        const char               *opaque = 0);
  

  XrdSysMutex  StatLock;
  time_t       StartupTime;
  time_t       LastOutputTime;
  long long    ReceivedMessages;
  long long    DeliveredMessages;
  long long    FanOutMessages;
  long long    AdvisoryMessages;
  long long    UndeliverableMessages;
  long long    DiscardedMonitoringMessages;
  long long    NoMessages;
  long long    BacklogDeferred;
  long long    QueueBacklogHits;
  void         Statistics();
  XrdOucString StatisticsFile;

private:
  
  static  XrdSysError *eDest;
};

#endif
