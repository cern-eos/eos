// ----------------------------------------------------------------------
// File: XrdMqOfs.hh
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



class XrdMqOfsFile : public XrdSfsFile
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
  
  XrdMqOfsFile(char *user=0) : XrdSfsFile(user) {
    QueueName = "";envOpaque=0;Out=0;IsOpen = false;tident="";}
  
  ~XrdMqOfsFile() { 
    if (envOpaque) delete envOpaque;
    close();
  }


  virtual int fctl(int, const char*, XrdOucErrInfo&) {return SFS_ERROR;}
  virtual const char* FName() {return "queue";}
  virtual int getMmap(void**, off_t&) {return SFS_ERROR;}
  virtual int read(XrdSfsFileOffset, XrdSfsXferSize) {return SFS_ERROR;}
  virtual int read(XrdSfsAio*) {return SFS_ERROR;}
  virtual XrdSfsXferSize write(XrdSfsFileOffset, const char*, XrdSfsXferSize) {return SFS_OK;}
  virtual int write(XrdSfsAio*) {return SFS_OK;}
  virtual int sync() {return SFS_OK;}
  virtual int sync(XrdSfsAio*) {return SFS_OK;}
  virtual int truncate(XrdSfsFileOffset) {return SFS_OK;}
  virtual int getCXinfo(char*, int&) {return SFS_ERROR;}


private:
  XrdOucEnv*             envOpaque;
  XrdMqMessageOut*       Out;
  
  XrdOucString           QueueName;
  bool                   IsOpen;
  const char             *tident;
};


class XrdMqOfsOutMutex {
public:
  XrdMqOfsOutMutex();
  ~XrdMqOfsOutMutex();
};

class XrdMqOfs : public XrdSfsFileSystem
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
  
  XrdSfsFile      *newFile(char *user=0, int MonID=0) {return (XrdSfsFile*) new XrdMqOfsFile(user);}
  XrdSfsDirectory      *newDir(char *user=0, int MonID=0) {return (XrdSfsDirectory*) 0;}

  int              Emsg(const char *, XrdOucErrInfo&, int, const char *x, const char *y="");
  int              myPort; 

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

  int              getStats(char *buff, int blen) {return 0;}

  virtual int chmod(const char*, XrdSfsMode, XrdOucErrInfo&, const XrdSecEntity*, const char*) {return 0;}
  virtual int fsctl(int, const char*, XrdOucErrInfo&, const XrdSecEntity*) {return 0;}
  virtual int exists(const char*, XrdSfsFileExistence&, XrdOucErrInfo&, const XrdSecEntity*, const char*) {return 0;}
  virtual int mkdir(const char*, XrdSfsMode, XrdOucErrInfo&, const XrdSecEntity*, const char*) {return 0;}
  virtual int prepare(XrdSfsPrep&, XrdOucErrInfo&, const XrdSecEntity*) {return 0;}
  virtual int rem(const char*, XrdOucErrInfo&, const XrdSecEntity*, const char*) {return 0;}
  virtual int remdir(const char*, XrdOucErrInfo&, const XrdSecEntity*, const char*) {return 0;}
  virtual int rename(const char*, const char*, XrdOucErrInfo&, const XrdSecEntity*, const char*, const char*) {return 0;}
  virtual int truncate(const char*, XrdSfsFileOffset, XrdOucErrInfo&, const XrdSecEntity*, const char*) {return 0;}
  
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
  char         *ConfigFN;

private:
  
  static  XrdSysError *eDest;
};

#endif
