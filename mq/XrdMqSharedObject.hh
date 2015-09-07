// ----------------------------------------------------------------------
// File: XrdMqSharedObject.hh
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

#ifndef __XRDMQ_SHAREDHASH_HH__
#define __XRDMQ_SHAREDHASH_HH__

/*----------------------------------------------------------------------------*/
#include "mq/XrdMqRWMutex.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
#include <vector>
#include <set>
#include <queue>
#include <sys/time.h>
#include <sys/types.h>
#include <regex.h>

/*----------------------------------------------------------------------------*/
#include "mq/XrdMqClient.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysAtomics.hh"
#include "XrdSys/XrdSysSemWait.hh"
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/

#define XRDMQSHAREDHASH_CMD       "mqsh.cmd"
#define XRDMQSHAREDHASH_UPDATE    "mqsh.cmd=update"
#define XRDMQSHAREDHASH_MUXUPDATE "mqsh.cmd=muxupdate"
#define XRDMQSHAREDHASH_BCREQUEST "mqsh.cmd=bcrequest"
#define XRDMQSHAREDHASH_BCREPLY   "mqsh.cmd=bcreply"
#define XRDMQSHAREDHASH_DELETE    "mqsh.cmd=delete"
#define XRDMQSHAREDHASH_REMOVE    "mqsh.cmd=remove"
#define XRDMQSHAREDHASH_SUBJECT   "mqsh.subject"
#define XRDMQSHAREDHASH_PAIRS     "mqsh.pairs"
#define XRDMQSHAREDHASH_KEYS      "mqsh.keys"
#define XRDMQSHAREDHASH_REPLY     "mqsh.reply"
#define XRDMQSHAREDHASH_TYPE      "mqsh.type"

class XrdMqSharedObjectManager;

class XrdMqSharedHashEntry {
public:
  struct timeval mtime;
  std::string entry;
  std::string key;
  unsigned long long ChangeId;

  XrdMqSharedHashEntry(){key="";entry = ""; UpdateTime();ChangeId=0;mtime.tv_sec=0;mtime.tv_usec=0;}

  ~XrdMqSharedHashEntry(){};

  struct timeval* GetTime() { return &mtime;}

  void Set(const char* s, const char* k=0)  { entry = s; UpdateTime();ChangeId++;if (k) key=k;}
  void Set(std::string &s) { entry = s; UpdateTime();ChangeId++;}
  void SetKey(const char* lkey) {key = lkey;}
  const char* GetKey() {return key.c_str();}

  long long GetAgeInMilliSeconds() { 
    struct timeval ntime; 
    gettimeofday(&ntime, 0);
    return (((ntime.tv_sec - mtime.tv_sec)*1000) + ((ntime.tv_usec - mtime.tv_usec)/1000));
  }
  
  double GetAgeInSeconds()      {return GetAgeInMilliSeconds()/1000.0;}
  
  void UpdateTime() {
    gettimeofday(&mtime, 0);
  }
  
  const char* GetEntry() { return entry.c_str(); }

  void Dump(XrdOucString &out) {
    char formatline[1024];
    snprintf(formatline, sizeof(formatline)-1,"value:%-32s age:%.2f changeid:%llu", entry.c_str(), GetAgeInSeconds(), ChangeId);
    out += formatline;
  }
};


class XrdMqSharedHash {
  friend class XrdMqSharedObjectManager;

private:
protected:
  unsigned long long ChangeId;
  std::string BroadCastQueue;
  std::string Subject;

  XrdMqRWMutex StoreMutex;
  std::map<std::string, XrdMqSharedHashEntry> Store;

  std::set<std::string> Transactions;
  std::set<std::string> Deletions;
  XrdSysMutex TransactionMutex;
  bool IsTransaction;

  XrdSysSemWait StoreSem;

  std::string Type;
  
  XrdMqSharedObjectManager* SOM;

public:

  XrdMqSharedHash(const char* subject = "", const char* broadcastqueue = "", XrdMqSharedObjectManager* som=0) ;

  virtual ~XrdMqSharedHash();

  void SetBroadCastQueue(const char* broadcastqueue) { BroadCastQueue = broadcastqueue;}
  
  bool Set(std::string key, std::string value, bool broadcast=true, bool tempmodsubjects=false, bool notify=true) {
    return Set(key.c_str(),value.c_str(), broadcast, tempmodsubjects, notify);
  }

  bool Set(const char* key, const char* value, bool broadcast=true, bool tempmodsubjects=false, bool notify=true);
  bool SetNoLockNoBroadCast(const char* key, const char* value, bool tempmodsubjects, bool notify=true);

  bool Set(std::map<std::string, std::string> &map, bool broadcast=true, bool tempmodsubjects=false, bool notify=true) {
    std::map<std::string, std::string>::const_iterator it;
    bool success=true;
    for (it=map.begin(); it!=map.end(); it++) {
      success *= Set(it->first.c_str(), it->second.c_str(),broadcast,tempmodsubjects,notify);
    }
    return success;
  }

  bool SetLongLong(const char* key, long long value, bool broadcast=true) {
    char convert[1024];
    snprintf(convert, sizeof(convert)-1,"%lld", value);
    return Set(key, convert, broadcast);
  }

  bool SetDouble(const char* key, const double &value, bool broadcast=true) {
    char convert[1024];
    snprintf(convert, sizeof(convert)-1,"%f", value);
    return Set(key, convert, broadcast);
  }

  bool Delete(const char* key, bool broadcast=true, bool notify=true);

  void Clear(bool broadcast=true);

  void GetKeys(std::vector<std::string> &keys) {
    XrdMqRWMutexWriteLock lock(StoreMutex);
    std::map<std::string, XrdMqSharedHashEntry>::iterator storeit;
    for (storeit = Store.begin(); storeit != Store.end(); storeit++) {
      keys.push_back(storeit->first);
    }
    return;
  }

  std::string StoreAsString(const char* notprefix=""); // serializes like 'key1=val1 key2=val2 ... keyn=keyn' and return's it if key does not start with <notprefix>

  bool OpenTransaction() {TransactionMutex.Lock(); Transactions.clear();IsTransaction= true; return true;}
  
  bool CloseTransaction();

  std::string Get(std::string key) {AtomicInc(GetCounter);std::string get=""; XrdMqRWMutexReadLock lock(StoreMutex);if (Store.count(key.c_str())) get = Store[key.c_str()].GetEntry(); return get;}

  std::string Get(const char* key) {AtomicInc(GetCounter);std::string get=""; XrdMqRWMutexReadLock lock(StoreMutex); if (Store.count(key)) get = Store[key].GetEntry(); return get;}

  long long   GetLongLong(const char* key) {
    std::string get = Get(key); if (!get.length()) {return 0;} else {errno=0; long long ret = strtoll(get.c_str(),0,10);if (!errno) return ret; else return 0;}
  }

  unsigned int GetUInt(const char* key) {
    return (unsigned int) GetLongLong(key);
  }

  double      GetDouble(const char* key) {
    std::string get = Get(key); return atof(get.c_str());
  }

  unsigned long long GetAgeInMilliSeconds(const char* key) { unsigned long long val=0;XrdMqRWMutexReadLock lock(StoreMutex);val = (Store.count(key))?Store[key].GetAgeInMilliSeconds():0; return val;}

  unsigned long long GetAgeInSeconds(const char* key) { unsigned long long val=0;XrdMqRWMutexReadLock lock(StoreMutex);val = (unsigned long long)(Store.count(key))?(unsigned long long)Store[key].GetAgeInSeconds():(unsigned long long)0; return val;}


  void MakeBroadCastEnvHeader(XrdOucString &out);
  void MakeUpdateEnvHeader(XrdOucString &out);
  void MakeDeletionEnvHeader(XrdOucString &out);
  void MakeRemoveEnvHeader(XrdOucString &out);
  void AddTransactionEnvString(XrdOucString &out, bool clearafter=true);
  void AddDeletionEnvString(XrdOucString &out);
  bool BroadCastEnvString(const char* receiver);
  void Dump(XrdOucString &out);

  void Print(std::string &out, std::string format);

  virtual void CallBackInsert(XrdMqSharedHashEntry *entry, const char* key) {};
  virtual void CallBackDelete(XrdMqSharedHashEntry *entry) {};

  bool BroadCastRequest(const char* requesttarget = 0); // the queue name which should respond or otherwise the default broad cast queue

  unsigned long long GetChangeId() { return ChangeId;}
  const char*         GetSubject() { return Subject.c_str();}
  const char*  GetBroadCastQueue() { return BroadCastQueue.c_str();}
  unsigned int        GetSize()    { XrdMqRWMutexReadLock lock(StoreMutex);unsigned int val = (unsigned int) Store.size(); return val; }

  static unsigned long long SetCounter;
  static unsigned long long SetNLCounter;
  static unsigned long long GetCounter;
};


class XrdMqSharedQueue : public XrdMqSharedHash  {
  friend class XrdMqSharedObjectManager;

private:
  std::deque<XrdMqSharedHashEntry*> Queue;
  unsigned long long LastObjectId;

public:
  XrdSysMutex QueueMutex;

  XrdMqSharedQueue(const char* subject = "", const char* broadcastqueue = "", XrdMqSharedObjectManager* som=0) : XrdMqSharedHash(subject,broadcastqueue) { Type = "queue"; LastObjectId=0; SOM = som;}
  virtual ~XrdMqSharedQueue(){}

  virtual void CallBackInsert(XrdMqSharedHashEntry *entry, const char* key);
  virtual void CallBackDelete(XrdMqSharedHashEntry *entry);

  std::deque<XrdMqSharedHashEntry*>* GetQueue() { return &Queue;}

  bool Delete(XrdMqSharedHashEntry* entry) {
    if (entry) {
      std::deque<XrdMqSharedHashEntry*>::iterator it;
      // remove hash entry ... this has a call back removing it also from the queue ...
      std::string key = entry->GetKey();
      return XrdMqSharedHash::Delete(key.c_str());
    }
    return false;
  }

  bool PushBack(const char* uid, const char* value) {
    std::string uuid;
    if (uid) {
      uuid = uid;
    } else {
      char lld[1024]; snprintf(lld, 1023,"%llu", LastObjectId+1);
      uuid = lld;
    }

    if (Store.count(uuid)) {
      return false;
    } else {
      Set(uuid.c_str(), value);
      return true;
    }
  }
};



class XrdMqSharedObjectManager {
  friend class XrdMqSharedHash;
  friend class XrdMqSharedQueue;
  friend class XrdMqSharedObjectChangeNotifier;

private:
  pthread_t dumper_tid;       // thread ID of the dumper thread

  std::map<std::string, XrdMqSharedHash*> hashsubjects;
  std::map<std::string, XrdMqSharedQueue> queuesubjects;

  std::string DumperFile;
  std::string AutoReplyQueue; // queue which is used to setup the reply queue of hashes which have been broadcasted
  bool AutoReplyQueueDerive;  // if this is true, the reply queue is derived from the subject e.g. 

                              // the subject "/eos/<host>/fst/<path>" derives as "/eos/<host>/fst"

protected:
  XrdSysMutex MuxTransactionMutex;  //! blocks mux transactions
  XrdSysMutex MuxTransactionsMutex; //! protects the mux transaction map
  std::string MuxTransactionType;
  std::string MuxTransactionBroadCastQueue;

  bool IsMuxTransaction;
  std::map<std::string, std::set<std::string> > MuxTransactions;

public:
  static bool debug;
  static bool broadcast;

  bool EnableQueue; // if this is true, creation/deletionsubjects are filled and SubjectsSem get's posted for every new creation/deletion

  typedef enum {kMqSubjectNothing=-1,kMqSubjectCreation=0, kMqSubjectDeletion=1, kMqSubjectModification=2, kMqSubjectKeyDeletion=3} notification_t;
  struct Notification {
    // notification about creation, modification or deletion of a subject.
    std::string mSubject;
    notification_t mType;
    
    Notification(std::string s, notification_t n) {
      mSubject = s;
      mType = n;
    }
    Notification() {
      mType = kMqSubjectNothing;
      mSubject = "";
    }
  };
  
protected:
  std::deque<Notification> NotificationSubjects;
  std::deque<std::string> ModificationTempSubjects;// these are posted as <queue>:<key>
  // semaphore to wait for new creations/deletions/modifications
  XrdSysSemWait SubjectsSem;

  // mutex to safeguard the creations/deletions/modifications & watch subjects
  XrdSysMutex SubjectsMutex;
 
public:
  
 
  // clean the bulk modification subject list
  void PostModificationTempSubjects();             
 

  XrdMqRWMutex HashMutex;
  XrdMqRWMutex ListMutex;
  
  XrdMqSharedObjectManager();
  ~XrdMqSharedObjectManager();

  void EnableBroadCast(bool enable) {broadcast = enable;} // switch to globally en-/disable broadcasting of changes into shared queues - default is enabled
  bool ShouldBroadCast() { return broadcast; }            // indicate if we are broadcasting

  void SetAutoReplyQueue(const char* queue);
  void SetAutoReplyQueueDerive(bool val) { AutoReplyQueueDerive = val;}
  
  bool CreateSharedObject(const char* subject, const char* broadcastqueue, const char* type = "hash", XrdMqSharedObjectManager* som=0) {
    std::string Type = type;
    if (Type == "hash") {
      return CreateSharedHash(subject,broadcastqueue, som?som:this);
    }
    if (Type == "queue") {
      return CreateSharedQueue(subject,broadcastqueue, som?som:this);
    }
    return false;
  }
  
  bool DeleteSharedObject(const char* subject, const char* type, bool broadcast) {
    std::string Type = type;
    if (Type == "hash") {
      return DeleteSharedHash(subject,broadcast);
    }
    if (Type == "queue") {
      return DeleteSharedQueue(subject,broadcast);
    }
    return false;
  }

  bool CreateSharedHash (const char* subject, const char* broadcastqueue, XrdMqSharedObjectManager* som=0);
  bool CreateSharedQueue(const char* subject, const char* broadcastqueue, XrdMqSharedObjectManager* som=0);
  
  bool DeleteSharedHash(const char* subject, bool broadcast = true);
  bool DeleteSharedQueue(const char* subject , bool broadcast = true);

  XrdMqSharedHash* GetObject(const char* subject, const char* type) {
    std::string Type = type;
    if (Type == "hash") {
      return GetHash(subject);
    }
    if (Type == "queue") {
      return GetQueue(subject);
    }
    return 0;
  }

  XrdMqSharedHash* GetHash(const char* subject) // don't forget to use the RWMutex for read or write locks
  {
    std::string ssubject = subject;
    if (hashsubjects.count(ssubject))
      return hashsubjects[ssubject];
    else 
      return 0;
  }

  XrdMqSharedQueue* GetQueue(const char* subject) // don't forget to use the RWMutex for read or write locks
  {
    std::string ssubject = subject;
    if (queuesubjects.count(ssubject))
      return &queuesubjects[ssubject];
    else 
      return 0;
  }

  void DumpSharedObjectList(XrdOucString& out);
  void DumpSharedObjects(XrdOucString& out);
  
  bool ParseEnvMessage(XrdMqMessage* message, XrdOucString &error);

  void SetDebug(bool dbg=false) {debug = dbg;}

  void StartDumper(const char* file); // starts a thread which continously dumps all the hashes
  static void* StartHashDumper(void* pp);
  void FileDumper();

  void Clear(); // calls clear on each managed hash and queue

  // multiplexed transactions doing a compound transaction for transactions on several hashes

  bool OpenMuxTransaction(const char* type="hash", const char* broadcastqueue=0);

  bool CloseMuxTransaction();

  void MakeMuxUpdateEnvHeader(XrdOucString &out);
  void AddMuxTransactionEnvString(XrdOucString &out);
};

class XrdMqSharedObjectChangeNotifier {
public:
  struct Subscriber{
    std::string Name;
    // some of the members are array of size 5, one for each type of notification
    // kMqSubjectNothing=-1,kMqSubjectCreation=0, kMqSubjectDeletion=1, kMqSubjectModification=2, kMqSubjectKeyDeletion=3
    // the value 4 is a variant of kMqSubjectModification that could be called kMqSubjectModificationStrict in which the value is actually checked for a change
    // the last value being recorded in LastValues
    std::set<std::string>   WatchKeys[5];
    std::set<std::string>   WatchKeysRegex[5];
    std::set<std::string>   WatchSubjects[5];
    std::set<std::string>   WatchSubjectsRegex[5];
    std::vector< std::pair<std::set<std::string>,std::set<std::string> > > WatchSubjectsXKeys[5];
    XrdSysMutex             WatchMutex; //< protects access to all Watch* c

    std::deque<XrdMqSharedObjectManager::Notification> NotificationSubjects;
    XrdSysSemWait           SubjectsSem;
    XrdSysMutex             SubjectsMutex;
    bool                    Notify;
    Subscriber(const std::string &name="") : Name(name),Notify(false) {}
    bool empty() {
      for(int k=0; k<4; k++) {
        if(
            WatchSubjects[k].size()
            || WatchKeys[k].size()
            || WatchSubjectsRegex[k].size()
            || WatchKeysRegex[k].size()
            || WatchSubjectsXKeys[k].size()
        )
          return false;
      }
      return true;
    }
  };
  typedef enum {
    kMqSubjectNothing=XrdMqSharedObjectManager::kMqSubjectNothing,
    kMqSubjectCreation=XrdMqSharedObjectManager::kMqSubjectCreation,
    kMqSubjectDeletion=XrdMqSharedObjectManager::kMqSubjectDeletion,
    kMqSubjectModification=XrdMqSharedObjectManager::kMqSubjectModification,
    kMqSubjectKeyDeletion=XrdMqSharedObjectManager::kMqSubjectKeyDeletion,
    kMqSubjectStrictModification=4
  } notification_t;
private:
  XrdMqSharedObjectManager *SOM;

  struct WatchItemInfo{
    std::set<Subscriber*> mSubscribers;
    regex_t *mRegex;
    WatchItemInfo(){ mRegex = NULL; }
  };
  // some of the members are array of size 5, one for each type of notification
  // kMqSubjectNothing=-1,kMqSubjectCreation=0, kMqSubjectDeletion=1, kMqSubjectModification=2, kMqSubjectKeyDeletion=3
  // the value 4 is a variant of kMqSubjectModification that could be called kMqSubjectModificationStrict in which the value is actually checked for a change
  // the last value being recorded in LastValues
  XrdSysMutex WatchMutex;
  std::map<std::string,WatchItemInfo > WatchKeys2Subscribers[5];
  std::map<std::string,WatchItemInfo > WatchSubjects2Subscribers[5];
  std::vector< std::pair< std::pair<std::set<std::string>,std::set<std::string> > ,std::set<Subscriber*> > > WatchSubjectsXKeys2Subscribers[5];
  std::map<std::string,std::string> LastValues;
  //<  listof((Subjects,Keys),Subscribers)

  pthread_t tid; //< Thread ID of the dispatching change thread
  void SomListener();
  static void* StartSomListener( void *pp);

  std::map<std::string,Subscriber*> pSubscribersCatalog;
  XrdSysMutex pCatalogMutex;

  bool StartNotifyKey(Subscriber *subscriber, const std::string &key, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StopNotifyKey(Subscriber *subscriber, const std::string &key, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StartNotifySubject(Subscriber *subscriber, const std::string &subject , XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StopNotifySubject(Subscriber *subscriber, const std::string &subject , XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StartNotifyKeyRegex(Subscriber *subscriber, const std::string &key , XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StopNotifyKeyRegex(Subscriber *subscriber, const std::string &key , XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StartNotifySubjectRegex(Subscriber *subscriber, const std::string &subject , XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StopNotifySubjectRegex(Subscriber *subscriber, const std::string &subject , XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StartNotifySubjectsAndKeys(Subscriber *subscriber, const std::set<std::string> &subjects, const std::set<std::string> &keys , XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StopNotifySubjectsAndKeys(Subscriber *subscriber, const std::set<std::string> &subjects, const std::set<std::string> &keys , XrdMqSharedObjectChangeNotifier::notification_t type);

public:
  inline Subscriber* GetSubscriberFromCatalog(const std::string &name, bool createIfNeeded=true){
    Subscriber *ret = NULL;
    if(createIfNeeded) {
      XrdSysMutexHelper lock(pCatalogMutex);
      if(pSubscribersCatalog.count(name))
        ret = pSubscribersCatalog[name];
      else
        ret = (pSubscribersCatalog[name] = new Subscriber(name));
    }
    else {
      XrdSysMutexHelper lock(pCatalogMutex);
      if(pSubscribersCatalog.count(name))
        ret = pSubscribersCatalog[name];
    }
    return ret;
  }

  inline Subscriber* BindCurrentThread(const std::string &name, bool createIfNeeded=true){
    return tlSubscriber = GetSubscriberFromCatalog(name, createIfNeeded);
  }

  static __thread Subscriber *tlSubscriber;
  void SetShareObjectManager( XrdMqSharedObjectManager* som) {
    SOM = som;
  }

  XrdMqSharedObjectChangeNotifier(){}
  ~XrdMqSharedObjectChangeNotifier() {}

  bool SubscribesToSubject(const std::string &susbcriber, const std::string &subject, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToKey(const std::string &susbcriber, const std::string &key, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToSubjectRegex(const std::string &susbcriber, const std::string &subject, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToKeyRegex(const std::string &susbcriber, const std::string &key, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToSubjectAndKey(const std::string &susbcriber, const std::set<std::string> &subjects,const std::set<std::string> &keys, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToSubjectAndKey(const std::string &susbcriber, const std::string &subject,const std::string &key, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToSubjectAndKey(const std::string &susbcriber, const std::string &subject,const std::set<std::string> &keys, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToSubjectAndKey(const std::string &susbcriber, const std::set<std::string> &subjects,const std::string &key, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubject(const std::string &susbcriber, const std::string &subject, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToKey(const std::string &susbcriber, const std::string &key, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubjectRegex(const std::string &susbcriber, const std::string &subject, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToKeyRegex(const std::string &susbcriber, const std::string &key, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubjectAndKey(const std::string &susbcriber, std::set<std::string> subjects,std::set<std::string> keys, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubjectAndKey(const std::string &susbcriber, const std::string &subject,const std::string &key, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubjectAndKey(const std::string &susbcriber, const std::string &subject,const std::set<std::string> &keys, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubjectAndKey(const std::string &susbcriber, const std::set<std::string> &subjects,const std::string &key, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToEverything(const std::string &susbcriber);
  bool StartNotifyCurrentThread();
  bool StopNotifyCurrentThread();
  bool Start();
  bool Stop();
};

#endif


