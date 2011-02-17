#ifndef __XRDMQ_SHAREDHASH_HH__
#define __XRDMQ_SHAREDHASH_HH__

/*----------------------------------------------------------------------------*/
#include "XrdMqOfs/XrdMqRWMutex.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
#include <vector>
#include <set>
#include <queue>

/*----------------------------------------------------------------------------*/
#include "XrdMqOfs/XrdMqClient.hh"
/*----------------------------------------------------------------------------*/

#define XRDMQSHAREDHASH_CMD       "mqsh.cmd"
#define XRDMQSHAREDHASH_UPDATE    "mqsh.cmd=update"
#define XRDMQSHAREDHASH_BCREQUEST "mqsh.cmd=bcrequest"
#define XRDMQSHAREDHASH_BCREPLY   "mqsh.cmd=bcreply"
#define XRDMQSHAREDHASH_DELETE    "mqsh.cmd=delete"
#define XRDMQSHAREDHASH_SUBJECT   "mqsh.subject"
#define XRDMQSHAREDHASH_PAIRS     "mqsh.pairs"
#define XRDMQSHAREDHASH_KEYS      "mqsh.keys"
#define XRDMQSHAREDHASH_REPLY     "mqsh.reply"
#define XRDMQSHAREDHASH_TYPE      "mqsh.type"

class XrdMqSharedObjectManager;


class XrdMqSharedHashEntry {
public:
  struct timespec mtime;
  std::string entry;
  std::string key;
  unsigned long long ChangeId;
  
  
  XrdMqSharedHashEntry(){key="";entry = ""; UpdateTime();ChangeId=0;}

  ~XrdMqSharedHashEntry(){};

  struct timespec* GetTime() { return &mtime;}

  void Set(const char* s)  { entry = s; UpdateTime();ChangeId++;}
  void Set(std::string &s) { entry = s; UpdateTime();ChangeId++;}
  void SetKey(const char* lkey) {key = lkey;}
  const char* GetKey() {return key.c_str();}

  long long GetAgeInMilliSeconds() { 
    struct timespec ntime; 
    clock_gettime(CLOCK_REALTIME, &ntime);
    return (((ntime.tv_sec - mtime.tv_sec)*1000) + ((ntime.tv_nsec - mtime.tv_nsec)/1000000));
  }
  
  double GetAgeInSeconds()      {return GetAgeInMilliSeconds()/1000.0;}
  
  void UpdateTime() {
    clock_gettime(CLOCK_REALTIME, &mtime);
  }
  
  const char* GetEntry() { return entry.c_str(); }

  void Dump(XrdOucString &out) {
    char formatline[1024];
    snprintf(formatline, sizeof(formatline)-1,"age:%.2f value:%s changeid:%llu", GetAgeInSeconds(), entry.c_str(), ChangeId);
    out += formatline;
  }
};


class XrdMqSharedHash {
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

public:

  XrdMqSharedHash(const char* subject = "", const char* broadcastqueue = "") ;

  virtual ~XrdMqSharedHash();

  void SetBroadCastQueue(const char* broadcastqueue) { BroadCastQueue = broadcastqueue;}
  
  bool Set(std::string key, std::string value, bool broadcast=true) {
    return Set(key.c_str(),value.c_str(), broadcast);
  }

  bool Set(const char* key, const char* value, bool broadcast=true) {
    std::string skey = key;

    XrdMqRWMutexWriteLock lock(StoreMutex);
    bool callback=false;

    if (!Store.count(skey)) {
      callback=true;
    }

    Store[skey].Set(value);
    if (callback) {
      CallBackInsert(&Store[skey], skey.c_str());
    }

    if (IsTransaction && broadcast) {
      Transactions.insert(skey);
    }
    return true;
  }

  bool Set(std::map<std::string, std::string> &map) {
    std::map<std::string, std::string>::const_iterator it;
    bool success=true;
    for (it=map.begin(); it!=map.end(); it++) {
      success *= Set(it->first.c_str(), it->second.c_str());
    }
    return success;
  }

  bool SetLongLong(const char* key, long long value) {
    char convert[1024];
    snprintf(convert, sizeof(convert)-1,"%lld", value);
    return Set(key, convert);
  }

  bool SetDouble(const char* key, double &value) {
    char convert[1024];
    snprintf(convert, sizeof(convert)-1,"%f", value);
    return Set(key, convert);
  }

  bool Delete(const char* key, bool broadcast=true) {
    bool deleted = false;
    XrdMqRWMutexWriteLock lock(StoreMutex);
    if (Store.count(key)) {
      CallBackDelete(&Store[key]);
      Store.erase(key);
      deleted = true;
      if (IsTransaction && broadcast) {
	Deletions.insert(key);
	Transactions.erase(key);
      }
    }
    return deleted;
  }

  void Clear() {
    XrdMqRWMutexWriteLock lock(StoreMutex);
    std::map<std::string, XrdMqSharedHashEntry>::iterator storeit;
    for (storeit = Store.begin(); storeit != Store.end(); storeit++) {
      CallBackDelete(&storeit->second);
      if (IsTransaction) {
	Deletions.insert(storeit->first);
	Transactions.erase(storeit->first);
      }
    }
    Store.clear();
  }

  bool OpenTransaction() {TransactionMutex.Lock(); Transactions.clear();IsTransaction= true; return true;}
  
  bool CloseTransaction();

  std::string Get(const char* key) {std::string get=""; XrdMqRWMutexReadLock lock(StoreMutex);if (Store.count(key)) get = Store[key].GetEntry(); return get;}

  long long   GetLongLong(const char* key) {
    std::string get = Get(key); return strtoll(get.c_str(),0,10);
  }

  double      GetDouble(const char* key) {
    std::string get = Get(key); return atof(get.c_str());
  }

  unsigned long long GetAgeInMilliSeconds(const char* key) { unsigned long long val=0;XrdMqRWMutexReadLock lock(StoreMutex);val = (Store.count(key))?Store[key].GetAgeInMilliSeconds():0; return val;}

  unsigned long long GetAgeInSeconds(const char* key) { unsigned long long val=0;XrdMqRWMutexReadLock lock(StoreMutex);val = (Store.count(key))?Store[key].GetAgeInSeconds():0; return val;}


  void MakeBroadCastEnvHeader(XrdOucString &out);
  void MakeUpdateEnvHeader(XrdOucString &out);
  void MakeDeletionEnvHeader(XrdOucString &out);
  void AddTransactionEnvString(XrdOucString &out);
  void AddDeletionEnvString(XrdOucString &out);
  bool BroadCastEnvString(const char* receiver);
  void Dump(XrdOucString &out);

  virtual void CallBackInsert(XrdMqSharedHashEntry *entry, const char* key) {};
  virtual void CallBackDelete(XrdMqSharedHashEntry *entry) {};

  bool BroadCastRequest(const char* requesttarget = 0); // the queue name which should respond or otherwise the default broad cast queue

  unsigned long long GetChangeId() { return ChangeId;}
  const char*         GetSubject() { return Subject.c_str();}
  const char*  GetBroadCastQueue() { return BroadCastQueue.c_str();}
  unsigned int        GetSize()    { XrdMqRWMutexReadLock lock(StoreMutex);unsigned int val = (unsigned int) Store.size(); return val; }

};


class XrdMqSharedQueue : public XrdMqSharedHash  {
private:
  std::deque<XrdMqSharedHashEntry*> Queue;
  unsigned long long LastObjectId;

public:
  XrdMqSharedQueue(const char* subject = "", const char* broadcastqueue = "") : XrdMqSharedHash(subject,broadcastqueue) { Type = "queue"; LastObjectId=0;}
  virtual ~XrdMqSharedQueue(){}

  virtual void CallBackInsert(XrdMqSharedHashEntry *entry, const char* key);
  virtual void CallBackDelete(XrdMqSharedHashEntry *entry);

  std::deque<XrdMqSharedHashEntry*>* GetQueue() { return &Queue;}

  bool Delete(XrdMqSharedHashEntry* entry) {
    if (entry) {
      std::deque<XrdMqSharedHashEntry*>::iterator it;
      // remove hash entry ... this has a call back removing it also from the queue ...
      return XrdMqSharedHash::Delete(entry->GetKey());
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
private:
  std::map<std::string, XrdMqSharedHash*> hashsubjects;
  std::map<std::string, XrdMqSharedQueue> queuesubjects;
  bool debug;

public:
  XrdMqRWMutex HashMutex;
  XrdMqRWMutex ListMutex;
  
  XrdMqSharedObjectManager();
  ~XrdMqSharedObjectManager();

  bool CreateSharedObject(const char* subject, const char* broadcastqueue, const char* type = "hash") {
    std::string Type = type;
    if (Type == "hash") {
      return CreateSharedHash(subject,broadcastqueue);
    }
    if (Type == "queue") {
      return CreateSharedQueue(subject,broadcastqueue);
    }
    return false;
  }

  bool CreateSharedHash(const char* subject, const char* broadcastqueue);
  bool CreateSharedQueue(const char* subject, const char* broadcastqueue);
  
  bool DeleteSharedHash(const char* subject);
  bool DeleteSharedQueue(const char* subject);

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

  bool ParseEnvMessage(XrdMqMessage* message, XrdOucString &error);

  void SetDebug(bool dbg=false) {debug = dbg;}
};

#endif


