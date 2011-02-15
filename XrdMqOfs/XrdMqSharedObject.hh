#ifndef __XRDMQ_SHAREDHASH_HH__
#define __XRDMQ_SHAREDHASH_HH__

/*----------------------------------------------------------------------------*/
#include "XrdMqOfs/XrdMqRWMutex.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
#include <vector>
#include <set>

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

class XrdMqSharedObjectManager;


class XrdMqSharedHashEntry {
public:
  struct timespec mtime;
  std::string entry;
  unsigned long long ChangeId;
  
  
  XrdMqSharedHashEntry(){entry = ""; UpdateTime();ChangeId=0;}

  ~XrdMqSharedHashEntry(){};

  struct timespec* GetTime() { return &mtime;}

  void Set(const char* s)  { entry = s; UpdateTime();ChangeId++;}
  void Set(std::string &s) { entry = s; UpdateTime();ChangeId++;}
  
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

public:
  XrdMqSharedHash(const char* subject = "", const char* broadcastqueue = "");

  virtual ~XrdMqSharedHash();

  void SetBroadCastQueue(const char* broadcastqueue) { BroadCastQueue = broadcastqueue;}
  
  bool Set(std::string key, std::string value, bool broadcast=true) {
    return Set(key.c_str(),value.c_str(), broadcast);
  }

  bool Set(const char* key, const char* value, bool broadcast=true) {
    std::string skey = key;

    XrdMqRWMutexWriteLock lock(StoreMutex);

    Store[skey].Set(value);

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

  bool BroadCastRequest(const char* requesttarget = 0); // the queue name which should respond or otherwise the default broad cast queue

  unsigned long long GetChangeId() { return ChangeId;}
  const char*         GetSubject() { return Subject.c_str();}
  const char*  GetBroadCastQueue() { return BroadCastQueue.c_str();}
  unsigned int        GetSize()    { XrdMqRWMutexReadLock lock(StoreMutex);unsigned int val = (unsigned int) Store.size(); return val; }

};


class XrdMqSharedList {
public:
  XrdMqSharedList(const char* subject = "", const char* broadcastqueue = "") {}
  virtual ~XrdMqSharedList(){}
};

class XrdMqSharedObjectManager {
private:
  std::map<std::string, XrdMqSharedHash*> hashsubjects;
  std::map<std::string, XrdMqSharedList> listsubjects;
  bool debug;

public:
  XrdMqRWMutex HashMutex;
  XrdMqRWMutex ListMutex;
  
  XrdMqSharedObjectManager();
  ~XrdMqSharedObjectManager();
  
  bool CreateSharedHash(const char* subject, const char* broadcastqueue);
  bool CreateSharedList(const char* subject, const char* broadcastqueue);
  
  bool DeleteSharedHash(const char* subject);
  bool DeleteSharedList(const char* subject);

  XrdMqSharedHash* GetHash(const char* subject) // don't forget to use the RWMutex for read or write locks
  {
    std::string ssubject = subject;
    if (hashsubjects.count(ssubject))
      return hashsubjects[ssubject];
    else 
      return 0;
  }

  XrdMqSharedList* GetList(const char* subject) // don't forget to use the RWMutex for read or write locks
  {
    std::string ssubject = subject;
    if (listsubjects.count(ssubject))
    return &listsubjects[ssubject];
    else 
      return 0;
  }
  
  void DumpSharedObjectList(XrdOucString& out);

  bool ParseEnvMessage(XrdMqMessage* message, XrdOucString &error);

  void SetDebug(bool dbg=false) {debug = dbg;}
};

#endif


