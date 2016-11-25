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

#include "mq/XrdMqClient.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysAtomics.hh"
#include "XrdSys/XrdSysSemWait.hh"
#include "common/RWMutex.hh"
#include "common/StringConversion.hh"
#include "mq/XrdMqRWMutex.hh"
#include <string>
#include <map>
#include <vector>
#include <set>
#include <queue>
#include <sys/time.h>
#include <sys/types.h>
#include <regex.h>

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

//------------------------------------------------------------------------------
//! Class XrdMqSharedHashEntry
//------------------------------------------------------------------------------
class XrdMqSharedHashEntry
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqSharedHashEntry();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdMqSharedHashEntry() {};

  //----------------------------------------------------------------------------
  //! Set entry and key
  //!
  //! @param entry entry value
  //! @param key key value
  //----------------------------------------------------------------------------
  void Set(const char* entry, const char* key = 0);

  //----------------------------------------------------------------------------
  //! Set entry
  //!
  //! @param entry entry value
  //----------------------------------------------------------------------------
  void Set(std::string& s);

  //----------------------------------------------------------------------------
  //! Get entry
  //!
  //! @return entry value
  //----------------------------------------------------------------------------
  inline const char* GetEntry() const
  {
    return mEntry.c_str();
  }

  //----------------------------------------------------------------------------
  //! Set key
  //!
  //! @param key key value
  //----------------------------------------------------------------------------
  inline void SetKey(const char* key)
  {
    mKey = key;
  }

  //----------------------------------------------------------------------------
  //! Get key value
  //!
  //! @return key value
  //----------------------------------------------------------------------------
  inline const char* GetKey() const
  {
    return mKey.c_str();
  }

  //----------------------------------------------------------------------------
  //! Get change id
  //!
  //! @return change id value
  //----------------------------------------------------------------------------
  inline unsigned long long GetChangeId() const
  {
    return mChangeId;
  }

  //----------------------------------------------------------------------------
  //! Get age in milliseconds
  //!
  //! @return age in milliseconds
  //----------------------------------------------------------------------------
  long long GetAgeInMilliSeconds();

  //----------------------------------------------------------------------------
  //! Get age in seconds
  //!
  //! @return age in seconds
  //----------------------------------------------------------------------------
  double GetAgeInSeconds();

  //----------------------------------------------------------------------------
  //! Append entry representation to output string
  //!
  //! @param out output string
  //----------------------------------------------------------------------------
  void Dump(XrdOucString& out);

private:
  unsigned long long mChangeId; ///< Entry change id
  std::string mKey; ///< Entry key value
  std::string mEntry; ///< Entry value
  struct timeval mMtime; ///< Last modification time of current entry
};


//------------------------------------------------------------------------------
//! Class XrdMqSharedHash
//------------------------------------------------------------------------------
class XrdMqSharedHash
{
  friend class XrdMqSharedObjectManager;

private:
  std::string mBroadcastQueue; ///< Name of the broadcast queue
  std::string mSubject; ///< Hash subject
  bool mIsTransaction; ///< True if ongoing transaction
  std::set<std::string> mDeletions; ///< Set of deletions
  XrdSysMutex mTransactMutex; ///< Mutex protecting the set of transactions
  std::set<std::string> mTransactions; ///< Set of transactions

protected:
  XrdMqRWMutex mStoreMutex; ///< RW Mutex protecting the mStore object
  std::map<std::string, XrdMqSharedHashEntry> mStore;
  std::string mType; ///< Type of object
  XrdMqSharedObjectManager* SOM; ///< Ponier to shared object manager

public:
  static unsigned long long sSetCounter; ///< Counter for set operations
  static unsigned long long sSetNLCounter; ///< Counter for set no-lock operations
  static unsigned long long sGetCounter; ///< Counter for get operations

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param subject subject name
  //! @param broadcastqueue broadcast queue name
  //! @param som shared object manager pointer
  //----------------------------------------------------------------------------
  XrdMqSharedHash(const char* subject = "", const char* broadcastqueue = "",
		  XrdMqSharedObjectManager* som = 0) ;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdMqSharedHash() {};

  //----------------------------------------------------------------------------
  //! Callback for insert operation
  //!
  //! @param entry entry which is inserted
  //----------------------------------------------------------------------------
  virtual void CallBackInsert(XrdMqSharedHashEntry* entry, const char* key) {};

  //----------------------------------------------------------------------------
  //! Callback for delete operation
  //!
  //! @param entry entry which is deleted
  //----------------------------------------------------------------------------
  virtual void CallBackDelete(XrdMqSharedHashEntry* entry) {};

  //----------------------------------------------------------------------------
  //! Get size of the hash
  //!
  //! @return size of hash
  //----------------------------------------------------------------------------
  unsigned int GetSize();

  //----------------------------------------------------------------------------
  //! Get age in milliseconds for a certain key
  //!
  //! @param key key value
  //!
  //! @return age in milliseconds if key exists, otherwise 0
  //----------------------------------------------------------------------------
  unsigned long long GetAgeInMilliSeconds(const char* key);

  //----------------------------------------------------------------------------
  //! Get age in seconds for a certain key
  //!
  //! @param key key value
  //!
  //! @return age in seconds if key exists, otherwise 0
  //----------------------------------------------------------------------------
  unsigned long long GetAgeInSeconds(const char* key);

  //----------------------------------------------------------------------------
  //! Get entry value for key
  //!
  //! @param key key value
  //!
  //! @return entry value corresponding to this key
  //----------------------------------------------------------------------------
  std::string Get(const char* key);

  //----------------------------------------------------------------------------
  //! Get a copy of all the keys
  //!
  //! @return vector containing all the keys in the hash
  //----------------------------------------------------------------------------
  std::vector<std::string> GetKeys();

  //----------------------------------------------------------------------------
  //! Set broadcast queue
  //!
  //! @param bcast_queue broadcast queue
  //----------------------------------------------------------------------------
  inline void SetBroadCastQueue(const char* bcast_queue)
  {
    mBroadcastQueue = bcast_queue;
  }

  //----------------------------------------------------------------------------
  //! Get broadcast queue
  //!
  //! @return broadcast queue
  //----------------------------------------------------------------------------
  inline const char* GetBroadCastQueue()
  {
    return mBroadcastQueue.c_str();
  }

  //----------------------------------------------------------------------------
  //! Get subject
  //!
  //! @return subject
  //----------------------------------------------------------------------------
  inline const char* GetSubject() const
  {
    return mSubject.c_str();
  }

  //----------------------------------------------------------------------------
  //! Build and send the broadcast request
  //!
  //! @param req_target queue name which should respond or otherwise the default
  //!        broadcast queue
  //!
  //! @return true if message sent successfully, otherwise false
  //----------------------------------------------------------------------------
  bool BroadcastRequest(const char* requesttarget = 0);

  //----------------------------------------------------------------------------
  //! Dump hash map representation to output string
  //!
  //! @param out output string
  //----------------------------------------------------------------------------
  void Dump(XrdOucString& out);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void Print(std::string& out, std::string format);

  //----------------------------------------------------------------------------
  //! Delete key entry
  //!
  //! @param key key value
  //! @param broadcast if true broadcast deletion
  //! @param notify if true send notification to the shared object manager
  //!
  //! @return true if deletion done, otherwise false
  //----------------------------------------------------------------------------
  bool Delete(const char* key, bool broadcast = true, bool notify = true);

  //----------------------------------------------------------------------------
  //! Clear contents of the hash
  //!
  //! @param broadcast if true then broadcast the deletions
  //----------------------------------------------------------------------------
  void Clear(bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Serializes hash contents as follows 'key1=val1 key2=val2 ... keyn=valn'
  //! but return only keys that don't start with filter_prefix
  //!
  //! @param filter_prefix prefix used for filtering keys
  //!
  //! @return string representation of the contenst for the hash
  //----------------------------------------------------------------------------
  std::string SerializeWithFilter(const char* filter_prefix = "");

  //----------------------------------------------------------------------------
  //! Open transaction
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool OpenTransaction();

  //----------------------------------------------------------------------------
  //! Close transaction
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool CloseTransaction();

  //============================================================================
  //                          THIS SHOULD BE REWRITTEN - BEGIN
  //============================================================================

  //----------------------------------------------------------------------------
  //! Set entry in hash map
  //!
  //! @param key key value
  //! @param value entry value
  //! @param broadcast do broadcast for current subject
  //! @param tempmodsubjects add to temporary modified subjects
  //! @param notify do notification for current subject
  //! @param do_lock if true then take all the necessary locks, otherwise
  //!        assume we are protected.
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool Set(const char* key, T&& value, bool broadcast = true,
	   bool tempmodsubjects = false, bool notify = true, bool do_lock = true);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  bool SetLongLong(const char* key, long long value, bool broadcast = true)
  {
    char convert[1024];
    snprintf(convert, sizeof(convert) - 1, "%lld", value);
    return Set(key, convert, broadcast);
  }

  //----------------------------------------------------------------------------
  //! Get key value as long long
  //!
  //! @param key key to look for
  //!
  //! @return value corresponding to the key
  //----------------------------------------------------------------------------
  long long GetLongLong(const char* key);

  //----------------------------------------------------------------------------
  //! Get key value as double
  //!
  //! @param key key to look for
  //!
  //! @return value corresponding to the key
  //----------------------------------------------------------------------------
  double GetDouble(const char* key);

  //----------------------------------------------------------------------------
  //! Get key value as unsigned int
  //!
  //! @param key key to look for
  //!
  //! @return value corresponding to the key
  //----------------------------------------------------------------------------
  unsigned int GetUInt(const char* key);

  //============================================================================
  //                          THIS SHOULD BE REWRITTEN - END
  //============================================================================

private:
  //----------------------------------------------------------------------------
  //! Construct broadcast env header
  //!
  //! @param out output string containing the header
  //----------------------------------------------------------------------------
  void MakeBroadCastEnvHeader(XrdOucString& out);

  //----------------------------------------------------------------------------
  //! Construct update env header
  //!
  //! @param out output string containing the header
  //----------------------------------------------------------------------------
  void MakeUpdateEnvHeader(XrdOucString& out);

  //----------------------------------------------------------------------------
  //! Construct deletion env header
  //!
  //! @param out output string containing the header
  //----------------------------------------------------------------------------
  void MakeDeletionEnvHeader(XrdOucString& out);

  //----------------------------------------------------------------------------
  //! Construct remove env header
  //!
  //! @param out output string containing the header
  //----------------------------------------------------------------------------
  void MakeRemoveEnvHeader(XrdOucString& out);

  //----------------------------------------------------------------------------
  //! Encode transactions as env string
  //!
  //! @param out output string
  //! @param clear_after if true clear transactions afterward, otherwise not
  //----------------------------------------------------------------------------
  void AddTransactionsToEnvString(XrdOucString& out, bool clearafter = true);

  //----------------------------------------------------------------------------
  //! Encode deletions as env string
  //!
  //! @param out ouput string
  //----------------------------------------------------------------------------
  void AddDeletionsToEnvString(XrdOucString& out);

  //----------------------------------------------------------------------------
  //! Broadcast hash as env string
  //!
  //! @param receiver target of the broadcast message
  //!
  //! @return true if message sent successful, otherwise false
  //----------------------------------------------------------------------------
  bool BroadCastEnvString(const char* receiver);
};


//------------------------------------------------------------------------------
//! Class XrdMqSharedQueue
//------------------------------------------------------------------------------
class XrdMqSharedQueue: public XrdMqSharedHash
{
  friend class XrdMqSharedObjectManager;

private:
  std::deque<XrdMqSharedHashEntry*> mQueue;
  unsigned long long mLastObjId; ///< Id of the last object added to the queue

public:

  // TODO(esindril): This should me made private
  XrdSysMutex mQMutex; ///< Mutex prtecting the mQueue object

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param subject queue subject
  //! @param bcast_queue broadcast queue
  //! @param som shard object manager pointer
  //----------------------------------------------------------------------------
  XrdMqSharedQueue(const char* subject = "", const char* bcast_queue = "",
		   XrdMqSharedObjectManager* som = 0);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdMqSharedQueue() {}

  virtual void CallBackInsert(XrdMqSharedHashEntry* entry, const char* key);
  virtual void CallBackDelete(XrdMqSharedHashEntry* entry);

  //----------------------------------------------------------------------------
  //! Get underlying queue object
  //----------------------------------------------------------------------------
  inline std::deque<XrdMqSharedHashEntry*>* GetQueue()
  {
    return &mQueue;
  }

  //----------------------------------------------------------------------------
  //! Delete entry from queue
  //!
  //! @param entry hash entry pointer
  //----------------------------------------------------------------------------
  bool Delete(XrdMqSharedHashEntry* entry);

  //----------------------------------------------------------------------------
  //! Push back entry into the queue
  //!
  //! @param key entry key
  //! @param value entry value
  //!
  //! @return true if entry added successfully, otherwise false
  //----------------------------------------------------------------------------
  bool PushBack(const char* key, const char* value);

  //----------------------------------------------------------------------------
  // TODO(esindril): Add PopFront function which will be used from the
  // common::TransferQueue::Get() method
  //----------------------------------------------------------------------------
  //std::string PopFront();
};


//------------------------------------------------------------------------------
//! Class XrdMqSharedObjectManager
//------------------------------------------------------------------------------
class XrdMqSharedObjectManager
{
  friend class XrdMqSharedHash;
  friend class XrdMqSharedQueue;
  friend class XrdMqSharedObjectChangeNotifier;

private:
  pthread_t dumper_tid;       // thread ID of the dumper thread

  std::map<std::string, XrdMqSharedHash*> hashsubjects;
  std::map<std::string, XrdMqSharedQueue> queuesubjects;

  std::string DumperFile;
  //! Queue which is used to setup the reply queue of hashes which have been broadcasted
  std::string AutoReplyQueue;
  //! If this is true, the reply queue is derived from the subject e.g.
  bool AutoReplyQueueDerive;
  // the subject "/eos/<host>/fst/<path>" derives as "/eos/<host>/fst"

public:
  static bool debug;
  static bool broadcast;

  // If this is true, creation/deletionsubjects are filled and SubjectsSem
  // get's posted for every new creation/deletion
  bool EnableQueue;

  typedef enum {
    kMqSubjectNothing = -1,
    kMqSubjectCreation = 0,
    kMqSubjectDeletion = 1,
    kMqSubjectModification = 2,
    kMqSubjectKeyDeletion = 3
  } notification_t;

  struct Notification {
    // notification about creation, modification or deletion of a subject.
    std::string mSubject;
    notification_t mType;

    Notification(std::string s, notification_t n)
    {
      mSubject = s;
      mType = n;
    }
    Notification()
    {
      mType = kMqSubjectNothing;
      mSubject = "";
    }
  };

  // Clean the bulk modification subject list
  void PostModificationTempSubjects();

  XrdMqRWMutex HashMutex;
  XrdMqRWMutex ListMutex;

  XrdMqSharedObjectManager();
  ~XrdMqSharedObjectManager();

  void EnableBroadCast(bool enable)
  {
    // Switch to globally en-/disable broadcasting of changes into
    // shared queues - default is enabled
    broadcast = enable;
  }
  bool ShouldBroadCast()
  {
    return broadcast;  // indicate if we are broadcasting
  }

  void SetAutoReplyQueue(const char* queue);
  void SetAutoReplyQueueDerive(bool val)
  {
    AutoReplyQueueDerive = val;
  }

  bool CreateSharedObject(const char* subject, const char* broadcastqueue,
			  const char* type = "hash", XrdMqSharedObjectManager* som = 0)
  {
    std::string stype = type;

    if (stype == "hash") {
      return CreateSharedHash(subject, broadcastqueue, som ? som : this);
    }

    if (stype == "queue") {
      return CreateSharedQueue(subject, broadcastqueue, som ? som : this);
    }

    return false;
  }

  bool DeleteSharedObject(const char* subject, const char* type, bool broadcast)
  {
    std::string stype = type;

    if (stype == "hash") {
      return DeleteSharedHash(subject, broadcast);
    }

    if (stype == "queue") {
      return DeleteSharedQueue(subject, broadcast);
    }

    return false;
  }

  bool CreateSharedHash(const char* subject, const char* broadcastqueue,
			XrdMqSharedObjectManager* som = 0);
  bool CreateSharedQueue(const char* subject, const char* broadcastqueue,
			 XrdMqSharedObjectManager* som = 0);

  bool DeleteSharedHash(const char* subject, bool broadcast = true);
  bool DeleteSharedQueue(const char* subject , bool broadcast = true);

  XrdMqSharedHash* GetObject(const char* subject, const char* type)
  {
    std::string stype = type;

    if (stype == "hash") {
      return GetHash(subject);
    }

    if (stype == "queue") {
      return GetQueue(subject);
    }

    return 0;
  }

  // Don't forget to use the RWMutex for read or write locks
  XrdMqSharedHash* GetHash(const char* subject)
  {
    std::string ssubject = subject;

    if (hashsubjects.count(ssubject)) {
      return hashsubjects[ssubject];
    } else {
      return 0;
    }
  }

  // Don't forget to use the RWMutex for read or write locks
  XrdMqSharedQueue* GetQueue(const char* subject)
  {
    std::string ssubject = subject;

    if (queuesubjects.count(ssubject)) {
      return &queuesubjects[ssubject];
    } else {
      return 0;
    }
  }

  void DumpSharedObjects(XrdOucString& out);

  bool ParseEnvMessage(XrdMqMessage* message, XrdOucString& error);

  void SetDebug(bool dbg = false)
  {
    debug = dbg;
  }

  // Starts a thread which continously dumps all the hashes
  void StartDumper(const char* file);
  static void* StartHashDumper(void* pp);
  void FileDumper();

  // Calls clear on each managed hash and queue
  void Clear();

  // Multiplexed transactions doing a compound transaction for transactions
  // on several hashes
  bool OpenMuxTransaction(const char* type = "hash",
			  const char* broadcastqueue = 0);

  bool CloseMuxTransaction();

  void MakeMuxUpdateEnvHeader(XrdOucString& out);
  void AddMuxTransactionEnvString(XrdOucString& out);

protected:
  XrdSysMutex MuxTransactionMutex;  //! blocks mux transactions
  XrdSysMutex MuxTransactionsMutex; //! protects the mux transaction map
  std::string MuxTransactionType;
  std::string MuxTransactionBroadCastQueue;
  bool IsMuxTransaction;
  std::map<std::string, std::set<std::string> > MuxTransactions;
  std::deque<Notification> NotificationSubjects;
  std::deque<std::string>
  ModificationTempSubjects;// these are posted as <queue>:<key>
  //! Semaphore to wait for new creations/deletions/modifications
  XrdSysSemWait SubjectsSem;
  //! Mutex to safeguard the creations/deletions/modifications & watch subjects
  XrdSysMutex SubjectsMutex;
};

//------------------------------------------------------------------------------
//! Class XrdMqSharedObjectChangeNotifier
//------------------------------------------------------------------------------
class XrdMqSharedObjectChangeNotifier
{
public:
  struct Subscriber {
    std::string Name;
    // some of the members are array of size 5, one for each type of notification
    // kMqSubjectNothing=-1,kMqSubjectCreation=0, kMqSubjectDeletion=1,
    // kMqSubjectModification=2, kMqSubjectKeyDeletion=3
    // the value 4 is a variant of kMqSubjectModification that could be called
    // kMqSubjectModificationStrict in which the value is actually checked for a change
    // the last value being recorded in LastValues
    std::set<std::string>   WatchKeys[5];
    std::set<std::string>   WatchKeysRegex[5];
    std::set<std::string>   WatchSubjects[5];
    std::set<std::string>   WatchSubjectsRegex[5];
    std::vector< std::pair<std::set<std::string>, std::set<std::string> > >
    WatchSubjectsXKeys[5];
    XrdSysMutex             WatchMutex; //< protects access to all Watch* c

    std::deque<XrdMqSharedObjectManager::Notification> NotificationSubjects;
    XrdSysSemWait           SubjectsSem;
    XrdSysMutex             SubjectsMutex;
    bool                    Notify;
    Subscriber(const std::string& name = "") : Name(name), Notify(false) {}
    bool empty()
    {
      for (int k = 0; k < 4; k++) {
	if (
	  WatchSubjects[k].size()
	  || WatchKeys[k].size()
	  || WatchSubjectsRegex[k].size()
	  || WatchKeysRegex[k].size()
	  || WatchSubjectsXKeys[k].size()
	) {
	  return false;
	}
      }

      return true;
    }
  };
  typedef enum {
    kMqSubjectNothing = XrdMqSharedObjectManager::kMqSubjectNothing,
    kMqSubjectCreation = XrdMqSharedObjectManager::kMqSubjectCreation,
    kMqSubjectDeletion = XrdMqSharedObjectManager::kMqSubjectDeletion,
    kMqSubjectModification = XrdMqSharedObjectManager::kMqSubjectModification,
    kMqSubjectKeyDeletion = XrdMqSharedObjectManager::kMqSubjectKeyDeletion,
    kMqSubjectStrictModification = 4
  } notification_t;

  inline Subscriber* GetSubscriberFromCatalog(const std::string& name,
      bool createIfNeeded = true)
  {
    Subscriber* ret = NULL;

    if (createIfNeeded) {
      XrdSysMutexHelper lock(pCatalogMutex);

      if (pSubscribersCatalog.count(name)) {
	ret = pSubscribersCatalog[name];
      } else {
	ret = (pSubscribersCatalog[name] = new Subscriber(name));
      }
    } else {
      XrdSysMutexHelper lock(pCatalogMutex);

      if (pSubscribersCatalog.count(name)) {
	ret = pSubscribersCatalog[name];
      }
    }

    return ret;
  }

  inline Subscriber* BindCurrentThread(const std::string& name,
				       bool createIfNeeded = true)
  {
    return tlSubscriber = GetSubscriberFromCatalog(name, createIfNeeded);
  }

  static __thread Subscriber* tlSubscriber;
  void SetShareObjectManager(XrdMqSharedObjectManager* som)
  {
    SOM = som;
  }

  XrdMqSharedObjectChangeNotifier() {}
  ~XrdMqSharedObjectChangeNotifier() {}

  bool SubscribesToSubject(const std::string& susbcriber,
			   const std::string& subject,
			   XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToKey(const std::string& susbcriber, const std::string& key,
		       XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToSubjectRegex(const std::string& susbcriber,
				const std::string& subject,
				XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToKeyRegex(const std::string& susbcriber, const std::string& key,
			    XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToSubjectAndKey(const std::string& susbcriber,
				 const std::set<std::string>& subjects, const std::set<std::string>& keys,
				 XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToSubjectAndKey(const std::string& susbcriber,
				 const std::string& subject, const std::string& key,
				 XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToSubjectAndKey(const std::string& susbcriber,
				 const std::string& subject, const std::set<std::string>& keys,
				 XrdMqSharedObjectChangeNotifier::notification_t type);
  bool SubscribesToSubjectAndKey(const std::string& susbcriber,
				 const std::set<std::string>& subjects, const std::string& key,
				 XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubject(const std::string& susbcriber,
			     const std::string& subject,
			     XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToKey(const std::string& susbcriber, const std::string& key,
			 XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubjectRegex(const std::string& susbcriber,
				  const std::string& subject,
				  XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToKeyRegex(const std::string& susbcriber,
			      const std::string& key, XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubjectAndKey(const std::string& susbcriber,
				   std::set<std::string> subjects, std::set<std::string> keys,
				   XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubjectAndKey(const std::string& susbcriber,
				   const std::string& subject, const std::string& key,
				   XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubjectAndKey(const std::string& susbcriber,
				   const std::string& subject, const std::set<std::string>& keys,
				   XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToSubjectAndKey(const std::string& susbcriber,
				   const std::set<std::string>& subjects, const std::string& key,
				   XrdMqSharedObjectChangeNotifier::notification_t type);
  bool UnsubscribesToEverything(const std::string& susbcriber);
  bool StartNotifyCurrentThread();
  bool StopNotifyCurrentThread();
  bool Start();
  bool Stop();

private:
  XrdMqSharedObjectManager* SOM;

  struct WatchItemInfo {
    std::set<Subscriber*> mSubscribers;
    regex_t* mRegex;
    WatchItemInfo()
    {
      mRegex = NULL;
    }
  };
  // some of the members are array of size 5, one for each type of notification
  // kMqSubjectNothing=-1,kMqSubjectCreation=0, kMqSubjectDeletion=1,
  // kMqSubjectModification=2, kMqSubjectKeyDeletion=3
  // the value 4 is a variant of kMqSubjectModification that could be called
  // kMqSubjectModificationStrict in which the value is actually checked for a change
  // the last value being recorded in LastValues
  XrdSysMutex WatchMutex;
  std::map<std::string, WatchItemInfo > WatchKeys2Subscribers[5];
  std::map<std::string, WatchItemInfo > WatchSubjects2Subscribers[5];
  std::vector< std::pair< std::pair<std::set<std::string>, std::set<std::string> >
			  , std::set<Subscriber*> > >
  WatchSubjectsXKeys2Subscribers[5];
  std::map<std::string, std::string> LastValues;
  //<  listof((Subjects,Keys),Subscribers)

  pthread_t tid; //< Thread ID of the dispatching change thread
  void SomListener();
  static void* StartSomListener(void* pp);

  std::map<std::string, Subscriber*> pSubscribersCatalog;
  XrdSysMutex pCatalogMutex;

  bool StartNotifyKey(Subscriber* subscriber, const std::string& key,
		      XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StopNotifyKey(Subscriber* subscriber, const std::string& key,
		     XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StartNotifySubject(Subscriber* subscriber, const std::string& subject ,
			  XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StopNotifySubject(Subscriber* subscriber, const std::string& subject ,
			 XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StartNotifyKeyRegex(Subscriber* subscriber, const std::string& key ,
			   XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StopNotifyKeyRegex(Subscriber* subscriber, const std::string& key ,
			  XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StartNotifySubjectRegex(Subscriber* subscriber,
			       const std::string& subject ,
			       XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StopNotifySubjectRegex(Subscriber* subscriber, const std::string& subject ,
			      XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StartNotifySubjectsAndKeys(Subscriber* subscriber,
				  const std::set<std::string>& subjects,
				  const std::set<std::string>& keys ,
				  XrdMqSharedObjectChangeNotifier::notification_t type);
  bool StopNotifySubjectsAndKeys(Subscriber* subscriber,
				 const std::set<std::string>& subjects,
				 const std::set<std::string>& keys ,
				 XrdMqSharedObjectChangeNotifier::notification_t type);
};


//-------------------------------------------------------------------------------
// Set entry in hash map
//-------------------------------------------------------------------------------
template <typename T>
bool
XrdMqSharedHash::Set(const char* key, T&& value, bool broadcast,
		     bool tempmodsubjects, bool notify, bool do_lock)
{
  std::string svalue = eos::common::StringConversion::stringify(value);
  AtomicInc(sSetCounter);

  if (svalue.empty()) {
    return false;
  }

  std::string skey = key;
  bool callback = false;

  if (do_lock) {
    mStoreMutex.LockWrite();
  }

  if (mStore.count(skey) == 0) {
    callback = true;
  }

  mStore[skey].Set(svalue.c_str(), key);

  if (callback) {
    CallBackInsert(&mStore[skey], skey.c_str());
  }

  if (do_lock) {
    mStoreMutex.UnLockWrite();
  }

  if (XrdMqSharedObjectManager::broadcast && broadcast) {
    if (SOM->IsMuxTransaction) {
      XrdSysMutexHelper mLock(SOM->MuxTransactionsMutex);
      SOM->MuxTransactions[mSubject].insert(skey);
    } else {
      // Emulate a transaction for a single set operation
      if (!mIsTransaction) {
	mTransactMutex.Lock();
	mTransactions.clear();
      }

      mTransactions.insert(skey);
    }
  }

  // Check if we have to post for this subject
  if (SOM && notify) {

    if (do_lock) {
      SOM->SubjectsMutex.Lock();
    }

    std::string fkey = mSubject.c_str();
    fkey += ";";
    fkey += skey;

    if (XrdMqSharedObjectManager::debug) {
      fprintf(stderr, "XrdMqSharedObjectManager::Set=>[%s:%s]=>%s notified\n",
	      mSubject.c_str(), skey.c_str(), svalue.c_str());
    }

    if (tempmodsubjects) {
      SOM->ModificationTempSubjects.push_back(fkey);
    } else {
      XrdMqSharedObjectManager::Notification
	event(fkey, XrdMqSharedObjectManager::kMqSubjectModification);
      SOM->NotificationSubjects.push_back(event);
      SOM->SubjectsSem.Post();
    }

    if (do_lock) {
      SOM->SubjectsMutex.UnLock();
    }
  }

  if (XrdMqSharedObjectManager::broadcast && broadcast) {
    if (!SOM->IsMuxTransaction) {
      if (!mIsTransaction) {
	CloseTransaction();
      }
    }
  }

  return true;
}

#endif
