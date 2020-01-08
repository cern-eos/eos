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
#include "XrdSys/XrdSysSemWait.hh"
#include "common/AssistedThread.hh"
#include "common/StringConversion.hh"
#include "common/RWMutex.hh"
#include "common/Logging.hh"
#include "common/table_formatter/TableCell.hh"
#include <string>
#include <map>
#include <vector>
#include <set>
#include <deque>
#include <regex.h>
#include <atomic>

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

//! Forward declaration
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
  //! Constructor with parameters
  //!
  //! @param key entry key
  //! @param value entry value
  //----------------------------------------------------------------------------
  XrdMqSharedHashEntry(const char* key, const char* value);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdMqSharedHashEntry() = default;

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  XrdMqSharedHashEntry(const XrdMqSharedHashEntry& other);

  //----------------------------------------------------------------------------
  //! Assignment operator
  //----------------------------------------------------------------------------
  XrdMqSharedHashEntry& operator=(const XrdMqSharedHashEntry& other);

  //----------------------------------------------------------------------------
  //! Move constructor
  //----------------------------------------------------------------------------
  XrdMqSharedHashEntry(XrdMqSharedHashEntry&& other);

  //----------------------------------------------------------------------------
  //! Move assignment operator
  //----------------------------------------------------------------------------
  XrdMqSharedHashEntry& operator =(XrdMqSharedHashEntry&& other);

  //----------------------------------------------------------------------------
  //! Get value
  //!
  //! @return entry value
  //----------------------------------------------------------------------------
  inline const char* GetValue() const
  {
    return mValue.c_str();
  }

  //----------------------------------------------------------------------------
  //! Set key
  //!
  //! @param key key value
  //----------------------------------------------------------------------------
  inline void SetKey(const char* key)
  {
    if (key) {
      mKey = key;
    } else {
      mKey = "";
    }
  }

  //----------------------------------------------------------------------------
  //! Get key
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
  std::string mKey; ///< Entry key value
  std::string mValue; ///< Entry value
  unsigned long long mChangeId; ///< Entry change id i.e. epoch
  struct timeval mMtime; ///< Last modification time of current entry
};


//------------------------------------------------------------------------------
//! Class XrdMqSharedHash
//------------------------------------------------------------------------------
class XrdMqSharedHash
{
  friend class XrdMqSharedObjectManager;
public:
  static std::atomic<unsigned long long>
  sSetCounter; ///< Counter for set operations
  static std::atomic<unsigned long long>
  sSetNLCounter; ///< Counter for set no-lock operations
  static std::atomic<unsigned long long>
  sGetCounter; ///< Counter for get operations

  std::recursive_mutex mMutex; ///< Mutex locked by external accessors.
                     ///< Temporary workaround until legacy MQ is removed
                     ///< altogether.

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
  virtual ~XrdMqSharedHash() = default;

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  XrdMqSharedHash(const XrdMqSharedHash& other) = delete;

  //----------------------------------------------------------------------------
  //! Copy assignment operator
  //----------------------------------------------------------------------------
  XrdMqSharedHash& operator =(XrdMqSharedHash& other) = delete;

  //----------------------------------------------------------------------------
  //! Move constructor
  //----------------------------------------------------------------------------
  XrdMqSharedHash(XrdMqSharedHash&& other);

  //----------------------------------------------------------------------------
  //! Move assignment operator
  //----------------------------------------------------------------------------
  XrdMqSharedHash& operator =(XrdMqSharedHash&& other);

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
  std::string Get(const std::string& key);

  //----------------------------------------------------------------------------
  //! Get a copy of all the keys
  //!
  //! @return vector containing all the keys in the hash
  //----------------------------------------------------------------------------
  std::vector<std::string> GetKeys();

  //----------------------------------------------------------------------------
  //! Get a copy of all the keys + values
  //!
  //! @return map containing all the key-value pairs in the hash
  //----------------------------------------------------------------------------
  std::map<std::string, std::string> GetContents();

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
  //! Delete key entry
  //!
  //! @param key key value
  //! @param broadcast if true broadcast deletion
  //!
  //! @return true if deletion done, otherwise false
  //----------------------------------------------------------------------------
  virtual bool Delete(const std::string& key, bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Clear contents of the hash
  //!
  //! @param broadcast if true then broadcast the deletions
  //----------------------------------------------------------------------------
  void Clear(bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Set entry in hash map
  //!
  //! @param key key value
  //! @param value entry value which can NOT be an empty string
  //! @param broadcast do broadcast for current subject
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool Set(const char* key, T&& value, bool broadcast = true);

  //============================================================================
  //                          THIS SHOULD BE REVIEWED - BEGIN
  //============================================================================

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
  //                      THIS SHOULD BE REVIEWED - END
  //============================================================================

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

protected:
  std::string mType; ///< Type of objec
  XrdMqSharedObjectManager* mSOM; ///< Pointer to shared object manager
  std::map<std::string, XrdMqSharedHashEntry> mStore; ///< Underlying map obj.

  //----------------------------------------------------------------------------
  //! Set entry in hash map - internal implementation
  //!
  //! @param key key value
  //! @param value entry value - non-empty string
  //! @param broadcast do broadcast for current subject
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual
  bool SetImpl(const char* key, const char* value,  bool broadcast);

private:
  std::string mSubject; ///< Hash subject
  std::atomic<bool> mIsTransaction; ///< True if ongoing transaction
  std::string mBroadcastQueue; ///< Name of the broadcast queue
  std::set<std::string> mDeletions; ///< Set of deletions
  std::set<std::string> mTransactions; ///< Set of transactions
  //1 Mutex protecting the set of transactions
  std::unique_ptr<XrdSysMutex> mTransactMutex;
  //! RW Mutex protecting the mStore object
  std::unique_ptr<eos::common::RWMutex> mStoreMutex;

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
public:
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
  virtual ~XrdMqSharedQueue() = default;

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  XrdMqSharedQueue(const XrdMqSharedQueue& other) = delete;

  //----------------------------------------------------------------------------
  //! Copy assignment operator
  //----------------------------------------------------------------------------
  XrdMqSharedQueue& operator =(XrdMqSharedQueue& other) = delete;

  //----------------------------------------------------------------------------
  //! Move constructor
  //----------------------------------------------------------------------------
  XrdMqSharedQueue(XrdMqSharedQueue&& other);

  //----------------------------------------------------------------------------
  //! Move assignment operator
  //----------------------------------------------------------------------------
  XrdMqSharedQueue& operator=(XrdMqSharedQueue&& other);

  //----------------------------------------------------------------------------
  //! Delete key entry
  //!
  //! @param key key value
  //! @param broadcast if true broadcast deletion
  //!
  //! @return true if deletion done, otherwise false
  //----------------------------------------------------------------------------
  bool Delete(const std::string& key, bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Push back entry into the queue
  //!
  //! @param key entry key
  //! @param value entry value
  //!
  //! @return true if entry added successfully, otherwise false
  //----------------------------------------------------------------------------
  bool PushBack(const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Pop entry value from the queue
  //!
  //! @return entry value or emtpy string if queue is empty
  //----------------------------------------------------------------------------
  std::string PopFront();

private:

  //----------------------------------------------------------------------------
  //! Set entry in hash map - internal implementation
  //!
  //! @param key key value
  //! @param value entry value - non-empty string
  //! @param broadcast do broadcast for current subject
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual
  bool SetImpl(const char* key, const char* value, bool broadcast);

private:
  std::unique_ptr<XrdSysMutex> mQMutex; ///< Mutex protecting the mQueue object
  std::deque<std::string> mQueue; ///< Underlying queue holding keys
  unsigned long long mLastObjId; ///< Id of the last object added to the queue
};


//------------------------------------------------------------------------------
//! Class XrdMqSharedObjectManager
//------------------------------------------------------------------------------
class XrdMqSharedObjectManager: public eos::common::LogId
{
  friend class XrdMqSharedHash;
  friend class XrdMqSharedQueue;
  friend class XrdMqSharedObjectChangeNotifier;

public:
  static std::atomic<bool> sDebug; ///< Set debug mode

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqSharedObjectManager();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdMqSharedObjectManager();

  // If true, creation/deletion subjects are filled and SubjectsSem gets posted
  // for every new creation/deletion.
  std::atomic<bool> mEnableQueue;

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

  eos::common::RWMutex HashMutex; ///< Mutex protecting access to the subject maps

  //----------------------------------------------------------------------------
  //! Switch to globally en-/disable broadcasting of changes into shared queues
  //!
  //! @param enable if true enable broadcast, otherwise disable - default enabled
  //----------------------------------------------------------------------------
  inline void EnableBroadCast(bool enable)
  {
    mBroadcast = enable;
  }

  //----------------------------------------------------------------------------
  //! Indicate if we are broadcasting
  //----------------------------------------------------------------------------
  inline bool ShouldBroadCast()
  {
    return mBroadcast;
  }

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void SetAutoReplyQueue(const char* queue)
  {
    AutoReplyQueue = queue;
  }

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void SetAutoReplyQueueDerive(bool val)
  {
    AutoReplyQueueDerive = val;
  }

  //----------------------------------------------------------------------------
  //! Create requested type of shared object
  //!
  //! @param subject shared object subject
  //! @param bcast_queue broadcast queue name
  //! @param type type of shared object hash/queue
  //! @param som pointer shared object manager
  //!
  //! @return true if create successful, otherwise false
  //----------------------------------------------------------------------------
  bool CreateSharedObject(const char* subject, const char* bcast_queue,
                          const char* type = "hash",
                          XrdMqSharedObjectManager* som = 0);

  //----------------------------------------------------------------------------
  //! Create shared hash object. Parameters are the same as for the
  //! CreateSharedObject method.
  //----------------------------------------------------------------------------
  bool CreateSharedHash(const char* subject, const char* bcast_queue,
                        XrdMqSharedObjectManager* som = 0);

  //----------------------------------------------------------------------------
  //! Create shared queue object. Parameters are the same as for the
  //! CreateSharedObject method.
  //----------------------------------------------------------------------------
  bool CreateSharedQueue(const char* subject, const char* bcast_queue,
                         XrdMqSharedObjectManager* som = 0);

  //----------------------------------------------------------------------------
  //! Delete shared object type
  //!
  //! @param subject shared object subject
  //! @param type shared object type
  //! @param broadcast if true broadcast deletion
  //!
  //! @return true if deletion successful, otherwise false
  //----------------------------------------------------------------------------
  bool DeleteSharedObject(const char* subject, const char* type, bool broadcast);

  //----------------------------------------------------------------------------
  //! Delete shared hash object
  //----------------------------------------------------------------------------
  bool DeleteSharedHash(const char* subject, bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Delete shared hash queue
  //----------------------------------------------------------------------------
  bool DeleteSharedQueue(const char* subject , bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Get pointer to shared object type
  //!
  //! @param subject shared object subject
  //! @param type shared object type
  //!
  //! @return pointer to shared object if found, otherwise null
  //----------------------------------------------------------------------------
  XrdMqSharedHash* GetObject(const char* subject, const char* type);

  //----------------------------------------------------------------------------
  //! Get shared hash object corresponding to the subject
  //!
  //! @param subject reuqested subject
  //!
  //! @return shared hash object or NULL
  //----------------------------------------------------------------------------
  XrdMqSharedHash* GetHash(const char* subject);

  //----------------------------------------------------------------------------
  //! Get shared queue object corresponding to the subject
  //!
  //! @param subject reuqested subject
  //!
  //! @return shared queue object or NULL
  //----------------------------------------------------------------------------
  XrdMqSharedQueue* GetQueue(const char* subject);

  //----------------------------------------------------------------------------
  //! Dump contents of all shared objects to the output string
  //!
  //! @param out output string
  //----------------------------------------------------------------------------
  void DumpSharedObjects(XrdOucString& out);

  //----------------------------------------------------------------------------
  //! Parse message and apply any modifications
  //!
  //! @param message incoming message
  //! @param error possible error message
  //----------------------------------------------------------------------------
  bool ParseEnvMessage(XrdMqMessage* message, XrdOucString& error);

  //----------------------------------------------------------------------------
  //! Set debug level
  //!
  //! @param dbg if true enable debug, otherwise disable
  //----------------------------------------------------------------------------
  void SetDebug(bool dbg = false)
  {
    sDebug = dbg;
  }

  //----------------------------------------------------------------------------
  //! Starts a thread which continously dumps all the hashes
  //----------------------------------------------------------------------------
  void StartDumper(const char* file);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void FileDumper(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Clear all managed hashes and queues
  //----------------------------------------------------------------------------
  void Clear();

  //----------------------------------------------------------------------------
  //! Multiplexed transactions doing a compound transaction for transactions
  //! on several hashes
  //----------------------------------------------------------------------------
  bool OpenMuxTransaction(const char* type = "hash",
                          const char* broadcastqueue = 0);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  bool CloseMuxTransaction();

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void MakeMuxUpdateEnvHeader(XrdOucString& out);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void AddMuxTransactionEnvString(XrdOucString& out);

protected:
  XrdSysMutex MuxTransactionsMutex; ///< protects the mux transaction map
  std::string MuxTransactionType; ///<
  std::string MuxTransactionBroadCastQueue;
  bool IsMuxTransaction;
  std::map<std::string, std::set<std::string> > MuxTransactions;
  std::deque<Notification> mNotificationSubjects;
  //! Semaphore to wait for new creations/deletions/modifications
  XrdSysSemWait SubjectsSem;
  //! Mutex to protect the creations/deletions/modifications & watch subjects
  XrdSysMutex mSubjectsMutex;

private:
  std::atomic<bool> mBroadcast {true}; ///< Broadcast mode, default on
  AssistedThread mDumperTid; ///< Dumper thread tid
  ///! Map of subjects to shared hash objects
  std::map<std::string, XrdMqSharedHash*> mHashSubjects;
  ///! Map of subjects to shared queue objects
  std::map<std::string, XrdMqSharedQueue> mQueueSubjects;
  std::string mDumperFile; ///< File where dumps are written
  //! Queue used to setup the reply queue of hashes which have been broadcasted
  std::string AutoReplyQueue;
  //! True if the reply queue is derived from the subject e.g. the subject
  // "/eos/<host>/fst/<path>" derives as "/eos/<host>/fst"
  bool AutoReplyQueueDerive;
};

//------------------------------------------------------------------------------
//! Class XrdMqSharedObjectChangeNotifier
//------------------------------------------------------------------------------
class XrdMqSharedObjectChangeNotifier: public eos::common::LogId
{
public:
  struct Subscriber {
    std::string Name;
    // some of the members are array of size 5, one for each type of notification
    // kMqSubjectNothing=-1,
    // kMqSubjectCreation=0,
    // kMqSubjectDeletion=1,
    // kMqSubjectModification=2,
    // kMqSubjectKeyDeletion=3
    // The value 4 is a variant of kMqSubjectModification that could be called
    // kMqSubjectModificationStrict in which the value is actually checked
    // for a change the last value being recorded in LastValues
    std::set<std::string>  WatchKeys[5];
    std::set<std::string>  WatchKeysRegex[5];
    std::set<std::string>  WatchSubjects[5];
    std::set<std::string>  WatchSubjectsRegex[5];
    std::vector< std::pair<std::set<std::string>, std::set<std::string> > >
    WatchSubjectsXKeys[5];
    XrdSysMutex WatchMutex; //< protects access to all Watch* objects

    std::deque<XrdMqSharedObjectManager::Notification> NotificationSubjects;
    XrdSysSemWait mSubjSem;
    XrdSysMutex mSubjMtx;
    bool Notify;

    Subscriber(const std::string& name = ""):
      Name(name), Notify(false)
    {}

    bool empty()
    {
      for (int k = 0; k < 4; k++) {
        if (WatchSubjects[k].size() ||
            WatchKeys[k].size() ||
            WatchSubjectsRegex[k].size() ||
            WatchKeysRegex[k].size() ||
            WatchSubjectsXKeys[k].size()) {
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

  static thread_local Subscriber* tlSubscriber;
  inline Subscriber* BindCurrentThread(const std::string& name,
                                       bool createIfNeeded = true)
  {
    return (tlSubscriber = GetSubscriberFromCatalog(name, createIfNeeded));
  }

  void SetShareObjectManager(XrdMqSharedObjectManager* som)
  {
    SOM = som;
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqSharedObjectChangeNotifier():
    SOM(nullptr) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~XrdMqSharedObjectChangeNotifier()
  {
    Stop();
  }

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
                                 const std::set<std::string>& subjects,
                                 const std::set<std::string>& keys,
                                 XrdMqSharedObjectChangeNotifier::notification_t type);

  bool SubscribesToSubjectAndKey(const std::string& susbcriber,
                                 const std::string& subject, const std::string& key,
                                 XrdMqSharedObjectChangeNotifier::notification_t type);

  bool SubscribesToSubjectAndKey(const std::string& susbcriber,
                                 const std::string& subject,
                                 const std::set<std::string>& keys,
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
                              const std::string& key,
                              XrdMqSharedObjectChangeNotifier::notification_t type);

  bool UnsubscribesToSubjectAndKey(const std::string& susbcriber,
                                   std::set<std::string> subjects, std::set<std::string> keys,
                                   XrdMqSharedObjectChangeNotifier::notification_t type);

  bool UnsubscribesToSubjectAndKey(const std::string& susbcriber,
                                   const std::string& subject, const std::string& key,
                                   XrdMqSharedObjectChangeNotifier::notification_t type);

  bool UnsubscribesToSubjectAndKey(const std::string& susbcriber,
                                   const std::string& subject,
                                   const std::set<std::string>& keys,
                                   XrdMqSharedObjectChangeNotifier::notification_t type);

  bool UnsubscribesToSubjectAndKey(const std::string& susbcriber,
                                   const std::set<std::string>& subjects,
                                   const std::string& key,
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

  XrdSysMutex WatchMutex;
  std::map<std::string, WatchItemInfo > WatchKeys2Subscribers[5];
  std::map<std::string, WatchItemInfo > WatchSubjects2Subscribers[5];
  std::vector< std::pair< std::pair<std::set<std::string>, std::set<std::string> >
  , std::set<Subscriber*> > >
  WatchSubjectsXKeys2Subscribers[5];
  //!  listof((Subjects,Keys),Subscribers)
  std::map<std::string, std::string> LastValues;

  AssistedThread mDispatchThread; ///< Dispatching change thread

  //----------------------------------------------------------------------------
  //! Loop ran by thread dispatching change notifications
  //!
  //! @param assistant executing thread
  //----------------------------------------------------------------------------
  void SomListener(ThreadAssistant& assistant) noexcept;

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
// Set entry in hash map /queue
//-------------------------------------------------------------------------------
template <typename T>
bool
XrdMqSharedHash::Set(const char* key, T&& value, bool broadcast)
{
  std::string svalue = eos::common::StringConversion::stringify(value);
  sSetCounter++;

  if (svalue.empty()) {
    fprintf(stderr, "Error: key=%s uses an empty value!\n", key);
    return false;
  }

  return SetImpl(key, svalue.c_str(), broadcast);
}

#endif
