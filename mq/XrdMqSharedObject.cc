// ----------------------------------------------------------------------
// File: XrdMqSharedObject.cc
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

#include "mq/XrdMqSharedObject.hh"
#include "mq/XrdMqMessaging.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/ParseUtils.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>

using eos::common::RWMutexReadLock;
using eos::common::RWMutexWriteLock;

std::atomic<bool> XrdMqSharedObjectManager::sDebug {false};

// Static counters
std::atomic<unsigned long long> XrdMqSharedHash::sSetCounter {0};
std::atomic<unsigned long long> XrdMqSharedHash::sSetNLCounter {0};
std::atomic<unsigned long long> XrdMqSharedHash::sGetCounter {0};

thread_local XrdMqSharedObjectChangeNotifier::Subscriber*
XrdMqSharedObjectChangeNotifier::tlSubscriber = NULL;


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
#define _NotifierMapUpdate(map,key,subscriber)     \
  {                                                \
  auto entry = map.find(key);                      \
  if( entry != map.end() ) {                       \
    entry->second.mSubscribers.erase(subscriber);  \
    if(entry->second.mSubscribers.empty()) {       \
      if(entry->second.mRegex) {                   \
        regfree(entry->second.mRegex);             \
        delete entry->second.mRegex;               \
      }                                            \
      map.erase(entry);                            \
    }                                              \
  }                                                \
  }

//------------------------------------------------------------------------------
//                  * * *  Class XrdMqSharedHashEntry * * *
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdMqSharedHashEntry::XrdMqSharedHashEntry():
  mKey(""), mValue(""), mChangeId(0)
{
  mMtime.tv_sec = 0;
  mMtime.tv_usec = 0;
}

//------------------------------------------------------------------------------
// Constructor with parameters
//------------------------------------------------------------------------------
XrdMqSharedHashEntry::XrdMqSharedHashEntry(const char* key, const char* value):
  mChangeId(0)
{
  gettimeofday(&mMtime, 0);
  mKey = (key ? key : "");
  mValue = (value ? value : "");
}

//------------------------------------------------------------------------------
// Copy assignment operator
//------------------------------------------------------------------------------
XrdMqSharedHashEntry&
XrdMqSharedHashEntry::operator=(const XrdMqSharedHashEntry& other)
{
  if (this != &other) {
    mChangeId = other.mChangeId;
    mKey = other.mKey;
    mValue = other.mValue;
    mMtime.tv_sec = other.mMtime.tv_sec;
    mMtime.tv_usec = other.mMtime.tv_usec;
  }

  return *this;
}

//------------------------------------------------------------------------------
// Copy constructor
//------------------------------------------------------------------------------
XrdMqSharedHashEntry::XrdMqSharedHashEntry(const XrdMqSharedHashEntry& other)
{
  *this = other;
}

//----------------------------------------------------------------------------
//! Move constructor
//----------------------------------------------------------------------------
XrdMqSharedHashEntry::XrdMqSharedHashEntry(XrdMqSharedHashEntry&& other):
  mKey(std::move(other.mKey)), mValue(std::move(other.mValue)),
  mChangeId(other.mChangeId), mMtime(other.mMtime)
{}

//------------------------------------------------------------------------------
// Move assignment operator
//------------------------------------------------------------------------------
XrdMqSharedHashEntry&
XrdMqSharedHashEntry::operator=(XrdMqSharedHashEntry&& other)
{
  if (this != &other) {
    mKey = std::move(other.mKey);
    mValue = std::move(other.mValue);
    mChangeId = other.mChangeId;
    mMtime = other.mMtime;
  }

  return *this;
}

//------------------------------------------------------------------------------
// Get age in milliseconds
//------------------------------------------------------------------------------
long long
XrdMqSharedHashEntry::GetAgeInMilliSeconds()
{
  struct timeval ntime;
  gettimeofday(&ntime, 0);
  return (((ntime.tv_sec - mMtime.tv_sec) * 1000) +
          ((ntime.tv_usec - mMtime.tv_usec) / 1000));
}

//------------------------------------------------------------------------------
// Get age in seconds
//------------------------------------------------------------------------------
double
XrdMqSharedHashEntry::GetAgeInSeconds()
{
  return GetAgeInMilliSeconds() / 1000.0;
}

//------------------------------------------------------------------------------
// Append entry representation the output string
//------------------------------------------------------------------------------
void
XrdMqSharedHashEntry::Dump(XrdOucString& out)
{
  char format_line[65536];
  snprintf(format_line, sizeof(format_line) - 1,
           "value:%-32s age:%.2f changeid:%llu", mValue.c_str(),
           GetAgeInSeconds(), mChangeId);
  out += format_line;
}


//------------------------------------------------------------------------------
//                 * * * Class XrdMqSharedObjectHash * * *
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdMqSharedHash::XrdMqSharedHash(const char* subject, const char* bcast_queue,
                                 XrdMqSharedObjectManager* som):
  mType("hash"), mSOM(som), mSubject((subject ? subject : "")),
  mIsTransaction(false), mBroadcastQueue((bcast_queue ? bcast_queue : "")),
  mTransactMutex(new XrdSysMutex()), mStoreMutex(new eos::common::RWMutex())
{}

//------------------------------------------------------------------------------
// Move constructor
//------------------------------------------------------------------------------
XrdMqSharedHash::XrdMqSharedHash(XrdMqSharedHash&& other):
  mSOM(nullptr)
{
  *this = std::move(other);
}

//------------------------------------------------------------------------------
// Move assignment operator
//------------------------------------------------------------------------------
XrdMqSharedHash&
XrdMqSharedHash::operator=(XrdMqSharedHash&& other)
{
  if (this != &other) {
    mSOM = nullptr;
    mTransactMutex.reset(nullptr);
    mStoreMutex.reset(nullptr);
    mType = other.mType;
    std::swap(mSOM, other.mSOM);
    mSubject = other.mSubject;
    mIsTransaction = other.mIsTransaction.load();
    mBroadcastQueue = other.mBroadcastQueue;
    std::swap(mStore, other.mStore);
    std::swap(mDeletions, other.mDeletions);
    std::swap(mTransactions, other.mTransactions);
    std::swap(mTransactMutex, other.mTransactMutex);
    std::swap(mStoreMutex, other.mStoreMutex);
  }

  return *this;
}

//------------------------------------------------------------------------------
// Get size of the hash
//------------------------------------------------------------------------------
unsigned int
XrdMqSharedHash::GetSize()
{
  RWMutexReadLock rd_lock(*mStoreMutex);
  return (unsigned int) mStore.size();
}

//------------------------------------------------------------------------------
// Get age in milliseconds for a certain key
//------------------------------------------------------------------------------
unsigned long long
XrdMqSharedHash::GetAgeInMilliSeconds(const char* key)
{
  RWMutexReadLock rd_lock(*mStoreMutex);
  unsigned long long val  = (mStore.count(key) ?
                             mStore[key].GetAgeInMilliSeconds() : 0);
  return val;
}

//------------------------------------------------------------------------------
// Get age in seconds for a certain key
//------------------------------------------------------------------------------
unsigned long long
XrdMqSharedHash::GetAgeInSeconds(const char* key)
{
  RWMutexReadLock rd_lock(*mStoreMutex);
  unsigned long long
  val = (mStore.count(key) ? (unsigned long long)
         mStore[key].GetAgeInSeconds() : (unsigned long long) 0);
  return val;
}

//------------------------------------------------------------------------------
// Get entry value for key
//------------------------------------------------------------------------------
std::string
XrdMqSharedHash::Get(const std::string& key)
{
  sGetCounter++;
  std::string value = "";
  RWMutexReadLock rd_lock(*mStoreMutex);

  if (mStore.count(key)) {
    value = mStore[key].GetValue();
  }

  return value;
}

//------------------------------------------------------------------------------
// Get a copy of all the keys
//------------------------------------------------------------------------------
std::vector<std::string>
XrdMqSharedHash::GetKeys()
{
  std::vector<std::string> keys;
  RWMutexReadLock rd_lock(*mStoreMutex);

  for (auto it = mStore.begin(); it != mStore.end(); ++it) {
    keys.push_back(it->first);
  }

  return keys;
}

//------------------------------------------------------------------------------
// Get a copy of all the keys + values
//
// @return map containing all the key-value pairs in the hash
//------------------------------------------------------------------------------
std::map<std::string, std::string>
XrdMqSharedHash::GetContents()
{
  std::map<std::string, std::string> contents;
  RWMutexReadLock rd_lock(*mStoreMutex);

  for (auto it = mStore.begin(); it != mStore.end(); ++it) {
    contents.emplace(it->first, it->second.GetValue());
  }

  return contents;
}

//------------------------------------------------------------------------------
// Get key value as long long
//------------------------------------------------------------------------------
long long
XrdMqSharedHash::GetLongLong(const char* key)
{
  return eos::common::ParseLongLong(Get(key));
}

//------------------------------------------------------------------------------
// Get key value as double
//------------------------------------------------------------------------------
double
XrdMqSharedHash::GetDouble(const char* key)
{
  return eos::common::ParseDouble(Get(key));
}

//------------------------------------------------------------------------------
// Get key value as unsigned int
//------------------------------------------------------------------------------
unsigned int
XrdMqSharedHash::GetUInt(const char* key)
{
  return (unsigned int) GetLongLong(key);
}

//------------------------------------------------------------------------------
// Open transaction
//------------------------------------------------------------------------------
bool
XrdMqSharedHash::OpenTransaction()
{
  mTransactMutex->Lock();
  mTransactions.clear();
  mIsTransaction = true;
  return true;
}

//-------------------------------------------------------------------------------
// Close transaction
//-------------------------------------------------------------------------------
bool
XrdMqSharedHash::CloseTransaction()
{
  bool retval = true;

  if (mSOM->mBroadcast && mTransactions.size()) {
    XrdOucString txmessage = "";
    MakeUpdateEnvHeader(txmessage);
    AddTransactionsToEnvString(txmessage, false);

    if (txmessage.length() > (2 * 1000 * 1000)) {
      // Set the message size limit to 2M, if the message is bigger then just
      // send transaction item by item.
      for (auto it = mTransactions.begin(); it != mTransactions.end(); ++it) {
        txmessage = "";
        MakeUpdateEnvHeader(txmessage);
        txmessage += "&";
        txmessage += XRDMQSHAREDHASH_PAIRS;
        txmessage += "=";
        RWMutexReadLock rd_lock(*mStoreMutex);

        if ((mStore.count(it->c_str()))) {
          txmessage += "|";
          txmessage += it->c_str();
          txmessage += "~";
          txmessage += mStore[it->c_str()].GetValue();
          txmessage += "%";
          char cid[1024];
          snprintf(cid, sizeof(cid) - 1, "%llu", mStore[it->c_str()].GetChangeId());
          txmessage += cid;
        }

        XrdMqMessage message("XrdMqSharedHashMessage");
        message.SetBody(txmessage.c_str());
        message.MarkAsMonitor();
        retval &= XrdMqMessaging::gMessageClient.SendMessage(message,
                  mBroadcastQueue.c_str(), false, false, true);
      }
    } else {
      XrdMqMessage message("XrdMqSharedHashMessage");
      message.SetBody(txmessage.c_str());
      message.MarkAsMonitor();
      retval &= XrdMqMessaging::gMessageClient.SendMessage(message,
                mBroadcastQueue.c_str(), false, false, true);
    }
  }

  if (mSOM->mBroadcast && mDeletions.size()) {
    XrdOucString txmessage = "";
    MakeDeletionEnvHeader(txmessage);
    AddDeletionsToEnvString(txmessage);
    XrdMqMessage message("XrdMqSharedHashMessage");
    message.SetBody(txmessage.c_str());
    message.MarkAsMonitor();
    retval &= XrdMqMessaging::gMessageClient.SendMessage(message,
              mBroadcastQueue.c_str(), false, false, true);
  }

  mTransactions.clear();
  mIsTransaction = false;
  mTransactMutex->UnLock();
  return retval;
}

//-------------------------------------------------------------------------------
// Construct broadcast env header
//-------------------------------------------------------------------------------
void
XrdMqSharedHash::MakeBroadCastEnvHeader(XrdOucString& out)
{
  out = XRDMQSHAREDHASH_BCREPLY;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += mSubject.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += mType.c_str();
}

//-------------------------------------------------------------------------------
// Construct update env header
//-------------------------------------------------------------------------------
void
XrdMqSharedHash::MakeUpdateEnvHeader(XrdOucString& out)
{
  out = XRDMQSHAREDHASH_UPDATE;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += mSubject.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += mType.c_str();
}

//-------------------------------------------------------------------------------
// Construct deletion env header
//-------------------------------------------------------------------------------
void
XrdMqSharedHash::MakeDeletionEnvHeader(XrdOucString& out)
{
  out = XRDMQSHAREDHASH_DELETE;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += mSubject.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += mType.c_str();
}

//-------------------------------------------------------------------------------
// Construct remove env header
//-------------------------------------------------------------------------------
void
XrdMqSharedHash::MakeRemoveEnvHeader(XrdOucString& out)
{
  out = XRDMQSHAREDHASH_REMOVE;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += mSubject.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += mType.c_str();
}

//-------------------------------------------------------------------------------
// Broadcast hash as env string
//-------------------------------------------------------------------------------
bool
XrdMqSharedHash::BroadCastEnvString(const char* receiver)
{
  XrdOucString txmessage = "";
  {
    XrdSysMutexHelper lock(*mTransactMutex);
    mTransactions.clear();
    mIsTransaction = true;
    {
      RWMutexReadLock rd_lock(*mStoreMutex);

      for (auto it = mStore.begin(); it != mStore.end(); ++it) {
        // @todo(esindril) needs review as there is no clear difference as some
        // stat. parameters do need to be broadcasted!
        // // Skip broadcasting transient values
        // if ((strncmp(it->first.c_str(), "stat.", 5) == 0) &&
        //     (it->first != "stat.active")) {
        //   continue;
        // }
        mTransactions.insert(it->first);
      }
    }
    MakeBroadCastEnvHeader(txmessage);
    // This will also clear the mTransactions set
    AddTransactionsToEnvString(txmessage);
    mIsTransaction = false;
  }

  if (mSOM->mBroadcast) {
    XrdMqMessage message("XrdMqSharedHashMessage");
    message.SetBody(txmessage.c_str());
    message.MarkAsMonitor();

    if (XrdMqSharedObjectManager::sDebug) {
      fprintf(stderr,
              "XrdMqSharedObjectManager::BroadCastEnvString=>[%s]=>%s msg=%s\n",
              mSubject.c_str(), receiver, txmessage.c_str());
    }

    return XrdMqMessaging::gMessageClient.SendMessage(message, receiver, false,
           false, true);
  }

  return true;
}

//-------------------------------------------------------------------------------
// Encode transactions to env string - this must be called with the
// mTransactMutex locked.
//-------------------------------------------------------------------------------
void
XrdMqSharedHash::AddTransactionsToEnvString(XrdOucString& out, bool clear_after)
{
  // Encode transactions as
  // "mysh.pairs=|<key1>~<value1>%<changeid1>|<key2>~<value2>%<changeid2 ..."
  out += "&";
  out += XRDMQSHAREDHASH_PAIRS;
  out += "=";
  RWMutexReadLock rd_lock(*mStoreMutex);

  for (auto it = mTransactions.begin(); it != mTransactions.end(); ++it) {
    if ((mStore.count(it->c_str()))) {
      out += "|";
      out += it->c_str();
      out += "~";
      out += mStore[it->c_str()].GetValue();
      out += "%";
      char cid[1024];
      snprintf(cid, sizeof(cid) - 1, "%llu", mStore[it->c_str()].GetChangeId());
      out += cid;
    }
  }

  if (clear_after) {
    mTransactions.clear();
  }
}

//-------------------------------------------------------------------------------
// Encode deletions as env string - this must be called with the mTransactMutex
// locked.
//-------------------------------------------------------------------------------
void
XrdMqSharedHash::AddDeletionsToEnvString(XrdOucString& out)
{
  // Encode deletions as "mysh.keys=|<key1>|<key2> ...."
  out += "&";
  out += XRDMQSHAREDHASH_KEYS;
  out += "=";

  for (auto it = mDeletions.begin(); it != mDeletions.end(); ++it) {
    out += "|";
    out += it->c_str();
  }

  mDeletions.clear();
}

//-------------------------------------------------------------------------------
// Build and send broadcast request
//-------------------------------------------------------------------------------
bool
XrdMqSharedHash::BroadcastRequest(const char* req_target)
{
  XrdOucString out;
  XrdMqMessage message("XrdMqSharedHashMessage");
  out += XRDMQSHAREDHASH_BCREQUEST;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += mSubject.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_REPLY;
  out += "=";
  out += XrdMqMessaging::gMessageClient.GetClientId();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += mType.c_str();
  message.SetBody(out.c_str());
  message.MarkAsMonitor();
  return XrdMqMessaging::gMessageClient.SendMessage(message, req_target, false,
         false, true);
}

//-------------------------------------------------------------------------------
// Dump hash map representation to output string
//-------------------------------------------------------------------------------
void
XrdMqSharedHash::Dump(XrdOucString& out)
{
  char key_print[64];
  RWMutexReadLock rd_lock(*mStoreMutex);

  for (auto it = mStore.begin(); it != mStore.end(); ++it) {
    snprintf(key_print, sizeof(key_print) - 1, "key=%-24s", it->first.c_str());
    out += key_print;
    out += " ";
    it->second.Dump(out);
    out += "\n";
  }
}

//-------------------------------------------------------------------------------
// Delete key entry
//-------------------------------------------------------------------------------
bool
XrdMqSharedHash::Delete(const std::string& key, bool broadcast)
{
  bool deleted = false;
  RWMutexWriteLock wr_lock(*mStoreMutex);

  if (mStore.count(key)) {
    mStore.erase(key);
    deleted = true;

    if (mSOM->mBroadcast && broadcast) {
      // Emulate transaction for single shot deletions
      if (!mIsTransaction) {
        mTransactMutex->Lock();
        mTransactions.clear();
      }

      mDeletions.insert(key);
      mTransactions.erase(key);

      // Emulate transaction for single shot deletions
      if (!mIsTransaction) {
        CloseTransaction();
      }
    }

    // Check if we have to post for this subject
    if (mSOM) {
      std::string fkey = mSubject.c_str();
      fkey += ";";
      fkey += key;

      if (XrdMqSharedObjectManager::sDebug) {
        fprintf(stderr, "XrdMqSharedObjectManager::Delete=>[%s:%s] notified\n",
                mSubject.c_str(), key.c_str());
      }

      XrdMqSharedObjectManager::Notification event(fkey,
          XrdMqSharedObjectManager::kMqSubjectKeyDeletion);
      mSOM->mSubjectsMutex.Lock();
      mSOM->mNotificationSubjects.push_back(event);
      mSOM->SubjectsSem.Post();
      mSOM->mSubjectsMutex.UnLock();
    }
  }

  return deleted;
}

//-------------------------------------------------------------------------------
// Clear contents of the hash
//-------------------------------------------------------------------------------
void
XrdMqSharedHash::Clear(bool broadcast)
{
  RWMutexWriteLock wr_lock(*mStoreMutex);

  for (auto it = mStore.begin(); it != mStore.end(); ++it) {
    if (mIsTransaction) {
      if (mSOM->mBroadcast && broadcast) {
        mDeletions.insert(it->first);
      }

      mTransactions.erase(it->first);
    }
  }

  mStore.clear();
}

//-------------------------------------------------------------------------------
// Set entry in hash map
//-------------------------------------------------------------------------------
bool
XrdMqSharedHash::SetImpl(const char* key, const char* value, bool broadcast)
{
  std::string skey = key;
  {
    RWMutexWriteLock wr_lock(*mStoreMutex);

    if (mStore.count(skey) == 0) {
      mStore.insert(std::make_pair(skey, XrdMqSharedHashEntry(key, value)));
    } else {
      mStore[skey] = XrdMqSharedHashEntry(key, value);
    }
  }

  if (mSOM->mBroadcast && broadcast) {
    bool is_transact = false;

    // mSOM->IsMuxTransaction is tested first to avoid contention on the
    // MuxTransactionsMutex and then is tested again when we actually have the
    // lock to check it didn't change in the meantime - hackish, needs fix!
    if (mSOM->IsMuxTransaction) {
      XrdSysMutexHelper lock(mSOM->MuxTransactionsMutex);

      if (mSOM->IsMuxTransaction) {
        mSOM->MuxTransactions[mSubject].insert(skey);
        is_transact = true;
      }
    }

    if (!is_transact) {
      // Emulate a transaction for a single set operation
      bool emulate_transact = false;

      if (!mIsTransaction) {
        mTransactMutex->Lock();
        mTransactions.clear();
        emulate_transact = true;
      }

      mTransactions.insert(skey);

      if (emulate_transact) {
        CloseTransaction();
      }
    }
  }

  // Check if we have to post for this subject
  if (mSOM) {
    std::string fkey = mSubject.c_str();
    fkey += ";";
    fkey += skey;

    if (XrdMqSharedObjectManager::sDebug) {
      fprintf(stderr, "XrdMqSharedObjectManager::Set=>[%s:%s]=>%s notified\n",
              mSubject.c_str(), skey.c_str(), value);
    }

    XrdSysMutexHelper lock(mSOM->mSubjectsMutex);
    XrdMqSharedObjectManager::Notification event
    (fkey, XrdMqSharedObjectManager::kMqSubjectModification);
    mSOM->mNotificationSubjects.push_back(event);
    mSOM->SubjectsSem.Post();
  }

  return true;
}

//------------------------------------------------------------------------------
//                 * * * Class XrdMqSharedQueue  * * *
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdMqSharedQueue::XrdMqSharedQueue(const char* subject, const char* bcast_queue,
                                   XrdMqSharedObjectManager* som):
  XrdMqSharedHash(subject, bcast_queue, som), mQMutex(new XrdSysMutex()),
  mLastObjId(0)
{
  mType = "queue";
}

//------------------------------------------------------------------------------
// Move constructor
//------------------------------------------------------------------------------
XrdMqSharedQueue::XrdMqSharedQueue(XrdMqSharedQueue&& other)
{
  *this = std::move(other);
}

//------------------------------------------------------------------------------
// Move assignment operator
//------------------------------------------------------------------------------
XrdMqSharedQueue&
XrdMqSharedQueue::operator=(XrdMqSharedQueue&& other)
{
  if (this != &other) {
    mQMutex.reset(nullptr);
    XrdMqSharedHash::operator=(std::move(other));
    std::swap(mQMutex, other.mQMutex);
    std::swap(mQueue, other.mQueue);
    std::swap(mLastObjId, other.mLastObjId);
  }

  return *this;
}

//------------------------------------------------------------------------------
// Delete entry from queue
//------------------------------------------------------------------------------
bool
XrdMqSharedQueue::Delete(const std::string& key, bool broadcast)
{
  if (!key.empty()) {
    XrdSysMutexHelper lock(*mQMutex);
    bool found = false;

    for (auto it = mQueue.begin(); it != mQueue.end(); ++it) {
      if (*it == key) {
        mQueue.erase(it);
        found = true;
        break;
      }
    }

    if (found) {
      return XrdMqSharedHash::Delete(key);
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Push back entry into the queue
//------------------------------------------------------------------------------
bool
XrdMqSharedQueue::PushBack(const std::string& key, const std::string& value)
{
  if (value.empty()) {
    fprintf(stderr, "Error: key=%s has empty value for queue!\n", key.c_str());
    return false;
  }

  return SetImpl(key.c_str(), value.c_str(), true);
}

//------------------------------------------------------------------------------
// Get first entry value from the queue
//------------------------------------------------------------------------------
std::string
XrdMqSharedQueue::PopFront()
{
  std::string value = "";
  XrdSysMutexHelper lock(*mQMutex);

  if (!mQueue.empty()) {
    std::string key = mQueue.front();
    mQueue.pop_front();
    value = XrdMqSharedHash::Get(key);
    (void) XrdMqSharedHash::Delete(key);
  }

  return value;
}

//-------------------------------------------------------------------------------
// Set entry in queue
//-------------------------------------------------------------------------------
bool
XrdMqSharedQueue::SetImpl(const char* key, const char* value, bool broadcast)
{
  std::string uuid;
  XrdSysMutexHelper lock(*mQMutex);

  if (!key || (*key == '\0')) {
    char lld[1024];
    mLastObjId++;
    snprintf(lld, 1023, "%llu", mLastObjId);
    uuid = lld;
  } else {
    uuid = key;
  }

  if (mStore.find(uuid) == mStore.end()) {
    if (XrdMqSharedHash::SetImpl(uuid.c_str(), value, broadcast)) {
      mQueue.push_back(uuid);
      return true;
    }
  }

  return false;
}


//------------------------------------------------------------------------------
//            * * * Class XrdMqSharedObjectChangeNotifier  * * *
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubject(const std::string&
    subscriber, const std::string& subject,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  eos_static_debug("subscribing to subject %s", subject.c_str());
  Subscriber* s = GetSubscriberFromCatalog(subscriber);
  XrdSysMutexHelper lock(s->WatchMutex);

  if (s->WatchSubjects[type].count(subject)) {
    return false;
  }

  s->WatchSubjects[type].insert(subject);

  if (s->Notify) {
    // if the notification is started for this process, update it
    if (!StartNotifySubject(s, subject, type)) {
      return false;
    }
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubjectRegex(
  const std::string& subscriber, const std::string& subject,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber* s = GetSubscriberFromCatalog(subscriber);
  XrdSysMutexHelper lock(s->WatchMutex);
  eos_static_debug("subscribing to subject regex %s", subject.c_str());

  if (s->WatchSubjectsRegex[type].count(subject)) {
    return false;
  }

  s->WatchSubjectsRegex[type].insert(subject);

  if (s->Notify) {
    // if the notification is started for this process, update it
    if (!StartNotifySubjectRegex(s, subject, type)) {
      return false;
    }
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::SubscribesToKey(const std::string& subscriber,
    const std::string& key,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber* s = GetSubscriberFromCatalog(subscriber);
  XrdSysMutexHelper lock(s->WatchMutex);
  eos_static_debug("subscribing to key %s", key.c_str());

  if (s->WatchKeys[type].count(key)) {
    return false;
  }

  s->WatchKeys[type].insert(key);

  if (s->Notify) {
    // if the notification is started for this process, update it
    if (!StartNotifyKey(s, key, type)) {
      return false;
    }
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::SubscribesToKeyRegex(const std::string&
    subscriber, const std::string& key,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber* s = GetSubscriberFromCatalog(subscriber);
  XrdSysMutexHelper lock(s->WatchMutex);
  eos_static_debug("subscribing to key regex %s", key.c_str());

  if (s->WatchKeysRegex[type].count(key)) {
    return false;
  }

  s->WatchKeysRegex[type].insert(key);

  if (s->Notify) {
    // if the notification is started for this process, update it
    if (!StartNotifyKeyRegex(s, key, type)) {
      return false;
    }
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubjectAndKey(
  const std::string& subscriber,
  const std::set<std::string>& subjects,
  const std::set<std::string>& keys,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  Subscriber* s = GetSubscriberFromCatalog(subscriber);
  XrdSysMutexHelper lock(s->WatchMutex);

  if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
    size_t bufsize = 0;

    for (auto it = subjects.begin(); it != subjects.end(); ++it) {
      bufsize += (it->size() + 1);
    }

    for (auto it = keys.begin(); it != keys.end(); ++it) {
      bufsize += (it->size() + 1);
    }

    bufsize += 64;
    int sz;
    char* buffer = new char[bufsize];
    char* buf = buffer;
    sz = snprintf(buf, bufsize, "subscribing to subjects [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = subjects.begin(); it != subjects.end(); ++it) {
      sz = snprintf(buf, bufsize, "%s ", it->c_str());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf(buf, bufsize, "] times keys [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = keys.begin(); it != keys.end(); ++it) {
      sz = snprintf(buf, bufsize, "%s ", it->c_str());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf(buf, bufsize, "]");
    eos_static_debug("%s", buffer);
    delete[] buffer;
  }

  // firstly update the thread-local vector
  bool insertIntoExisiting = false;
  {
    for (auto it = s->WatchSubjectsXKeys[type].begin();
         it != s->WatchSubjectsXKeys[type].end(); ++it) {
//      {
//         size_t bufsize=0;
//           for(auto it2 = it->first.begin(); it2 != it->first.end(); ++it2)
//             bufsize+=(it2->size()+1);
//           for(auto it2 = it->second.begin(); it2 != it->second.end(); ++it2)
//             bufsize+=(it2->size()+1);
//           bufsize += 64;
//           int sz;
//
//           char *buffer = new char[bufsize];
//           char *buf=buffer;
//           sz = snprintf(buf,bufsize,"WatchSubjectsXKeys item : subjects [ ");
//           buf += sz;
//           bufsize -=sz;
//
//           for(auto it2 = it->first.begin(); it2 != it->first.end(); ++it2) {
//             sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//             buf += sz;
//             bufsize -=sz;
//           }
//
//           sz = snprintf(buf,bufsize,"] times keys [ ");
//           buf += sz;
//           bufsize -=sz;
//
//           for(auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
//             sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//             buf += sz;
//             bufsize -=sz;
//           }
//           sz = snprintf(buf,bufsize,"]");
//
//           eos_static_info("%s", buffer);
//           delete[] buffer;
//       }
      if (subjects == it->first) {
        size_t sizeBefore = it->second.size();
        it->second.insert(keys.begin(), keys.end());

        if (sizeBefore == it->second.size()) {
          return false;  // nothing to insert
        } else {
          insertIntoExisiting = true;
          break;
        }
      } else if (keys == it->second) {
        size_t sizeBefore = it->first.size();
        it->first.insert(subjects.begin(), subjects.end());

        if (sizeBefore == it->first.size()) {
          return false;  // nothing to insert
        } else {
          insertIntoExisiting = true;
          break;
        }
      }
    }

    if (!insertIntoExisiting) {
      s->WatchSubjectsXKeys[type].push_back(make_pair(subjects, keys));
    }
  }

  if (s->Notify) {
    // update the ongoing notification
    return StartNotifySubjectsAndKeys(s, subjects, keys, type);
  }

  return true;
}

bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubjectAndKey(
  const std::string& subscriber, const std::string& subject,
  const std::string& key,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> s, k;
  s.insert(subject);
  k.insert(key);
  return SubscribesToSubjectAndKey(subscriber, s, k, type);
}
bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubjectAndKey(
  const std::string& subscriber, const std::string& subject,
  const std::set<std::string>& keys,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> s;
  s.insert(subject);
  return SubscribesToSubjectAndKey(subscriber, s, keys, type);
}
bool
XrdMqSharedObjectChangeNotifier::SubscribesToSubjectAndKey(
  const std::string& subscriber,
  const std::set<std::string>& subjects,
  const std::string& key,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> k;
  k.insert(key);
  return SubscribesToSubjectAndKey(subscriber, subjects, k, type);
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubject(const std::string&
    subscriber, const std::string& subject,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber* s = GetSubscriberFromCatalog(subscriber, false);

  if (!s) {
    return false;
  }

  XrdSysMutexHelper lock(s->WatchMutex);

  if (s->Notify) {
    // if the notification is started for this process, update it
    if (!StopNotifySubject(s, subject, type)) {
      return false;
    }
  }

  if (s->empty()) {
    delete s;
    s = NULL;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubjectRegex(
  const std::string& subscriber, const std::string& subject,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber* s = GetSubscriberFromCatalog(subscriber, false);

  if (!s) {
    return false;
  }

  XrdSysMutexHelper lock(s->WatchMutex);

  if (s->Notify) {
    // if the notification is started for this process, update it
    if (!StopNotifySubjectRegex(s, subject, type)) {
      return false;
    }
  }

  if (s->empty()) {
    delete s;
    s = NULL;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToKey(const std::string&
    subscriber, const std::string& key,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber* s = GetSubscriberFromCatalog(subscriber, false);

  if (!s) {
    return false;
  }

  XrdSysMutexHelper lock(s->WatchMutex);

  if (s->Notify) {
    // if the notification is started for this process, update it
    if (!StopNotifyKey(s, key, type)) {
      return false;
    }
  }

  if (s->empty()) {
    delete s;
    s = NULL;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToKeyRegex(
  const std::string& subscriber, const std::string& key,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  Subscriber* s = GetSubscriberFromCatalog(subscriber, false);

  if (!s) {
    return false;
  }

  XrdSysMutexHelper lock(s->WatchMutex);

  if (s->Notify) {
    // if the notification is started for this process, update it
    if (!StopNotifyKeyRegex(s, key, type)) {
      return false;
    }
  }

  if (s->empty()) {
    delete s;
    s = NULL;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToEverything(
  const std::string& subscriber)
{
  Subscriber* s = GetSubscriberFromCatalog(subscriber, false);

  if (!s) {
    return false;
  }

  XrdSysMutexHelper lock(s->WatchMutex);

  if (s->Notify) {
    StopNotifyCurrentThread();
  }

  delete s;
  s = NULL;
  return true;
}

///*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubjectAndKey(
  const std::string& subscriber,
  std::set<std::string> subjects,
  std::set<std::string> keys,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

  if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
    size_t bufsize = 0;

    for (auto it = subjects.begin(); it != subjects.end(); ++it) {
      bufsize += (it->size() + 1);
    }

    for (auto it = keys.begin(); it != keys.end(); ++it) {
      bufsize += (it->size() + 1);
    }

    bufsize += 64;
    int sz;
    char* buffer = new char[bufsize];
    char* buf = buffer;
    sz = snprintf(buf, bufsize, "unsubscribing to subjects [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = subjects.begin(); it != subjects.end(); ++it) {
      sz = snprintf(buf, bufsize, "%s ", it->c_str());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf(buf, bufsize, "] times keys [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = keys.begin(); it != keys.end(); ++it) {
      sz = snprintf(buf, bufsize, "%s ", it->c_str());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf(buf, bufsize, "]");
    eos_static_debug("%s", buffer);
    delete[] buffer;
  }

  Subscriber* s = GetSubscriberFromCatalog(subscriber, false);

  if (!s) {
    return false;
  }

  XrdSysMutexHelper lock(s->WatchMutex);
  // firstly update the thread-local vector
  bool removedAll = false;
  {
    for (auto it = s->WatchSubjectsXKeys[type].begin();
         it != s->WatchSubjectsXKeys[type].end(); ++it) {
      if (it->first == subjects &&
          std::includes(it->second.begin(), it->second.end(), keys.begin(), keys.end())) {
        set<string> newKeys;
        set_difference(it->second.begin(), it->second.end(), keys.begin(), keys.end(),
                       inserter(newKeys, newKeys.end()));
        it->second = newKeys;
        //it->second.erase(keys.begin(),keys.end());
        removedAll = true;

        if (it->second.empty()) {
          s->WatchSubjectsXKeys[type].erase(it);
        }

        break;
      } else if (it->second == keys
                 && std::includes(it->first.begin(), it->first.end(), subjects.begin(),
                                  subjects.end())) {
        set<string> newSubjects;
        set_difference(it->first.begin(), it->first.end(), subjects.begin(),
                       subjects.end(),
                       inserter(newSubjects, newSubjects.end()));
        it->first = newSubjects;
        //it->first.erase(subjects.begin(),subjects.end());
        removedAll = true;

        if (it->first.empty()) {
          s->WatchSubjectsXKeys[type].erase(it);
        }

        break;
      }
    }

    if (!removedAll) {
      return false;
    }
  }
  // SYNCHRONIZE AITH GLOBAL MAP

  if (s->Notify) {
    return StopNotifySubjectsAndKeys(s, subjects, keys, type);
  }

  return true;
}

bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubjectAndKey(
  const std::string& subscriber, const std::string& subject,
  const std::string& key,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> s, k;
  s.insert(subject);
  k.insert(key);
  return UnsubscribesToSubjectAndKey(subscriber, s, k, type);
}
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubjectAndKey(
  const std::string& subscriber, const std::string& subject,
  const std::set<std::string>& keys,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> s;
  s.insert(subject);
  return UnsubscribesToSubjectAndKey(subscriber, s, keys, type);
}
bool
XrdMqSharedObjectChangeNotifier::UnsubscribesToSubjectAndKey(
  const std::string& subscriber,
  const std::set<std::string>& subjects,
  const std::string& key,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  std::set<std::string> k;
  k.insert(key);
  return UnsubscribesToSubjectAndKey(subscriber, subjects, k, type);
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifyKey(Subscriber* subscriber,
    const std::string& key,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock(WatchMutex);
  return (WatchKeys2Subscribers[type][key].mSubscribers.insert(
            subscriber)).second;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifyKeyRegex(Subscriber* subscriber,
    const std::string& key,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock(WatchMutex);
  bool res = (WatchKeys2Subscribers[type][key].mSubscribers.insert(
                subscriber)).second;

  if (WatchKeys2Subscribers[type][key].mRegex == NULL) {
    regex_t* r = new regex_t;

    if (regcomp(r, key.c_str(), REG_NOSUB)) {
      WatchKeys2Subscribers[type].erase(key);
      delete r;
      return false;
    }

    WatchKeys2Subscribers[type][key].mRegex = r;
  }

  return res;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifyKey(Subscriber* subscriber,
    const std::string& key,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock(WatchMutex);
  _NotifierMapUpdate(WatchKeys2Subscribers[type], key, subscriber);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifyKeyRegex(Subscriber* subscriber,
    const std::string& key,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock(WatchMutex);
  _NotifierMapUpdate(WatchKeys2Subscribers[type], key, subscriber);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifySubject(Subscriber* subscriber,
    const std::string& subject,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock(WatchMutex);
  return (WatchSubjects2Subscribers[type][subject].mSubscribers.insert(
            subscriber)).second;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifySubjectRegex(Subscriber* subscriber,
    const std::string& subject,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock(WatchMutex);
  bool res = (WatchSubjects2Subscribers[type][subject].mSubscribers.insert(
                subscriber)).second;

  if (WatchSubjects2Subscribers[type][subject].mRegex) {
    regex_t* r = new regex_t;

    if (regcomp(r, subject.c_str(), REG_NOSUB)) {
      WatchSubjects2Subscribers[type].erase(subject);
      delete r;
      return false;
    }

    WatchSubjects2Subscribers[type][subject].mRegex = r;
  }

  return res;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifySubject(Subscriber* subscriber,
    const std::string& subject,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock(WatchMutex);
  _NotifierMapUpdate(WatchSubjects2Subscribers[type], subject, subscriber);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifySubjectRegex(Subscriber* subscriber,
    const std::string& subject,
    XrdMqSharedObjectChangeNotifier::notification_t type)
{
  XrdSysMutexHelper lock(WatchMutex);
  _NotifierMapUpdate(WatchSubjects2Subscribers[type], subject, subscriber);
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifySubjectsAndKeys(
  Subscriber* subscriber,
  const std::set<std::string>& subjects,
  const std::set<std::string>& keys,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

  if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
    size_t bufsize = 0;

    for (auto it = subjects.begin(); it != subjects.end(); ++it) {
      bufsize += (it->size() + 1);
    }

    for (auto it = keys.begin(); it != keys.end(); ++it) {
      bufsize += (it->size() + 1);
    }

    bufsize += 64;
    int sz;
    char* buffer = new char[bufsize];
    char* buf = buffer;
    sz = snprintf(buf, bufsize, "starting notification for subjects [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = subjects.begin(); it != subjects.end(); ++it) {
      sz = snprintf(buf, bufsize, "%s ", it->c_str());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf(buf, bufsize, "] times keys [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = keys.begin(); it != keys.end(); ++it) {
      sz = snprintf(buf, bufsize, "%s ", it->c_str());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf(buf, bufsize, "]");
    eos_static_debug("%s", buffer);
    delete[] buffer;
  }

  bool insertIntoExisiting = false;
  XrdSysMutexHelper lock(WatchMutex);

  for (auto it = WatchSubjectsXKeys2Subscribers[type].begin();
       it != WatchSubjectsXKeys2Subscribers[type].end(); ++it) {
//    {
//    size_t bufsize=0;
//      for(auto it2 = it->first.first.begin(); it2 != it->first.first.end(); ++it2)
//        bufsize+=(it2->size()+1);
//      for(auto it2 = it->first.second.begin(); it2 != it->first.second.end(); ++it2)
//        bufsize+=(it2->size()+1);
//      bufsize += 64;
//      int sz;
//
//      char *buffer = new char[bufsize];
//      char *buf=buffer;
//      sz = snprintf(buf,bufsize,"WatchSubjectsXKeys2Subscribers item : subjects [ ");
//      buf += sz;
//      bufsize -=sz;
//
//      for(auto it2 = it->first.first.begin(); it2 != it->first.first.end(); ++it2) {
//        sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//        buf += sz;
//        bufsize -=sz;
//      }
//
//      sz = snprintf(buf,bufsize,"] times keys [ ");
//      buf += sz;
//      bufsize -=sz;
//
//      for(auto it2 = it->first.second.begin(); it2 != it->first.second.end(); ++it2) {
//        sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//        buf += sz;
//        bufsize -=sz;
//      }
//      sz = snprintf(buf,bufsize,"]");
//
//      eos_static_info("%s", buffer);
//      delete[] buffer;
//  }
    if (subjects == it->first.first) {
      if (it->second.size() == 1 && it->second.count(subscriber)) {
        // only one subscriber and it's the same, factor
        size_t sizeBefore = it->first.second.size();
        it->first.second.insert(keys.begin(), keys.end());

        if (sizeBefore == it->first.second.size()) {
          return false;  // nothing to insert
        } else {
          insertIntoExisiting = true;
          break;
        }
      } else if (keys == it->first.second && it->second.count(subscriber) == 0) {
        it->second.insert(
          subscriber);  // same SubjectXKey without this subscriber -> we insert it
        break;
      }
    } else if (keys == it->first.second) {
      if (it->second.size() == 1 && it->second.count(subscriber)) {
        // only one subscriber and it's the same, factor
        size_t sizeBefore = it->first.first.size();
        it->first.first.insert(subjects.begin(), subjects.end());

        if (sizeBefore == it->first.first.size()) {
          return false;  // nothing to insert
        } else {
          insertIntoExisiting = true;
          break;
        }
      } else if (subjects == it->first.first && it->second.count(subscriber) == 0) {
        it->second.insert(
          subscriber);  // same SubjectXKey without this subscriber -> we insert it
        break;
      }
    }
  }

  if (!insertIntoExisiting) {
    std::set<Subscriber*> s;
    s.insert(subscriber);
    WatchSubjectsXKeys2Subscribers[type].push_back(make_pair(make_pair(subjects,
        keys), s));
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifySubjectsAndKeys(
  Subscriber* subscriber,
  const std::set<std::string>& subjects,
  const std::set<std::string>& keys,
  XrdMqSharedObjectChangeNotifier::notification_t type)
{
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

  if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
    size_t bufsize = 0;

    for (auto it = subjects.begin(); it != subjects.end(); ++it) {
      bufsize += (it->size() + 1);
    }

    for (auto it = keys.begin(); it != keys.end(); ++it) {
      bufsize += (it->size() + 1);
    }

    bufsize += 64;
    int sz;
    char* buffer = new char[bufsize];
    char* buf = buffer;
    sz = snprintf(buf, bufsize, "stopping notifications for subjects [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = subjects.begin(); it != subjects.end(); ++it) {
      sz = snprintf(buf, bufsize, "%s ", it->c_str());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf(buf, bufsize, "] times keys [ ");
    buf += sz;
    bufsize -= sz;

    for (auto it = keys.begin(); it != keys.end(); ++it) {
      sz = snprintf(buf, bufsize, "%s ", it->c_str());
      buf += sz;
      bufsize -= sz;
    }

    sz = snprintf(buf, bufsize, "]");
    eos_static_debug("%s", buffer);
    delete[] buffer;
  }

  bool removedAll = false;
  // secondly update the global vector
  XrdSysMutexHelper lock(WatchMutex);

  for (auto it = WatchSubjectsXKeys2Subscribers[type].begin();
       it != WatchSubjectsXKeys2Subscribers[type].end(); ++it) {
//    {
//      size_t bufsize=0;
//        for(auto it2 = it->first.first.begin(); it2 != it->first.first.end(); ++it2)
//          bufsize+=(it2->size()+1);
//        for(auto it2 = it->first.second.begin(); it2 != it->first.second.end(); ++it2)
//          bufsize+=(it2->size()+1);
//        bufsize += 64;
//        int sz;
//
//        char *buffer = new char[bufsize];
//        char *buf=buffer;
//        sz = snprintf(buf,bufsize,"WatchSubjectsXKeys2Subscribers item : subjects [ ");
//        buf += sz;
//        bufsize -=sz;
//
//        for(auto it2 = it->first.first.begin(); it2 != it->first.first.end(); ++it2) {
//          sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//          buf += sz;
//          bufsize -=sz;
//        }
//
//        sz = snprintf(buf,bufsize,"] times keys [ ");
//        buf += sz;
//        bufsize -=sz;
//
//        for(auto it2 = it->first.second.begin(); it2 != it->first.second.end(); ++it2) {
//          sz = snprintf(buf,bufsize,"%s ",it2->c_str());
//          buf += sz;
//          bufsize -=sz;
//        }
//        sz = snprintf(buf,bufsize,"]");
//
//        eos_static_info("%s", buffer);
//        delete[] buffer;
//    }
    if ((it->first.first == subjects) &&
        std::includes(it->first.second.begin(), it->first.second.end(), keys.begin(),
                      keys.end())) {
      if (it->second.count(subscriber)) {
        // if the subscriber is there
        if (it->second.size() > 1) {
          // there's some other subscriber, split before update
          it->second.erase(subscriber);
          WatchSubjectsXKeys2Subscribers[type].push_back(
            std::make_pair(it->first, std::set<Subscriber*> (&subscriber,
                           &subscriber + 1)));
          it = WatchSubjectsXKeys2Subscribers[type].end() - 1;
        }

        if (it->second.size() == 1) {
          // if the subscriber is the only guy there
          for (auto itk = keys.begin(); itk != keys.end(); ++itk) {
            it->first.second.erase(*itk);
          }

          if (it->first.second.empty()) { // if this entry is now empty, remove it
            WatchSubjectsXKeys2Subscribers[type].erase(it);
          }
        }

        removedAll = true;
        break;
      }
    } else if ((it->first.second == keys) &&
               std::includes(it->first.first.begin(), it->first.first.end(),
                             subjects.begin(), subjects.end())) {
      if (it->second.count(subscriber)) {
        // if the subscriber is there
        // eos_static_debug("1 element size is %d vector size is %d",
        // it->second.size(),WatchSubjectsXKeys2Subscribers[type].size());
        if (it->second.size() > 1) {
          // there's some other subscriber, split before update
          it->second.erase(subscriber);
          WatchSubjectsXKeys2Subscribers[type].push_back(
            std::make_pair(it->first, std::set<Subscriber*> (&subscriber,
                           &subscriber + 1)));
          it = WatchSubjectsXKeys2Subscribers[type].end() - 1;
        }

        // eos_static_debug("2 element size is %d vector size is %d",
        // it->second.size(), WatchSubjectsXKeys2Subscribers[type].size());
        if (it->second.size() == 1) {
          // if the subscriber is the only guy there
          for (auto its = subjects.begin(); its != subjects.end(); ++its) {
            it->first.first.erase(*its);
          }

          // eos_static_debug("3 element size is %d vector size is %d",
          // it->second.size(),WatchSubjectsXKeys2Subscribers[type].size());
          if (it->first.first.empty()) { // if this entry is now empty, remove it
            WatchSubjectsXKeys2Subscribers[type].erase(it);
          }
        }

        removedAll = true;
        break;
      }
    }
  }

  if (!removedAll) {
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StartNotifyCurrentThread()
{
  // to start notifying, we just copy the references to the watched words
  // from the thread local to the global map
  if (!tlSubscriber) {
    eos_static_err("the current thread is not bound to any subscriber");
    return false;
  }

  if (tlSubscriber->Notify) {
    return false;
  }

  eos_static_info("Starting notification");
  {
    XrdSysMutexHelper lock1(tlSubscriber->WatchMutex);
    {
      XrdSysMutexHelper lock2(WatchMutex);

      for (int type = 0; type < 5; type++) {
        for (auto it = tlSubscriber->WatchKeys[type].begin();
             it != tlSubscriber->WatchKeys[type].end(); ++it) {
          WatchKeys2Subscribers[type][*it].mSubscribers.insert(tlSubscriber);
        }

        for (auto it = tlSubscriber->WatchSubjects[type].begin();
             it != tlSubscriber->WatchSubjects[type].end(); ++it) {
          WatchSubjects2Subscribers[type][*it].mSubscribers.insert(tlSubscriber);
        }

        for (auto it = tlSubscriber->WatchKeysRegex[type].begin();
             it != tlSubscriber->WatchKeysRegex[type].end(); ++it) {
          WatchKeys2Subscribers[type][*it].mSubscribers.insert(tlSubscriber);

          if (!WatchKeys2Subscribers[type][*it].mRegex) {
            regex_t* r = new regex_t;

            if (regcomp(r, it->c_str(), REG_NOSUB)) {
              WatchKeys2Subscribers[type].erase(*it);
              delete r;
              return false;
            }

            WatchKeys2Subscribers[type][*it].mRegex = r;
          }
        }

        for (auto it = tlSubscriber->WatchSubjectsRegex[type].begin();
             it != tlSubscriber->WatchSubjectsRegex[type].end(); ++it) {
          WatchSubjects2Subscribers[type][*it].mSubscribers.insert(tlSubscriber);

          if (!WatchSubjects2Subscribers[type][*it].mRegex) {
            regex_t* r = new regex_t;

            if (regcomp(r, it->c_str(), REG_NOSUB)) {
              WatchSubjects2Subscribers[type].erase(*it);
              delete r;
              return false;
            }

            WatchSubjects2Subscribers[type][*it].mRegex = r;
          }
        }
      }
    }
  }

  for (int type = 0; type < 5; ++type) {
    for (auto it = tlSubscriber->WatchSubjectsXKeys[type].begin();
         it != tlSubscriber->WatchSubjectsXKeys[type].end(); ++it) {
      StartNotifySubjectsAndKeys(tlSubscriber, it->first, it->second,
                                 static_cast<XrdMqSharedObjectChangeNotifier::notification_t>(type));
    }
  }

  tlSubscriber->Notify = true;
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdMqSharedObjectChangeNotifier::StopNotifyCurrentThread()
{
  if (!tlSubscriber) {
    eos_static_err("the current thread is not bound to any subscriber");
    return false;
  }

  // to stop notifying, we just remove the references to the watched words
  // from the thread local to the global map
  if (!tlSubscriber->Notify) {
    return false;
  }

  eos_static_info("Stopping notification");
  {
    XrdSysMutexHelper lock1(tlSubscriber->WatchMutex);
    {
      XrdSysMutexHelper lock2(WatchMutex);

      for (int type = 0; type < 5; type++) {
        for (auto it = tlSubscriber->WatchKeys[type].begin();
             it != tlSubscriber->WatchKeys[type].end(); ++it) {
          _NotifierMapUpdate(WatchKeys2Subscribers[type], *it, tlSubscriber);
        }

        for (auto it = tlSubscriber->WatchSubjects[type].begin();
             it != tlSubscriber->WatchSubjects[type].end(); ++it) {
          _NotifierMapUpdate(WatchSubjects2Subscribers[type], *it, tlSubscriber);
        }

        for (auto it = tlSubscriber->WatchKeysRegex[type].begin();
             it != tlSubscriber->WatchKeysRegex[type].end(); ++it) {
          _NotifierMapUpdate(WatchKeys2Subscribers[type], *it, tlSubscriber);
        }

        for (auto it = tlSubscriber->WatchSubjectsRegex[type].begin();
             it != tlSubscriber->WatchSubjectsRegex[type].end(); ++it) {
          _NotifierMapUpdate(WatchSubjects2Subscribers[type], *it, tlSubscriber);
        }

        std::vector<decltype(WatchSubjectsXKeys2Subscribers[type].begin())> toRemove;

        for (auto it = WatchSubjectsXKeys2Subscribers[type].begin();
             it != WatchSubjectsXKeys2Subscribers[type].end(); ++it) {
          auto entry = it->second.find(tlSubscriber);

          if (entry != it->second.end()) {
            // if the current threads is a subscriber of this entry
            it->second.erase(tlSubscriber);

            if (it->second.empty()) {
              // if the set of subscribers is empty, remove this entry
              toRemove.push_back(it);
            }
          }
        }
      }
    }
  }

  for (int type = 0; type < 5; type++) {
    for (auto it = tlSubscriber->WatchSubjectsXKeys[type].begin();
         it != tlSubscriber->WatchSubjectsXKeys[type].end(); ++it) {
      StopNotifySubjectsAndKeys(tlSubscriber, it->first, it->second,
                                static_cast<XrdMqSharedObjectChangeNotifier::notification_t>(type));
    }
  }

  tlSubscriber->Notify = false;
  return true;
}

/*----------------------------------------------------------------------------*/
void
XrdMqSharedObjectChangeNotifier::SomListener(ThreadAssistant& assistant)
noexcept
{
  // thread listening on filesystem errors and configuration changes
  eos_static_info("%s", "mgm=\"starting SOM listener\"");

  while (!assistant.terminationRequested()) {
    SOM->SubjectsSem.Wait();

    if (assistant.terminationRequested()) {
      eos_static_notice("%s", "msg=\"exiting SOM listener thread\"");
      break;
    }

    // we always take a lock to take something from the queue and then release it
    WatchMutex.Lock();
    SOM->mSubjectsMutex.Lock();
    std::set<Subscriber*> notifiedSubscribers;

    while (SOM->mNotificationSubjects.size()) {
      XrdMqSharedObjectManager::Notification event;
      event = SOM->mNotificationSubjects.front();
      SOM->mNotificationSubjects.pop_front();
      SOM->mSubjectsMutex.UnLock();
      std::string newsubject = event.mSubject;
      //eos_info("msg=\"SOM Listener new notification\" subject=%s, type=%i",
      //         event.mSubject.c_str(), event.mType);
      int type = static_cast<int>(event.mType);
      std::set<Subscriber*> notifiedSubscribersForCurrentEvent;
      std::string key = newsubject;
      std::string queue = newsubject;
      size_t dpos = 0;

      if ((dpos = queue.find(";")) != std::string::npos) {
        key.erase(0, dpos + 1);
        queue.erase(dpos);
      }

      // these are useful only if type == 4
      std::string newVal;
      bool newValAsserted = false;
      bool isNewVal = false;

      do {
        // Check if there is a matching key
        for (auto it = WatchKeys2Subscribers[type].begin();
             it != WatchKeys2Subscribers[type].end(); ++it) {
          if (((it->second.mRegex == NULL) && (key == it->first)) ||
              ((it->second.mRegex != NULL) &&
               !regexec(it->second.mRegex, key.c_str(), 0, NULL, 0))) {
            if (type == 4) {
              if (!newValAsserted) {
                auto lvIt = LastValues.find(newsubject);
                SOM->HashMutex.LockRead();
                XrdMqSharedHash* hash = SOM->GetObject(queue.c_str(), "hash");
                SOM->HashMutex.UnLockRead();

                if (hash) {
                  newVal = hash->Get(key.c_str());

                  if (lvIt == LastValues.end() || lvIt->second != newVal) {
                    isNewVal = true;
                  }

                  newValAsserted = true;
                } else {
                  continue;
                }
              }

              if (isNewVal) {
                // eos_static_debug("notification on %s : new value %s IS a"
                // "strict change",newsubject.c_str(),newVal.c_str());
                LastValues[newsubject] = newVal;
              } else {
                //eos_static_debug("notification on %s : new value %s IS NOT"
                //" a strict change",newsubject.c_str(),newVal.c_str());
                continue;
              }
            }

            for (auto it2 = it->second.mSubscribers.begin();
                 it2 != it->second.mSubscribers.end(); ++it2) {
              if (notifiedSubscribersForCurrentEvent.count(*it2) == 0) {
                // Don't notify twice for the same event
                (*it2)->mSubjMtx.Lock();
                (*it2)->NotificationSubjects.push_back(event);
                (*it2)->mSubjMtx.UnLock();
                notifiedSubscribersForCurrentEvent.insert(*it2);
                notifiedSubscribers.insert(*it2);
              }
            }
          }
        }

        // Check if there is a matching subject
        for (auto it = WatchSubjects2Subscribers[type].begin();
             it != WatchSubjects2Subscribers[type].end(); ++it) {
          if (((it->second.mRegex == NULL) && (queue == it->first)) ||
              ((it->second.mRegex != NULL) &&
               !regexec(it->second.mRegex, queue.c_str(), 0, NULL, 0))) {
            if (type == 4) {
              if (!newValAsserted) {
                auto lvIt = LastValues.find(newsubject);
                SOM->HashMutex.LockRead();
                XrdMqSharedHash* hash = SOM->GetObject(queue.c_str(), "hash");
                SOM->HashMutex.UnLockRead();

                if (hash) {
                  newVal = hash->Get(key.c_str());

                  if (lvIt == LastValues.end() || lvIt->second != newVal) {
                    isNewVal = true;
                  }

                  newValAsserted = true;
                }
              }

              if (isNewVal) {
                // eos_static_debug("notification on %s : new value %s IS a "
                // "strict change",newsubject.c_str(),newVal.c_str());
                LastValues[newsubject] = newVal;
              } else {
                //eos_static_debug("notification on %s : new value %s IS NOT "
                // "a strict change",newsubject.c_str(),newVal.c_str());
                continue;
              }
            }

            for (auto it2 = it->second.mSubscribers.begin();
                 it2 != it->second.mSubscribers.end(); ++it2) {
              if (notifiedSubscribersForCurrentEvent.count(*it2) == 0) {
                // Don't notify twice for the same event
                (*it2)->mSubjMtx.Lock();
                (*it2)->NotificationSubjects.push_back(event);
                (*it2)->mSubjMtx.UnLock();
                notifiedSubscribersForCurrentEvent.insert(*it2);
                notifiedSubscribers.insert(*it2);
              }
            }
          } else {
            // eos_static_info("regex %s DID NOT MATCH %s",it->first.c_str(),
            // queue.c_str());
          }
        }

        // Check if there is a matching subjectXkey
        for (auto it = WatchSubjectsXKeys2Subscribers[type].begin();
             it != WatchSubjectsXKeys2Subscribers[type].end(); ++it) {
          if (it->first.first.count(queue) && it->first.second.count(key)) {
            if (type == 4) {
              if (!newValAsserted) {
                auto lvIt = LastValues.find(newsubject);
                SOM->HashMutex.LockRead();
                XrdMqSharedHash* hash = SOM->GetObject(queue.c_str(), "hash");
                SOM->HashMutex.UnLockRead();

                if (hash) {
                  newVal = hash->Get(key.c_str());

                  if (lvIt == LastValues.end() || lvIt->second != newVal) {
                    isNewVal = true;
                  }

                  newValAsserted = true;
                } else {
                  continue;
                }
              }

              if (isNewVal) {
                //if(key=="id") {
                // eos_static_warning("WARNING ID CHANGE in queue %s FROM %s "
                // "to %s",queue.c_str(),LastValues[newsubject].c_str(),newVal.c_str());
                //}
                LastValues[newsubject] = newVal;
              } else {
                // eos_static_info("notification on %s : new value %s IS NOT "
                // "a strict change",newsubject.c_str(),newVal.c_str());
                continue;
              }
            }

            for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
              if (notifiedSubscribersForCurrentEvent.count(*it2) == 0) {
                // Don't notify twice for the same event
                (*it2)->mSubjMtx.Lock();
                (*it2)->NotificationSubjects.push_back(event);
                (*it2)->mSubjMtx.UnLock();
                notifiedSubscribersForCurrentEvent.insert(*it2);
                notifiedSubscribers.insert(*it2);
              }
            }
          }
        }

        if (type == 2) {
          // If it's a modification, check also the strict modifications to be notified
          type = 4;
        } else {
          break;
        }
      } while (true);

      SOM->mSubjectsMutex.Lock();
    }

    // wake up all subscriber threads
    for (auto it = notifiedSubscribers.begin();
         it != notifiedSubscribers.end(); ++it) {
      (*it)->mSubjSem.Post();
    }

    SOM->mSubjectsMutex.UnLock();
    WatchMutex.UnLock();
  }
}

//------------------------------------------------------------------------------
// Start dispatching change thread
//------------------------------------------------------------------------------
bool
XrdMqSharedObjectChangeNotifier::Start()
{
  try {
    mDispatchThread.reset(&XrdMqSharedObjectChangeNotifier::SomListener, this);
  } catch (const std::system_error& e) {
    eos_static_err("%s", "msg=\"failed to start SOM listener\"");
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Stop dispatcher thread
//------------------------------------------------------------------------------
bool
XrdMqSharedObjectChangeNotifier::Stop()
{
  auto start = std::chrono::steady_clock::now();
  auto stop_objnotifier = std::thread([&]() {
    mDispatchThread.join();
  });
  // We now need to signal to the SomListener thread to unblock it
  {
    if (SOM) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      XrdSysMutexHelper lock(SOM->mSubjectsMutex);
      SOM->SubjectsSem.Post();
    }
  }
  stop_objnotifier.join();
  auto end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
                  (end - start);
  eos_static_notice("msg=\"SOM listener shutdown duration: %llu millisec",
                    duration.count());
  return true;
}

//------------------------------------------------------------------------------
//                * * * Class XrdMqSharedObjectManager  * * *
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdMqSharedObjectManager::XrdMqSharedObjectManager():
  mEnableQueue(false), mDumperFile("")
{
  AutoReplyQueue = "";
  AutoReplyQueueDerive = false;
  IsMuxTransaction = false;
  {
    XrdSysMutexHelper mLock(MuxTransactionsMutex);
    MuxTransactions.clear();
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdMqSharedObjectManager::~XrdMqSharedObjectManager()
{
  mDumperTid.join();

  for (auto it = mHashSubjects.begin(); it != mHashSubjects.end(); ++it) {
    delete it->second;
  }
}

//----------------------------------------------------------------------------
// Create requested shared object type
//----------------------------------------------------------------------------
bool
XrdMqSharedObjectManager::CreateSharedObject(const char* subject,
    const char* bcast_queue,
    const char* type,
    XrdMqSharedObjectManager* som)
{
  std::string stype = type;

  if (stype == "hash") {
    return CreateSharedHash(subject, bcast_queue, som ? som : this);
  }

  if (stype == "queue") {
    return CreateSharedQueue(subject, bcast_queue, som ? som : this);
  }

  return false;
}

//------------------------------------------------------------------------------
// Create shared hash object
//------------------------------------------------------------------------------
bool
XrdMqSharedObjectManager::CreateSharedHash(const char* subject,
    const char* broadcastqueue,
    XrdMqSharedObjectManager* som)
{
  std::string ss = subject;
  Notification event(ss, XrdMqSharedObjectManager::kMqSubjectCreation);
  HashMutex.LockWrite();

  if (mHashSubjects.count(ss) > 0) {
    mHashSubjects[ss]->SetBroadCastQueue(broadcastqueue);
    HashMutex.UnLockWrite();
    return false;
  } else {
    XrdMqSharedHash* newhash = new XrdMqSharedHash(subject, broadcastqueue,
        som ? som : this);
    mHashSubjects.insert(std::pair<std::string, XrdMqSharedHash*> (ss, newhash));
    HashMutex.UnLockWrite();

    if (mEnableQueue) {
      mSubjectsMutex.Lock();
      mNotificationSubjects.push_back(event);
      mSubjectsMutex.UnLock();
      SubjectsSem.Post();
    }

    return true;
  }
}

//------------------------------------------------------------------------------
// Create shared queue object
//------------------------------------------------------------------------------
bool
XrdMqSharedObjectManager::CreateSharedQueue(const char* subject,
    const char* broadcastqueue,
    XrdMqSharedObjectManager* som)
{
  std::string ss = subject;
  Notification event(ss, XrdMqSharedObjectManager::kMqSubjectCreation);
  HashMutex.LockWrite();

  if (mQueueSubjects.count(ss) > 0) {
    HashMutex.UnLockWrite();
    return false;
  } else {
    mQueueSubjects.emplace(ss, XrdMqSharedQueue(subject, broadcastqueue,
                           som ? som : this));
    HashMutex.UnLockWrite();

    if (mEnableQueue) {
      mSubjectsMutex.Lock();
      mNotificationSubjects.push_back(event);
      mSubjectsMutex.UnLock();
      SubjectsSem.Post();
    }

    return true;
  }
}

//------------------------------------------------------------------------------
// Delete shared object type
//------------------------------------------------------------------------------
bool
XrdMqSharedObjectManager::DeleteSharedObject(const char* subject,
    const char* type, bool broadcast)
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

//------------------------------------------------------------------------------
// Delete shared hash object
//------------------------------------------------------------------------------
bool
XrdMqSharedObjectManager::DeleteSharedHash(const char* subject, bool broadcast)
{
  std::string ss = subject;
  Notification event(ss, XrdMqSharedObjectManager::kMqSubjectDeletion);
  HashMutex.LockWrite();

  if ((mHashSubjects.count(ss) > 0)) {
    if (mBroadcast && broadcast) {
      XrdOucString txmessage = "";
      mHashSubjects[ss]->MakeRemoveEnvHeader(txmessage);
      XrdMqMessage message("XrdMqSharedHashMessage");
      message.SetBody(txmessage.c_str());
      message.MarkAsMonitor();
      XrdMqMessaging::gMessageClient.SendMessage(message, 0, false, false, true);
    }

    delete(mHashSubjects[ss]);
    mHashSubjects.erase(ss);
    HashMutex.UnLockWrite();

    if (mEnableQueue) {
      mSubjectsMutex.Lock();
      mNotificationSubjects.push_back(event);
      mSubjectsMutex.UnLock();
      SubjectsSem.Post();
    }

    return true;
  } else {
    HashMutex.UnLockWrite();
    return true;
  }
}

//------------------------------------------------------------------------------
// Delete shared queue
//------------------------------------------------------------------------------
bool
XrdMqSharedObjectManager::DeleteSharedQueue(const char* subject, bool broadcast)
{
  std::string ss = subject;
  Notification event(ss, XrdMqSharedObjectManager::kMqSubjectDeletion);
  HashMutex.LockWrite();

  if ((mQueueSubjects.count(ss) > 0)) {
    if (mBroadcast && broadcast) {
      XrdOucString txmessage = "";
      mQueueSubjects[ss].MakeRemoveEnvHeader(txmessage);
      XrdMqMessage message("XrdMqSharedHashMessage");
      message.SetBody(txmessage.c_str());
      message.MarkAsMonitor();
      XrdMqMessaging::gMessageClient.SendMessage(message, 0, false, false, true);
    }

    mQueueSubjects.erase(ss);
    HashMutex.UnLockWrite();

    if (mEnableQueue) {
      mSubjectsMutex.Lock();
      mNotificationSubjects.push_back(event);
      mSubjectsMutex.UnLock();
      SubjectsSem.Post();
    }

    return true;
  } else {
    HashMutex.UnLockWrite();
    return true;
  }
}

//------------------------------------------------------------------------------
// Get pointer to shared object type
//------------------------------------------------------------------------------
XrdMqSharedHash*
XrdMqSharedObjectManager::GetObject(const char* subject, const char* type)
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

//------------------------------------------------------------------------------
// Get shared hash object
//------------------------------------------------------------------------------
XrdMqSharedHash*
XrdMqSharedObjectManager::GetHash(const char* subject)
{
  std::string ssubject = subject;

  if (mHashSubjects.count(ssubject)) {
    return mHashSubjects[ssubject];
  } else {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Get shared queue object
//------------------------------------------------------------------------------
XrdMqSharedQueue*
XrdMqSharedObjectManager::GetQueue(const char* subject)
{
  std::string ssubject = subject;

  if (mQueueSubjects.count(ssubject)) {
    return &mQueueSubjects[ssubject];
  } else {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Dump contents of all shared objects to the output string
//------------------------------------------------------------------------------
void
XrdMqSharedObjectManager::DumpSharedObjects(XrdOucString& out)
{
  out = "";
  RWMutexReadLock lock(HashMutex);

  for (auto it = mHashSubjects.begin(); it != mHashSubjects.end(); ++it) {
    std::unique_lock lock(it->second->mMutex);
    out += "===================================================\n";
    out += it->first.c_str();
    out += " [ hash=>  ";
    out += it->second->GetBroadCastQueue();
    out += " ]\n";
    out += "---------------------------------------------------\n";
    it->second->Dump(out);
  }

  for (auto it = mQueueSubjects.begin(); it != mQueueSubjects.end(); ++it) {
    out += "===================================================\n";
    out += it->first.c_str();
    out += " [ queue=> ";
    out += it->second.GetBroadCastQueue();
    out += " ]\n";
    out += "---------------------------------------------------\n";
    it->second.Dump(out);
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdMqSharedObjectManager::StartDumper(const char* file)
{
  mDumperFile = file;

  try {
    mDumperTid.reset(&XrdMqSharedObjectManager::FileDumper, this);
  } catch (const std::system_error& e) {
    fprintf(stderr, "XrdMqSharedObjectManager::StartDumper=> failed to run "
            "dumper thread\n");
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdMqSharedObjectManager::FileDumper(ThreadAssistant& assistant) noexcept
{
  while (!assistant.terminationRequested()) {
    XrdOucString s;
    DumpSharedObjects(s);
    std::string df = mDumperFile;
    df += ".tmp";
    FILE* f = fopen(df.c_str(), "w+");

    if (f) {
      fprintf(f, "%s\n", s.c_str());
      fclose(f);
    }

    if (chmod(mDumperFile.c_str(), S_IRWXU | S_IRGRP | S_IROTH)) {
      fprintf(stderr, "XrdMqSharedObjectManager::FileDumper=> unable to set "
              "755 permissions on file %s\n", mDumperFile.c_str());
    }

    if (rename(df.c_str(), mDumperFile.c_str())) {
      fprintf(stderr, "XrdMqSharedObjectManager::FileDumper=> unable to write "
              "dumper file %s\n", mDumperFile.c_str());
    }

    assistant.wait_for(std::chrono::seconds(60));
  }
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool
XrdMqSharedObjectManager::ParseEnvMessage(XrdMqMessage* message,
    XrdOucString& error)
{
  error = "";
  std::string subject = "";
  std::string reply = "";
  std::string type = "";

  if (!message) {
    error = "no message provided";
    return false;
  }

  XrdOucEnv env(message->GetBody());
  int envlen;

  if (sDebug) {
    char* senv = env.Env(envlen);
    fprintf(stderr, "XrdMqSharedObjectManager::ParseEnvMessage=> size=%d text=%s\n",
            envlen, senv);
  }

  if (env.Get(XRDMQSHAREDHASH_SUBJECT)) {
    subject = env.Get(XRDMQSHAREDHASH_SUBJECT);
  } else {
    error = "no subject in message body";
    return false;
  }

  if (env.Get(XRDMQSHAREDHASH_REPLY)) {
    reply = env.Get(XRDMQSHAREDHASH_REPLY);
  } else {
    reply = "";
  }

  if (env.Get(XRDMQSHAREDHASH_TYPE)) {
    type = env.Get(XRDMQSHAREDHASH_TYPE);
  } else {
    error = "no hash type in message body";
    return false;
  }

  if (env.Get(XRDMQSHAREDHASH_CMD)) {
    HashMutex.LockRead();
    XrdMqSharedHash* sh = 0;
    std::vector<std::string> subjectlist;
    int wpos = 0;

    // support 'wild card' broadcasts with <name>/*
    if ((wpos = subject.find("/*")) != STR_NPOS) {
      XrdOucString wmatch = subject.c_str();
      wmatch.erase(wpos);

      for (auto it = mHashSubjects.begin(); it != mHashSubjects.end(); ++it) {
        XrdOucString hs = it->first.c_str();

        if (hs.beginswith(wmatch)) {
          subjectlist.push_back(hs.c_str());
        }
      }

      for (auto it = mQueueSubjects.begin(); it != mQueueSubjects.end(); ++it) {
        XrdOucString hs = it->first.c_str();

        if (hs.beginswith(wmatch)) {
          subjectlist.push_back(hs.c_str());
        }
      }
    } else {
      // support 'wild card' broadcasts with */<name>
      if ((subject.find("*/")) == 0) {
        XrdOucString wmatch = subject.c_str();
        wmatch.erase(0, 2);

        for (auto it = mHashSubjects.begin(); it != mHashSubjects.end(); ++it) {
          XrdOucString hs = it->first.c_str();

          if (hs.endswith(wmatch)) {
            subjectlist.push_back(hs.c_str());
          }
        }

        for (auto it = mQueueSubjects.begin(); it != mQueueSubjects.end(); ++it) {
          XrdOucString hs = it->first.c_str();

          if (hs.endswith(wmatch)) {
            subjectlist.push_back(hs.c_str());
          }
        }
      } else {
        std::string delimiter = "%";
        // we support also multiplexed subject updates and split the list
        eos::common::StringConversion::Tokenize(subject, subjectlist, delimiter);
      }
    }

    XrdOucString ftag = XRDMQSHAREDHASH_CMD;
    ftag += "=";
    ftag += env.Get(XRDMQSHAREDHASH_CMD);

    if (subjectlist.size() > 0) {
      sh = GetObject(subjectlist[0].c_str(), type.c_str());
    }

    if ((ftag == XRDMQSHAREDHASH_BCREQUEST) ||
        (ftag == XRDMQSHAREDHASH_DELETE) ||
        (ftag == XRDMQSHAREDHASH_REMOVE)) {
      // if we don't know the subject, we don't create it with a BCREQUEST
      if ((ftag == XRDMQSHAREDHASH_BCREQUEST) && (reply == "")) {
        HashMutex.UnLockRead();
        error = "bcrequest: no reply address present";
        return false;
      }

      if (!sh) {
        if (ftag == XRDMQSHAREDHASH_BCREQUEST) {
          error = "bcrequest: don't know this subject ";

          if (!subjectlist.empty()) {
            error += subjectlist[0].c_str();
          }
        }

        if (ftag == XRDMQSHAREDHASH_DELETE) {
          error = "delete: don't know this subject ";

          if (!subjectlist.empty()) {
            error += subjectlist[0].c_str();
          }
        }

        if (ftag == XRDMQSHAREDHASH_REMOVE) {
          error = "remove: don't know this subject ";

          if (!subjectlist.empty()) {
            error += subjectlist[0].c_str();
          }
        }

        HashMutex.UnLockRead();
        return false;
      } else {
        HashMutex.UnLockRead();
      }
    } else {
      // automatically create the subject, if it does not exist
      if (!sh) {
        HashMutex.UnLockRead();

        if (AutoReplyQueueDerive) {
          AutoReplyQueue = subject.c_str();
          int pos = 0;

          for (int i = 0; i < 4; i++) {
            pos = subject.find("/", pos);

            if (i < 3) {
              if (pos == STR_NPOS) {
                AutoReplyQueue = "";
                error = "cannot derive the reply queue from ";
                error += subject.c_str();
                return false;
              } else {
                pos++;
              }
            } else {
              AutoReplyQueue.erase(pos);
            }
          }
        }

        // create the list of subjects
        for (size_t i = 0; i < subjectlist.size(); i++) {
          if (!CreateSharedObject(subjectlist[i].c_str(), AutoReplyQueue.c_str(),
                                  type.c_str())) {
            error = "cannot create shared object for ";
            error += subject.c_str();
            error += " and type ";
            error += type.c_str();
            eos_err("%s", error.c_str());
            return false;
          }
        }

        {
          RWMutexReadLock lock(HashMutex);
          sh = GetObject(subject.c_str(), type.c_str());
        }
      } else {
        HashMutex.UnLockRead();
      }
    }

    {
      RWMutexReadLock lock(HashMutex);
      // from here on we have a read lock on 'sh'

      if ((ftag == XRDMQSHAREDHASH_UPDATE) || (ftag == XRDMQSHAREDHASH_BCREPLY)) {
        std::string val = (env.Get(XRDMQSHAREDHASH_PAIRS) ? env.Get(
                             XRDMQSHAREDHASH_PAIRS) : "");

        if (val.length() == 0) {
          error = "no pairs in message body";
          return false;
        }

        if ((ftag == XRDMQSHAREDHASH_BCREPLY) && sh) {
          // Don't broadcast this one ... is a broadcast reply
          sh->Clear(false);
        }

        std::string key;
        std::string value;
        std::string cid;
        std::vector<int> keystart;
        std::vector<int> valuestart;
        std::vector<int> cidstart;

        for (unsigned int i = 0; i < val.length(); i++) {
          if (val.c_str()[i] == '|') {
            keystart.push_back(i);
          }

          if (val.c_str()[i] == '~') {
            valuestart.push_back(i);
          }

          if (val.c_str()[i] == '%') {
            cidstart.push_back(i);
          }
        }

        if (keystart.size() != valuestart.size()) {
          error = "update: parsing error in pairs tag";
          return false;
        }

        if (keystart.size() != cidstart.size()) {
          error = "update: parsing error in pairs tag";
          return false;
        }

        int parseindex = 0;

        for (size_t s = 0; s < subjectlist.size(); s++) {
          sh = GetObject(subjectlist[s].c_str(), type.c_str());

          if (!sh) {
            error = "update: subject ";
            error += subjectlist[s].c_str();
            error += " does not exist";
            return false;
          }

          std::string sstr;

          for (unsigned int i = parseindex; i < keystart.size(); i++) {
            key.assign(val, keystart[i] + 1, valuestart[i] - 1 - (keystart[i]));
            value.assign(val, valuestart[i] + 1, cidstart[i] - 1 - (valuestart[i]));

            if (i == (keystart.size() - 1)) {
              cid.assign(val, cidstart[i] + 1, val.length() - cidstart[i] - 1);
            } else {
              cid.assign(val, cidstart[i] + 1, keystart[i + 1] - 1 - (cidstart[i]));
            }

            // eos_info("got bcreply subject=%s, key=%s, val=%s obj_ptr=%p",
            //          subject.c_str(), key.c_str(), value.c_str(), (void *)sh);

            if (subjectlist.size() > 1) {
              // This is a multiplexed update, where we have to remove the
              // subject from the key if there is a match with the current subject
              // MUX transactions have the #<subject-index># as key prefix
              XrdOucString skey = "#";
              skey += (int) s;
              skey += "#";

              if (!key.compare(0, skey.length(), skey.c_str())) {
                // this is the right key for the subject we are dealing with
                key.erase(0, skey.length());
              } else {
                parseindex = i;
                break;
              }
            } else {
              // This can be the case for a single multiplexed message, so
              // we have also to remove the prefix in that case
              XrdOucString skey = "#";
              skey += (int) s;
              skey += "#";

              if (!key.compare(0, skey.length(), skey.c_str())) {
                // this is the right key for the subject we are dealing with
                key.erase(0, skey.length());
              }
            }

            // Set entry without broadcast
            sh->Set(key.c_str(), value.c_str(), false);
          }
        }

        return true;
      }

      if (ftag == XRDMQSHAREDHASH_BCREQUEST) {
        bool success = true;

        for (unsigned int l = 0; l < subjectlist.size(); l++) {
          // try 'queue' and 'hash' to have wildcard broadcasts for both
          sh = GetObject(subjectlist[l].c_str(), "queue");

          if (!sh) {
            sh = GetObject(subjectlist[l].c_str(), "hash");
          }

          if (sh) {
            success *= sh->BroadCastEnvString(reply.c_str());
          }
        }

        return success;
      }

      if (ftag == XRDMQSHAREDHASH_DELETE) {
        std::string val = (env.Get(XRDMQSHAREDHASH_KEYS) ? env.Get(
                             XRDMQSHAREDHASH_KEYS) : "");

        if (val.length() <= 1) {
          error = "no keys in message body : ";
          error += env.Env(envlen);
          return false;
        }

        std::string key;
        std::vector<int> keystart;

        for (unsigned int i = 0; i < val.length(); i++) {
          if (val.c_str()[i] == '|') {
            keystart.push_back(i);
          }
        }

        std::string sstr;

        for (unsigned int i = 0; i < keystart.size(); i++) {
          if (i < (keystart.size() - 1)) {
            sstr = val.substr(keystart[i] + 1, keystart[i + 1] - 1 - (keystart[i]));
          } else {
            sstr = val.substr(keystart[i] + 1);
          }

          key = sstr;
          // message->Print();
          // fprintf(stderr,"XrdMqSharedObjectManager::ParseEnvMessage=>Deleting"
          //         " [%s] %s\n", subject.c_str(),key.c_str());
          sh->Delete(key, false);
        }
      }
    } // end of read mutex on HashMutex

    if (ftag == XRDMQSHAREDHASH_REMOVE) {
      for (unsigned int l = 0; l < subjectlist.size(); l++) {
        if (!DeleteSharedObject(subjectlist[l].c_str(), type.c_str(), false)) {
          error = "cannot delete subject ";
          error += subjectlist[l].c_str();
          return false;
        }
      }
    }

    return true;
  }

  error = "unknown message: ";
  error += message->GetBody();
  return false;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdMqSharedObjectManager::Clear()
{
  RWMutexReadLock lock(HashMutex);

  for (auto it  = mHashSubjects.begin(); it != mHashSubjects.end(); ++it) {
    it->second->Clear();
  }

  for (auto it = mQueueSubjects.begin(); it != mQueueSubjects.end(); ++it) {
    it->second.Clear();
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool
XrdMqSharedObjectManager::CloseMuxTransaction()
{
  // Mux Transactions can only update values with the same broadcastqueue,
  // no deletions of subjects
  XrdSysMutexHelper mLock(MuxTransactionsMutex);

  if (MuxTransactions.size()) {
    XrdOucString txmessage = "";
    MakeMuxUpdateEnvHeader(txmessage);
    AddMuxTransactionEnvString(txmessage);
    XrdMqMessage message("XrdMqSharedHashMessage");
    message.SetBody(txmessage.c_str());
    message.MarkAsMonitor();
    XrdMqMessaging::gMessageClient.SendMessage(message,
        MuxTransactionBroadCastQueue.c_str(), false, false, true);
  }

  IsMuxTransaction = false;
  MuxTransactions.clear();
  return true;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdMqSharedObjectManager::MakeMuxUpdateEnvHeader(XrdOucString& out)
{
  std::string subjects = "";

  for (auto it = MuxTransactions.begin(); it != MuxTransactions.end(); ++it) {
    subjects += it->first;
    subjects += "%";
  }

  // remove trailing '%'
  if (subjects.length() > 0) {
    subjects.erase(subjects.length() - 1, 1);
  }

  out = XRDMQSHAREDHASH_UPDATE;
  out += "&";
  out += XRDMQSHAREDHASH_SUBJECT;
  out += "=";
  out += subjects.c_str();
  out += "&";
  out += XRDMQSHAREDHASH_TYPE;
  out += "=";
  out += MuxTransactionType.c_str();
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdMqSharedObjectManager::AddMuxTransactionEnvString(XrdOucString& out)
{
  // Encoding has the following format
  // "mysh.pairs=|<key1>~<value1>%<changeid1>|<key2>~<value2>%<changeid2 ...."
  out += "&";
  out += XRDMQSHAREDHASH_PAIRS;
  out += "=";
  size_t index = 0;

  for (auto it_subj = MuxTransactions.begin(); it_subj != MuxTransactions.end();
       ++it_subj) {
    XrdOucString sindex = "";
    sindex += (int) index;
    // loop over subjects
    XrdMqSharedHash* hash = GetObject(it_subj->first.c_str(),
                                      MuxTransactionType.c_str());

    if (hash) {
      RWMutexReadLock lock(*(hash->mStoreMutex));

      // loop over variables
      for (auto it = it_subj->second.begin(); it != it_subj->second.end(); ++it) {
        if ((hash->mStore.count(it->c_str()))) {
          out += "|";
          // the subject is a prefix to the key as #<subject-index>#
          out += "#";
          out += sindex.c_str();
          out += "#";
          out += it->c_str();
          out += "~";
          out += hash->mStore[it->c_str()].GetValue();
          out += "%";
          char cid[1024];
          snprintf(cid, sizeof(cid) - 1, "%llu", hash->mStore[it->c_str()].GetChangeId());
          out += cid;
        }
      }
    }

    index++;
  }
}


//-------------------------------------------------------------------------------
//
//-------------------------------------------------------------------------------
bool
XrdMqSharedObjectManager::OpenMuxTransaction(const char* type,
    const char* broadcastqueue)
{
  XrdSysMutexHelper lock(MuxTransactionsMutex);
  MuxTransactionType = type;

  if (MuxTransactionType != "hash") {
    return false;
  }

  if (!broadcastqueue) {
    if (AutoReplyQueue.length()) {
      MuxTransactionBroadCastQueue = AutoReplyQueue;
    } else {
      return false;
    }
  } else {
    MuxTransactionBroadCastQueue = broadcastqueue;
  }

  MuxTransactions.clear();
  IsMuxTransaction = true;
  return true;
}
