//------------------------------------------------------------------------------
// File: XrdMqOfs.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#ifndef __XFTSOFS_NS_H__
#define __XFTSOFS_NS_H__

#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysSemWait.hh"
#include "common/Logging.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <memory.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <map>
#include <string>
#include <vector>
#include <deque>
#include <atomic>

// if we have too many messages pending we don't take new ones for the moment
#define MQOFSMAXMESSAGEBACKLOG 100000
#define MQOFSMAXQUEUEBACKLOG 50000
#define MQOFSREJECTQUEUEBACKLOG 100000

#define MAYREDIRECT {                                         \
    int port=0;                                               \
    XrdOucString host="";                                     \
    if (gMqFS->ShouldRedirect(host,port)) {                   \
      return gMqFS->Redirect(error,host,port);                \
    }                                                         \
  }

//! Forward declarations
class XrdSecEntity;
class XrdSysError;
class XrdSysLogger;

namespace qclient
{
class QClient;
}

//------------------------------------------------------------------------------
//! Class XrdSmartOucEnv
//------------------------------------------------------------------------------
class XrdSmartOucEnv : public XrdOucEnv
{
public:
  XrdSmartOucEnv(const char* vardata = 0, int vardlen = 0) :
    XrdOucEnv(vardata, vardlen), nref(0)
  {}

  virtual ~XrdSmartOucEnv() {}

  int Refs()
  {
    return nref;
  }

  void DecRefs()
  {
    --nref;
  }

  void AddRefs(int nrefs)
  {
    nref += nrefs;
  }

  XrdSysMutex procmutex;

private:
  std::atomic<int> nref;
};

//------------------------------------------------------------------------------
//! Class XrdMqOfsMatches
//------------------------------------------------------------------------------
class XrdMqOfsMatches
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqOfsMatches(const char* qname, XrdSmartOucEnv* msg, const char* t,
                  int type, const char* sender = "ignore"):
    matches(0), messagetype(type), backlog(false), backlogrejected(false),
    backlogqueues(""), sendername(sender), queuename(qname), message(msg),
    mTident(t)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~XrdMqOfsMatches() = default;

  int matches;
  int messagetype;
  bool backlog;
  bool backlogrejected;
  XrdOucString backlogqueues;
  XrdOucString sendername;
  XrdOucString queuename;
  XrdSmartOucEnv* message;
  const char* mTident;
};

//------------------------------------------------------------------------------
//! Class XrdMqMessageOut
//------------------------------------------------------------------------------
class XrdMqMessageOut
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqMessageOut(const char* queuename):
    AdvisoryStatus(false), AdvisoryQuery(false), AdvisoryFlushBackLog(false),
    BrokenByFlush(false), QueueName(queuename), mMsgBuffer("")
  {
    mMsgQueue.clear();
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdMqMessageOut()
  {
    RetrieveMessages();
  }

  //----------------------------------------------------------------------------
  //! Lock current object
  //----------------------------------------------------------------------------
  void Lock() const
  {
    mMutex.Lock();
  }

  //----------------------------------------------------------------------------
  //! Unlock current object
  //----------------------------------------------------------------------------
  void UnLock() const
  {
    mMutex.UnLock();
  }

  //----------------------------------------------------------------------------
  //! Collect all messages from the queue and append them to the internal
  //! buffer. Also delete messages if this was the last reference towards them.
  //!
  //! @return size of the internal buffer
  //----------------------------------------------------------------------------
  size_t RetrieveMessages();

  bool AdvisoryStatus;
  bool AdvisoryQuery;
  bool AdvisoryFlushBackLog;
  bool BrokenByFlush;
  XrdOucString QueueName;
  std::string mMsgBuffer;
  XrdSysSemWait DeletionSem;
  std::deque<XrdSmartOucEnv*> mMsgQueue;

private:
  mutable XrdSysMutex mMutex; ///< Mutex protecting access to the msg queue
};

//------------------------------------------------------------------------------
//! Class XrdMqOfsFile
//------------------------------------------------------------------------------
class XrdMqOfsFile : public XrdSfsFile, public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqOfsFile(char* user = 0):
    XrdSfsFile(user), eos::common::LogId(),
    mMsgOut(nullptr), mQueueName(), mIsOpen(false), tident("")
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~XrdMqOfsFile()
  {
    close();
  }

  int open(const char* fileName, XrdSfsFileOpenMode openMode, mode_t createMode,
           const XrdSecEntity* client = 0, const char* opaque = 0);

  int close();

  XrdSfsXferSize read(XrdSfsFileOffset fileOffset, char* buffer,
                      XrdSfsXferSize buffer_size);

  int stat(struct stat* buf);

  virtual int fctl(int, const char*, XrdOucErrInfo&)
  {
    return SFS_ERROR;
  }

  virtual const char* FName()
  {
    return "queue";
  }

  virtual int getMmap(void**, off_t&)
  {
    return SFS_ERROR;
  }

  virtual int read(XrdSfsFileOffset, XrdSfsXferSize)
  {
    return SFS_ERROR;
  }

  virtual int read(XrdSfsAio*)
  {
    return SFS_ERROR;
  }

  virtual XrdSfsXferSize write(XrdSfsFileOffset, const char*, XrdSfsXferSize)
  {
    return SFS_OK;
  }

  virtual int write(XrdSfsAio*)
  {
    return SFS_OK;
  }

  virtual int sync()
  {
    return SFS_OK;
  }

  virtual int sync(XrdSfsAio*)
  {
    return SFS_OK;
  }

  virtual int truncate(XrdSfsFileOffset)
  {
    return SFS_OK;
  }

  virtual int getCXinfo(char*, int&)
  {
    return SFS_ERROR;
  }

private:
  XrdMqMessageOut* mMsgOut;
  std::string mQueueName;
  bool mIsOpen;
  const char* tident;
};

//------------------------------------------------------------------------------
//! Class XrdMqOfs
//------------------------------------------------------------------------------
class XrdMqOfs : public XrdSfsFileSystem, public eos::common::LogId
{
  friend class XrdMqOfsFile;

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqOfs(XrdSysError* lp = 0);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdMqOfs()
  {
    if (HostName) {
      free(HostName);
    }

    if (HostPref) {
      free(HostPref);
    }
  }

  virtual bool Init(XrdSysError&)
  {
    return true;
  }

  const char* getVersion();

  int Stall(XrdOucErrInfo& error, int stime, const char* msg);

  int Redirect(XrdOucErrInfo&   error, XrdOucString& host, int& port);

  //----------------------------------------------------------------------------
  //! Decide if client should be redirected to a different host based on the
  //! current master-slave status
  //!
  //! @param host redirection host
  //! @param port redirection port
  //!
  //! @return true if client should be redirected, otherwise false
  //----------------------------------------------------------------------------
  bool ShouldRedirect(XrdOucString& host, int& port);

  int Configure(XrdSysError& Eroute);

  bool ResolveName(const char* inname, XrdOucString& outname);

  XrdSfsFile* newFile(char* user = 0, int MonID = 0)
  {
    return (XrdSfsFile*) new XrdMqOfsFile(user);
  }

  XrdSfsDirectory* newDir(char* user = 0, int MonID = 0)
  {
    return (XrdSfsDirectory*) 0;
  }

  //----------------------------------------------------------------------------
  //! Deliver a message into matching output queues
  //----------------------------------------------------------------------------
  bool Deliver(XrdMqOfsMatches& Match);

  int stat(const char* Name, struct stat* buf, XrdOucErrInfo& error,
           const XrdSecEntity* client = 0, const char* opaque = 0);

  int stat(const char* Name, mode_t& mode, XrdOucErrInfo& error,
           const XrdSecEntity* client = 0, const char* opaque = 0);

  //----------------------------------------------------------------------------
  //! Plugin function
  //----------------------------------------------------------------------------
  int FSctl(int, XrdSfsFSctl&, XrdOucErrInfo&, const XrdSecEntity*);

  //----------------------------------------------------------------------------
  //! Build error response
  //----------------------------------------------------------------------------
  int Emsg(const char*, XrdOucErrInfo&, int, const char* x,
           const char* y = "");

  XrdSysMutex StoreMutex; ///< Mutex protecting the string store hash
  int myPort; ///< Port on which the MQ is running
  char* HostName; ///< Our hostname as derived in XrdOfs
  char* HostPref; ///< Our hostname as derived in XrdOfs without domain
  XrdOucString ManagerId; ///< Manager id in <host>:<port> format
  XrdOucString QueuePrefix; ///< Prefix of the accepted queues to server
  XrdOucString QueueAdvisory; ///< "<queueprefix>/*" for advisory message matches
  XrdOucString BrokerId; ///< Manger id + queue name as path
  std::map<std::string, XrdSmartOucEnv*> Messages; ///< Hash with all messages
  XrdSysMutex mMsgsMutex;  ///< Mutex protecting the message hash

  XrdSysMutex  StatLock;
  time_t       StartupTime;
  time_t       LastOutputTime;
  long long    ReceivedMessages;
  std::atomic<uint64_t> mDeliveredMessages;
  std::atomic<uint64_t> mFanOutMessages;
  long long    AdvisoryMessages;
  long long    UndeliverableMessages;
  long long    DiscardedMonitoringMessages;
  long long    NoMessages;
  long long    BacklogDeferred;
  long long    QueueBacklogHits;
  long long    MaxMessageBacklog;
  uint64_t     mMaxQueueBacklog;
  uint64_t     mRejectQueueBacklog;
  void         Statistics();
  XrdOucString StatisticsFile;
  char*         ConfigFN;

private:
  static XrdSysError* eDest;
  static std::string sLeaseKey;
  //! Hash of all output's connected
  std::map<std::string, XrdMqMessageOut*> mQueueOut;
  XrdSysMutex mQueueOutMutex;  ///< Mutex protecting the output hash
  std::string mQdbCluster; ///< Quarkdb cluster info host1:port1 host2:port2 ..
  std::string mQdbPassword; ///< Quarkdb cluster password
  eos::QdbContactDetails mQdbContactDetails; ///< QuarkDB contact details
  std::unique_ptr<qclient::QClient>
  mQcl; ///< qclient for talking to the QDB cluster
  std::string mMasterId; ///< Current master id in <fqdn>:<port> format
  std::string mMgmId; ///< MGM id <host>:1094 format

  //----------------------------------------------------------------------------
  //! Decide if client should be redirected to a different host based on the
  //! current master-slave status. Used for the in-memory namespace.
  //!
  //! @param host redirection host
  //! @param port redirection port
  //!
  //! @return true if client should be redirected, otherwise false
  //----------------------------------------------------------------------------
  bool ShouldRedirectInMem(XrdOucString& host, int& port);

  //----------------------------------------------------------------------------
  //! Decide if client should be redirected to a different host based on the
  //! current master-slave status. Used for the QuarkDB namespace.
  //!
  //! @param host redirection host
  //! @param port redirection port
  //!
  //! @return true if client should be redirected, otherwise false
  //----------------------------------------------------------------------------
  bool ShouldRedirectQdb(XrdOucString& host, int& port);

  //----------------------------------------------------------------------------
  //! Get the identity of the current lease holder
  //!
  //! @return identity (fqdn:port) string or empty string if none holds the
  //!         lease
  //----------------------------------------------------------------------------
  std::string GetLeaseHolder();

  int getStats(char* buff, int blen)
  {
    return 0;
  }

  virtual int chmod(const char*, XrdSfsMode, XrdOucErrInfo&, const XrdSecEntity*,
                    const char*)
  {
    return 0;
  }

  virtual int fsctl(int, const char*, XrdOucErrInfo&, const XrdSecEntity*)
  {
    return 0;
  }

  virtual int exists(const char*, XrdSfsFileExistence&, XrdOucErrInfo&,
                     const XrdSecEntity*, const char*)
  {
    return 0;
  }

  virtual int mkdir(const char*, XrdSfsMode, XrdOucErrInfo&, const XrdSecEntity*,
                    const char*)
  {
    return 0;
  }

  virtual int prepare(XrdSfsPrep&, XrdOucErrInfo&, const XrdSecEntity*)
  {
    return 0;
  }

  virtual int rem(const char*, XrdOucErrInfo&, const XrdSecEntity*, const char*)
  {
    return 0;
  }

  virtual int remdir(const char*, XrdOucErrInfo&, const XrdSecEntity*,
                     const char*)
  {
    return 0;
  }

  virtual int rename(const char*, const char*, XrdOucErrInfo&,
                     const XrdSecEntity*, const char*, const char*)
  {
    return 0;
  }

  virtual int truncate(const char*, XrdSfsFileOffset, XrdOucErrInfo&,
                       const XrdSecEntity*, const char*)
  {
    return 0;
  }
};

#endif
