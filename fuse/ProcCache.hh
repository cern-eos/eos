// ----------------------------------------------------------------------
// File: ProcCache.hh
// Author: Geoffray Adde - CERN
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

#ifndef __PROCCACHE__HH__
#define __PROCCACHE__HH__

#include <common/RWMutex.hh>
#include <fstream>
#include <map>
#include <vector>
#include <string>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <krb5.h>
#include "common/Logging.hh"

/*----------------------------------------------------------------------------*/
class ProcCache;

/*----------------------------------------------------------------------------*/
/**
 * @brief Class to read the command line of a pid through proc files
 *
 */
/*----------------------------------------------------------------------------*/
class ProcReaderCmdLine
{
  std::string pFileName;
public:
  ProcReaderCmdLine (const std::string &filename) :
      pFileName (filename)
  {
  }
  ~ProcReaderCmdLine ()
  {
  }
  int ReadContent (std::vector<std::string> &cmdLine);
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class to read the fsuid and the fsgid of a pid through proc files
 *
 */
/*----------------------------------------------------------------------------*/
class ProcReaderFsUid
{
  std::string pFileName;
public:
  ProcReaderFsUid (const std::string &filename) :
      pFileName (filename)
  {
  }
  ~ProcReaderFsUid ()
  {
  }
  int Read();
  int ReadContent (uid_t &fsUid, gid_t &fsGid);
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class to read /proc/<pid>/stat file starting time , ppid and sid
 *
 */
/*----------------------------------------------------------------------------*/
class ProcReaderPsStat
{
  std::string pFileName;
  time_t pStartTime;
  pid_t pPpid, pSid;
public:
  ProcReaderPsStat (const std::string &filename) :
      pFileName (filename), pStartTime(), pPpid(), pSid()
  {
  }
  ~ProcReaderPsStat ()
  {
  }
  int ReadContent (long long unsigned &startTime, pid_t &ppid, pid_t &sid);
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class to read the Krb5 login in a credential cache file
 *
 */
/*----------------------------------------------------------------------------*/
class ProcReaderKrb5UserName
{
  std::string pKrb5CcFile;

  static eos::common::RWMutex sMutex;
  static bool sMutexOk;
  static krb5_context sKcontext;
  static bool sKcontextOk;

public:
  ProcReaderKrb5UserName (const std::string &krb5ccfile) :
      pKrb5CcFile (krb5ccfile) //, pKcontext(), pKcontextOk(true)
  {
    eos::common::RWMutexWriteLock lock(sMutex);
    if(!sMutexOk) {
      ProcReaderKrb5UserName::sMutex.SetBlocking(true);
      sMutexOk = true;
    }
  }
  ~ProcReaderKrb5UserName ()
  {
  }
  bool ReadUserName (std::string &userName);
  time_t GetModifTime ();
  static void StaticDestroy();
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class to read the GSI identity in a GSI proxy file
 *
 */
/*----------------------------------------------------------------------------*/
class ProcReaderGsiIdentity
{
  std::string pGsiProxyFile;
  static bool sInitOk;
public:
  ProcReaderGsiIdentity (const std::string &gsiproxyfile) :
	  pGsiProxyFile (gsiproxyfile)
  {
  }
  ~ProcReaderGsiIdentity ()
  {
  }
  bool ReadIdentity (std::string &sidentity);
  time_t GetModifTime ();
  static void StaticDestroy();
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class representing a Proc File information cache entry for one pid.
 *
 */
/*----------------------------------------------------------------------------*/
class ProcCacheEntry
{
  friend class ProcCache;
  // RWMutex to protect entry
  mutable eos::common::RWMutex pMutex;

  // internal values
  pid_t pPid;
  pid_t pPPid;
  pid_t pSid;
  uid_t pFsUid;
  gid_t pFsGid;
  unsigned long long pStartTime;
  std::string pProcPrefix;
  std::string pCmdLineStr;
  std::vector<std::string> pCmdLineVect;
  std::string pAuthMethod;
  mutable int pError;
  mutable std::string pErrMessage;

  //! return true fs success, false if failure
  int
  ReadContentFromFiles (ProcCache *procCache);
  //! return true if the information is up-to-date after the call, false else
  int
  UpdateIfPsChanged (ProcCache *procCache);

public:
  ProcCacheEntry (unsigned int pid) :
      pPid (pid), pPPid(), pSid(), pFsUid(-1), pFsGid(-1), pStartTime (0), pError (0)
  {
    std::stringstream ss;
    ss << "/proc/" << pPid;
    pProcPrefix = ss.str ();
    pMutex.SetBlocking(true);
  }

  ~ProcCacheEntry ()
  {
  }

  //
  bool GetAuthMethod (std::string &value) const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    if (pAuthMethod.empty () || pAuthMethod=="none") return false;
    value = pAuthMethod;
    return true;
  }

  bool SetAuthMethod (const std::string &value)
  {
    eos::common::RWMutexReadLock lock (pMutex);
    pAuthMethod = value;
    return true;
  }

  bool GetFsUidGid (uid_t &uid, gid_t &gid) const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    uid = pFsUid;
    gid = pFsGid;
    return true;
  }

  bool GetSid (pid_t &sid) const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    sid = pSid;
    return true;
  }

  bool GetStartupTime (time_t &sut) const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    sut = pStartTime/sysconf(_SC_CLK_TCK);
    return true;
  }

  const std::vector<std::string>&
  GetArgsVec () const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    return pCmdLineVect;
  }

  const std::string&
  GetArgsStr () const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    return pCmdLineStr;
  }

  bool HasError () const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    return pError;
  }

  const std::string &
  GetErrorMessage () const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    return pErrMessage;
  }

  time_t
  GetProcessStartTime () const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    return pStartTime;
  }

};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class representing a Proc File information cache catalog.
 *
 */
/*----------------------------------------------------------------------------*/
class ProcCache
{
  // RWMUtex; Mutex to protect catalog
  std::map<int, ProcCacheEntry*> pCatalog;
  // RWMutex to protect entry
  eos::common::RWMutex pMutex;

public:
  ProcCache ()
  {
    pMutex.SetBlocking(true);
  }
  ~ProcCache ()
  {
    eos::common::RWMutexWriteLock lock(pMutex);
    for(auto it=pCatalog.begin(); it!=pCatalog.end(); it++)
      delete it->second;
  }

  //! returns true if the cache has an entry for the given pid, false else
  //! regardless of the fact it's up-to-date or not
  bool HasEntry (int pid)
  {
    eos::common::RWMutexReadLock lock (pMutex);
    return static_cast<bool> (pCatalog.count (pid));
  }

  //! returns true if the cache has an up-to-date entry after the call
  int
  InsertEntry (int pid)
  {
    int errCode;

    if (!HasEntry (pid))
    {
      //eos_static_debug("There and pid is %d",pid);
      eos::common::RWMutexWriteLock lock (pMutex);
      pCatalog[pid] = new ProcCacheEntry (pid);
    }
    auto entry = GetEntry (pid);
    if ((errCode = entry->UpdateIfPsChanged (this)))
    {
      //eos_static_debug("something wrong happened in reading proc stuff %d : %s",pid,pCatalog[pid]->pErrMessage.c_str());
      eos::common::RWMutexWriteLock lock (pMutex);
      delete pCatalog[pid];
      pCatalog.erase (pid);
      return errCode;
    }

    return 0;
  }

  //! returns true if the entry is removed after the call
  bool RemoveEntry (int pid)
  {
    if (!HasEntry (pid))
      return true;
    else
    {
      eos::common::RWMutexWriteLock lock (pMutex);
      delete pCatalog[pid];
      pCatalog.erase (pid);
      return true;
    }
  }

  //! get the entry associated to the pid if it exists
  //! gets NULL if the the cache does not have such an entry
  ProcCacheEntry* GetEntry (int pid)
  {
    eos::common::RWMutexReadLock lock (pMutex);
    auto entry = pCatalog.find (pid);
    if (entry == pCatalog.end ())
      return NULL;
    else
      return entry->second;
  }
};

#endif
