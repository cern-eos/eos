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
  bool ReadContent (std::vector<std::string> &cmdLine);
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
  bool ReadContent (uid_t &fsUid, gid_t &fsGid);
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class to read the startup environment of a pid through proc files
 *
 */
/*----------------------------------------------------------------------------*/
class ProcReaderEnv
{
  std::string pFileName;
public:
  ProcReaderEnv (const std::string &filename) :
      pFileName (filename)
  {
  }
  ~ProcReaderEnv ()
  {
  }
  bool ReadContent (std::map<std::string, std::string> &dict);
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
  static krb5_context sKcontext;
  static bool sKcontextOk;
public:
  ProcReaderKrb5UserName (const std::string &krb5ccfile) :
      pKrb5CcFile (krb5ccfile)
  {
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
  uid_t pFsUid;
  gid_t pFsGid;
  time_t pStartTime;
  time_t pKrb5CcModTime;
  time_t pGsiProxyModTime;
  std::string pProcPrefix;
  std::map<std::string, std::string> pEnv;
  std::string pCmdLineStr;
  std::vector<std::string> pCmdLineVect;
  std::string pKrb5UserName;
  std::string pGsiIdentity;
  std::string pAuthMethod;
  mutable int pError;
  mutable std::string pErrMessage;

  //! return >0 if success 0 if failure
  time_t
  ReadStartingTime () const;
  //! return true fs success, false if failure
  bool
  ReadContentFromFiles ();
  //! return true if the information is up-to-date after the call, false else
  int
  UpdateIfPsChanged ();
  //! return true if the kerberos information is up-to-date after the call, false else
  int
  UpdateIfKrb5Changed ();
  bool ResolveKrb5CcFile();

  //! return true if the GSI information is up-to-date after the call, false else
  int
  UpdateIfGsiChanged ();
  bool ResolveGsiProxyFile();

public:

  ProcCacheEntry (unsigned int pid) :
      pPid (pid), pFsUid(-1), pFsGid(-1), pStartTime (0), pKrb5CcModTime (0), pGsiProxyModTime(0), pError (0)
  {
    std::stringstream ss;
    ss << "/proc/" << pPid;
    pProcPrefix = ss.str ();
  }

  ~ProcCacheEntry ()
  {
  }

  // return NULL if the env variable is not defined, a ptr to the value string if it is defined
  bool GetEnv (std::map<std::string, std::string> &dict) const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    dict = pEnv;
    return true;
  }

  // return NULL if the env variable is not defined, a ptr to the value string if it is defined
  bool GetEnv (const std::string &varName, std::string &value) const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    if (!pEnv.count (varName)) return false;
    value = pEnv.at (varName);
    return true;
  }

  //
  bool GetKrb5UserName (std::string &value) const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    if (pKrb5UserName.empty ()) return false;
    value = pKrb5UserName;
    return true;
  }

  //
  bool GetGsiIdentity (std::string &value) const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    if (pGsiIdentity.empty ()) return false;
    value = pGsiIdentity;
    return true;
  }

  //
  bool GetAuthMethod (std::string &value) const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    if (pAuthMethod.empty () || pAuthMethod=="none") return false;
    value = pAuthMethod;
    return true;
  }

  // return NULL if the env variable is not defined, a ptr to the value string if it is defined
  bool GetFsUidGid (uid_t &uid, gid_t &gid) const
  {
    eos::common::RWMutexReadLock lock (pMutex);
    uid = pFsUid;
    gid = pFsGid;
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
  InsertEntry (int pid, bool useKrb5, bool useGsi, bool tryKrb5First = false)
  {
    int errCode;

    if (!HasEntry (pid))
    {
      //eos_static_debug("There and pid is %d",pid);
      eos::common::RWMutexWriteLock lock (pMutex);
      pCatalog[pid] = new ProcCacheEntry (pid);
    }
    auto entry = GetEntry (pid);
    if ((errCode = entry->UpdateIfPsChanged ()))
    {
      //eos_static_debug("something wrong happened in reading proc stuff %d : %s",pid,pCatalog[pid]->pErrMessage.c_str());
      eos::common::RWMutexWriteLock lock (pMutex);
      delete pCatalog[pid];
      pCatalog.erase (pid);
      return errCode;
    }

    // if we don't use any strong authentication, we're done
    if (!useKrb5 && !useGsi) return 0;

    // resolve the strong authentication file to use
    // this is done only once when inserting the process in the cache (not when refreshing)
    // since it all depends on environment variables and the environment read from proc is static
    if (entry->pAuthMethod.empty ())
    {
      // we use at least one strong authentication method
      std::string userAuth (entry->pEnv.count ("EOS_FUSE_AUTH") ? entry->pEnv["EOS_FUSE_AUTH"] : "");
      bool userWantsKrb5 = (!userAuth.empty ()) && !userAuth.compare (0, 5, "krb5:");
      auto tryFirst = useGsi ? &ProcCacheEntry::ResolveGsiProxyFile : NULL;
      auto trySecond = useKrb5 ? &ProcCacheEntry::ResolveKrb5CcFile : NULL;
      if (tryKrb5First || userWantsKrb5) std::swap (tryFirst, trySecond);

      if (tryFirst && (entry->*tryFirst) ())
      {
	// sucessfully parsed on first try
      }
      else if (trySecond && (entry->*trySecond) ())
      {
	// sucessfully parsed on second try
      }
      else // using strong authentication and failed
      {
	entry->pAuthMethod = "none";
	eos_static_debug("strong authentication enabled but could not resolve any identity");
	// no identity could be parsed;
      }
    }

    if(entry->pAuthMethod.compare(0,5,"krb5:")==0)
    {
      entry->UpdateIfKrb5Changed();
    }
    else if (entry->pAuthMethod.compare(0,4,"gsi:")==0)
    {
      entry->UpdateIfGsiChanged();
    }
    else
    {
      return EACCES;
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
