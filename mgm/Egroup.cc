// ----------------------------------------------------------------------
// File: Egroup.cc
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

/*----------------------------------------------------------------------------*/
#include "mgm/Egroup.hh"
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

XrdSysMutex Egroup::Mutex;
std::map < std::string, std::map < std::string, bool > > Egroup::Map;
std::map < std::string, std::map < std::string, time_t > > Egroup::LifeTime;
std::deque <std::pair<std::string, std::string > > Egroup::LdapQueue;
XrdSysCondVar Egroup::mCond;

/*----------------------------------------------------------------------------*/
Egroup::Egroup () { }

bool
Egroup::Start ()
{
  // run an asynchronous refresh thread
  eos_static_info("constructor");
  mThread = 0;
  XrdSysThread::Run(&mThread, Egroup::StaticRefresh, static_cast<void *> (this), XRDSYSTHREAD_HOLD, "Egroup refresh Thread");
  return (mThread ? true : false);
}

/*----------------------------------------------------------------------------*/
Egroup::~Egroup ()
{
  // cancel the asynchronous resfresh thread
  if (mThread)
  {
    XrdSysThread::Cancel(mThread);
    XrdSysThread::Join(mThread, 0);
  }
}

/*----------------------------------------------------------------------------*/
bool
Egroup::Member (std::string &username, std::string & egroupname)
{
  Mutex.Lock();
  time_t now = time(NULL);

  bool iscached = false;
  bool member = false;

  if (Map.count(egroupname))
  {
    if (Map[egroupname].count(username))
    {
      member = Map[egroupname][username];
      // we know that user
      if (LifeTime[egroupname][username] > now)
      {
        // that is ok, we can return member or not member from the cache
        Mutex.UnLock();
        return member;
      }
      else
      {
        // we have already an entry, we just schedule an asynchronous update
        iscached = true;
      }
    }
  }
  Mutex.UnLock();
  // run the command not in the locked section !!!

  if (!iscached)
  {
    eos_static_info("refresh=sync user=%s egroup=%s", username.c_str(), egroupname.c_str());
    // we don't have any cached value, we have to ask for it now
    std::string cmd = "ldapsearch -LLL -l 15 -h xldap -x -b 'OU=Users,Ou=Organic Units,DC=cern,DC=ch' 'sAMAccountName=";
    cmd += username;
    cmd += "' memberOf | grep CN=";
    cmd += egroupname;
    cmd += ",";
    cmd += ">& /dev/null";
    int rc = system(cmd.c_str());

    Mutex.Lock();

    if (!WEXITSTATUS(rc))
    {
      Map[egroupname][username] = true;
      LifeTime[egroupname][username] = now + EOSEGROUPCACHETIME;
      eos_static_info("msg=\"update-sync\" member=%d user=%s egroup=%s", Map[egroupname][username], username.c_str(), egroupname.c_str());
      Mutex.UnLock();
      return true;
    }
    else
    {
      Map[egroupname][username] = false;
      LifeTime[egroupname][username] = now + EOSEGROUPCACHETIME;
      eos_static_info("msg=\"update-sync\" member=%d user=%s egroup=%s", Map[egroupname][username], username.c_str(), egroupname.c_str());
      Mutex.UnLock();
      return false;
    }
  }
  else
  {
    // just ask for asynchronous refresh
    AsyncRefresh(egroupname, username);
    return member;
  }
}

/*----------------------------------------------------------------------------*/
void*
Egroup::StaticRefresh (void* arg)
{
  return reinterpret_cast<Egroup*> (arg)->Refresh();
}

/*----------------------------------------------------------------------------*/
void*
Egroup::Refresh ()
{
  eos_static_info("msg=\"async egroup fetch thread started\"");
  // infinite loop waiting to run refresh requests indicated by conditions variable
  while (1)
  { // wait for anything to do ...
    mCond.Wait();

    XrdSysThread::SetCancelOff();
    std::pair<std::string, std::string> resolve;
    {
      XrdSysMutexHelper lLock(Mutex);

      if (LdapQueue.size())
      {
        resolve = LdapQueue.front();
        LdapQueue.pop_front();
      }
    }
    if (resolve.first.length())
    {
      DoRefresh(resolve.first, resolve.second);
    }
    XrdSysThread::SetCancelOn();
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
void
Egroup::AsyncRefresh (std::string& egroupname, std::string & username)
{
  // push a egroup/username pair into the async refresh queue
  {
    XrdSysMutexHelper qMutex(Mutex);
    LdapQueue.push_back(std::make_pair(egroupname, username));
  }
  {
    // signal to async thread
    mCond.Signal();
  }
}

/*----------------------------------------------------------------------------*/
void
Egroup::DoRefresh (std::string& egroupname, std::string& username)
{
  // routine run from an asynchronous update thread

  Mutex.Lock();
  time_t now = time(NULL);

  if (Map.count(egroupname))
  {
    if (Map[egroupname].count(username))
    {
      // we know that user
      if (LifeTime[egroupname][username] > now)
      {
        // we don't update, we have already a fresh value
        Mutex.UnLock();
        return;
      }
    }
  }
  Mutex.UnLock();

  eos_static_info("refresh-async user=%s egroup=%s", username.c_str(), egroupname.c_str());

  std::string cmd = "ldapsearch -LLL -l 15 -h xldap -x -b 'OU=Users,Ou=Organic Units,DC=cern,DC=ch' 'sAMAccountName=";
  cmd += username;
  cmd += "' memberOf | grep CN=";
  cmd += egroupname;
  cmd += ",";
  cmd += ">& /dev/null";
  int rc = system(cmd.c_str());

  Mutex.Lock();

  if (!WEXITSTATUS(rc))
  {
    Map[egroupname][username] = true;
  }
  else
  {
    Map[egroupname][username] = false;
  }

  LifeTime[egroupname][username] = now + EOSEGROUPCACHETIME;

  eos_static_info("msg=\"update-async\" member=%d user=%s egroup=%s", Map[egroupname][username], username.c_str(), egroupname.c_str());

  Mutex.UnLock();
  return;
}


EOSMGMNAMESPACE_END
