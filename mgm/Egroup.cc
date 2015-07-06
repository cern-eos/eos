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

Egroup::Egroup ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Constructor
 */
/*----------------------------------------------------------------------------*/ { }

bool
/*----------------------------------------------------------------------------*/
Egroup::Start ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Asynchronous thread start function
 */
/*----------------------------------------------------------------------------*/
{
  // run an asynchronous refresh thread
  eos_static_info("Start");
  mThread = 0;
  XrdSysThread::Run(&mThread, Egroup::StaticRefresh,
                    static_cast<void *> (this),
                    XRDSYSTHREAD_HOLD,
                    "Egroup refresh Thread");
  return (mThread ? true : false);
}

void
/*----------------------------------------------------------------------------*/
Egroup::Stop ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Asynchronous thread stop function
 * 
 */
/*----------------------------------------------------------------------------*/
{
  // cancel the asynchronous resfresh thread
  if (mThread)
  {
    XrdSysThread::Cancel(mThread);
    XrdSysThread::Join(mThread, 0);
  }
}

/*----------------------------------------------------------------------------*/
Egroup::~Egroup ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 * 
 * We are cancelling and joining the asynchronous prefetch thread here.
 */
/*----------------------------------------------------------------------------*/
{
  // cancel the asynchronous resfresh thread
  Stop();
}

/*----------------------------------------------------------------------------*/
bool
Egroup::Member (std::string &username, std::string & egroupname)
/*----------------------------------------------------------------------------*/
/**
 * @brief Member
 * @param username name of the user to check Egroup membership
 * @param egroupname name of Egroup where to look for membership
 * 
 * @return true if member otherwise false
 */
/*----------------------------------------------------------------------------*/
{
  Mutex.Lock();
  time_t now = time(NULL);

  bool iscached = false;
  bool member = false;
  bool isMember = false;

  if (Map.count(egroupname))
  {
    if (Map[egroupname].count(username))
    {
      member = Map[egroupname][username];
      time_t age = labs( LifeTime[egroupname][username] - now );

      // we know that user, it has a cached entry which is not too old
      if (LifeTime[egroupname].count(username) && (LifeTime[egroupname][username] > now) && (age < EOSEGROUPCACHETIME) )
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
    eos_static_info("msg=\"lookup\" user=\"%s\" e-group=\"%s\"", username.c_str(), egroupname.c_str());
    // run the LDAP query
    LDAP *ld = NULL;
    int version = LDAP_VERSION3;
    // currently hard coded to server name 'lxadp'
    ldap_initialize(&ld, "ldap://xldap");
    if (ld == NULL)
    {
      fprintf(stderr, "error: failed to initialize LDAP\n");
    }
    else
    {
      (void) ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &version);
      // the LDAP base
      std::string sbase = "CN=";
      sbase += username;
      sbase += ",OU=Users,Ou=Organic Units,DC=cern,DC=ch";
      // the LDAP attribute (recursive search)      
      std::string attr="cn";
      // the LDAP filter
      std::string filter;
      filter = "(memberOf:1.2.840.113556.1.4.1941:=CN=";
      filter += egroupname; 
      filter += ",OU=e-groups,OU=Workgroups,DC=cern,DC=ch)";

      char* attrs[2];
      attrs[0] = (char*) attr.c_str();
      attrs[1] = NULL;
      LDAPMessage *res = NULL;
      struct timeval timeout;
      timeout.tv_sec = 10;
      timeout.tv_usec = 0;

      std::string match = username;

      eos_static_debug("base=%s attr=%s filter=%s match=%s\n",sbase.c_str(), attr.c_str(), filter.c_str(), match.c_str());
      int rc = ldap_search_ext_s(ld, sbase.c_str(), LDAP_SCOPE_SUBTREE,
                                 filter.c_str(), 
                                 attrs, 0, NULL, NULL, 
                                 &timeout, LDAP_NO_LIMIT, &res);



      if ((rc == LDAP_SUCCESS) && (ldap_count_entries(ld, res) != 0))
      {
        LDAPMessage* e = NULL;

        for (e = ldap_first_entry(ld, res); e != NULL; e = ldap_next_entry(ld, e))
        {
          struct berval **v = ldap_get_values_len(ld, e, attr.c_str());

          if (v != NULL)
          {
            int n = ldap_count_values_len(v);
            int j;

            for (j = 0; j < n; j++)
            {
              std::string result = v[ j ]->bv_val;
	      eos_static_info("result=%d %s\n", n, result.c_str());
              if ((result.find(match)) != std::string::npos)
              {
                isMember = true;
              }
            }
            ldap_value_free_len(v);
          }
        }
      }
      else
      {
	if (rc != LDAP_SUCCESS) 
        {
	  eos_static_warning("member=false user=\"%s\" e-group=\"%s\" "
			     "cachetime=<stall-information> "
			     "msg=\"ldap query failed or timed out\"",
			     username.c_str(),
			     egroupname.c_str());
	}
      }

      ldap_msgfree(res);

      if (ld != NULL)
      {
        ldap_unbind_ext(ld, NULL, NULL);
      }
    }

    if (isMember)
      eos_static_info("member=true user=\"%s\" e-group=\"%s\" cachetime=%lu", 
                      username.c_str(), egroupname.c_str(), 
                      now + EOSEGROUPCACHETIME);
    else
      eos_static_info("member=false user=\"%s\" e-group=\"%s\" cachetime=%lu", 
                      username.c_str(), egroupname.c_str(), 
                      now + EOSEGROUPCACHETIME);


    Mutex.Lock();
    Map[egroupname][username] = isMember;
    LifeTime[egroupname][username] = now + EOSEGROUPCACHETIME;
    Mutex.UnLock();
    return isMember;
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
/*----------------------------------------------------------------------------*/
/**
 * @brief Thread startup function
 * @param arg Egroup object
 * @return Should never return until cancelled.
 */
/*----------------------------------------------------------------------------*/
{
  return reinterpret_cast<Egroup*> (arg)->Refresh();
}

/*----------------------------------------------------------------------------*/
void*
/*----------------------------------------------------------------------------*/
/**
 * @brief Asynchronous refresh loop
 * 
 * The looping thread takes Egroup requests and run's LDAP queries pushing 
 * results into the Egroup membership map and updates the lifetime of the 
 * resolved entry.
 * 
 * @return Should never return until cancelled.
 */
/*----------------------------------------------------------------------------*/
Egroup::Refresh ()
{
  eos_static_info("msg=\"async egroup fetch thread started\"");
  // infinite loop waiting to run refresh requests indicated by conditions 
  // variable
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

void
Egroup::AsyncRefresh (std::string& egroupname, std::string & username)
/*----------------------------------------------------------------------------*/
/**
 * @brief Pushes an Egroup/user resolution request into the asynchronous queue
 */
/*----------------------------------------------------------------------------*/
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
/*----------------------------------------------------------------------------*/
/**
 * @brief Run a synchronous LDAP query for Egroup/username and update the Map
 * 
 * The asynchronous thread uses this function to update the Egroup Map.
 */
/*----------------------------------------------------------------------------*/
{
  Mutex.Lock();
  time_t now = time(NULL);
  bool isMember = false;

  if (Map.count(egroupname))
  {
    if (Map[egroupname].count(username))
    {
      // we know that user
      if (LifeTime[egroupname].count(username) && (LifeTime[egroupname][username] > now))
      {
        // we don't update, we have already a fresh value
        Mutex.UnLock();
        return;
      }
    }
  }
  Mutex.UnLock();

  eos_static_info("msg=\"async-lookup\" user=\"%s\" e-group=\"%s\"", 
                  username.c_str(), egroupname.c_str());
  // run the LDAP query
  LDAP *ld = NULL;
  int version = LDAP_VERSION3;
  // currently hard coded to server name 'xldap'
  ldap_initialize(&ld, "ldap://xldap");

  bool keepCached = true;
  if (ld == NULL)
  {
    fprintf(stderr, "error: failed to initialize LDAP\n");
  }
  else
  {
    (void) ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &version);
    // the LDAP base
    std::string sbase = "CN=";
    sbase += username;
    sbase += ",OU=Users,Ou=Organic Units,DC=cern,DC=ch";
    // the LDAP attribute (recursive search)      
    std::string attr="cn";
    // the LDAP filter
    std::string filter;
    filter = "(memberOf:1.2.840.113556.1.4.1941:=CN=";
    filter += egroupname; 
    filter += ",OU=e-groups,OU=Workgroups,DC=cern,DC=ch)";
    
    char* attrs[2];
    attrs[0] = (char*) attr.c_str();
    attrs[1] = NULL;
    LDAPMessage *res = NULL;
    struct timeval timeout;
    timeout.tv_sec = 10;
    timeout.tv_usec = 0;
    
    std::string match = username;
    
    eos_static_debug("base=%s attr=%s filter=%s match=%s\n",sbase.c_str(), attr.c_str(), filter.c_str(), match.c_str());
    int rc = ldap_search_ext_s(ld, sbase.c_str(), LDAP_SCOPE_SUBTREE,
                               filter.c_str(), attrs, 0, NULL, NULL, 
                               &timeout, LDAP_NO_LIMIT, &res);
    if ((rc == LDAP_SUCCESS) && (ldap_count_entries(ld, res) != 0))
    {
      LDAPMessage* e = NULL;
      keepCached = false;
      for (e = ldap_first_entry(ld, res); e != NULL; e = ldap_next_entry(ld, e))
      {
        struct berval **v = ldap_get_values_len(ld, e, attr.c_str());

        if (v != NULL)
        {
          int n = ldap_count_values_len(v);
          int j;

          for (j = 0; j < n; j++)
          {
            std::string result = v[ j ]->bv_val;
            if ((result.find(match)) != std::string::npos)
            {
              isMember = true;
            }
          }
          ldap_value_free_len(v);
        }
      }
    }

    ldap_msgfree(res);

    if (ld != NULL)
    {
      ldap_unbind_ext(ld, NULL, NULL);
    }
  }

  if (!keepCached)
  {
    if (isMember)
      eos_static_info("member=true user=\"%s\" e-group=\"%s\" cachetime=%lu", 
                      username.c_str(), egroupname.c_str(), 
                      now + EOSEGROUPCACHETIME);
    else
      eos_static_info("member=false user=\"%s\" e-group=\"%s\" cachetime=%lu", 
                      username.c_str(), egroupname.c_str(), 
                      now + EOSEGROUPCACHETIME);

    Mutex.Lock();

    Map[egroupname][username] = isMember;
    LifeTime[egroupname][username] = now + EOSEGROUPCACHETIME;

    Mutex.UnLock();
  }
  else
  {
    Mutex.Lock();
    if (Map.count(egroupname) && Map[egroupname].count(username))
      isMember = Map[egroupname][username];
    else 
      isMember = false;
    Mutex.UnLock();

    if (isMember)
    {
      eos_static_warning("member=true user=\"%s\" e-group=\"%s\" "
                         "cachetime=<stale-information",
                         username.c_str(),
                         egroupname.c_str());
    }
    else
    {
      eos_static_warning("member=false user=\"%s\" e-group=\"%s\" "
                         "cachetime=<stale-information",
                         username.c_str(),
                         egroupname.c_str());
    }
  }
  return;
}

/*----------------------------------------------------------------------------*/
std::string
Egroup::DumpMember (std::string &username, std::string & egroupname)
/*----------------------------------------------------------------------------*/
/**
 * @brief DumpMember
 * @param username name of the user to dump Egroup membership
 * @param egroupname name of Egroup where to look for membership
 * 
 * @return egroup dump for username
 */
/*----------------------------------------------------------------------------*/
{
  // trigger refresh
  Member(username,egroupname);

  XrdSysMutexHelper lLock(Mutex);

  bool member = false;
  time_t timetolive = 0;
  time_t now = time(NULL);
  if (Map.count(egroupname))
  {
    if (Map[egroupname].count(username))
    {
      member = Map[egroupname][username];
      timetolive = labs( LifeTime[egroupname][username] - now );
    }
  }
  std::string rs;
  rs += "egroup=";
  rs += egroupname;
  rs += " user=";
  rs += username;
  if (member)
    rs += " member=true ";
  else
    rs += " member=false";
  rs += " lifetime=";
  rs += std::to_string((long long)timetolive);
  return rs;
}


/*----------------------------------------------------------------------------*/
std::string
Egroup::DumpMembers ()
/*----------------------------------------------------------------------------*/
/**
 * @brief DumpMember
 *
 * @return egroup dump for all users
 */
/*----------------------------------------------------------------------------*/
{
  XrdSysMutexHelper lLock(Mutex);

  time_t timetolive = 0;
  time_t now = time(NULL);
  std::string rs;
  std::map < std::string, std::map <std::string, bool > >::iterator it;
  for (it = Map.begin(); it != Map.end(); ++it) 
  {
    std::map <std::string, bool>::iterator uit;
    for (uit = it->second.begin(); uit != it->second.end(); ++uit)
    {
      rs += "egroup=";
      rs += it->first;
      rs += " user=";
      rs += uit->first;
      if (uit->second) 
	rs += " member=true";
      else
	rs += " member=false";
      timetolive = labs( LifeTime[it->first][uit->first] - now );
      rs += " lifetime=";
      rs += std::to_string((long long)timetolive);
      rs += "\n";
    }
  }
  return rs;
}

EOSMGMNAMESPACE_END
