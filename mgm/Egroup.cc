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

#include "mgm/Egroup.hh"
#include "common/Logging.hh"
#include "common/DBG.hh"
#include <ldap.h>
#include <memory>

//------------------------------------------------------------------------------
// Delete the LDAP object
//------------------------------------------------------------------------------
static void ldap_uninitialize(LDAP *ld) {
  if(ld != nullptr) {
    ldap_unbind_ext(ld, NULL, NULL);
  }
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor - launch asynchronous refresh thread
//------------------------------------------------------------------------------
Egroup::Egroup()
{
  PendingQueue.setBlockingMode(true);
  mThread.reset(&Egroup::Refresh, this);
}

//------------------------------------------------------------------------------
// Destructor - join asynchronous refresh thread
//------------------------------------------------------------------------------
Egroup::~Egroup()
{
  PendingQueue.setBlockingMode(false);
  mThread.join();
}

//------------------------------------------------------------------------------
// Main LDAP lookup function - bypasses the cache, hits the LDAP server.
//------------------------------------------------------------------------------
Egroup::Status Egroup::isMemberUncached(const std::string &username,
  const std::string &egroupname) {

  // run the LDAP query
  LDAP* ld = nullptr;

  //----------------------------------------------------------------------------
  // Initialize the LDAP context.
  //----------------------------------------------------------------------------
  ldap_initialize(&ld, "ldap://xldap");
  std::unique_ptr<LDAP, decltype(ldap_uninitialize)*> ldOwnership(
    ld, ldap_uninitialize);

  if(ld == nullptr) {
    eos_static_crit("Could not initialize ldap context");
    return Status::kError;
  }

  int version = LDAP_VERSION3;
  if(ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &version) !=
     LDAP_OPT_SUCCESS) {
    eos_static_crit("Failure when calling ldap_set_option");
    return Status::kError;
  }

  //----------------------------------------------------------------------------
  // These hardcoded values are CERN specific... we should pass them through
  // the configuration, or something.
  //----------------------------------------------------------------------------
  std::string sbase = "CN=";
  sbase += username;
  sbase += ",OU=Users,Ou=Organic Units,DC=cern,DC=ch";

  // the LDAP attribute (recursive search)
  std::string attr = "cn";
  // the LDAP filter
  std::string filter;
  filter = "(memberOf:1.2.840.113556.1.4.1941:=CN=";
  filter += egroupname;
  filter += ",OU=e-groups,OU=Workgroups,DC=cern,DC=ch)";

  char* attrs[2];
  attrs[0] = (char*) attr.c_str();
  attrs[1] = NULL;

  LDAPMessage* res = nullptr;
  struct timeval timeout;
  timeout.tv_sec = 10;
  timeout.tv_usec = 0;

  std::string match = username;
  eos_static_debug("base=%s attr=%s filter=%s match=%s\n", sbase.c_str(),
                   attr.c_str(), filter.c_str(), match.c_str());

  int rc = ldap_search_ext_s(ld, sbase.c_str(), LDAP_SCOPE_SUBTREE,
                                 filter.c_str(),
                                 attrs, 0, NULL, NULL,
                                 &timeout, LDAP_NO_LIMIT, &res);

  std::unique_ptr<LDAPMessage, decltype(ldap_msgfree)*> resOwnership(res, ldap_msgfree);

  if(res == nullptr || rc != LDAP_SUCCESS) {
    eos_static_warning("Having trouble connecting to ldap server, user=%s, e-group=%s",
      username.c_str(), egroupname.c_str());
    return Status::kError;
  }

  if(ldap_count_entries(ld, res) == 0) {
    return Status::kNotMember;
  }

  //----------------------------------------------------------------------------
  // We have a response from the server, check if we're member of given egroup
  //----------------------------------------------------------------------------
  bool isMember = false;
  for(LDAPMessage *e = ldap_first_entry(ld, res); e != nullptr;
    e = ldap_next_entry(ld, e)) {

    struct berval** v = ldap_get_values_len(ld, e, attr.c_str());
    if (v != nullptr) {
      int n = ldap_count_values_len(v);
      int j;

      for (j = 0; j < n; j++) {
        std::string result = v[ j ]->bv_val;
        eos_static_info("result=%d %s\n", n, result.c_str());

        if ((result.find(match)) != std::string::npos) {
          isMember = true;
        }
      }

      ldap_value_free_len(v);
    }
  }

  if(isMember) {
    return Status::kMember;
  }

  return Status::kNotMember;
}

/*----------------------------------------------------------------------------*/
bool
Egroup::Member(const std::string& username, const std::string& egroupname)
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

  if (Map.count(egroupname)) {
    if (Map[egroupname].count(username)) {
      member = Map[egroupname][username];
      time_t age = labs(LifeTime[egroupname][username] - now);

      // we know that user, it has a cached entry which is not too old
      if (LifeTime[egroupname].count(username) &&
          (LifeTime[egroupname][username] > now) && (age < EOSEGROUPCACHETIME)) {
        // that is ok, we can return member or not member from the cache
        Mutex.UnLock();
        return member;
      } else {
        // we have already an entry, we just schedule an asynchronous update
        iscached = true;
      }
    }
  }

  Mutex.UnLock();
  // run the command not in the locked section !!!

  if (!iscached) {
    isMember = (isMemberUncached(username, egroupname) == Status::kMember);

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
  } else {
    // just ask for asynchronous refresh
    AsyncRefresh(egroupname, username);
    return member;
  }
}

/*----------------------------------------------------------------------------*/
void
/*----------------------------------------------------------------------------*/
/**
 * @brief Asynchronous refresh loop
 *
 * The looping thread takes Egroup requests and run's LDAP queries pushing
 * results into the Egroup membership map and updates the lifetime of the
 * resolved entry.
 */
/*----------------------------------------------------------------------------*/
Egroup::Refresh(ThreadAssistant& assistant) noexcept
{
  eos_static_info("msg=\"async egroup fetch thread started\"");
  // infinite loop waiting to run refresh requests
  auto iterator = PendingQueue.begin();

  while (!assistant.terminationRequested()) {
    std::pair<std::string, std::string>* resolve = iterator.getItemBlockOrNull();

    if (!resolve) {
      break;
    }

    if (!resolve->first.empty()) {
      DoRefresh(resolve->first, resolve->second);
    }

    iterator.next();
  }
}

void
Egroup::AsyncRefresh(const std::string& egroupname, const std::string& username)
/*----------------------------------------------------------------------------*/
/**
 * @brief Pushes an Egroup/user resolution request into the asynchronous queue
 */
/*----------------------------------------------------------------------------*/
{
  // push a egroup/username pair into the async refresh queue
  PendingQueue.emplace_back(std::make_pair(egroupname, username));
}

/*----------------------------------------------------------------------------*/
void
Egroup::DoRefresh(const std::string& egroupname, const std::string& username)
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

  if (Map.count(egroupname)) {
    if (Map[egroupname].count(username)) {
      // we know that user
      if (LifeTime[egroupname].count(username) &&
          (LifeTime[egroupname][username] > now)) {
        // we don't update, we have already a fresh value
        Mutex.UnLock();
        return;
      }
    }
  }

  Mutex.UnLock();
  eos_static_info("msg=\"async-lookup\" user=\"%s\" e-group=\"%s\"",
                  username.c_str(), egroupname.c_str());

  Status status = isMemberUncached(username, egroupname);
  bool keepCached = (status != Status::kError);
  bool isMember = (status == Status::kMember);

  if (!keepCached) {
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
  } else {
    Mutex.Lock();

    if (Map.count(egroupname) && Map[egroupname].count(username)) {
      isMember = Map[egroupname][username];
    } else {
      isMember = false;
    }

    Mutex.UnLock();

    if (isMember) {
      eos_static_warning("member=true user=\"%s\" e-group=\"%s\" "
                         "cachetime=<stale-information>",
                         username.c_str(),
                         egroupname.c_str());
    } else {
      eos_static_warning("member=false user=\"%s\" e-group=\"%s\" "
                         "cachetime=<stale-information>",
                         username.c_str(),
                         egroupname.c_str());
    }
  }

  return;
}

/*----------------------------------------------------------------------------*/
std::string
Egroup::DumpMember(const std::string& username, const std::string& egroupname)
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
  Member(username, egroupname);
  XrdSysMutexHelper lLock(Mutex);
  bool member = false;
  time_t timetolive = 0;
  time_t now = time(NULL);

  if (Map.count(egroupname)) {
    if (Map[egroupname].count(username)) {
      member = Map[egroupname][username];
      timetolive = labs(LifeTime[egroupname][username] - now);
    }
  }

  std::string rs;
  rs += "egroup=";
  rs += egroupname;
  rs += " user=";
  rs += username;

  if (member) {
    rs += " member=true";
  } else {
    rs += " member=false";
  }

  rs += " lifetime=";
  rs += std::to_string((long long)timetolive);
  return rs;
}


/*----------------------------------------------------------------------------*/
std::string
Egroup::DumpMembers()
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

  for (it = Map.begin(); it != Map.end(); ++it) {
    std::map <std::string, bool>::iterator uit;

    for (uit = it->second.begin(); uit != it->second.end(); ++uit) {
      rs += "egroup=";
      rs += it->first;
      rs += " user=";
      rs += uit->first;

      if (uit->second) {
        rs += " member=true";
      } else {
        rs += " member=false";
      }

      timetolive = labs(LifeTime[it->first][uit->first] - now);
      rs += " lifetime=";
      rs += std::to_string((long long)timetolive);
      rs += "\n";
    }
  }

  return rs;
}

//------------------------------------------------------------------------------
// Reset all stored information
//------------------------------------------------------------------------------
void Egroup::Reset() {
  XrdSysMutexHelper mLock(Mutex);
  Map.clear();
  LifeTime.clear();
}

EOSMGMNAMESPACE_END
