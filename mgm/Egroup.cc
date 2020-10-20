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
#include "common/StringUtils.hh"
#include <ldap.h>
#include <memory>

//------------------------------------------------------------------------------
// Delete the LDAP object
//------------------------------------------------------------------------------
static void ldap_uninitialize(LDAP* ld)
{
  if (ld != nullptr) {
    ldap_unbind_ext(ld, NULL, NULL);
  }
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor - launch asynchronous refresh thread
//------------------------------------------------------------------------------
Egroup::Egroup(common::SteadyClock* clock_) : clock(clock_)
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

//----------------------------------------------------------------------------
// Return number of asynchronous refresh requests currently pending
//----------------------------------------------------------------------------
size_t Egroup::getPendingQueueSize() const
{
  return PendingQueue.size();
}

//------------------------------------------------------------------------------
// Main LDAP lookup function - bypasses the cache, hits the LDAP server.
//------------------------------------------------------------------------------
Egroup::Status Egroup::isMemberUncached(const std::string& username,
                                        const std::string& egroupname)
{
  //----------------------------------------------------------------------------
  // Serving real, or simulated data?
  //----------------------------------------------------------------------------
  if (!injections.empty()) {
    auto it = injections.find(egroupname);

    if (it == injections.end()) {
      return Status::kNotMember;
    }

    auto it2 = it->second.find(username);

    if (it2 == it->second.end()) {
      return Status::kNotMember;
    }

    return it2->second;
  }

  // run the LDAP query
  LDAP* ld = nullptr;
  //----------------------------------------------------------------------------
  // Initialize the LDAP context.
  //----------------------------------------------------------------------------
  ldap_initialize(&ld, "ldap://xldap");
  std::unique_ptr<LDAP, decltype(ldap_uninitialize)*> ldOwnership(
    ld, ldap_uninitialize);

  if (ld == nullptr) {
    eos_static_crit("Could not initialize ldap context");
    return Status::kError;
  }

  int version = LDAP_VERSION3;

  if (ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &version) !=
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
  std::unique_ptr<LDAPMessage, decltype(ldap_msgfree)*> resOwnership(res,
      ldap_msgfree);

  if (res == nullptr || rc != LDAP_SUCCESS) {
    eos_static_warning("Having trouble connecting to ldap server, user=%s, "
                       "e-group=%s ldap_rc=%i ldap_err_msg=\"%s\"",
                       username.c_str(), egroupname.c_str(), rc,
                       ldap_err2string(rc));
    return Status::kError;
  }

  if (ldap_count_entries(ld, res) == 0) {
    return Status::kNotMember;
  }

  //----------------------------------------------------------------------------
  // We have a response from the server, check if we're member of given egroup
  //----------------------------------------------------------------------------
  bool isMember = false;

  for (LDAPMessage* e = ldap_first_entry(ld, res); e != nullptr;
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

  if (isMember) {
    return Status::kMember;
  }

  return Status::kNotMember;
}

//------------------------------------------------------------------------------
// Store entry into the cache
//------------------------------------------------------------------------------
void Egroup::storeIntoCache(const std::string& username,
                            const std::string& egroupname, bool isMember,
                            std::chrono::steady_clock::time_point timestamp)
{
  eos::common::RWMutexWriteLock wr_lock(mMutex);
  cache[egroupname][username] = CachedEntry(isMember, timestamp);
}

//------------------------------------------------------------------------------
// Fetch cached value. Returns false if there's no such cached value.
//------------------------------------------------------------------------------
bool Egroup::fetchCached(const std::string& username,
                         const std::string& egroupname, Egroup::CachedEntry& out)
{
  eos::common::RWMutexReadLock rd_lock(mMutex);
  auto it = cache.find(egroupname);

  if (it == cache.end()) {
    return false;
  }

  auto it2 = it->second.find(username);

  if (it2 == it->second.end()) {
    return false;
  }

  out = it2->second;
  return true;
}

//------------------------------------------------------------------------------
// Check if cache entry is stale
//------------------------------------------------------------------------------
bool Egroup::isStale(const CachedEntry& entry) const
{
  std::chrono::steady_clock::time_point now = common::SteadyClock::now(clock);

  if (entry.timestamp + kCacheDuration < now) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Inject item into the fake LDAP server. If injections are active, any time
// this class tries to contact the LDAP server, it will serve injected data
// instead.
//
// Simulates response of "isMemberUncached" function.
//------------------------------------------------------------------------------
void Egroup::inject(const std::string& username, const std::string& egroupname,
                    Status status)
{
  injections[egroupname][username] = status;
}

//------------------------------------------------------------------------------
// Major query method - uses cache
//------------------------------------------------------------------------------
Egroup::CachedEntry Egroup::query(const std::string& username,
                                  const std::string& egroupname)
{
  CachedEntry entry;

  if (fetchCached(username, egroupname, entry)) {
    //--------------------------------------------------------------------------
    // Cache hit - do we need to schedule an asynchronous refresh?
    //--------------------------------------------------------------------------
    if (isStale(entry)) {
      scheduleRefresh(username, egroupname);
    }

    return entry;
  }

  //----------------------------------------------------------------------------
  // Cache miss, need to talk to LDAP server
  //----------------------------------------------------------------------------
  Status status = isMemberUncached(username, egroupname);
  bool isMember = (status == Status::kMember);
  std::chrono::steady_clock::time_point now = common::SteadyClock::now(clock);
  uint64_t expiration = common::SteadyClock::secondsSinceEpoch(
                          now + kCacheDuration).count();
  eos_static_info("member=%s user=\"%s\" e-group=\"%s\" expiration=%lu",
                  common::boolToString(isMember).c_str(), username.c_str(),
                  egroupname.c_str(), expiration);
  //----------------------------------------------------------------------------
  // Store into the cache
  //----------------------------------------------------------------------------
  storeIntoCache(username, egroupname, isMember, now);
  return CachedEntry(isMember, now);
}

//------------------------------------------------------------------------------
// Asynchronous refresh loop.
//
// The looping thread takes Egroup requests and run's LDAP queries pushing
// results into the Egroup membership map and updates the lifetime of the
// resolved entry.
//------------------------------------------------------------------------------
void Egroup::Refresh(ThreadAssistant& assistant) noexcept
{
  eos_static_info("msg=\"async egroup fetch thread started\"");
  auto iterator = PendingQueue.begin();

  while (!assistant.terminationRequested()) {
    std::pair<std::string, std::string>* resolve = iterator.getItemBlockOrNull();

    if (!resolve) {
      break;
    }

    if (!resolve->first.empty()) {
      refresh(resolve->first, resolve->second);
    }

    iterator.next();
    PendingQueue.pop_front();
  }
}

//------------------------------------------------------------------------------
// Pushes an egroup/user resolution request into the asynchronous queue
//------------------------------------------------------------------------------
void Egroup::scheduleRefresh(const std::string& username,
                             const std::string& egroupname)
{
  PendingQueue.emplace_back(std::make_pair(username, egroupname));
}

//------------------------------------------------------------------------------
// Run a synchronous LDAP query for Egroup/username and update the cache
//------------------------------------------------------------------------------
Egroup::CachedEntry Egroup::refresh(const std::string& username,
                                    const std::string& egroupname)
{
  eos_static_info("msg=\"async-lookup\" user=\"%s\" e-group=\"%s\"",
                  username.c_str(), egroupname.c_str());
  Status status = isMemberUncached(username, egroupname);

  if (status == Status::kError) {
    eos_static_err("Could not do asynchronous refresh for egroup membership for username=%s, e-group=%s",
                   username.c_str(), egroupname.c_str());
    return CachedEntry(false, {});
  }

  bool isMember = (status == Status::kMember);
  std::chrono::steady_clock::time_point now = common::SteadyClock::now(clock);
  uint64_t expiration = common::SteadyClock::secondsSinceEpoch(
                          now + kCacheDuration).count();
  eos_static_info("member=%s user=\"%s\" e-group=\"%s\" expiration=%lu",
                  common::boolToString(isMember).c_str(), username.c_str(),
                  egroupname.c_str(), expiration);
  storeIntoCache(username, egroupname, isMember, now);
  return CachedEntry(isMember, now);
}

//------------------------------------------------------------------------------
// Return membership information as string.
// @param username name of the user to dump Egroup membership
// @param egroupname name of Egroup where to look for membership
//
// @return egroup dump for username
//------------------------------------------------------------------------------
std::string
Egroup::DumpMember(const std::string& username, const std::string& egroupname)
{
  // trigger refresh
  CachedEntry entry = query(username, egroupname);
  std::chrono::steady_clock::time_point now = common::SteadyClock::now(clock);
  std::chrono::seconds lifetime;
  lifetime = std::chrono::duration_cast<std::chrono::seconds>(
               entry.timestamp + kCacheDuration - now);
  std::stringstream ss;
  ss << "egroup=" << egroupname;
  ss << " user=" << username;
  ss << " member=" << common::boolToString(entry.isMember);
  ss << " lifetime=" << std::to_string(lifetime.count());
  return ss.str();
}

//------------------------------------------------------------------------------
// DumpMembers: return egroup dump for all users
//------------------------------------------------------------------------------
std::string
Egroup::DumpMembers()
{
  std::chrono::steady_clock::time_point now = common::SteadyClock::now(clock);
  std::stringstream ss;
  eos::common::RWMutexReadLock rd_lock(mMutex);

  for (auto it = cache.begin(); it != cache.end(); it++) {
    for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) {
      ss << "egroup=" << it->first;
      ss << " user=" << it2->first;
      ss << " member=" << common::boolToString(it2->second.isMember);
      std::chrono::seconds lifetime = std::chrono::duration_cast <
                                      std::chrono::seconds > ((it2->second.timestamp + kCacheDuration) - now);
      ss << " lifetime=" << std::to_string(lifetime.count()) << std::endl;
    }
  }

  return ss.str();
}

//------------------------------------------------------------------------------
// Reset all stored information
//------------------------------------------------------------------------------
void Egroup::Reset()
{
  eos::common::RWMutexWriteLock wr_lock(mMutex);
  cache.clear();
}

EOSMGMNAMESPACE_END
