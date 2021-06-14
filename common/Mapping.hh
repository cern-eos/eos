//------------------------------------------------------------------------------
//! @file Mapping.hh
//! @brief Class implementing virtual ID mapping.
//! @author Andreas-Joachim Peters - CERN
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

#pragma once
#include "common/Namespace.hh"
#include "common/RWMutex.hh"
#include "common/OAuth.hh"
#include "common/VirtualIdentity.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include <map>
#include <set>
#include <string>
#include <google/dense_hash_map>

//! Forward declaration
class XrdSecEntity;

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class Mapping
//------------------------------------------------------------------------------
class Mapping
{
public:

  //! Typedef of list storing valid uids of a user
  typedef std::set<uid_t> uid_set;
  //! Typedef of list storing valid gids of a user
  typedef std::set<gid_t> gid_set;
  //! Typedef of map storing uid set per uid
  typedef std::map<uid_t, uid_set > UserRoleMap_t;
  //! Typedef of map storing gid set per gid
  typedef std::map<uid_t, gid_set > GroupRoleMap_t;
  //! Typedef of map storing translation rules from auth methods to uids
  typedef std::map<std::string, uid_t> VirtualUserMap_t;
  //! Typedef of map storing translation rules from auth methods to gids
  typedef std::map<std::string, gid_t> VirtualGroupMap_t;
  //! Typedef of map storing members of the suid group
  typedef std::map<uid_t, bool > SudoerMap_t;
  //! Typedef of map storing translation of string(IP) => geo location string
  typedef std::map<std::string, std::string> GeoLocationMap_t;
  //! Typedef of set storing all host patterns which are allowed to use tident mapping
  typedef std::set<std::pair<std::string, std::string>>  AllowedTidentMatches_t;

  //----------------------------------------------------------------------------
  //! Class wrapping an uid/gid pari
  //----------------------------------------------------------------------------
  class id_pair
  {
  public:
    uid_t uid;
    gid_t gid;

    id_pair(uid_t iuid, gid_t igid)
    {
      uid = iuid;
      gid = igid;
    }

    ~id_pair() = default;
  };

  class ip_cache
  {
  public:
    // IP host entry and last resolution time pair
    typedef std::pair<time_t, std::string> entry_t;
    // Constructor

    ip_cache(int lifetime = 300)
    {
      mLifeTime = lifetime;
    }

    // Destructor
    virtual ~ip_cache() = default;

    // Getter translates host name to IP string
    std::string GetIp(const char* hostname);

  private:
    std::map<std::string, entry_t> mIp2HostMap;
    RWMutex mLocker;
    int mLifeTime;
  };

  //----------------------------------------------------------------------------
  //! Function converting vid to a string representation
  //----------------------------------------------------------------------------
  static std::string VidToString(VirtualIdentity& vid);

  //----------------------------------------------------------------------------
  //! Function converting vid from a string representation
  //----------------------------------------------------------------------------
  static bool VidFromString(VirtualIdentity& vid, const char* vidstring);

  // ---------------------------------------------------------------------------
  //! Main mapping function to create a virtual identity from authentication information
  // ---------------------------------------------------------------------------
  static void IdMap(const XrdSecEntity* client, const char* env,
                    const char* tident, VirtualIdentity& vid, bool log = true);

  // ---------------------------------------------------------------------------
  //! Map describing which virtual user roles a user with a given uid has
  // ---------------------------------------------------------------------------
  static UserRoleMap_t gUserRoleVector;

  // ---------------------------------------------------------------------------
  //! Map describing which virtual group roles a user with a given uid has
  // ---------------------------------------------------------------------------
  static GroupRoleMap_t gGroupRoleVector;

  // ---------------------------------------------------------------------------
  //! Map describing auth info to virtual uid mapping rules
  // ---------------------------------------------------------------------------
  static VirtualUserMap_t gVirtualUidMap;

  // ---------------------------------------------------------------------------
  //! Map describing auth info to virtual gid mapping rules
  // ---------------------------------------------------------------------------
  static VirtualGroupMap_t gVirtualGidMap;

  // ---------------------------------------------------------------------------
  //! Map keeping the super user members
  // ---------------------------------------------------------------------------
  static SudoerMap_t gSudoerMap;

  // ---------------------------------------------------------------------------
  //! Map keeping the geo location HostName=>GeoTag translation
  // ---------------------------------------------------------------------------
  static GeoLocationMap_t gGeoMap;


  // ---------------------------------------------------------------------------
  //! Max. subdirectory deepness where anonymous access is allowed
  // ---------------------------------------------------------------------------
  static int gNobodyAccessTreeDeepness;

  // ---------------------------------------------------------------------------
  //! Vector having pattern matches of hosts which are allowed to use tident mapping
  // ---------------------------------------------------------------------------
  static AllowedTidentMatches_t gAllowedTidentMatches;

  // ---------------------------------------------------------------------------
  //! A cache for physical user id caching (e.g. from user name to uid)
  static XrdOucHash<id_pair> gPhysicalUidCache;

  // ---------------------------------------------------------------------------
  //! A cache for physical group id caching (e.g. from group name to gid)
  // ---------------------------------------------------------------------------
  static XrdOucHash<gid_set> gPhysicalGidCache;

  // ---------------------------------------------------------------------------
  //! A mutex protecting the physical id->name caches
  // ---------------------------------------------------------------------------
  static XrdSysMutex gPhysicalNameCacheMutex;

  // ---------------------------------------------------------------------------
  //! A cache for physical user name caching (e.g. from uid to name)
  static std::map<uid_t, std::string> gPhysicalUserNameCache;
  static std::map<std::string, uid_t> gPhysicalUserIdCache;

  // ---------------------------------------------------------------------------
  //! A cache for physical group id caching (e.g. from gid name to name)
  // ---------------------------------------------------------------------------
  static std::map<gid_t, std::string> gPhysicalGroupNameCache;
  static std::map<std::string, gid_t> gPhysicalGroupIdCache;

  // ---------------------------------------------------------------------------
  //! Mutex to protect the physical ID caches
  // ---------------------------------------------------------------------------
  static XrdSysMutex gPhysicalIdMutex;

  // ---------------------------------------------------------------------------
  //! RWMutex protecting all global hash maps
  // ---------------------------------------------------------------------------
  static RWMutex gMapMutex;

  // ---------------------------------------------------------------------------
  //! Mutex protecting the active tident map
  // ---------------------------------------------------------------------------
  static XrdSysMutex ActiveLock;

  // ---------------------------------------------------------------------------
  //! Cache for host to ip translation used by geo mapping
  // ---------------------------------------------------------------------------
  static ip_cache gIpCache;

  // ---------------------------------------------------------------------------
  //! OAuth interface
  // ---------------------------------------------------------------------------
  static OAuth gOAuth;

  // ---------------------------------------------------------------------------
  //! Function to expire unused ActiveTident entries by default after 1 day
  // ---------------------------------------------------------------------------
  static void ActiveExpire(int interval = 300, bool force = false);

  // ---------------------------------------------------------------------------
  //! Function initializing static maps
  // ---------------------------------------------------------------------------
  static void Init();

  // ---------------------------------------------------------------------------
  //! Map storing the client identifiers and last usage time
  // ---------------------------------------------------------------------------
  static google::dense_hash_map<std::string, time_t> ActiveTidents;

  // ---------------------------------------------------------------------------
  //! Map storing the active client uids
  // ---------------------------------------------------------------------------
  static google::dense_hash_map<uid_t, size_t> ActiveUids;

  // ---------------------------------------------------------------------------
  //! Retrieve the user ID from a trace identifier
  // ---------------------------------------------------------------------------
  static uid_t UidFromTident(const std::string& tident);

  // ---------------------------------------------------------------------------
  //! Get the number of active sessions (for a given uid)
  static size_t ActiveSessions(uid_t uid);
  static size_t ActiveSessions();

  // ---------------------------------------------------------------------------
  //! Variable to forbid remote root mounts - by default true
  // ---------------------------------------------------------------------------
  static bool gRootSquash;

  // ---------------------------------------------------------------------------
  //! Reset clears all cached information
  // ---------------------------------------------------------------------------
  static void Reset();

  // ---------------------------------------------------------------------------
  //! Convert a komma separated uid string to a vector uid list
  // ---------------------------------------------------------------------------
  static void
  CommaListToUidSet(const char* list, std::set<uid_t>& uids_set);

  // ---------------------------------------------------------------------------
  //! Convert a komma separated gid string to a vector gid list
  // ---------------------------------------------------------------------------
  static void
  CommaListToGidSet(const char* list, std::set<gid_t>& gids_set);

  //----------------------------------------------------------------------------
  //! Printout mapping in the format specified by option
  //!
  //! @param stdOut string stat stores the output
  //! @param option can be:
  //!         'u' for user role mappings
  //!         'g' for group role mappings
  //!         's' for sudoer list
  //!         'U' for user alias mapping
  //!         'G' for group alias mapping
  //!         'y' for gateway mappings (tidents)
  //!         'a' for authentication mapping rules
  //!         'l' for geo location rules
  //!         'n' for the anonymous access deepness of user nobody
  //----------------------------------------------------------------------------
  static void Print(XrdOucString& stdOut, XrdOucString option = "");

  // ---------------------------------------------------------------------------
  //! Add physical ids for name to vid
  // ---------------------------------------------------------------------------
  static void getPhysicalIds(const char* name, VirtualIdentity& vid);

  // ---------------------------------------------------------------------------
  //! Compare a uid with the string representation
  // ---------------------------------------------------------------------------
  static bool IsUid(XrdOucString idstring, uid_t& id);

  // ---------------------------------------------------------------------------
  //! Compare a gid with the string representation
  // ---------------------------------------------------------------------------
  static bool IsGid(XrdOucString idstring, gid_t& id);

  // ---------------------------------------------------------------------------
  //! Reduce the trace identifier information to user@host
  // ---------------------------------------------------------------------------
  static const char* ReduceTident(XrdOucString& tident,
                                  XrdOucString& wildcardtident,
                                  XrdOucString& mytident, XrdOucString& myhost);

  // ---------------------------------------------------------------------------
  //! Convert a uid to a user name
  // ---------------------------------------------------------------------------
  static std::string UidToUserName(uid_t uid, int& errc);

  // ---------------------------------------------------------------------------
  //! Convert a gid to a group name
  // ---------------------------------------------------------------------------
  static std::string GidToGroupName(uid_t gid, int& errc);

  // ---------------------------------------------------------------------------
  //! Convert a user name to a uid
  // ---------------------------------------------------------------------------
  static uid_t UserNameToUid(const std::string& username, int& errc);

  // ---------------------------------------------------------------------------
  //! Convert a group name to a gid
  // ---------------------------------------------------------------------------
  static gid_t GroupNameToGid(const std::string& groupname, int& errc);

  // ---------------------------------------------------------------------------
  //! Convert a uid into a string
  // ---------------------------------------------------------------------------
  static std::string UidAsString(uid_t uid);

  // ---------------------------------------------------------------------------
  //! Convert a gid into a string
  // ---------------------------------------------------------------------------
  static std::string GidAsString(gid_t gid);

  static int GetPublicAccessLevel()
  {
    RWMutexReadLock lock(gMapMutex);
    return Mapping::gNobodyAccessTreeDeepness;
  }

  // ---------------------------------------------------------------------------
  //! Get a VID from a name
  // ---------------------------------------------------------------------------
  static VirtualIdentity Someone(const std::string& name);

  // ---------------------------------------------------------------------------
  //! Get a VID from a uid/gid pari
  // ---------------------------------------------------------------------------
  static VirtualIdentity Someone(uid_t uid, gid_t gid);

  // ---------------------------------------------------------------------------
  //! Check if a resource is allowed for OAUTH2
  // ---------------------------------------------------------------------------
  static bool IsOAuth2Resource(const std::string& resource);

private:

  //----------------------------------------------------------------------------
  //! Handle VOMS mapping
  //!
  //! @param client XrdSecEntity object
  //! @parma vid virtual identity
  //! @note needs to be called with the gMapMutex locked
  //----------------------------------------------------------------------------
  static void HandleVOMS(const XrdSecEntity* client, VirtualIdentity& vid);
};

EOSCOMMONNAMESPACE_END
