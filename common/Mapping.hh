// ----------------------------------------------------------------------
// File: Mapping.hh
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

/**
 * @file   Mapping.hh
 *
 * @brief  Class implementing virtual ID mapping.
 *
 *
 */

#ifndef __EOSCOMMON_MAPPING__
#define __EOSCOMMON_MAPPING__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
/*----------------------------------------------------------------------------*/
#include <map>
#include <set>
#include <vector>
#include <string>
#include <google/dense_hash_map>

/*----------------------------------------------------------------------------*/

class XrdSecEntity;

EOSCOMMONNAMESPACE_BEGIN

class Mapping
{
private:
public:

  // Constants used throughout the Mapping class
  static const std::string PROXY_GEOTAG;

  typedef std::vector<uid_t>
  uid_vector; //< typdef of list storing valid uids of a user
  typedef std::vector<gid_t>
  gid_vector; //< typdef of list storing valid gids of a user
  typedef std::map<uid_t, uid_vector >
  UserRoleMap_t; //< typedef of map storing uid vectors per uid
  typedef std::map<uid_t, gid_vector >
  GroupRoleMap_t; //< typedef of map storing gid vectors per gid
  typedef std::map<std::string, uid_t>
  VirtualUserMap_t; //< typedef of map storing translation rules from auth methods to uids
  typedef std::map<std::string, gid_t>
  VirtualGroupMap_t; //< typedef of map storing translation rules from auth methods to gids
  typedef std::map<uid_t, bool >
  SudoerMap_t; //< typde of map storing members of the suid group
  typedef std::map<std::string, std::string>
  GeoLocationMap_t; //< typdef of map storing translation of string(IP) => geo location string
  typedef std::set<std::pair<std::string, std::string>>
      AllowedTidentMatches_t; //< typdef of set storing all host patterns which are allowed to use tident mapping
  // ---------------------------------------------------------------------------
  //! Class wrapping an uid/gid pari
  // ---------------------------------------------------------------------------

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

    ~id_pair()
    {
    };
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

    virtual ~ip_cache()
    {
    }

    // Getter translates host name to IP string
    std::string GetIp(const char* hostname);

  private:
    std::map<std::string, entry_t> mIp2HostMap;
    RWMutex mLocker;
    int mLifeTime;
  };

  //----------------------------------------------------------------------------
  //! Struct defining the virtual identity of a client e.g. his memberships and
  //! authentication information
  //----------------------------------------------------------------------------
  struct VirtualIdentity_t {
    uid_t uid;
    gid_t gid;
    std::string uid_string;
    std::string gid_string;
    uid_vector uid_list;
    gid_vector gid_list;
    XrdOucString tident;
    XrdOucString name;
    XrdOucString prot;
    std::string host;
    std::string domain;
    std::string grps;
    std::string role;
    std::string dn;
    std::string geolocation;
    std::string app;
    bool sudoer;

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    VirtualIdentity_t(): uid(99), gid(99), sudoer(false) {};

    //--------------------------------------------------------------------------
    //! Copy constuctor
    //--------------------------------------------------------------------------
    VirtualIdentity_t(const VirtualIdentity_t& other)
    {
      *this = other;
    }
  };

  //----------------------------------------------------------------------------
  //! Virtual identity type
  //----------------------------------------------------------------------------
  typedef struct VirtualIdentity_t VirtualIdentity;

  //----------------------------------------------------------------------------
  //! Function creating the Nobody identity
  //----------------------------------------------------------------------------
  static void Nobody(VirtualIdentity& vid);

  //----------------------------------------------------------------------------
  //! Function creating the root identity
  //----------------------------------------------------------------------------
  static void Root(VirtualIdentity& vid);

  //----------------------------------------------------------------------------
  //! Function converting vid to a string representation
  //----------------------------------------------------------------------------
  static std::string VidToString(VirtualIdentity& vid);

  //----------------------------------------------------------------------------
  //! Function converting vid frin a string representation
  //----------------------------------------------------------------------------
  static bool VidFromString(VirtualIdentity& vid, const char* vidstring);

  //----------------------------------------------------------------------------
  //! Function checking if we come from a localhost connection
  //----------------------------------------------------------------------------
  static bool IsLocalhost(VirtualIdentity& vid);

  // ---------------------------------------------------------------------------
  //! Check for a role in the user id list
  // ---------------------------------------------------------------------------
  static bool HasUid(uid_t uid, VirtualIdentity& vid);

  // ---------------------------------------------------------------------------
  //! Check for a role in the group id list
  // ---------------------------------------------------------------------------
  static bool HasGid(gid_t gid, VirtualIdentity& vid);

  // ---------------------------------------------------------------------------
  //! Copy function for virtual identities
  // ---------------------------------------------------------------------------
  static void Copy(VirtualIdentity& vidin, VirtualIdentity& vidout);

  // ---------------------------------------------------------------------------
  //! Main mapping function to create a virtual identity from authentication information
  // ---------------------------------------------------------------------------
  static void IdMap(const XrdSecEntity* client, const char* env,
                    const char* tident, Mapping::VirtualIdentity& vid, bool log = true);

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
  //! Vector having pattern matches of hosts which are allowed to use tident mapping
  // ---------------------------------------------------------------------------
  static AllowedTidentMatches_t gAllowedTidentMatches;

  // ---------------------------------------------------------------------------
  //! A cache for physical user id caching (e.g. from user name to uid)
  static XrdOucHash<id_pair> gPhysicalUidCache;

  // ---------------------------------------------------------------------------
  //! A cache for physical group id caching (e.g. from group name to gid)
  // ---------------------------------------------------------------------------
  static XrdOucHash<gid_vector> gPhysicalGidCache;

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
  //! RWMutex protecting all global hashmaps
  // ---------------------------------------------------------------------------
  static RWMutex gMapMutex;

  // ---------------------------------------------------------------------------
  //! Mutex protecting the active tident map
  // ---------------------------------------------------------------------------
  static XrdSysMutex ActiveLock;

  // ---------------------------------------------------------------------------
  //! Cache for host to ip translatiosn used by geo mapping
  // ---------------------------------------------------------------------------
  static ip_cache gIpCache;

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
  KommaListToUidVector(const char* list, std::vector<uid_t>& vector_list);

  // ---------------------------------------------------------------------------
  //! Convert a komma separated gid string to a vector gid list
  // ---------------------------------------------------------------------------
  static void
  KommaListToGidVector(const char* list, std::vector<gid_t>& vector_list);

  // ---------------------------------------------------------------------------
  //! Printout mapping in the format specified by option
  // ---------------------------------------------------------------------------

  static void Print(XrdOucString& stdOut, XrdOucString option = "");

  // ---------------------------------------------------------------------------
  //! Add physical ids for name to vid
  // ---------------------------------------------------------------------------
  static void getPhysicalIds(const char* name, VirtualIdentity& vid);

  // ---------------------------------------------------------------------------
  //! Check if a vector contains uid
  // ---------------------------------------------------------------------------
  static bool HasUid(uid_t uid, uid_vector vector);

  // ---------------------------------------------------------------------------
  //! Check if vector contains gid
  // ---------------------------------------------------------------------------
  static bool HasGid(gid_t gid, gid_vector vector);

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
    XrdOucString& wildcardtident, XrdOucString& mytident, XrdOucString& myhost);

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

};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif
