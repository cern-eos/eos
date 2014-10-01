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
#include "XrdSec/XrdSecEntity.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysPthread.hh"

/*----------------------------------------------------------------------------*/
#include <pwd.h>
#include <grp.h>
#include <map>
#include <set>
#include <vector>
#include <string>
#include <google/dense_hash_map>

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class Mapping {
private:
public:

  typedef std::vector<uid_t> uid_vector; //< typdef of list storing valid uids of a user
  typedef std::vector<gid_t> gid_vector; //< typdef of list storing valid gids of a user
  typedef std::map<uid_t, uid_vector > UserRoleMap_t; //< typedef of map storing uid vectors per uid
  typedef std::map<uid_t, gid_vector > GroupRoleMap_t; //< typedef of map storing gid vectors per gid
  typedef std::map<std::string, uid_t> VirtualUserMap_t; //< typedef of map storing translation rules from auth methods to uids
  typedef std::map<std::string, gid_t> VirtualGroupMap_t; //< typedef of map storing translation rules from auth methods to gids
  typedef std::map<uid_t, bool > SudoerMap_t; //< typde of map storing members of the suid group
  typedef std::map<std::string, std::string> GeoLocationMap_t; //< typdef of map storing translation of string(IP) => geo location string
  typedef std::set<std::pair<std::string, std::string>> AllowedTidentMatches_t; //< typdef of set storing all host patterns which are allowed to use tident mapping
  // ---------------------------------------------------------------------------
  //! Class wrapping an uid/gid pari
  // ---------------------------------------------------------------------------

  class id_pair {
  public:
    uid_t uid;
    gid_t gid;

    id_pair (uid_t iuid, gid_t igid)
    {
      uid = iuid;
      gid = igid;
    }

    ~id_pair ()
    {
    };
  };

  class ip_cache {
  public:
    // IP host entry and last resolution time pair
    typedef std::pair<time_t, std::string> entry_t;
    // Constructor

    ip_cache (int lifetime = 300)
    {
      mLifeTime = lifetime;
    }
    // Destructor

    virtual ~ip_cache ()
    {
    }

    // Getter translates host name to IP string
    std::string GetIp (const char* hostname);

  private:
    std::map<std::string, entry_t> mIp2HostMap;
    RWMutex mLocker;
    int mLifeTime;
  };
  // ---------------------------------------------------------------------------
  //! Struct defining the virtual identity of a client e.g. his memberships and authentication information
  // ---------------------------------------------------------------------------

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
    std::string grps;
    std::string role;
    std::string dn;
    std::string geolocation;
    bool sudoer;
  };

  // ---------------------------------------------------------------------------
  //! Virtual identity type
  // ---------------------------------------------------------------------------
  typedef struct VirtualIdentity_t VirtualIdentity;

  // ---------------------------------------------------------------------------
  //! Function creating the Nobody identity
  // ---------------------------------------------------------------------------

  static void
  Nobody (VirtualIdentity &vid)
  {
    vid.uid = vid.gid = 99;
    vid.uid_list.clear();
    vid.gid_list.clear();
    vid.uid_list.push_back(99);
    vid.gid_list.push_back(99);
    vid.name = "nobody";
    vid.sudoer = false;
  }

  // ---------------------------------------------------------------------------
  //! Function creating the root identity
  // ---------------------------------------------------------------------------

  static void
  Root (VirtualIdentity &vid)
  {
    vid.uid = vid.gid = 0;
    vid.uid_list.clear();
    vid.gid_list.clear();
    vid.uid_list.push_back(0);
    vid.gid_list.push_back(0);
    vid.name = "root";
    vid.prot = "local";
    vid.tident = "service@localhost";
    vid.sudoer = false;
  }

  // ---------------------------------------------------------------------------
  //! Function checking if we come from a localhost connection
  // ---------------------------------------------------------------------------

  static bool
  IsLocalhost (VirtualIdentity &vid)
  {
    if ( (vid.host == "localhost") || 
	 (vid.host == "localhost.localdomain") ||
	 (vid.host == "localhost6") || 
	 (vid.host == "localhost6.localdomain6") )
      return true;
    return false;
  }

  // ---------------------------------------------------------------------------
  //! Check for a role in the user id list
  // ---------------------------------------------------------------------------

  static bool
  HasUid (uid_t uid, VirtualIdentity &vid)
  {
    for (size_t i = 0; i < vid.uid_list.size(); i++)
      if (vid.uid_list[i] == uid)
        return true;
    return false;
  }

  // ---------------------------------------------------------------------------
  //! Check for a role in the group id list
  // ---------------------------------------------------------------------------

  static bool
  HasGid (gid_t gid, VirtualIdentity &vid)
  {
    for (size_t i = 0; i < vid.gid_list.size(); i++)
      if (vid.gid_list[i] == gid)
        return true;
    return false;
  }
  // ---------------------------------------------------------------------------
  //! Copy function for virtual identities
  // ---------------------------------------------------------------------------

  static void
  Copy (VirtualIdentity &vidin, VirtualIdentity &vidout)
  {
    vidout.uid = vidin.uid;
    vidout.gid = vidin.gid;
    vidout.sudoer = vidin.sudoer;
    vidout.name = vidin.name;
    vidout.tident = vidin.tident;
    vidout.prot = vidin.prot;
    vidout.uid_list.clear();
    vidout.gid_list.clear();
    vidout.uid_string = vidin.uid_string;
    vidout.gid_string = vidin.gid_string;

    for (unsigned int i = 0; i < vidin.uid_list.size(); i++) vidout.uid_list.push_back(vidin.uid_list[i]);
    for (unsigned int i = 0; i < vidin.gid_list.size(); i++) vidout.gid_list.push_back(vidin.gid_list[i]);

    vidout.host = vidin.host;
    vidout.host = vidin.host;
    vidout.grps = vidin.grps;
    vidout.role = vidin.role;
    vidout.dn = vidin.dn;
    vidout.geolocation = vidin.geolocation;
  }

  // ---------------------------------------------------------------------------
  //! Main mapping function to create a virtual identity from authentication information
  // ---------------------------------------------------------------------------
  static void IdMap (const XrdSecEntity* client, const char* env, const char* tident, Mapping::VirtualIdentity &vid, bool log = true);

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
  //! Mutex protecting all getpwxxx calls to avoid problems with sssd
  // ---------------------------------------------------------------------------
  static XrdSysMutex gSssdLock;

  // ---------------------------------------------------------------------------
  //! Cache for host to ip translatiosn used by geo mapping
  // ---------------------------------------------------------------------------
  static ip_cache gIpCache;


  // ---------------------------------------------------------------------------
  //! Function to expire unused ActiveTident entries by default after 1 day
  // ---------------------------------------------------------------------------
  static void ActiveExpire (int interval = 300, bool force = false);

  // ---------------------------------------------------------------------------
  //! Function initializing static maps
  // ---------------------------------------------------------------------------
  static void Init ();

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

  static void Reset ()
  {
    {
      XrdSysMutexHelper mLock(gPhysicalIdMutex);
      gPhysicalUidCache.Purge();
      gPhysicalGidCache.Purge();
    }
    {
      XrdSysMutexHelper mLock(gPhysicalNameCacheMutex);
      gPhysicalGroupNameCache.clear();
      gPhysicalUserNameCache.clear();
      gPhysicalGroupIdCache.clear();
      gPhysicalUserIdCache.clear();
    }
    {
      XrdSysMutexHelper mLock(ActiveLock);
      ActiveTidents.clear();
    }
  }

  // ---------------------------------------------------------------------------
  //! Convert a komma separated uid string to a vector uid list
  // ---------------------------------------------------------------------------

  static void
  KommaListToUidVector (const char* list, std::vector<uid_t> &vector_list)
  {
    XrdOucString slist = list;
    XrdOucString number = "";
    int kommapos;
    if (!slist.endswith(","))
      slist += ",";
    do
    {
      kommapos = slist.find(",");
      if (kommapos != STR_NPOS)
      {
        number.assign(slist, 0, kommapos - 1);
        int errc;
        std::string username = number.c_str();
        uid_t uid = UserNameToUid(username, errc);
        if (!errc)
          vector_list.push_back(uid);
        slist.erase(0, kommapos + 1);
      }
    }
    while (kommapos != STR_NPOS);
  }

  // ---------------------------------------------------------------------------
  //! Convert a komma separated gid string to a vector gid list
  // ---------------------------------------------------------------------------

  static void
  KommaListToGidVector (const char* list, std::vector<gid_t> &vector_list)
  {
    XrdOucString slist = list;
    XrdOucString number = "";
    int kommapos;
    if (!slist.endswith(","))
      slist += ",";
    do
    {
      kommapos = slist.find(",");
      if (kommapos != STR_NPOS)
      {
        number.assign(slist, 0, kommapos - 1);
        int errc;
        std::string groupname = number.c_str();
        gid_t gid = GroupNameToGid(groupname, errc);
        if (!errc)
          vector_list.push_back(gid);
        slist.erase(0, kommapos + 1);
      }
    }
    while (kommapos != STR_NPOS);
  }

  // ---------------------------------------------------------------------------
  //! Printout mapping in the format specified by option
  // ---------------------------------------------------------------------------

  static void Print (XrdOucString &stdOut, XrdOucString option = "");

  // ---------------------------------------------------------------------------
  //! Add physical ids for name to vid
  // ---------------------------------------------------------------------------
  static void getPhysicalIds (const char* name, VirtualIdentity &vid);

  // ---------------------------------------------------------------------------
  //! Check if a vector contains uid
  // ---------------------------------------------------------------------------

  static bool
  HasUid (uid_t uid, uid_vector vector)
  {
    uid_vector::const_iterator it;
    for (it = vector.begin(); it != vector.end(); ++it)
    {
      if ((*it) == uid)
        return true;
    }
    return false;
  }

  // ---------------------------------------------------------------------------
  //! Check if vector contains gid
  // ---------------------------------------------------------------------------

  static bool
  HasGid (gid_t gid, gid_vector vector)
  {
    uid_vector::const_iterator it;
    for (it = vector.begin(); it != vector.end(); ++it)
    {
      if ((*it) == gid)
        return true;
    }
    return false;
  }

  // ---------------------------------------------------------------------------
  //! Compare a uid with the string representation
  // ---------------------------------------------------------------------------

  static bool
  IsUid (XrdOucString idstring, uid_t &id)
  {
    id = strtoul(idstring.c_str(), 0, 10);
    char revid[1024];
    sprintf(revid, "%lu", (unsigned long) id);
    XrdOucString srevid = revid;
    if (idstring == srevid)
      return true;
    return false;
  }


  // ---------------------------------------------------------------------------
  //! Compare a gid with the string representation
  // ---------------------------------------------------------------------------

  static bool
  IsGid (XrdOucString idstring, gid_t &id)
  {
    id = strtoul(idstring.c_str(), 0, 10);
    char revid[1024];
    sprintf(revid, "%lu", (unsigned long) id);
    XrdOucString srevid = revid;
    if (idstring == srevid)
      return true;
    return false;
  }

  // ---------------------------------------------------------------------------
  //! Reduce the trace identifier information to user@host
  // ---------------------------------------------------------------------------

  static const char*
  ReduceTident (XrdOucString &tident, XrdOucString &wildcardtident, XrdOucString &mytident, XrdOucString &myhost)
  {
    int dotpos = tident.find(".");
    int addpos = tident.find("@");
    wildcardtident = tident;
    mytident = tident;
    mytident.erase(dotpos, addpos - dotpos);
    myhost = mytident;
    dotpos = mytident.find("@");
    myhost.erase(0, dotpos + 1);
    wildcardtident = mytident;
    addpos = wildcardtident.find("@");
    wildcardtident.erase(0, addpos);
    wildcardtident = "*" + wildcardtident;
    return mytident.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Convert a uid to a user name
  // ---------------------------------------------------------------------------
  static std::string UidToUserName (uid_t uid, int &errc);

  // ---------------------------------------------------------------------------
  //! Convert a gid to a group name
  // ---------------------------------------------------------------------------
  static std::string GidToGroupName (uid_t gid, int &errc);

  // ---------------------------------------------------------------------------
  //! Convert a user name to a uid
  // ---------------------------------------------------------------------------
  static uid_t UserNameToUid (std::string &username, int &errc);

  // ---------------------------------------------------------------------------
  //! Convert a group name to a gid
  // ---------------------------------------------------------------------------
  static gid_t GroupNameToGid (std::string &groupname, int &errc);

  // ---------------------------------------------------------------------------
  //! Convert a uid into a string
  // ---------------------------------------------------------------------------

  static std::string
  UidAsString (uid_t uid)
  {
    std::string uidstring = "";
    char suid[1024];
    snprintf(suid, sizeof (suid) - 1, "%u", uid);
    uidstring = suid;
    return uidstring;
  }

  // ---------------------------------------------------------------------------
  //! Convert a gid into a string
  // ---------------------------------------------------------------------------

  static std::string
  GidAsString (gid_t gid)
  {
    std::string gidstring = "";
    char sgid[1024];
    snprintf(sgid, sizeof (sgid) - 1, "%u", gid);
    gidstring = sgid;
    return gidstring;
  }

};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif
