// ----------------------------------------------------------------------
// File: Mapping.cc
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
#include "common/Namespace.hh"
#include "common/Mapping.hh"
#include "common/Logging.hh"
#include "common/SecEntity.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysDNS.hh"
#include <regex>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
// global mapping objects
/*----------------------------------------------------------------------------*/
RWMutex Mapping::gMapMutex;
XrdSysMutex Mapping::gPhysicalIdMutex;

Mapping::UserRoleMap_t Mapping::gUserRoleVector;
Mapping::GroupRoleMap_t Mapping::gGroupRoleVector;
Mapping::VirtualUserMap_t Mapping::gVirtualUidMap;
Mapping::VirtualGroupMap_t Mapping::gVirtualGidMap;
Mapping::SudoerMap_t Mapping::gSudoerMap;
bool Mapping::gRootSquash = true;

Mapping::GeoLocationMap_t Mapping::gGeoMap;

Mapping::AllowedTidentMatches_t Mapping::gAllowedTidentMatches;

XrdSysMutex Mapping::ActiveLock;
google::dense_hash_map<std::string, time_t> Mapping::ActiveTidents;

XrdOucHash<Mapping::id_pair> Mapping::gPhysicalUidCache;
XrdOucHash<Mapping::gid_vector> Mapping::gPhysicalGidCache;

XrdSysMutex Mapping::gPhysicalNameCacheMutex;
std::map<uid_t, std::string> Mapping::gPhysicalUserNameCache;
std::map<gid_t, std::string> Mapping::gPhysicalGroupNameCache;


/*----------------------------------------------------------------------------*/
/** 
 * Initialize Google maps
 * 
 */

/*----------------------------------------------------------------------------*/

void
Mapping::Init ()
{
  ActiveTidents.set_empty_key("");
  ActiveTidents.set_deleted_key("#__DELETED__#");
}

/*----------------------------------------------------------------------------*/
/** 
 * Expire Active client entries which have not been used since interval
 * 
 * @param interval seconds of idle time for expiration
 */

/*----------------------------------------------------------------------------*/
void
Mapping::ActiveExpire (int interval, bool force)
{
  static time_t expire = 0;
  // needs to have Active Lock locked
  time_t now = time(NULL);
  if (force || (now > expire))
  {
    // expire tidents older than interval
    google::dense_hash_map<std::string, time_t>::iterator it1;
    google::dense_hash_map<std::string, time_t>::iterator it2;
    for (it1 = Mapping::ActiveTidents.begin(); it1 != Mapping::ActiveTidents.end();)
    {
      if ((now - it1->second) > interval)
      {
        it2 = it1;
        it1++;
        Mapping::ActiveTidents.erase(it2);
      }
      else
      {
        it1++;
      }
    }
    Mapping::ActiveTidents.resize(0);
    expire = now + 1800;
  }
}

/*----------------------------------------------------------------------------*/
/** 
 * Map a client to its virtual identity
 * 
 * @param client xrootd client authenticatino object
 * @param env opaque information containing role selection like 'eos.ruid' and 'eos.rgid'
 * @param tident trace identifier of the client
 * @param vid returned virtual identity
 */

/*----------------------------------------------------------------------------*/
void
Mapping::IdMap (const XrdSecEntity* client, const char* env, const char* tident, Mapping::VirtualIdentity &vid, bool log)
{
  if (!client)
    return;

  eos_static_debug("name:%s role:%s group:%s tident:%s", client->name, client->role, client->grps, client->tident);

  // you first are 'nobody'
  Nobody(vid);
  XrdOucEnv Env(env);

  vid.name = client->name;
  vid.tident = tident;
  vid.sudoer = false;

  // first map by alias
  XrdOucString useralias = client->prot;
  useralias += ":";
  useralias += "\"";
  useralias += client->name;
  useralias += "\"";
  useralias += ":";
  XrdOucString groupalias = useralias;
  useralias += "uid";
  groupalias += "gid";

  RWMutexReadLock lock(gMapMutex);

  vid.prot = client->prot;

  // ---------------------------------------------------------------------------
  // kerberos mapping
  // ---------------------------------------------------------------------------
  if ((vid.prot == "krb5"))
  {
    eos_static_debug("krb5 mapping");
    if (gVirtualUidMap.count("krb5:\"<pwd>\":uid"))
    {
      // use physical mapping for kerberos names
      Mapping::getPhysicalIds(client->name, vid);
      vid.gid = 99;
      vid.gid_list.clear();
    }

    if (gVirtualGidMap.count("krb5:\"<pwd>\":gid"))
    {
      // use physical mapping for kerberos names
      uid_t uid = vid.uid;
      Mapping::getPhysicalIds(client->name, vid);
      vid.uid = uid;
      vid.uid_list.clear();
      vid.uid_list.push_back(uid);
      vid.uid_list.push_back(99);
    }
  }

  // ---------------------------------------------------------------------------
  // gsi mapping
  // ---------------------------------------------------------------------------
  if ((vid.prot == "gsi"))
  {
    eos_static_debug("gsi mapping");
    if (gVirtualUidMap.count("gsi:\"<pwd>\":uid"))
    {
      // use physical mapping for gsi names
      Mapping::getPhysicalIds(client->name, vid);
      vid.gid = 99;
      vid.gid_list.clear();
    }

    if (gVirtualGidMap.count("gsi:\"<pwd>\":gid"))
    {
      // use physical mapping for gsi names
      uid_t uid = vid.uid;
      Mapping::getPhysicalIds(client->name, vid);
      vid.uid = uid;
      vid.uid_list.clear();
      vid.uid_list.push_back(uid);
      vid.uid_list.push_back(99);
    }

    // ---------------------------------------------------------------------------
    // VOMS mapping
    // ---------------------------------------------------------------------------
    if (client->grps)
    {
      std::string vomsstring = "voms:\"";
      vomsstring += client->grps;
      vomsstring += ":";
      vid.grps = client->grps;
      if (client->role)
      {
        // the role might be NULL
        vomsstring += client->role;
        vid.role = client->role;
      }
      vomsstring += "\"";
      std::string vomsuidstring = vomsstring;
      std::string vomsgidstring = vomsstring;
      vomsuidstring += ":uid";
      vomsgidstring += ":gid";

      // mapping to user
      if (gVirtualUidMap.count(vomsuidstring))
      {
        vid.uid_list.clear();
        vid.gid_list.clear();

        // use physical mapping for VOMS roles
        std::string cname = "";
        // convert mapped uid to user name
        int errc = 0;
        cname = Mapping::UidToUserName(gVirtualUidMap[vomsuidstring], errc);
        if (!errc)
        {
          Mapping::getPhysicalIds(cname.c_str(), vid);
        }
        else
        {
          Nobody(vid);
          eos_static_err("voms-mapping: cannot translate uid=%d to user name with the password db", (int) gVirtualUidMap[vomsuidstring]);
        }
      }

      // mapping to group
      if (gVirtualGidMap.count(vomsgidstring))
      {
        // use group mapping for VOMS roles
        vid.gid_list.clear();
        vid.gid = gVirtualGidMap[vomsgidstring];
        vid.gid_list.push_back(vid.gid);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // gsi mapping
  // ---------------------------------------------------------------------------
  if ((vid.prot == "https"))
  {
    eos_static_debug("https mapping");
    if (gVirtualUidMap.count("https:\"<pwd>\":uid"))
    {
      if (gVirtualGidMap["https:\"<pwd>\":uid"] == 0)
      {
        // use physical mapping for https names
        Mapping::getPhysicalIds(client->name, vid);
        vid.gid = 99;
        vid.gid_list.clear();
      }
      else
      {
        vid.uid_list.clear();
        vid.uid_list.push_back(gVirtualGidMap["https:\"<pwd>\":uid"]);
        vid.uid_list.push_back(99);
        vid.gid = 99;
        vid.gid_list.clear();
      }
    }

    if (gVirtualGidMap.count("https:\"<pwd>\":gid"))
    {
      if (gVirtualGidMap["https:\"<pwd>\":gid"] == 0)
      {
        // use physical mapping for gsi names
        uid_t uid = vid.uid;
        Mapping::getPhysicalIds(client->name, vid);
        vid.uid = uid;
        vid.uid_list.clear();
        vid.uid_list.push_back(uid);
        vid.uid_list.push_back(99);
      }
      else
      {
        vid.gid_list.clear();
        vid.gid_list.push_back(gVirtualGidMap["https:\"<pwd>\":gid"]);
        vid.gid_list.push_back(99);
      }
    }
  }
  // ---------------------------------------------------------------------------
  // sss mapping
  // ---------------------------------------------------------------------------
  if ((vid.prot == "sss"))
  {
    eos_static_debug("sss mapping");
    if (gVirtualUidMap.count("sss:\"<pwd>\":uid"))
    {
      if (gVirtualUidMap["sss:\"<pwd>\":uid"] == 0)
      {
        eos_static_debug("sss uid mapping");
        Mapping::getPhysicalIds(client->name, vid);
        vid.gid = 99;
        vid.gid_list.clear();
      }
      else
      {
        eos_static_debug("sss uid forced mapping");
        // map to the requested id
        vid.uid_list.clear();
        vid.uid = gVirtualUidMap["sss:\"<pwd>\":uid"];
        vid.uid_list.push_back(vid.uid);
        if (vid.uid != 99)
          vid.uid_list.push_back(99);
        vid.gid_list.clear();
        vid.gid = 99;
        vid.gid_list.push_back(99);
      }
    }

    if (gVirtualGidMap.count("sss:\"<pwd>\":gid"))
    {
      if (gVirtualGidMap["sss:\"<pwd>\":gid"] == 0)
      {
        eos_static_debug("sss gid mapping");
        // use physical mapping for sss names
        uid_t uid = vid.uid;
        Mapping::getPhysicalIds(client->name, vid);

        vid.uid = uid;
        vid.uid_list.clear();
        vid.uid_list.push_back(uid);
        vid.uid_list.push_back(99);
      }
      else
      {
        eos_static_debug("sss forced gid mapping");
        // map to the requested id
        vid.gid_list.clear();
        vid.gid = gVirtualGidMap["sss:\"<pwd>\":gid"];
        vid.gid_list.push_back(vid.gid);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // unix mapping
  // ---------------------------------------------------------------------------
  if ((vid.prot == "unix"))
  {
    eos_static_debug("unix mapping");
    if (gVirtualUidMap.count("unix:\"<pwd>\":uid"))
    {
      if (gVirtualUidMap["unix:\"<pwd>\":uid"] == 0)
      {
        eos_static_debug("unix uid mapping");
        // use physical mapping for unix names
        Mapping::getPhysicalIds(client->name, vid);
        vid.gid = 99;
        vid.gid_list.clear();
      }
      else
      {
        eos_static_debug("unix uid forced mapping");
        // map to the requested id
        vid.uid_list.clear();
        vid.uid = gVirtualUidMap["unix:\"<pwd>\":uid"];
        vid.uid_list.push_back(vid.uid);
        if (vid.uid != 99)
          vid.uid_list.push_back(99);
        vid.gid_list.clear();
        vid.gid = 99;
        vid.gid_list.push_back(99);
      }
    }

    if (gVirtualGidMap.count("unix:\"<pwd>\":gid"))
    {
      if (gVirtualGidMap["unix:\"<pwd>\":gid"] == 0)
      {
        eos_static_debug("unix gid mapping");
        // use physical mapping for unix names
        uid_t uid = vid.uid;
        Mapping::getPhysicalIds(client->name, vid);
        vid.uid = uid;
        vid.uid_list.clear();
        vid.uid_list.push_back(uid);
        vid.uid_list.push_back(99);
      }
      else
      {
        eos_static_debug("unix forced gid mapping");
        // map to the requested id
        vid.gid_list.clear();
        vid.gid = gVirtualGidMap["unix:\"<pwd>\":gid"];
        vid.gid_list.push_back(vid.gid);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // tident mapping
  // ---------------------------------------------------------------------------
  XrdOucString mytident = "";
  XrdOucString myrole = "";
  XrdOucString wildcardtident = "";
  XrdOucString host = "";
  XrdOucString stident = "tident:";
  stident += "\"";
  stident += ReduceTident(vid.tident, wildcardtident, mytident, host);

  if (host == "127.0.0.1")
  {
    host = "localhost";
  }

  myrole = mytident;
  myrole.erase(mytident.find("@"));
  // FUSE select's now the role via <uid>[:connectionid]
  // the connection id is already removed by ReduceTident
  myrole.erase(myrole.find("."));

  XrdOucString swctident = "tident:";
  swctident += "\"";
  swctident += wildcardtident;
  XrdOucString suidtident = stident;
  suidtident += "\":uid";
  XrdOucString sgidtident = stident;
  sgidtident += "\":gid";
  XrdOucString swcuidtident = swctident;
  swcuidtident += "\":uid";
  XrdOucString swcgidtident = swctident;
  swcgidtident += "\":gid";

  XrdOucString sprotuidtident = swcuidtident;
  XrdOucString sprotgidtident = swcgidtident;
  sprotuidtident.replace("*", vid.prot); // there can be a protocol specific rule like sss:@<host>:uid...
  sprotgidtident.replace("*", vid.prot); // there can be a protocol specific rule like sss:@<host>:gid...

  eos_static_debug("swcuidtident=%s sprotuidtident=%s myrole=%s", swcuidtident.c_str(), sprotuidtident.c_str(), myrole.c_str());

  if ((gVirtualUidMap.count(suidtident.c_str())))
  {
    //    eos_static_debug("tident mapping");
    vid.uid = gVirtualUidMap[suidtident.c_str()];
    if (!HasUid(vid.uid, vid.uid_list)) vid.uid_list.push_back(vid.uid);
    if (!HasUid(99, vid.uid_list)) vid.uid_list.push_back(99);
  }

  if ((gVirtualGidMap.count(sgidtident.c_str())))
  {
    //    eos_static_debug("tident mapping");
    vid.gid = gVirtualGidMap[sgidtident.c_str()];
    if (!HasGid(vid.gid, vid.gid_list)) vid.gid_list.push_back(vid.gid);
    if (!HasGid(99, vid.gid_list)) vid.gid_list.push_back(99);
  }

  // ---------------------------------------------------------------------------
  // wild card tidents/protocol tidents
  // one can define mapping entries like '*@host:uid=>0' e.g. for fuse mounts
  // or only for a certain protocol like 'sss@host:uid=>0'
  // ---------------------------------------------------------------------------

  XrdOucString tuid = "";
  XrdOucString tgid = "";

  if (gVirtualUidMap.count(swcuidtident.c_str()))
  {
    // there is an entry like "*@<host:uid" matching all protocols
    tuid = swcuidtident.c_str();
  }
  else
  {
    if (gVirtualUidMap.count(sprotuidtident.c_str()))
    {
      // there is a protocol specific entry "<prot>@<host>:uid"
      tuid = sprotuidtident.c_str();
    }
    else
    {
      if ( gAllowedTidentMatches.size() )
      {
        std::string sprot=vid.prot.c_str();
        for (auto it = gAllowedTidentMatches.begin(); it != gAllowedTidentMatches.end(); ++it)
        {
          if ( sprot != it->first.c_str() )
            continue;

          if ( host.matches(it->second.c_str()) )
          {
            sprotuidtident.replace(host.c_str(), it->second.c_str());
            if ( gVirtualUidMap.count(sprotuidtident.c_str()) )
            {
              tuid = sprotuidtident.c_str();
              break;
            }
          }
        }
      }
    }
  }

  if (gVirtualGidMap.count(swcgidtident.c_str()))
  {
    // there is an entry like "*@<host>:gid" matching all protocols
    tgid = swcgidtident.c_str();
  }
  else
  {
    if (gVirtualGidMap.count(sprotgidtident.c_str()))
    {
      // there is a protocol specific entry "<prot>@<host>:uid"
      tgid = sprotgidtident.c_str();
    }
    else
    {
      if ( gAllowedTidentMatches.size() )
      {
        std::string sprot=vid.prot.c_str();
        for (auto it = gAllowedTidentMatches.begin(); it != gAllowedTidentMatches.end(); ++it)
        {
          if ( sprot != it->first.c_str() )
            continue;

          if ( host.matches(it->second.c_str()) )
          {
            sprotuidtident.replace(host.c_str(), it->second.c_str());
            if ( gVirtualUidMap.count(sprotuidtident.c_str()) )
            {
              tuid = sprotuidtident.c_str();
              break;
            }
          }
        }
      }
    }
  }

  eos_static_debug("tuid=%s tgid=%s", tuid.c_str(), tgid.c_str());

  if (gVirtualUidMap.count(tuid.c_str()))
  {
    if (!gVirtualUidMap[tuid.c_str()])
    {
      if (gRootSquash && (host != "localhost") && (host != "localhost.localdomain") && (vid.name == "root") && (myrole == "root"))
      {
        eos_static_debug("tident root uid squash");
        vid.uid_list.clear();
        vid.uid_list.push_back(2);
        vid.uid = 2;
        vid.gid_list.clear();
        vid.gid = 2;
        vid.gid_list.push_back(2);
      }
      else
      {
        eos_static_debug("tident uid mapping");
        vid.uid_list.clear();
        // use physical mapping 

        // unix protocol maps to the role if the client is the root account
        // otherwise it maps to the unix ID on the client host
        if (((vid.prot == "unix") && (vid.name == "root")) ||
            ((vid.prot == "sss") && (vid.name == "daemon")))
          Mapping::getPhysicalIds(myrole.c_str(), vid);
        else
          Mapping::getPhysicalIds(client->name, vid);

      }
    }
    else
    {
      eos_static_debug("tident uid forced mapping");
      // map to the requested id
      vid.uid_list.clear();
      vid.uid = gVirtualUidMap[tuid.c_str()];
      vid.uid_list.push_back(vid.uid);
      if (vid.uid != 99)
        vid.uid_list.push_back(99);
      vid.gid_list.clear();
      vid.gid = 99;
      vid.gid_list.push_back(99);
    }
  }

  if (gVirtualGidMap.count(tgid.c_str()))
  {
    if (!gVirtualGidMap[tgid.c_str()])
    {
      if (gRootSquash && (host != "localhost") && (host != "localhost.localdomain") && (vid.name == "root") && (myrole == "root"))
      {
        eos_static_debug("tident root gid squash");
        vid.gid_list.clear();
        vid.gid_list.push_back(2);
        vid.gid = 2;
      }
      else
      {
        eos_static_debug("tident gid mapping");
        uid_t uid = vid.uid;

        if (((vid.prot == "unix") && (vid.name == "root")) ||
            ((vid.prot == "sss") && (vid.name == "daemon")))
          Mapping::getPhysicalIds(myrole.c_str(), vid);
        else
          Mapping::getPhysicalIds(client->name, vid);

        vid.uid = uid;
        vid.uid_list.clear();
        vid.uid_list.push_back(uid);
        vid.uid_list.push_back(99);
      }
    }
    else
    {
      eos_static_debug("tident gid forced mapping");
      // map to the requested id
      vid.gid_list.clear();
      vid.gid = gVirtualGidMap[tgid.c_str()];
      vid.gid_list.push_back(vid.gid);
    }
  }

  eos_static_debug("suidtident:%s sgidtident:%s", suidtident.c_str(), sgidtident.c_str());

  // ---------------------------------------------------------------------------
  // the configuration door for localhost clients adds always the adm/adm vid's
  // ---------------------------------------------------------------------------
  if ((suidtident == "tident:\"root@localhost.localdomain\":uid") ||
      (suidtident == "tident:\"root@localhost\":uid"))
  {
    vid.sudoer = true;
    vid.uid = 3;
    vid.gid = 4;
    if (!HasUid(3, vid.uid_list)) vid.uid_list.push_back(vid.uid);
    if (!HasGid(4, vid.gid_list)) vid.gid_list.push_back(vid.gid);
  }

  // ---------------------------------------------------------------------------
  // explicit virtual mapping overrules physical mappings - the second one comes from the physical mapping before
  // ---------------------------------------------------------------------------
  vid.uid = (gVirtualUidMap.count(useralias.c_str())) ? gVirtualUidMap[useralias.c_str() ] : vid.uid;
  if (!HasUid(vid.uid, vid.uid_list)) vid.uid_list.insert(vid.uid_list.begin(), vid.uid);
  vid.gid = (gVirtualGidMap.count(groupalias.c_str())) ? gVirtualGidMap[groupalias.c_str()] : vid.gid;

  // eos_static_debug("mapped %d %d", vid.uid,vid.gid);

  if (!HasGid(vid.gid, vid.gid_list)) vid.gid_list.insert(vid.gid_list.begin(), vid.gid);

  // ---------------------------------------------------------------------------
  // add virtual user and group roles - if any 
  // ---------------------------------------------------------------------------
  if (gUserRoleVector.count(vid.uid))
  {
    uid_vector::const_iterator it;
    for (it = gUserRoleVector[vid.uid].begin(); it != gUserRoleVector[vid.uid].end(); ++it)
      if (!HasUid((*it), vid.uid_list)) vid.uid_list.push_back((*it));
  }

  if (gGroupRoleVector.count(vid.uid))
  {
    gid_vector::const_iterator it;
    for (it = gGroupRoleVector[vid.uid].begin(); it != gGroupRoleVector[vid.uid].end(); ++it)
      if (!HasGid((*it), vid.gid_list)) vid.gid_list.push_back((*it));
  }

  // ---------------------------------------------------------------------------
  // Environment selected roles
  // ---------------------------------------------------------------------------
  XrdOucString ruid = Env.Get("eos.ruid");
  XrdOucString rgid = Env.Get("eos.rgid");
  uid_t sel_uid = vid.uid;
  uid_t sel_gid = vid.gid;

  if (ruid.length())
  {
    if (!IsUid(ruid, sel_uid))
    {
      int errc=0;
      // try alias conversion
      std::string luid=ruid.c_str();
      sel_uid = (gVirtualUidMap.count(ruid.c_str())) ? gVirtualUidMap[ruid.c_str() ] : 99;
      if (sel_uid == 99)
        sel_uid = UserNameToUid(luid, errc);
      if (errc)
	sel_uid=99;
    }
  }

  if (rgid.length())
  {
    if (!IsGid(rgid, sel_gid))
    {
      int errc=0;
      // try alias conversion
      std::string lgid=rgid.c_str();
      sel_gid = (gVirtualGidMap.count(rgid.c_str())) ? gVirtualGidMap[rgid.c_str()] : 99;
      if (sel_gid == 99)
	sel_gid = GroupNameToGid(lgid, errc);
      if (errc)
	sel_gid=99;
    }
  }

  // ---------------------------------------------------------------------------    
  // Sudoer flag setting
  // ---------------------------------------------------------------------------
  if (gSudoerMap.count(vid.uid))
  {
    vid.sudoer = true;
  }

  // ---------------------------------------------------------------------------
  // Check if we are allowed to take sel_uid & sel_gid
  // ---------------------------------------------------------------------------
  if (!vid.sudoer)
  {
    // if we are not a sudore, scan the allowed ids
    if (HasUid(sel_uid, vid.uid_list))
      vid.uid = sel_uid;
    else
      vid.uid = 99;

    if (HasGid(sel_gid, vid.gid_list))
      vid.gid = sel_gid;
    else
      vid.gid = 99;
  }
  else
  {
    vid.uid = sel_uid;
    vid.gid = sel_gid;
  }

  if (client->host)
    vid.host = client->host;
  else
    vid.host = host.c_str();

  {
    int errc = 0;
    // add the uid/gid as strings
    vid.uid_string = UidToUserName(vid.uid, errc);
    vid.gid_string = GidToGroupName(vid.gid, errc);
  }

  time_t now = time(NULL);

  // ---------------------------------------------------------------------------    
  // Check the Geo Location
  // ---------------------------------------------------------------------------
  if ((!vid.geolocation.length()) && (gGeoMap.size()))
  {
    // if the geo location was not set externally and we have some recipe we try 
    // to translate the host name and match a rule
    unsigned int ipaddr;

    // if we have a default geo location we assume that a client in that one
    if (gGeoMap.count("default"))
    {
      vid.geolocation = gGeoMap["default"];
    }
    if (XrdSysDNS::Host2IP(host.c_str(), &ipaddr) == 1)
    {
      char ipstring[64];
      int hostlen = XrdSysDNS::IP2String(ipaddr, 0, ipstring, 64);
      if (hostlen > 0)
      {
        std::string sipstring = ipstring;
        GeoLocationMap_t::const_iterator it;
        // we use the geo location with the longest name match
        for (it = gGeoMap.begin(); it != gGeoMap.end(); it++)
        {
          size_t l = 0;
          for (l = 0; l < it->first.length(); l++)
          {
            if (it->first.at(l) != sipstring.at(l))
            {
              break;
            }
          }
          if (l > vid.geolocation.length())
          {
            vid.geolocation = it->second;
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Maintain the active client map and expire old entries
  // ---------------------------------------------------------------------------
  ActiveLock.Lock();

  // ---------------------------------------------------------------------------
  // safty measures not to exceed memory by 'nasty' clients
  // ---------------------------------------------------------------------------
  if (ActiveTidents.size() > 25000)
  {
    ActiveExpire();
  }
  if (ActiveTidents.size() < 60000)
  {
    char actident[1024];
    snprintf(actident, sizeof (actident) - 1, "%d^%s^%s^%s", vid.uid, mytident.c_str(), vid.prot.c_str(), vid.host.c_str());
    std::string intident = actident;
    ActiveTidents[intident] = now;
  }
  ActiveLock.UnLock();

  eos_static_debug("selected %d %d [%s %s]", vid.uid, vid.gid, ruid.c_str(), rgid.c_str());
  if (log)
  {
    eos_static_info("%s sec.tident=\"%s\"", eos::common::SecEntity::ToString(client, Env.Get("eos.app")).c_str(), tident);
  }
}

/*----------------------------------------------------------------------------*/
/** 
 * Print the current mappings
 * 
 * @param stdOut the output is stored here
 * @param option can be 'u' for user role mappings 'g' for group role mappings 's' for sudoer list 'U' for user alias mapping 'G' for group alias mapping 'y' for gateway mappings (tidents) 'a' for authentication mapping rules 'l' for geo location rules
 */

/*----------------------------------------------------------------------------*/
void
Mapping::Print (XrdOucString &stdOut, XrdOucString option)
{
  bool translateids = true;
  if (option.find("n") != STR_NPOS)
  {
    translateids = false;
    option.replace("n", "");
  }

  if ((!option.length()) || ((option.find("u")) != STR_NPOS))
  {
    UserRoleMap_t::const_iterator it;
    for (it = gUserRoleVector.begin(); it != gUserRoleVector.end(); ++it)
    {
      char iuid[4096];
      sprintf(iuid, "%d", it->first);
      char suid[4096];
      sprintf(suid, "%-6s", iuid);
      if (translateids)
      {
        int errc = 0;
        std::string username = UidToUserName(it->first, errc);
        if (!errc)
          sprintf(suid, "%-12s", username.c_str());
      }
      stdOut += "membership uid: ";
      stdOut += suid;
      stdOut += " => uids(";
      for (unsigned int i = 0; i < (it->second).size(); i++)
      {
        if (translateids)
        {
          int errc = 0;
          std::string username = UidToUserName(it->second[i], errc);
          if (!errc)
            stdOut += username.c_str();
          else
            stdOut += (int) (it->second)[i];
        }
        else
        {
          stdOut += (int) (it->second)[i];
        }
        if (i < ((it->second).size() - 1))
          stdOut += ",";
      }
      stdOut += ")\n";
    }
  }

  if ((!option.length()) || ((option.find("g")) != STR_NPOS))
  {
    UserRoleMap_t::const_iterator it;
    for (it = gGroupRoleVector.begin(); it != gGroupRoleVector.end(); ++it)
    {
      char iuid[4096];
      sprintf(iuid, "%d", it->first);
      char suid[4096];
      sprintf(suid, "%-6s", iuid);
      if (translateids)
      {
        int errc = 0;
        std::string username = UidToUserName(it->first, errc);
        if (!errc)
          sprintf(suid, "%-12s", username.c_str());
      }
      stdOut += "membership uid: ";
      stdOut += suid;
      stdOut += " => gids(";
      for (unsigned int i = 0; i < (it->second).size(); i++)
      {
        if (translateids)
        {
          int errc = 0;
          std::string username = GidToGroupName(it->second[i], errc);
          if (!errc)
            stdOut += username.c_str();
          else
            stdOut += (int) (it->second)[i];
        }
        else
        {
          stdOut += (int) (it->second)[i];
        }
        if (i < ((it->second).size() - 1))
          stdOut += ",";
      }
      stdOut += ")\n";
    }
  }

  if ((!option.length()) || ((option.find("s")) != STR_NPOS))
  {
    SudoerMap_t::const_iterator it;
    // print sudoer line
    stdOut += "sudoer                 => uids(";
    for (it = gSudoerMap.begin(); it != gSudoerMap.end(); ++it)
    {
      if (it->second)
      {
        int errc = 0;
        std::string username = UidToUserName(it->first, errc);
        if (!errc && translateids)
          stdOut += username.c_str();
        else
          stdOut += (int) (it->first);
        stdOut += ",";
      }
    }
    if (stdOut.endswith(","))
    {
      stdOut.erase(stdOut.length() - 1);
    }
    stdOut += ")\n";
  }

  if ((!option.length()) || ((option.find("U")) != STR_NPOS))
  {
    VirtualUserMap_t::const_iterator it;
    for (it = gVirtualUidMap.begin(); it != gVirtualUidMap.end(); ++it)
    {
      stdOut += it->first.c_str();
      stdOut += " => ";
      int errc = 0;
      std::string username = UidToUserName(it->second, errc);
      if (!errc && translateids)
        stdOut += username.c_str();
      else
        stdOut += (int) it->second;
      stdOut += "\n";
    }
  }

  if ((!option.length()) || ((option.find("G")) != STR_NPOS))
  {
    VirtualGroupMap_t::const_iterator it;
    for (it = gVirtualGidMap.begin(); it != gVirtualGidMap.end(); ++it)
    {
      stdOut += it->first.c_str();
      stdOut += " => ";
      int errc = 0;
      std::string groupname = GidToGroupName(it->second, errc);
      if (!errc && translateids)
        stdOut += groupname.c_str();
      else
        stdOut += (int) it->second;
      stdOut += "\n";
    }
  }

  if (((option.find("y")) != STR_NPOS))
  {
    VirtualUserMap_t::const_iterator it;
    for (it = gVirtualUidMap.begin(); it != gVirtualUidMap.end(); ++it)
    {
      if (!it->second)
      {
        XrdOucString authmethod = it->first.c_str();
        if (!authmethod.beginswith("tident:"))
          continue;
        int dpos = authmethod.find("@");
        authmethod.erase(0, dpos + 1);
        dpos = authmethod.find("\"");
        authmethod.erase(dpos);
        stdOut += "gateway=";
        stdOut += authmethod;
        stdOut += "\n";
      }
    }
  }

  if (((option.find("a")) != STR_NPOS))
  {
    VirtualUserMap_t::const_iterator it;
    for (it = gVirtualUidMap.begin(); it != gVirtualUidMap.end(); ++it)
    {
      if (!it->second)
      {
        XrdOucString authmethod = it->first.c_str();
        if (authmethod.beginswith("tident:"))
          continue;
        int dpos = authmethod.find(":");
        authmethod.erase(dpos);
        stdOut += "auth=";
        stdOut += authmethod;
        stdOut += "\n";
      }
    }
  }

  if ((!option.length()) || ((option.find("l")) != STR_NPOS))
  {
    eos::common::Mapping::GeoLocationMap_t::const_iterator it;
    for (it = gGeoMap.begin(); it != gGeoMap.end(); it++)
    {
      char sline[1024];
      snprintf(sline, sizeof (sline) - 1, "geotag:\"%s\" => \"%s\"\n", it->first.c_str(), it->second.c_str());
      stdOut += sline;
    }
  }

  if ((!option.length()))
  {
    for ( auto it = gAllowedTidentMatches.begin(); it != gAllowedTidentMatches.end();  ++it)
    {
      char sline[1024];
      snprintf(sline, sizeof (sline) - 1, "hostmatch:\"protocol=%s pattern=%s\n", it->first.c_str(), it->second.c_str());
      stdOut += sline;
    }
  }
}

/*----------------------------------------------------------------------------*/
/** 
 * Store the physical Ids for name in the virtual identity
 * 
 * @param name user name
 * @param vid virtual identity to store
 */

/*----------------------------------------------------------------------------*/
void
Mapping::getPhysicalIds (const char* name, VirtualIdentity &vid)
{
  struct passwd passwdinfo;
  char buffer[131072];

  if (!name)
    return;

  gid_vector * gv;
  id_pair * id;

  memset(&passwdinfo, 0, sizeof (passwdinfo));

  eos_static_debug("find in uid cache %s", name);

  XrdSysMutexHelper cLock(gPhysicalIdMutex);

  // cache short cut's
  if (!(id = gPhysicalUidCache.Find(name)))
  {
    eos_static_debug("not found in uid cache");

    XrdOucString sname = name;
    bool use_pw=true;
    if (sname.length() == 8)
    {
      // -------------------------------------------------------------------------
      // check if name is a 8 digit hex number indication <uid-hex><gid-hex>
      // -------------------------------------------------------------------------
      unsigned long long hexid = strtoull(sname.c_str(), 0, 16);
      char rhexid[16];
      snprintf(rhexid, sizeof (rhexid) - 1, "%08llx", hexid);
      eos_static_debug("hexname=%s hexid=%llu name=%s", rhexid, hexid, name);
      if (sname == rhexid)
      {
        // that is a hex id
        XrdOucString suid = sname;
        suid.erase(4);
        XrdOucString sgid = sname;
        sgid.erase(0, 4);

        id = new id_pair(strtol(suid.c_str(), 0, 16), strtol(sgid.c_str(), 0, 16));
        eos_static_debug("using hexmapping %s %d %d", sname.c_str(), id->uid, id->gid);
        if (!id->uid || !id->gid)
          return;

        vid.uid = id->uid;
        vid.gid = id->gid;
        vid.uid_list.clear();
        vid.uid_list.push_back(vid.uid);
        vid.gid_list.clear();
        vid.gid_list.push_back(vid.gid);

        gid_vector* vec = new uid_vector;
        *vec = vid.gid_list;

        gPhysicalUidCache.Add(name, id, 3600);
        eos_static_debug("adding to cache uid=%u gid=%u", id->uid, id->gid);
        gPhysicalGidCache.Add(name, vec, 3600);
	use_pw = false;
      }
    }
    if (use_pw)
    {
      struct passwd *pwbufp = 0;

      if (getpwnam_r(name, &passwdinfo, buffer, 16384, &pwbufp) || (!pwbufp))
      {
        return;
      }
      id = new id_pair(passwdinfo.pw_uid, passwdinfo.pw_gid);
      gPhysicalUidCache.Add(name, id, 3600);
      eos_static_debug("adding to cache uid=%u gid=%u", id->uid, id->gid);
    }
  };

  vid.uid = id->uid;
  vid.gid = id->gid;

  if ((gv = gPhysicalGidCache.Find(name)))
  {

    vid.uid_list.push_back(id->uid);
    vid.gid_list = *gv;
    vid.uid = id->uid;
    vid.gid = id->gid;
    eos_static_debug("returning uid=%u gid=%u", id->uid, id->gid);
    return;
  }


  // ----------------------------------------------------------------------------------------
  // TODO: Because of problems with the LDAP database we have commented the secondary group support
  // ----------------------------------------------------------------------------------------
  /* remove secondary searches in the database -> LDAP assertion
  struct group* gr;
  
  eos_static_debug("group lookup");
  gid_t gid = id->gid;

  setgrent();

  while( (gr = getgrent() ) ) {
  int cnt;
  cnt=0;
  if (gr->gr_gid == gid) {
  if (!vid.gid_list.size()) {
  vid.gid_list.push_back(gid);
  vid.gid = gid;
  }
  }

  while (gr->gr_mem[cnt]) {
  if (!strcmp(gr->gr_mem[cnt],name)) {
  vid.gid_list.push_back(gr->gr_gid);
  }
  cnt++;
  }
  }
  endgrent();

   */

  // add to the cache
  gid_vector* vec = new uid_vector;
  *vec = vid.gid_list;


  gPhysicalGidCache.Add(name, vec, 3600);

  return;
}

/*----------------------------------------------------------------------------*/
/** 
 * Convert uid to user name
 * 
 * @param uid unix user id
 * @param errc 0 if success, EINVAL if does not exist
 * 
 * @return user name as string
 */

/*----------------------------------------------------------------------------*/
std::string
Mapping::UidToUserName (uid_t uid, int &errc)
{
  errc = 0;
  XrdSysMutexHelper cMutex(gPhysicalNameCacheMutex);
  if (gPhysicalUserNameCache.count(uid))
  {
    return gPhysicalUserNameCache[uid];
  }
  else
  {
    char buffer[131072];
    int buflen = sizeof (buffer);
    std::string uid_string = "";
    struct passwd pwbuf;
    struct passwd *pwbufp = 0;
    if (getpwuid_r(uid, &pwbuf, buffer, buflen, &pwbufp) || (!pwbufp))
    {
      char suid[1024];
      snprintf(suid, sizeof (suid) - 1, "%u", uid);
      uid_string = suid;
      errc = EINVAL;
    }
    else
    {

      uid_string = pwbuf.pw_name;
      errc = 0;
    }
    gPhysicalUserNameCache[uid] = uid_string;
    return uid_string;
  }
}

/*----------------------------------------------------------------------------*/
/** 
 * Convert gid to group name
 * 
 * @param gid unix group id
 * @param errc 0 if success, EINVAL if does not exist
 * 
 * @return user name as string
 */

/*----------------------------------------------------------------------------*/
std::string
Mapping::GidToGroupName (gid_t gid, int &errc)
{
  errc = 0;
  XrdSysMutexHelper cMutex(gPhysicalNameCacheMutex);
  if (gPhysicalGroupNameCache.count(gid))
  {
    return gPhysicalGroupNameCache[gid];
  }
  else
  {
    char buffer[131072];
    int buflen = sizeof (buffer);
    struct group grbuf;
    struct group *grbufp = 0;
    std::string gid_string = "";

    if (getgrgid_r(gid, &grbuf, buffer, buflen, &grbufp) || (!grbufp))
    {
      // cannot translate this name
      char sgid[1024];
      snprintf(sgid, sizeof (sgid) - 1, "%u", gid);
      gid_string = sgid;
      errc = EINVAL;
    }
    else
    {

      gid_string = grbuf.gr_name;
      errc = 0;
    }
    gPhysicalGroupNameCache[gid] = gid_string;
    return gid_string;
  }
}

/*----------------------------------------------------------------------------*/
/** 
 * Convert string name to uid
 * 
 * @param username name as string
 * @param errc 0 if success, EINVAL if does not exist
 * 
 * @return user id
 */

/*----------------------------------------------------------------------------*/
uid_t
Mapping::UserNameToUid (std::string &username, int &errc)
{
  char buffer[131072];
  int buflen = sizeof (buffer);
  uid_t uid = 99;
  struct passwd pwbuf;
  struct passwd *pwbufp = 0;
  errc = 0;
  getpwnam_r(username.c_str(), &pwbuf, buffer, buflen, &pwbufp);
  if (!pwbufp)
  {
    bool is_number = true;
    for (size_t i=0; i< username.length(); i++) 
    {
      if (!isdigit(username[i]))
      {
	is_number = false;
	break;
      }
    }

    uid = atoi(username.c_str());
    if ( (uid != 0) && (is_number) )
      errc = 0;
    else
    {
      errc = EINVAL;
      uid = 99;
    }
  }
  else
  {

    uid = pwbuf.pw_uid;
    errc = 0;
  }
  return uid;
}

/*----------------------------------------------------------------------------*/
/** 
 * Convert string name to gid
 * 
 * @param groupname name as string
 * @param errc 0 if success, EINVAL if does not exist
 * 
 * @return group id
 */

/*----------------------------------------------------------------------------*/
gid_t
Mapping::GroupNameToGid (std::string &groupname, int &errc)
{
  char buffer[131072];
  int buflen = sizeof (buffer);
  struct group grbuf;
  struct group *grbufp = 0;
  gid_t gid = 99;
  errc = 0;

  getgrnam_r(groupname.c_str(), &grbuf, buffer, buflen, &grbufp);
  if (!grbufp)
  {
    // cannot translate this name
    gid = atoi(groupname.c_str());
    if (gid != 0)
      errc = 0;
    else
    {
      errc = EINVAL;
      gid = 99;
    }
  }
  else
  {
    gid = grbuf.gr_gid;
    errc = 0;
  }
  return gid;
}


/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

