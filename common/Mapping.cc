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

#include "common/Namespace.hh"
#include "common/Mapping.hh"
#include "common/Macros.hh"
#include "common/Logging.hh"
#include "common/SecEntity.hh"
#include "common/SymKeys.hh"
#include "common/StringUtils.hh"
#include "common/token/EosTok.hh"
#include <XrdNet/XrdNetUtils.hh>
#include <XrdNet/XrdNetAddr.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSec/XrdSecEntityAttr.hh>
#include <XrdAcc/XrdAccAuthorize.hh>
#include <pwd.h>
#include <grp.h>
#include <sys/stat.h>
#include <jwt-cpp/jwt.h>

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
// global mapping objects
/*----------------------------------------------------------------------------*/
RWMutex Mapping::gMapMutex;

Mapping::UserRoleMap_t Mapping::gUserRoleVector;
Mapping::GroupRoleMap_t Mapping::gGroupRoleVector;
Mapping::VirtualUserMap_t Mapping::gVirtualUidMap;
Mapping::VirtualGroupMap_t Mapping::gVirtualGidMap;
Mapping::SudoerMap_t Mapping::gSudoerMap;
std::atomic<bool> Mapping::gRootSquash = true;
std::atomic<bool> Mapping::gSecondaryGroups = false;
std::atomic<int> Mapping::gTokenSudo = Mapping::kAlways;

Mapping::GeoLocationMap_t Mapping::gGeoMap;
int Mapping::gNobodyAccessTreeDeepness(1024);

Mapping::AllowedTidentMatches_t Mapping::gAllowedTidentMatches;

ShardedCache<std::string, Mapping::id_pair> Mapping::gShardedPhysicalUidCache(
  8);
ShardedCache<std::string, Mapping::gid_set> Mapping::gShardedPhysicalGidCache(
  8);
ShardedCache<uid_t, std::string> Mapping::gShardedNegativeUserNameCache(8);
ShardedCache<gid_t, std::string> Mapping::gShardedNegativeGroupNameCache(8);
ShardedCache<std::string, bool> Mapping::gShardedNegativePhysicalUidCache(8);
ShardedCache<std::string, time_t> Mapping::ActiveTidentsSharded(16);
ShardedCache<uid_t, size_t> Mapping::ActiveUidsSharded(16);

std::mutex Mapping::gPhysicalUserNameCacheMutex;
std::mutex Mapping::gPhysicalGroupNameCacheMutex;
std::map<uid_t, std::string> Mapping::gPhysicalUserNameCache;
std::map<gid_t, std::string> Mapping::gPhysicalGroupNameCache;
std::map<std::string, uid_t> Mapping::gPhysicalUserIdCache;
std::map<std::string, gid_t> Mapping::gPhysicalGroupIdCache;

Mapping::ip_cache Mapping::gIpCache(300);

std::unique_ptr<UnixGroupsFetcher> Mapping::gGroupsFetcher(
  new UnixGroupListFetcher());

OAuth Mapping::gOAuth;

static std::string g_pwd_key = "\"<pwd>\"";
static std::string g_pwd_uid_key = g_pwd_key + ":uid";
static std::string g_pwd_gid_key = g_pwd_key + ":gid";
static std::string g_https_uid_key = "https:" + g_pwd_uid_key;
static std::string g_https_gid_key = "https:" + g_pwd_gid_key;
static std::string g_sss_uid_key = "sss:" + g_pwd_uid_key;
static std::string g_sss_gid_key = "sss:" + g_pwd_gid_key;
static std::string g_unix_uid_key = "unix:" + g_pwd_uid_key;
static std::string g_unix_gid_key = "unix:" + g_pwd_gid_key;
static std::string g_gsi_uid_key = "gsi:" + g_pwd_uid_key;
static std::string g_gsi_gid_key = "gsi:" + g_pwd_gid_key;
static std::string g_krb_uid_key = "krb5:" + g_pwd_uid_key;
static std::string g_krb_gid_key = "krb5:" + g_pwd_gid_key;
static std::string g_oauth2_uid_key = "oauth2:" + g_pwd_uid_key;
static std::string g_oauth2_gid_key = "oauth2:" + g_pwd_gid_key; // not used yet
static std::string g_ztn_uid_key = "ztn:" + g_pwd_uid_key;
static std::string g_ztn_gid_key = "ztn:" + g_pwd_gid_key;

// flag to indicate whether the mapping is initialized
std::once_flag g_cache_map_init;

//------------------------------------------------------------------------------
// Initialize static maps
//------------------------------------------------------------------------------
void
Mapping::Init()
{
  // allow FUSE client access as root via env variable
  if (getenv("EOS_FUSE_NO_ROOT_SQUASH") &&
      !strcmp("1", getenv("EOS_FUSE_NO_ROOT_SQUASH"))) {
    gRootSquash = false;
  }

  if (getenv("EOS_SECONDARY_GROUPS") &&
      !strcmp("1", getenv("EOS_SECONDARY_GROUPS"))) {
    gSecondaryGroups = true;

    if (getenv("EOS_SECONDARY_GROUPS_GRENT") &&
        !strcmp("1", getenv("EOS_SECONDARY_GROUPS_GRENT"))) {
      gGroupsFetcher.reset(new UnixGrentFetcher());
    }
  }

  gOAuth.Init();

  try {
    std::call_once(g_cache_map_init, []() {
      // Force expiry of UID/GID cache every 2 cycles
      gShardedPhysicalUidCache.set_force_expiry(true, 2);
      gShardedPhysicalUidCache.reset_cleanup_thread(3600 * 1000,
          "UidCacheGC");
      gShardedPhysicalGidCache.set_force_expiry(true, 2);
      gShardedPhysicalGidCache.reset_cleanup_thread(3600 * 1000,
          "GidCacheGC");
      gShardedNegativeUserNameCache.set_force_expiry(true, 8);
      gShardedNegativeUserNameCache.reset_cleanup_thread(3600 * 1000,
          "NegUserNameGC");
      gShardedNegativeGroupNameCache.set_force_expiry(true, 8);
      gShardedNegativeGroupNameCache.reset_cleanup_thread(3600 * 1000,
          "NegGroupNameGC");
      gShardedNegativePhysicalUidCache.set_force_expiry(true, 2);
      gShardedNegativePhysicalUidCache.reset_cleanup_thread(3600 * 1000,
          "NegUidGC");
      ActiveUidsSharded.reset_cleanup_thread(300 * 1000,
                                             "ActiveUidsSharded");
      ActiveTidentsSharded.reset_cleanup_thread(300 * 1000,
          "ActiveTidentsGC");
    });
  } catch (...) {
    // we can't log here as the logging system is not initialized yet
  }
}

//------------------------------------------------------------------------------
// Reset
//------------------------------------------------------------------------------

void
Mapping::Reset()
{
  {
    std::scoped_lock lock{gPhysicalUserNameCacheMutex, gPhysicalGroupNameCacheMutex};
    gPhysicalUserNameCache.clear();
    gPhysicalGroupNameCache.clear();
    gPhysicalGroupIdCache.clear();
    gPhysicalUserIdCache.clear();
    gShardedPhysicalUidCache.clear();
    gShardedPhysicalGidCache.clear();
    gShardedNegativeUserNameCache.clear();
    gShardedNegativeGroupNameCache.clear();
    gShardedNegativePhysicalUidCache.clear();
  }
  ActiveTidentsSharded.clear();
  ActiveUidsSharded.clear();
}

//------------------------------------------------------------------------------
// Map a client to its virtual identity
//------------------------------------------------------------------------------
void
Mapping::IdMap(const XrdSecEntity* client, const char* env, const char* tident,
               VirtualIdentity& vid, XrdAccAuthorize* authz_obj,
               Access_Operation acc_op, std::string path, bool log)
{
  if (!client) {
    return;
  }

  eos_static_debug("msg=\"XrdSecEntity client\" name=\"%s\" role=\"%s\" "
                   "group=\"%s\" tident=\"%s\" cred=\"%s\"",
                   (client->name ? client->name : "null"),
                   (client->role ? client->role : "null"),
                   (client->grps ? client->grps : "null"),
                   (client->tident ? client->tident : "null"),
                   (client->creds ? client->creds : "null"));
  // We start as 'nobody'
  vid = VirtualIdentity::Nobody();
  XrdOucEnv Env(env);
  std::string authz = (Env.Get("authz") ? Env.Get("authz") : "");
  vid.name = (client->name ? client->name : "");
  vid.tident = tident;
  vid.sudoer = false;
  vid.gateway = false;
  // first map by alias
  XrdOucString useralias = client->prot;
  useralias += ":";
  useralias += "\"";
  useralias += (client->name ? client->name : "");
  useralias += "\"";
  useralias += ":";
  XrdOucString groupalias = useralias;
  useralias += "uid";
  groupalias += "gid";
  RWMutexReadLock lock(gMapMutex);
  vid.prot = client->prot;

  // @todo (esindril) this is just a workaround for the fact that XrdHttp
  // does not properly populate the prot field in the XrdSecEntity object.
  // See https://github.com/xrootd/xrootd/issues/1122
  if ((strlen(client->tident) == 4) &&
      (strcmp(client->tident, "http") == 0)) {
    vid.prot = "https";
  }

  // HTTPS, SSS and GRPC might contain a key embedded in the endorsements field
  if ((vid.prot == "sss") || (vid.prot == "grpc") || (vid.prot == "https")) {
    vid.key = (client->endorsements ? client->endorsements : "");
    eos_static_debug("msg=\"client endorsement\" key=\"%s\"", vid.key.c_str());
  }

  // KRB5 mapping
  if ((vid.prot == "krb5")) {
    eos_static_debug("%s", "msg=\"krb5 mapping\"");

    // Use physical mapping for kerberos names
    if (gVirtualUidMap.count(g_krb_uid_key)) {
      Mapping::getPhysicalUids(client->name, vid);
    }

    if (gVirtualGidMap.count(g_krb_gid_key)) {
      Mapping::getPhysicalGids(client->name, vid);
    }
  }

  // GSI mapping
  if ((vid.prot == "gsi")) {
    eos_static_debug("%s", "msg=\"gsi mapping\"");

    // Use physical mapping for gsi names
    if (gVirtualUidMap.count(g_gsi_uid_key)) {
      Mapping::getPhysicalUids(client->name, vid);
    }

    if (gVirtualGidMap.count(g_gsi_gid_key)) {
      Mapping::getPhysicalGids(client->name, vid);
    }

    HandleVOMS(client, vid);
  }

  // HTTPS mapping
  if (vid.prot == "https") {
    eos_static_debug("%s", "msg=\"https mapping\"");

    // Handle bearer token authorization
    if (authz_obj && !authz.empty() && (authz.find("Bearer%20") == 0)) {
      if (authz_obj->Access(client, path.c_str(), acc_op, &Env) ==
          XrdAccPriv_None) {
        vid = VirtualIdentity::Nobody();
        std::string nobearer = authz.substr(9);
        eos_static_err("msg=\"failed token authz\" path=\"%s\" opaque=\"%s\" "
                       "jwt={%s}[%s]", path.c_str(), env,
                       PrintJWT(nobearer).c_str(), nobearer.c_str());
        return;
      }
    }

    // Check if we have the request.name in the attributes of the XrdSecEntity
    // object which is the client username according to the authz mapping.
    std::string client_username;
    std::string user_value;
    static const std::string user_key = "request.name";

    if (client->eaAPI->Get(user_key, user_value)) {
      client_username = user_value;
    } else {
      if (client->name) {
        client_username = client->name;
      }
    }

    HandleUidGidMapping(client_username.c_str(), vid,
                        g_https_uid_key, g_https_gid_key);
    HandleVOMS(client, vid);
    HandleKEYS(client, vid);
  }

  // ZTN mapping
  if ((vid.prot == "ztn") && client->creds) {
    // Handle bearer token authorization
    eos_static_debug("msg=\"dumping client credentials/token\" creds=\"%s\"",
                     client->creds);

    if (authz_obj) {
      authz = "&authz=";
      authz += client->creds;
      XrdOucEnv op_env(authz.c_str());

      if (authz_obj->Access(client, path.c_str(), acc_op, &op_env) ==
          XrdAccPriv_None) {
        vid = VirtualIdentity::Nobody();
        eos_static_err("msg=\"failed token authz\" path=\"%s\" opaque=\"%s\" "
                       "authz=\"%s\" jwt={%s}", path.c_str(), env,
                       authz.c_str(),
                       PrintJWT(std::string(client->creds)).c_str());;
        return;
      }

      // Check if we have the request.name in the attributes of the XrdSecEntity
      // object which is the client username according to the authz mapping.
      std::string client_username;
      std::string user_value;
      static const std::string user_key = "request.name";

      if (client->eaAPI->Get(user_key, user_value)) {
        // we got a user name from the token
        client_username = user_value;
      } else {
        if (client->name) {
          client_username = client->name;
        }
      }

      HandleUidGidMapping(client_username.c_str(), vid,
                          g_ztn_uid_key, g_ztn_gid_key);
    } else {
      // add the ZTN credential if there is not another one provided
      if (authz.empty()) {
        authz = client->creds;
      }
    }
  }

  // sss mapping
  if ((vid.prot == "sss")) {
    HandleUidGidMapping(client->name, vid, g_sss_uid_key, g_sss_gid_key);
  }

  // unix mapping
  if ((vid.prot == "unix")) {
    if (authz_obj && authz.length()) {
      if (authz_obj->Access(client, path.c_str(), acc_op, &Env) == XrdAccPriv_None) {
        // In principle we will never get here if XrdMgmAuthz is chained since
        // it says ok if there is a user name defined
        vid = VirtualIdentity::Nobody();
        eos_static_err("msg=\"failed token authz\" path=\"%s\" opaque=\"%s\" "
                       "authz=\"%s\" jwt={%s}",  path.c_str(), env,
                       authz.c_str(),
                       PrintJWT(Env.Get("authz") ?
                                std::string(Env.Get("authz")) :
                                std::string("")).c_str());
        return;
      }

      // Check if we have the request.name in the attributes of the XrdSecEntity
      // object which is the client username according to the authz mapping.
      std::string client_username = "nobody";
      std::string user_value;
      static const std::string user_key = "request.name";
      bool force_mapping = false;

      if (client->eaAPI->Get(user_key, user_value)) {
        force_mapping = true;
        client_username = user_value;
      } else {
        // No user from the token, we are 'anonymous'
        client_username = "nobody";
      }

      HandleUidGidMapping(client_username.c_str(), vid,
                          g_unix_uid_key, g_unix_gid_key, force_mapping);
    } else {
      HandleUidGidMapping(client->name, vid, g_unix_uid_key, g_unix_gid_key);
    }
  }

  // tident mapping
  XrdOucString mytident = "";
  XrdOucString myrole = "";
  XrdOucString wildcardtident = "";
  XrdOucString host = "";
  XrdOucString stident = "tident:";
  stident += "\"";
  stident += ReduceTident(vid.tident, wildcardtident, mytident, host);

  if (host == "127.0.0.1") {
    host = "localhost";
  }

  myrole = mytident;
  myrole.erase(mytident.find("@"));
  // FUSE selects now the role via <uid>[:connectionid]
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
  // there can be a protocol specific rule like sss:@<host>:uid...
  sprotuidtident.replace("*", vid.prot);
  // there can be a protocol specific rule like sss:@<host>:gid...
  sprotgidtident.replace("*", vid.prot);
  eos_static_debug("swcuidtident=%s sprotuidtident=%s myrole=%s",
                   swcuidtident.c_str(), sprotuidtident.c_str(), myrole.c_str());

  if (auto kv = gVirtualUidMap.find(suidtident.c_str());
      kv != gVirtualUidMap.end()) {
    vid.uid = kv->second;
    vid.allowed_uids.insert(vid.uid);
    vid.allowed_uids.insert(VirtualIdentity::kNobodyUid);
  }

  if (auto kv = gVirtualGidMap.find(sgidtident.c_str());
      kv != gVirtualGidMap.end()) {
    vid.gid = kv->second;
    vid.allowed_gids.insert(vid.gid);
    vid.allowed_gids.insert(VirtualIdentity::kNobodyGid);
  }

  // Wildcard tidents/protocol tidents - one can define mapping entries like
  // '*@host:uid=>0' e.g. for fuse mounts or only for a certain protocol
  // like 'sss@host:uid=>0'
  XrdOucString tuid = "";
  XrdOucString tgid = "";

  if (gVirtualUidMap.count(swcuidtident.c_str())) {
    // there is an entry like "*@<host:uid" matching all protocols
    tuid = swcuidtident.c_str();
  } else {
    if (gVirtualUidMap.count(sprotuidtident.c_str())) {
      // there is a protocol specific entry "<prot>@<host>:uid"
      tuid = sprotuidtident.c_str();
    } else {
      if (gAllowedTidentMatches.size()) {
        std::string sprot = vid.prot.c_str();

        for (auto it = gAllowedTidentMatches.begin(); it != gAllowedTidentMatches.end();
             ++it) {
          if (sprot != it->first.c_str()) {
            continue;
          }

          if (host.matches(it->second.c_str())) {
            sprotuidtident.replace(host.c_str(), it->second.c_str());

            if (gVirtualUidMap.count(sprotuidtident.c_str())) {
              tuid = sprotuidtident.c_str();
              break;
            }
          }
        }
      }
    }
  }

  if (gVirtualGidMap.count(swcgidtident.c_str())) {
    // there is an entry like "*@<host>:gid" matching all protocols
    tgid = swcgidtident.c_str();
  } else {
    if (gVirtualGidMap.count(sprotgidtident.c_str())) {
      // there is a protocol specific entry "<prot>@<host>:uid"
      tgid = sprotgidtident.c_str();
    } else {
      if (gAllowedTidentMatches.size()) {
        std::string sprot = vid.prot.c_str();

        for (auto it = gAllowedTidentMatches.begin();
             it != gAllowedTidentMatches.end(); ++it) {
          if (sprot != it->first.c_str()) {
            continue;
          }

          if (host.matches(it->second.c_str())) {
            sprotgidtident.replace(host.c_str(), it->second.c_str());

            if (gVirtualGidMap.count(sprotgidtident.c_str())) {
              tgid = sprotgidtident.c_str();
              break;
            }
          }
        }
      }
    }
  }

  eos_static_debug("tuid=%s tgid=%s", tuid.c_str(), tgid.c_str());

  if (gVirtualUidMap.count(tuid.c_str())) {
    if (!gVirtualUidMap[tuid.c_str()]) {
      if (gRootSquash && (host != "localhost") && (host != "localhost.localdomain") &&
          (host != "localhost6.localdomain6") && (vid.name == "root") &&
          (myrole == "root")) {
        eos_static_debug("%s", "msg=\"tident root uid squash\"");
        vid.allowed_uids.clear();
        vid.allowed_uids.insert(DAEMONUID);
        vid.uid = DAEMONUID;
        vid.allowed_gids.clear();
        vid.gid = DAEMONGID;
        vid.allowed_gids.insert(DAEMONGID);
      } else {
        eos_static_debug("msg=\"tident uid mapping\" prot=%s name=%s",
                         vid.prot.c_str(), vid.name.c_str());
        vid.allowed_uids.clear();

        // use physical mapping
        // unix protocol maps to the role if the client is the root account
        // otherwise it maps to the unix ID on the client host
        if (((vid.prot == "unix") && (vid.name == "root")) ||
            ((vid.prot == "sss") && (vid.name == "daemon"))) {
          Mapping::getPhysicalIdShards(myrole.c_str(), vid);
        } else {
          if (client->name != nullptr) {
            Mapping::getPhysicalIdShards(client->name, vid);
          }
        }

        vid.gateway = true;
      }
    } else {
      eos_static_debug("%s", "msg=\"tident uid forced mapping\"");
      // map to the requested id
      vid.allowed_uids.clear();
      vid.uid = gVirtualUidMap[tuid.c_str()];
      vid.allowed_uids.insert(vid.uid);
      vid.allowed_uids.insert(VirtualIdentity::kNobodyUid);
      vid.allowed_gids.clear();
      vid.gid = VirtualIdentity::kNobodyGid;
      vid.allowed_gids.insert(vid.gid);
    }
  }

  if (gVirtualGidMap.count(tgid.c_str())) {
    if (!gVirtualGidMap[tgid.c_str()]) {
      if (gRootSquash && (host != "localhost") && (host != "localhost.localdomain") &&
          (vid.name == "root") && (myrole == "root")) {
        eos_static_debug("%s", "msg=\"tident root gid squash\"");
        vid.allowed_gids.clear();
        vid.allowed_gids.insert(DAEMONGID);
        vid.gid = DAEMONGID;
      } else {
        eos_static_debug("%s", "msg=\"tident gid mapping\"");
        uid_t uid = vid.uid;

        if (((vid.prot == "unix") && (vid.name == "root")) ||
            ((vid.prot == "sss") && (vid.name == "daemon"))) {
          Mapping::getPhysicalIdShards(myrole.c_str(), vid);
        } else {
          if (client->name != nullptr) {
            Mapping::getPhysicalIdShards(client->name, vid);
          }
        }

        vid.uid = uid;
        vid.allowed_uids.clear();
        vid.allowed_uids.insert(uid);
        vid.allowed_uids.insert(VirtualIdentity::kNobodyUid);
        vid.gateway = true;
      }
    } else {
      eos_static_debug("%s", "msg=\"tident gid forced mapping\"");
      // map to the requested id
      vid.allowed_gids.clear();
      vid.gid = gVirtualGidMap[tgid.c_str()];
      vid.allowed_gids.insert(vid.gid);
    }
  }

  eos_static_debug("suidtident:%s sgidtident:%s", suidtident.c_str(),
                   sgidtident.c_str());

  // Configuration door for localhost clients adds always the adm/adm vid's
  if ((suidtident == "tident:\"root@localhost.localdomain\":uid") ||
      (suidtident == "tident:\"root@localhost\":uid")) {
    vid.sudoer = true;
    vid.uid = 3;
    vid.gid = 4;
    vid.allowed_uids.insert(vid.uid);
    vid.allowed_gids.insert(vid.gid);
  }

  // GRPC key mapping
  if ((vid.prot == "grpc") && vid.key.length()) {
    std::string keyname = vid.key.c_str();

    if (keyname.substr(0, 8) == "zteos64:") {
      // this is an eos token
      authz = vid.key;
      vid = VirtualIdentity::Nobody();
    }  else {
      std::string maptident = "tident:\"grpc@";
      std::string wildcardmaptident = "tident:\"grpc@*\":uid";
      std::vector<std::string> vtident;
      eos::common::StringConversion::Tokenize(client->tident, vtident, "@");

      if (vtident.size() == 2) {
        maptident += vtident[1];
      }

      maptident += "\":uid";
      eos_static_info("%d %s %s %s", vtident.size(), client->tident,
                      maptident.c_str(), wildcardmaptident.c_str());

      if (gVirtualUidMap.count(maptident.c_str()) ||
          gVirtualUidMap.count(wildcardmaptident.c_str())) {
        // if this is an allowed gateway, map according to client name or authkey
        std::string uidkey = "grpc:\"";
        uidkey += "key:";
        uidkey += keyname;
        uidkey += "\":uid";
        vid.uid = VirtualIdentity::kNobodyUid;
        vid.allowed_uids.clear();
        vid.allowed_uids.insert(VirtualIdentity::kNobodyUid);
        vid.gateway = true;

        if (gVirtualUidMap.count(uidkey.c_str())) {
          vid.uid = gVirtualUidMap[uidkey.c_str()];
          vid.allowed_uids.insert(vid.uid);
        }

        std::string gidkey = "grpc:\"";
        gidkey += "key:";
        gidkey += keyname;
        gidkey += "\":gid";
        vid.gid = VirtualIdentity::kNobodyGid;
        vid.allowed_gids.clear();
        vid.allowed_gids.insert(VirtualIdentity::kNobodyGid);

        if (gVirtualGidMap.count(gidkey.c_str())) {
          vid.gid = gVirtualGidMap[gidkey.c_str()];
          vid.allowed_gids.insert(vid.gid);
        }
      } else {
        // we are nobody if we are not an authorized host
        vid = VirtualIdentity::Nobody();
      }
    }
  }

  // Environment selected roles
  XrdOucString ruid = Env.Get("eos.ruid");
  XrdOucString rgid = Env.Get("eos.rgid");
  XrdOucString rapp = Env.Get("eos.app");

  // SSS key mapping
  if ((vid.prot == "sss") && vid.key.length()) {
    std::string keyname = vid.key;
    std::string maptident = "tident:\"sss@";
    std::string wildcardmaptident = "tident:\"sss@*\":uid";
    std::vector<std::string> vtident;
    eos::common::StringConversion::Tokenize(client->tident, vtident, "@");

    // token provided as key
    if (keyname.substr(0, 8) == "zteos64:") {
      // this is an eos token
      authz = vid.key;
    } else {
      // try oauth2
      std::string oauthname;
      bool oauth2_enabled = (gVirtualUidMap.find(g_oauth2_uid_key) !=
                             gVirtualUidMap.end());

      if (oauth2_enabled) {
        // Release the map mutex to avoid any inteference with a queued up
        // write lock and an oauth callout being slow
        lock.Release();
        oauthname = gOAuth.Handle(keyname, vid);
        lock.Grab(gMapMutex);
      }

      // Check for OAuth contents
      if (oauthname.empty() || !oauth2_enabled) {
        // Treat as mapping key
        if (vtident.size() == 2) {
          maptident += vtident[1];
        }

        maptident += "\":uid";
        eos_static_info("%d %s %s %s", vtident.size(), client->tident,
                        maptident.c_str(), wildcardmaptident.c_str());

        if (gVirtualUidMap.count(maptident) ||
            gVirtualUidMap.count(wildcardmaptident)) {
          vid.gateway = true;
          // if this is an allowed gateway, map according to client name or authkey
          std::string uidkey = "sss:\"";
          uidkey += "key:";
          uidkey += keyname;
          uidkey += "\":uid";
          vid.uid = VirtualIdentity::kNobodyUid;
          vid.allowed_uids.clear();
          vid.allowed_uids.insert(VirtualIdentity::kNobodyUid);

          if (auto kv = gVirtualUidMap.find(uidkey);
              kv != gVirtualUidMap.end()) {
            vid.uid = kv->second;
            vid.allowed_uids.insert(vid.uid);
          }

          std::string gidkey = "sss:\"";
          gidkey += "key:";
          gidkey += keyname;
          gidkey += "\":gid";
          vid.gid = VirtualIdentity::kNobodyGid;
          vid.allowed_gids.clear();
          vid.allowed_gids.insert(VirtualIdentity::kNobodyGid);

          if (auto kv = gVirtualGidMap.find(gidkey);
              kv != gVirtualGidMap.end()) {
            vid.gid = kv->second;
            vid.allowed_gids.insert(vid.gid);
          }
        } else {
          // we are nobody if we are not an authorized host
          vid = VirtualIdentity::Nobody();
          vid.prot = "sss";
        }
      } else {
        int errc = 0;
        std::string uidkey = "oauth2:\"";
        uidkey += "sub:";
        uidkey += oauthname;
        uidkey += "\":uid";

        if (auto kv = gVirtualUidMap.find(uidkey);
            kv != gVirtualUidMap.end()) {
          // map oauthname from static sub mapping
          oauthname = UidToUserName(kv->second, errc);
        }

        if (errc) {
          // we have no mapping for this uid
          Mapping::getPhysicalIdShards("nobody", vid);
        } else {
          // map oauthname
          Mapping::getPhysicalIdShards(oauthname.c_str(), vid);
        }

        vid.prot = "oauth2";
      }
    }
  }

  // Explicit virtual mapping overrules physical mappings - the second one
  // comes from the physical mapping before
  {
    auto userkey = gVirtualUidMap.find(useralias.c_str());
    vid.uid = userkey != gVirtualUidMap.end() ? userkey->second : vid.uid;
    vid.allowed_uids.insert(vid.uid);
  }
  {
    auto groupkey = gVirtualGidMap.find(groupalias.c_str());
    vid.gid = groupkey != gVirtualGidMap.end() ? groupkey->second : vid.gid;
    vid.allowed_gids.insert(vid.gid);
  }

  // Add virtual user and group roles - if any
  if (gUserRoleVector.count(vid.uid)) {
    for (auto it = gUserRoleVector[vid.uid].cbegin();
         it != gUserRoleVector[vid.uid].cend(); ++it) {
      vid.allowed_uids.insert(*it);
    }
  }

  if (gGroupRoleVector.count(vid.uid)) {
    for (auto it = gGroupRoleVector[vid.uid].cbegin();
         it != gGroupRoleVector[vid.uid].cend(); ++it) {
      vid.allowed_gids.insert(*it);
    }
  }

  bool token_sudo = false;

  // Handle token based mapping
  if (!authz.empty()) {
    static const std::string http_enc_tag = "Bearer%20";
    static const std::string http_tag = "Bearer ";

    // Remove extra characters and decode when passed as a bearer
    // authorization HTTPS header
    if (authz.find(http_enc_tag) == 0) {
      authz.erase(0, http_enc_tag.size());
      authz = StringConversion::curl_default_unescaped(authz);
    } else {
      if (authz.find(http_tag) == 0) {
        authz.erase(0, http_tag.size());
      }
    }

    if (authz.substr(0, 8) == "zteos64:") {
      // This is an eos token
      eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
      std::string key = symkey ? symkey->GetKey64() : "0123457890defaultkey";
      bool skip_key = false;

      if (getenv("EOS_MGM_TOKEN_KEYFILE")) {
        struct stat buf;

        if (::stat(getenv("EOS_MGM_TOKEN_KEYFILE"), &buf)) {
          eos_static_err("msg=\"token keyfile does not exist\" location=\"%s\"",
                         getenv("EOS_MGM_TOKEN_KEYFILE"));
          skip_key = true;
        } else {
          if ((buf.st_uid != DAEMONUID) ||
              (buf.st_mode != 0100400)) {
            skip_key = true;
            eos_static_err("msg=\"token keyfile mode bit\" mode=%o", buf.st_mode);
          }
        }

        if (!skip_key) {
          key = eos::common::StringConversion::LoadFileIntoString(
                  getenv("EOS_MGM_TOKEN_KEYFILE"), key);
        }
      }

      int rc = 0;
      vid.token = std::make_shared<EosTok>();

      if ((rc = vid.token->Read(authz, key, eos::common::EosTok::sTokenGeneration,
                                false))) {
        vid.token->Reset();
        eos_static_err("msg=\"failed to decode token\" tident=\"%s\" token=\"%s\" errno=%d",
                       tident, authz.c_str(), -rc);
      } else {
        bool validated = true;

        if (path.length() && path.substr(0, 6) != "/proc/") {
          if (vid.token->ValidatePath(path)) {
            eos_static_err("msg=\"token path validation failed\" path=\"%s\"",
                           path.c_str());
            validated = false;
          }
        }

        // if the path is screened we change owner/group
        if (validated && !vid.token->Owner().empty()) {
          token_sudo = true;
          ruid = vid.token->Owner().c_str();
        }

        if (validated && !vid.token->Group().empty()) {
          token_sudo = true;
          rgid = vid.token->Group().c_str();
        }

        if (EOS_LOGS_INFO) {
          std::string dump;
          vid.token->Dump(dump, true, true);
          eos_static_info("%s {tokensudo:%d (%d)}", dump.c_str(), token_sudo,
                          gTokenSudo.load());
        }
      }
    } else {
      eos_static_debug("jwt={%s}", PrintJWT(Env.Get("authz") ?
                                            std::string(Env.Get("authz")) :
                                            std::string("")).c_str());
    }
  }

  // apply policy if a token can change the identity (authenticate)
  if (gTokenSudo != Mapping::kAlways) {
    if (gTokenSudo == kNever) {
      token_sudo = false;
    } else {
      if (gTokenSudo == Mapping::kEncrypted) {
        if ((vid.prot != "sss") &&
            (vid.prot != "https") &&
            (vid.prot != "ztn") &&
            (vid.prot != "grpc")) {
          token_sudo = false;
        }
      } else {
        if (gTokenSudo == Mapping::kStrong) {
          if (vid.prot == "unix") {
            token_sudo = false;
          }
        }
      }
    }
  }

  uid_t sel_uid = vid.uid;
  uid_t sel_gid = vid.gid;

  if (ruid.length()) {
    if (!IsUid(ruid, sel_uid)) {
      int errc = 0;
      // try alias conversion
      std::string luid = ruid.c_str();
      sel_uid = (gVirtualUidMap.count(ruid.c_str())) ? gVirtualUidMap[ruid.c_str() ] :
                VirtualIdentity::kNobodyUid;

      if (sel_uid == VirtualIdentity::kNobodyUid) {
        sel_uid = UserNameToUid(luid, errc);
      }

      if (errc) {
        sel_uid = VirtualIdentity::kNobodyUid;
      }
    }
  }

  if (rgid.length()) {
    if (!IsGid(rgid, sel_gid)) {
      int errc = 0;
      // try alias conversion
      std::string lgid = rgid.c_str();
      sel_gid = (gVirtualGidMap.count(rgid.c_str())) ? gVirtualGidMap[rgid.c_str()] :
                VirtualIdentity::kNobodyGid;

      if (sel_gid == VirtualIdentity::kNobodyGid) {
        sel_gid = GroupNameToGid(lgid, errc);
      }

      if (errc) {
        sel_gid = VirtualIdentity::kNobodyGid;
      }
    }
  }

  // Sudoer flag setting
  if (gSudoerMap.count(vid.uid)) {
    vid.sudoer = true;
  }

  // Check if we are allowed to take sel_uid & sel_gid
  if (!vid.sudoer && !token_sudo) {
    // if we are not a sudore, scan the allowed ids
    if (vid.hasUid(sel_uid)) {
      vid.uid = sel_uid;
    } else {
      vid.uid = VirtualIdentity::kNobodyUid;
    }

    if (vid.hasGid(sel_gid)) {
      vid.gid = sel_gid;
    } else {
      vid.gid = VirtualIdentity::kNobodyGid;
    }
  } else {
    vid.uid = sel_uid;
    vid.gid = sel_gid;

    if (ruid.length() || rgid.length()) {
      vid.allowed_gids.insert(sel_gid);
      vid.allowed_uids.insert(sel_uid);
    }
  }

  if (client->host) {
    vid.host = client->host;
  } else {
    vid.host = host.c_str();
  }

  size_t dotpos = vid.host.find(".");

  // remove hostname
  if (dotpos != std::string::npos) {
    vid.domain = vid.host.substr(dotpos + 1);
  } else {
    vid.domain = "localdomain";
  }

  {
    int errc = 0;

    // add the uid/gid as strings
    if (vid.uid_string.empty()) {
      vid.uid_string = UidToUserName(vid.uid, errc);
    }

    if (vid.gid_string.empty()) {
      vid.gid_string = GidToGroupName(vid.gid, errc);
    }
  }

  // verify origin
  if (vid.token) {
    if (vid.token->Valid()) {
      if (vid.token->VerifyOrigin(vid.host, vid.uid_string,
                                  std::string(vid.prot.c_str()))) {
        // invalidate this token
        eos_static_err("msg=\"invalid token due to origin mismatch\" "
                       "\"%s#%s#%s\"", vid.host.c_str(),
                       vid.uid_string.c_str(), vid.prot.c_str());
        vid.token->Reset();
        // reset the vid to nobody if the origin does not match
        vid.toNobody();
      }
    } else {
      eos_static_debug("msg=\"token invalid\" host=\"%s\" uid=\"%s\" prot=\"%s\"",
                       vid.host.c_str(), vid.uid_string.c_str(), vid.prot.c_str());
    }
  }

  if (rapp.length()) {
    vid.app = rapp.c_str();
  }

  // Check the Geo Location
  if ((!vid.geolocation.length()) && (gGeoMap.size())) {
    // if the geo location was not set externally and we have some recipe we try
    // to translate the host name and match a rule

    // if we have a default geo location we assume that a client in that one
    if (auto kv = gGeoMap.find("default");
        kv != gGeoMap.end()) {
      vid.geolocation = kv->second;
    }

    std::string ipstring = gIpCache.GetIp(host.c_str());

    if (ipstring.length()) {
      std::string sipstring = ipstring;
      GeoLocationMap_t::const_iterator it;
      GeoLocationMap_t::const_iterator longuestmatch = gGeoMap.end();

      // we use the geo location with the longest name match
      for (it = gGeoMap.begin(); it != gGeoMap.end(); ++it) {
        // If we have a previously matched geoloc and if it's longer that the
        // current one, try the next one
        if (longuestmatch != gGeoMap.end() &&
            it->first.length() <= longuestmatch->first.length()) {
          continue;
        }

        if (sipstring.compare(0, it->first.length(), it->first) == 0) {
          vid.geolocation = it->second;
          longuestmatch = it;
        }
      }
    }
  }

  char actident[1024];
  snprintf(actident, sizeof(actident) - 1, "%d^%s^%s^%s^%s", vid.uid,
           mytident.c_str(), vid.prot.c_str(), vid.host.c_str(), vid.app.c_str());
  std::string intident = actident;

  if (!ActiveTidentsSharded.contains(intident)) {
    ActiveUidsSharded.fetch_add(vid.uid, 1);
  }

  ActiveTidentsSharded.store(intident, std::make_unique<time_t>(time(NULL)));
  eos_static_debug("selected %d %d [%s %s]", vid.uid, vid.gid, ruid.c_str(),
                   rgid.c_str());

  if (log) {
    eos_static_info("%s sec.tident=\"%s\" vid.uid=%d vid.gid=%d sudo=%d gateway=%d",
                    eos::common::SecEntity::ToString(client, Env.Get("eos.app")).c_str(),
                    tident, vid.uid, vid.gid, vid.sudoer, vid.gateway);
  }
}

//------------------------------------------------------------------------------
// Handle VOMS mapping
//------------------------------------------------------------------------------
void
Mapping::HandleVOMS(const XrdSecEntity* client, VirtualIdentity& vid)
{
  // No VOMS info available
  if ((client->grps == nullptr) || (strlen(client->grps) == 0)) {
    return;
  }

  std::string group = client->grps;
  size_t g_pos = group.find(" ");

  if (g_pos != std::string::npos) {
    group.erase(g_pos);
  }

  // VOMS mapping
  std::string vomsstring = "voms:\"";
  vomsstring += group;
  vomsstring += ":";
  vid.grps = group;

  if (client->role && strlen(client->role) &&
      (strncmp(client->role, "NULL", 4) != 0)) {
    // the role might be NULL
    std::string role = client->role;
    size_t r_pos = role.find(" ");

    if (r_pos != std::string::npos) {
      role.erase(r_pos);
    }

    vomsstring += role;
    vid.role = role;
  }

  vomsstring += "\"";
  std::string vomsuidstring = vomsstring;
  std::string vomsgidstring = vomsstring;
  vomsuidstring += ":uid";
  vomsgidstring += ":gid";

  // Mapping to user
  if (gVirtualUidMap.count(vomsuidstring)) {
    vid.allowed_uids.clear();
    vid.allowed_gids.clear();
    // Use physical mapping for VOMS roles, convert mapped uid to user name
    int errc = 0;
    std::string cname = Mapping::UidToUserName(gVirtualUidMap[vomsuidstring], errc);

    if (!errc) {
      Mapping::getPhysicalIdShards(cname.c_str(), vid);
    } else {
      vid = VirtualIdentity::Nobody();
      eos_static_err("voms-mapping: cannot translate uid=%d to user name with "
                     "the password db", (int) gVirtualUidMap[vomsuidstring]);
    }
  }

  // Mapping to group
  if (gVirtualGidMap.count(vomsgidstring)) {
    // se group mapping for VOMS roles
    vid.allowed_gids.clear();
    vid.gid = gVirtualGidMap[vomsgidstring];
    vid.allowed_gids.insert(vid.gid);
  }
}

//------------------------------------------------------------------------------
// Handle HTTPS authz keys mapping
//------------------------------------------------------------------------------
void
Mapping::HandleKEYS(const XrdSecEntity* client, VirtualIdentity& vid)
{
  // No VOMS info available
  if (vid.key.empty()) {
    return;
  }

  std::string uidkey = "https:\"";
  uidkey += "key:";
  uidkey += vid.key;
  uidkey += "\":uid";

  if (gVirtualUidMap.count(uidkey.c_str())) {
    vid.uid = VirtualIdentity::kNobodyUid;
    vid.allowed_uids.clear();
    vid.allowed_uids.insert(VirtualIdentity::kNobodyUid);
    vid.uid = gVirtualUidMap[uidkey.c_str()];
    vid.allowed_uids.insert(vid.uid);
    vid.gateway = true;
  }

  std::string gidkey = "https:\"";
  gidkey += "key:";
  gidkey += vid.key;
  gidkey += "\":gid";

  if (gVirtualGidMap.count(gidkey.c_str())) {
    vid.gid = VirtualIdentity::kNobodyGid;
    vid.allowed_gids.clear();
    vid.allowed_gids.insert(VirtualIdentity::kNobodyGid);
    vid.gid = gVirtualGidMap[gidkey.c_str()];
    vid.allowed_gids.insert(vid.gid);
    vid.gateway = true;
  }
}

//------------------------------------------------------------------------------
// Print the current mappings
//------------------------------------------------------------------------------
void
Mapping::Print(XrdOucString& stdOut, XrdOucString option)
{
  bool translateids = true;

  if (option.find("n") != STR_NPOS) {
    translateids = false;
    option.replace("n", "");
  }

  if ((!option.length()) || ((option.find("u")) != STR_NPOS)) {
    for (auto it = gUserRoleVector.cbegin(); it != gUserRoleVector.cend(); ++it) {
      char iuid[4096];
      sprintf(iuid, "%d", it->first);
      char suid[4096];
      sprintf(suid, "%-6s", iuid);

      if (translateids) {
        int errc = 0;
        std::string username = UidToUserName(it->first, errc);

        if (!errc) {
          sprintf(suid, "%-12s", username.c_str());
        }
      }

      stdOut += "membership uid: ";
      stdOut += suid;
      stdOut += " => uids(";

      for (const auto& uid : it->second) {
        if (translateids) {
          int errc = 0;
          std::string username = UidToUserName(uid, errc);

          if (!errc) {
            stdOut += username.c_str();
          } else {
            stdOut += (int)uid;
          }
        } else {
          stdOut += (int)uid;
        }

        stdOut += ",";
      }

      if (!it->second.empty()) {
        stdOut.erasefromend(1);
      }

      stdOut += ")\n";
    }
  }

  if ((!option.length()) || ((option.find("g")) != STR_NPOS)) {
    for (auto it = gGroupRoleVector.cbegin(); it != gGroupRoleVector.cend(); ++it) {
      char iuid[4096];
      sprintf(iuid, "%d", it->first);
      char suid[4096];
      sprintf(suid, "%-6s", iuid);

      if (translateids) {
        int errc = 0;
        std::string username = UidToUserName(it->first, errc);

        if (!errc) {
          sprintf(suid, "%-12s", username.c_str());
        }
      }

      stdOut += "membership uid: ";
      stdOut += suid;
      stdOut += " => gids(";

      for (const auto& gid : it->second) {
        if (translateids) {
          int errc = 0;
          std::string grpname = GidToGroupName(gid, errc);

          if (!errc) {
            stdOut += grpname.c_str();
          } else {
            stdOut += (int)gid;
          }
        } else {
          stdOut += (int)gid;
        }

        stdOut += ",";
      }

      if (!it->second.empty()) {
        stdOut.erasefromend(1);
      }

      stdOut += ")\n";
    }
  }

  if ((!option.length()) || ((option.find("s")) != STR_NPOS)) {
    // print sudoer line
    stdOut += "sudoer                 => uids(";

    for (auto it = gSudoerMap.cbegin(); it != gSudoerMap.cend(); ++it) {
      if (it->second) {
        int errc = 0;
        std::string username = UidToUserName(it->first, errc);

        if (!errc && translateids) {
          stdOut += username.c_str();
        } else {
          stdOut += (int)(it->first);
        }

        stdOut += ",";
      }
    }

    if (stdOut.endswith(",")) {
      stdOut.erase(stdOut.length() - 1);
    }

    stdOut += ")\n";
    stdOut += "tokensudo              => ";

    if (gTokenSudo == Mapping::kAlways) {
      stdOut += "always";
    } else if (gTokenSudo == Mapping::kEncrypted) {
      stdOut += "encrypted";
    } else if (gTokenSudo == Mapping::kStrong) {
      stdOut += "strong";
    } else if (gTokenSudo == Mapping::kNever) {
      stdOut += "never";
    } else {
      stdOut += "inval";
    }

    stdOut += "\n";
  }

  if ((!option.length()) || ((option.find("U")) != STR_NPOS)) {
    for (auto it = gVirtualUidMap.cbegin(); it != gVirtualUidMap.cend(); ++it) {
      stdOut += it->first.c_str();
      stdOut += " => ";
      int errc = 0;
      std::string username = UidToUserName(it->second, errc);

      if (!errc && translateids) {
        stdOut += username.c_str();
      } else {
        stdOut += (int) it->second;
      }

      stdOut += "\n";
    }
  }

  if ((!option.length()) || ((option.find("G")) != STR_NPOS)) {
    for (auto it = gVirtualGidMap.cbegin(); it != gVirtualGidMap.cend(); ++it) {
      stdOut += it->first.c_str();
      stdOut += " => ";
      int errc = 0;
      std::string groupname = GidToGroupName(it->second, errc);

      if (!errc && translateids) {
        stdOut += groupname.c_str();
      } else {
        stdOut += (int) it->second;
      }

      stdOut += "\n";
    }
  }

  if ((!option.length()) || ((option.find("N")) != STR_NPOS)) {
    char sline[1024];
    snprintf(sline, sizeof(sline), "publicaccesslevel: => %d\n",
             gNobodyAccessTreeDeepness);
    stdOut += sline;
  }

  if ((!option.length()) || ((option.find("l")) != STR_NPOS)) {
    for (auto it = gGeoMap.cbegin(); it != gGeoMap.cend(); ++it) {
      char sline[1024];
      snprintf(sline, sizeof(sline) - 1, "geotag:\"%s\" => \"%s\"\n",
               it->first.c_str(), it->second.c_str());
      stdOut += sline;
    }
  }

  if ((!option.length())) {
    for (auto it = gAllowedTidentMatches.cbegin();
         it != gAllowedTidentMatches.cend(); ++it) {
      char sline[1024];
      snprintf(sline, sizeof(sline) - 1, "hostmatch:\"protocol=%s pattern=%s\n",
               it->first.c_str(), it->second.c_str());
      stdOut += sline;
    }
  }

  if ((option.find("y") != STR_NPOS)) {
    for (auto it = gVirtualUidMap.cbegin(); it != gVirtualUidMap.cend(); ++it) {
      std::string authmethod = it->first.c_str();

      if (authmethod.find("tident:") != 0) {
        continue;
      }

      // Get the gid mapping for the current key
      std::string sgid = "n/a";
      std::string gid_key = authmethod;
      gid_key.replace(gid_key.find(":uid"), 4, ":gid");
      auto it_gid = gVirtualGidMap.find(gid_key);

      if (it_gid != gVirtualGidMap.end()) {
        sgid = std::to_string(it_gid->second);
      }

      authmethod.erase(0, 7); // delete "tident:"
      // delete all the " characters
      authmethod.erase(std::remove(authmethod.begin(), authmethod.end(), '"'),
                       authmethod.end());
      auto dpos = authmethod.find("@");

      if ((dpos != std::string::npos) && (authmethod.length() > dpos + 1)) {
        std::string protocol = authmethod.substr(0, dpos);
        protocol = ((protocol == "*") ? "all" : protocol);
        auto cpos = authmethod.rfind(':');
        std::string hostname = authmethod.substr(dpos + 1, cpos - dpos - 1);
        std::ostringstream oss;
        oss << "gateway=" << hostname << " auth=" << protocol
            << " uid=" << it->second << " gid=" << sgid << std::endl;
        stdOut += oss.str().c_str();
      }
    }
  }

  if ((option.find("a") != STR_NPOS)) {
    for (auto it = gVirtualUidMap.cbegin(); it != gVirtualUidMap.cend(); ++it) {
      if (it->second == 0) {
        XrdOucString authmethod = it->first.c_str();

        if (authmethod.beginswith("tident:")) {
          continue;
        }

        int dpos = authmethod.find(":");
        authmethod.erase(dpos);
        stdOut += "auth=";
        stdOut += authmethod;
        stdOut += "\n";
      }
    }
  }
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
Mapping::UidToUserName(uid_t uid, int& errc)
{
  errc = 0;
  {
    std::scoped_lock lock(gPhysicalUserNameCacheMutex);
    auto kv = gPhysicalUserNameCache.find(uid);

    if (kv != gPhysicalUserNameCache.end()) {
      return kv->second;
    }
  }

  if (auto user_ptr = gShardedNegativeUserNameCache.retrieve(uid)) {
    return *user_ptr;
  }

  char buffer[131072];
  int buflen = sizeof(buffer);
  std::string uid_string = "";
  struct passwd pwbuf;
  struct passwd* pwbufp = 0;
  (void) getpwuid_r(uid, &pwbuf, buffer, buflen, &pwbufp);

  if (pwbufp == NULL) {
    char buffer[131072];
    int buflen = sizeof(buffer);
    std::string uid_string = "";
    struct passwd pwbuf;
    struct passwd* pwbufp = 0;
    {
      if (getpwuid_r(uid, &pwbuf, buffer, buflen, &pwbufp) || (!pwbufp)) {
        char suid[1024];
        snprintf(suid, sizeof(suid) - 1, "%u", uid);
        uid_string = suid;
        errc = EINVAL;
        gShardedNegativeUserNameCache.store(uid,
                                            std::make_unique<std::string>(uid_string));
        return uid_string;
      } else {
        uid_string = pwbuf.pw_name;
        errc = 0;
      }
    }
    cacheUserIds(uid, uid_string);
    return uid_string;
  } else {
    uid_string = pwbuf.pw_name;
    errc = 0;
  }

  cacheUserIds(uid, uid_string);
  return uid_string;
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
Mapping::GidToGroupName(gid_t gid, int& errc, size_t buffersize)
{
  errc = 0;
  {
    std::scoped_lock lock(gPhysicalGroupNameCacheMutex);
    auto kv = gPhysicalGroupNameCache.find(gid);

    if (kv != gPhysicalGroupNameCache.end()) {
      return kv->second;
    }
  }

  if (auto group_ptr = gShardedNegativeGroupNameCache.retrieve(gid)) {
    return *group_ptr;
  }

  {
    char buffer[buffersize];
    int buflen = sizeof(buffer);
    struct group grbuf;
    struct group* grbufp = 0;
    std::string gid_string = "";

    if (getgrgid_r(gid, &grbuf, buffer, buflen, &grbufp) || (!grbufp)) {
      if (errno == ERANGE) {
        if (buffersize < (16 * 1024 * 1024)) {
          // try doubling the buffer
          return GidToGroupName(gid, errc, 2 * buffersize);
        }

        // just give up here
      }

      // cannot translate this name
      char sgid[1024];
      snprintf(sgid, sizeof(sgid) - 1, "%u", gid);
      gid_string = sgid;
      errc = EINVAL;
      gShardedNegativeGroupNameCache.store(gid,
                                           std::make_unique<std::string>(gid_string));
      return gid_string;
    } else {
      gid_string = grbuf.gr_name;
      errc = 0;
    }

    cacheGroupIds(gid, gid_string);
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
Mapping::UserNameToUid(const std::string& username, int& errc)
{
  {
    std::scoped_lock lock(gPhysicalUserNameCacheMutex);

    if (auto kv = gPhysicalUserIdCache.find(username);
        kv != gPhysicalUserIdCache.end()) {
      return kv->second;
    }
  }
  char buffer[131072];
  int buflen = sizeof(buffer);
  uid_t uid = VirtualIdentity::kNobodyUid;
  struct passwd pwbuf;
  struct passwd* pwbufp = 0;
  errc = 0;
  (void) getpwnam_r(username.c_str(), &pwbuf, buffer, buflen, &pwbufp);

  if (pwbufp == NULL) {
    bool is_number = true;

    for (size_t i = 0; i < username.length(); i++) {
      if (!isdigit(username[i])) {
        is_number = false;
        break;
      }
    }

    uid = atoi(username.c_str());

    if ((uid != 0) && (is_number)) {
      errc = 0;
      return uid;
    } else {
      errc = EINVAL;
      uid = VirtualIdentity::kNobodyUid;
      return uid;
    }
  } else {
    uid = pwbuf.pw_uid;
    errc = 0;
  }

  if (!errc) {
    cacheUserIds(uid, username);
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
Mapping::GroupNameToGid(const std::string& groupname, int& errc)
{
  {
    std::scoped_lock lock(gPhysicalGroupNameCacheMutex);

    if (auto kv = gPhysicalGroupIdCache.find(groupname);
        kv != gPhysicalGroupIdCache.end()) {
      return kv->second;
    }
  }
  char buffer[131072];
  int buflen = sizeof(buffer);
  struct group grbuf;
  struct group* grbufp = 0;
  gid_t gid = VirtualIdentity::kNobodyGid;
  errc = 0;
  (void) getgrnam_r(groupname.c_str(), &grbuf, buffer, buflen, &grbufp);

  if (!grbufp) {
    bool is_number = true;

    for (size_t i = 0; i < groupname.length(); i++) {
      if (!isdigit(groupname[i])) {
        is_number = false;
        break;
      }
    }

    gid = atoi(groupname.c_str());

    if ((gid != 0) && (is_number)) {
      errc = 0;
      return gid;
    } else {
      errc = EINVAL;
      gid = VirtualIdentity::kNobodyGid;
    }
  } else {
    gid = grbuf.gr_gid;
    errc = 0;
  }

  if (!errc) {
    cacheGroupIds(gid, groupname);
  }

  return gid;
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

std::string
Mapping::ip_cache::GetIp(const char* hostname)
{
  time_t now = time(NULL);
  {
    // check for an existing translation
    RWMutexReadLock guard(mLocker);

    if (mIp2HostMap.count(hostname) &&
        mIp2HostMap[hostname].first > now) {
      eos_static_debug("status=cached host=%s ip=%s", hostname,
                       mIp2HostMap[hostname].second.c_str());
      // give cached entry
      return mIp2HostMap[hostname].second;
    }
  }
  {
    // refresh an entry
    XrdNetAddr* addrs  = 0;
    int         nAddrs = 0;
    const char* err    = XrdNetUtils::GetAddrs(hostname, &addrs, nAddrs,
                         XrdNetUtils::allIPv64,
                         XrdNetUtils::NoPortRaw);

    if (err || nAddrs == 0) {
      return "";
    }

    char buffer[64];
    int hostlen = addrs[0].Format(buffer, sizeof(buffer),
                                  XrdNetAddrInfo::fmtAddr,
                                  XrdNetAddrInfo::noPortRaw);
    delete [] addrs;

    if (hostlen > 0) {
      RWMutexWriteLock guard(mLocker);
      std::string sip(buffer, hostlen);
      mIp2HostMap[hostname] = std::make_pair(now + mLifeTime, sip);
      eos_static_debug("status=refresh host=%s ip=%s", hostname,
                       mIp2HostMap[hostname].second.c_str());
      return sip;
    }

    return "";
  }
}

//------------------------------------------------------------------------------
// Convert a comma separated uid string to a vector uid list
//------------------------------------------------------------------------------
void
Mapping::CommaListToUidSet(const char* list, std::set<uid_t>& uids_set)
{
  XrdOucString slist = list;
  XrdOucString number = "";
  int kommapos;

  if (!slist.endswith(",")) {
    slist += ",";
  }

  do {
    kommapos = slist.find(",");

    if (kommapos != STR_NPOS) {
      number.assign(slist, 0, kommapos - 1);
      std::string username = number.c_str();
      int errc = 0;
      uid_t uid ;

      if (std::find_if(username.begin(), username.end(),
      [](unsigned char c) {
      return std::isalpha(c);
      })
      != username.end()) {
        uid = eos::common::Mapping::UserNameToUid(username, errc);
      } else {
        try {
          uid = std::stoul(username);
        } catch (const std::exception& e) {
          uid = VirtualIdentity::kNobodyUid;
        }
      }

      if (!errc) {
        uids_set.insert(uid);
      }

      slist.erase(0, kommapos + 1);
    }
  } while (kommapos != STR_NPOS);
}

//------------------------------------------------------------------------------
// Convert a komma separated gid string to a vector gid list
//------------------------------------------------------------------------------
void
Mapping::CommaListToGidSet(const char* list, std::set<gid_t>& gids_set)
{
  XrdOucString slist = list;
  XrdOucString number = "";
  int kommapos;

  if (!slist.endswith(",")) {
    slist += ",";
  }

  do {
    kommapos = slist.find(",");

    if (kommapos != STR_NPOS) {
      number.assign(slist, 0, kommapos - 1);
      int errc = 0;
      std::string groupname = number.c_str();
      gid_t gid = GroupNameToGid(groupname, errc);

      if (!errc) {
        gids_set.insert(gid);
      }

      slist.erase(0, kommapos + 1);
    }
  } while (kommapos != STR_NPOS);
}


// -----------------------------------------------------------------------------
//! Compare a uid with the string representation
// -----------------------------------------------------------------------------

bool Mapping::IsUid(XrdOucString idstring, uid_t& id)
{
  id = strtoul(idstring.c_str(), 0, 10);
  char revid[1024];
  sprintf(revid, "%lu", (unsigned long) id);
  XrdOucString srevid = revid;

  if (idstring == srevid) {
    return true;
  }

  return false;
}

// -----------------------------------------------------------------------------
//! Compare a gid with the string representation
// -----------------------------------------------------------------------------

bool Mapping::IsGid(XrdOucString idstring, gid_t& id)
{
  id = strtoul(idstring.c_str(), 0, 10);
  char revid[1024];
  sprintf(revid, "%lu", (unsigned long) id);
  XrdOucString srevid = revid;

  if (idstring == srevid) {
    return true;
  }

  return false;
}

// -----------------------------------------------------------------------------
//! Reduce the trace identifier information to user@host
// -----------------------------------------------------------------------------

const char* Mapping::ReduceTident(XrdOucString& tident,
                                  XrdOucString& wildcardtident, XrdOucString& mytident, XrdOucString& myhost)
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

std::string Mapping::ReduceTident(std::string_view tident,
                                  std::string& wildcardtident, std::string& myhost)
{
  auto dotpos = tident.find(".");
  auto addpos = tident.find("@");
  std::string mytident{tident};
  mytident.erase(dotpos, addpos - dotpos);
  myhost = tident.substr(addpos + 1);
  wildcardtident = "*@" + myhost;
  return mytident;
}

// -----------------------------------------------------------------------------
//! Convert a uid into a string
// -----------------------------------------------------------------------------
std::string Mapping::UidAsString(uid_t uid)
{
  std::string uidstring = "";
  char suid[1024];
  snprintf(suid, sizeof(suid) - 1, "%u", uid);
  uidstring = suid;
  return uidstring;
}

// -----------------------------------------------------------------------------
//! Convert a gid into a string
// -----------------------------------------------------------------------------

std::string Mapping::GidAsString(gid_t gid)
{
  std::string gidstring = "";
  char sgid[1024];
  snprintf(sgid, sizeof(sgid) - 1, "%u", gid);
  gidstring = sgid;
  return gidstring;
}

//------------------------------------------------------------------------------
//! Function converting vid frin a string representation
//------------------------------------------------------------------------------

bool Mapping::VidFromString(VirtualIdentity& vid,
                            const char* vidstring)
{
  std::string svid = vidstring;
  std::vector<std::string> tokens;
  eos::common::StringConversion::EmptyTokenize(
    vidstring,
    tokens,
    ":");

  if (tokens.size() != 7) {
    return false;
  }

  vid.uid = strtoul(tokens[0].c_str(), 0, 10);
  vid.gid = strtoul(tokens[1].c_str(), 0, 10);
  vid.uid_string = tokens[2].c_str();
  vid.gid_string = tokens[3].c_str();
  vid.name = tokens[4].c_str();
  vid.prot = tokens[5].c_str();
  vid.tident = tokens[6].c_str();
  return true;
}

//----------------------------------------------------------------------------
//! Function converting vid to a string representation
//----------------------------------------------------------------------------

std::string Mapping::VidToString(VirtualIdentity& vid)
{
  char vids[4096];
  snprintf(vids, sizeof(vids), "%u:%u:%s:%s:%s:%s:%s",
           vid.uid,
           vid.gid,
           vid.uid_string.c_str(),
           vid.gid_string.c_str(),
           vid.name.c_str(),
           vid.prot.c_str(),
           vid.tident.c_str());
  return std::string(vids);
}

//------------------------------------------------------------------------------
//! Function returning a VID from a name
//------------------------------------------------------------------------------
VirtualIdentity Mapping::Someone(const std::string& name)
{
  VirtualIdentity vid;
  vid = VirtualIdentity::Nobody();
  int errc = 0;
  uid_t uid = UserNameToUid(name, errc);

  if (!errc) {
    vid.uid = uid;
    vid.uid_string = name;
    vid.name = name.c_str();
    vid.tident = std::string(name + "@grpc").c_str();
  }

  return vid;
}

//------------------------------------------------------------------------------
//! Function returning a VID from a uid/gid pair
//------------------------------------------------------------------------------
VirtualIdentity Mapping::Someone(uid_t uid, gid_t gid)
{
  VirtualIdentity vid;
  vid = VirtualIdentity::Nobody();
  int errc = 0;
  vid.uid = uid;
  vid.gid = gid;
  vid.allowed_uids = {uid, VirtualIdentity::kNobodyUid};
  vid.allowed_gids = {gid, VirtualIdentity::kNobodyGid};
  vid.sudoer = false;
  vid.gateway = false;
  vid.uid_string = UidToUserName(uid, errc);

  if (!errc) {
    vid.name = vid.uid_string.c_str();
  } else {
    vid.name = UidAsString(uid).c_str();
  }

  vid.gid_string = GidToGroupName(gid, errc);
  vid.tident = std::string(vid.uid_string + "@grpc").c_str();
  return vid;
}

//------------------------------------------------------------------------------
//! Function testing if an OAUTH2 resource is allowed by the configuration
//------------------------------------------------------------------------------
bool
Mapping::IsOAuth2Resource(const std::string& resource)
{
  eos::common::RWMutexReadLock lock(eos::common::Mapping::gMapMutex);
  std::string uidkey = "oauth2:\"";
  uidkey += "key:";
  uidkey += resource;
  uidkey += "\":uid";
  return gVirtualUidMap.count(uidkey);
}

//------------------------------------------------------------------------------
//! Decode the uid from a trace ID string
//------------------------------------------------------------------------------
uid_t
Mapping::UidFromTident(const std::string& tident)
{
  std::vector<std::string> tokens;
  std::string delimiter = "^";
  eos::common::StringConversion::Tokenize(tident, tokens, delimiter);

  if (tokens.size()) {
    return atoi(tokens[0].c_str());
  }

  return 0;
}

//------------------------------------------------------------------------------
//! Return number of active sessions for a given uid
//------------------------------------------------------------------------------
size_t
Mapping::ActiveSessions(uid_t uid)
{
  //XrdSysMutexHelper mLock(ActiveLock);
  if (auto n = ActiveUidsSharded.retrieve(uid)) {
    return *n;
  }

  return 0;
}


size_t
Mapping::ActiveSessions()
{
  return ActiveTidentsSharded.num_entries();
}

void
Mapping::addSecondaryGroups(VirtualIdentity& vid, const std::string& name,
                            gid_t gid)
{
  if (!gSecondaryGroups) {
    return;
  }

  populateGroups(name, gid, vid, gGroupsFetcher.get());
}

void
Mapping::getPhysicalIdShards(const std::string& name, VirtualIdentity& vid)
{
  if (name.empty()) {
    return;
  }

  struct passwd passwdinfo;

  char buffer[131072];

  size_t buflen = sizeof(buffer);

  memset(&passwdinfo, 0, sizeof(passwdinfo));

  eos_static_debug("find in uid cache %s cache shard=%d", name.c_str(),
                   gShardedPhysicalUidCache.calculateShard(name));

  std::unique_ptr<id_pair> idp {nullptr};

  bool in_uid_cache {false};

  if (auto id_ptr = gShardedPhysicalUidCache.retrieve(name)) {
    vid.uid = id_ptr->uid;
    vid.gid = id_ptr->gid;
    vid.uid_string = name;
    // FIXME use a value type!
    idp.reset(new id_pair(vid.uid, vid.gid));
    in_uid_cache = true;
    eos_static_debug("msg=\"found in uid cache\" name=%s", name.c_str());
  } else {
    eos_static_debug("msg=\"not found in uid cache\" name=%s", name.c_str());
    bool use_pw = true;

    if (name.length() == 8) {
      bool known_tident = false;

      if (startsWith(name, "*") || startsWith(name, "~") || startsWith(name, "_")) {
        known_tident = true;
        vid.allowed_uids.clear();
        vid.allowed_gids.clear();
        // that is a new base-64 encoded id following the format '*1234567'
        // where 1234567 is the base64 encoded 42-bit value of 20-bit uid |
        // 16-bit gid | 6-bit session id.
        std::string b64name = name;
        b64name.erase(0, 1);
        // Decoden '_' -> '/', '-' -> '+' that was done to ensure the validity
        // of the XRootD URL.
        std::replace(b64name.begin(), b64name.end(), '_', '/');
        std::replace(b64name.begin(), b64name.end(), '-', '+');
        b64name += "=";
        unsigned long long bituser = 0;
        char* out = 0;
        ssize_t outlen;

        if (eos::common::SymKey::Base64Decode(b64name.c_str(), out, outlen)) {
          if (outlen <= 8) {
            memcpy((((char*) &bituser)) + 8 - outlen, out, outlen);
            eos_static_debug("msg=\"decoded base-64 uid/gid/sid\" val=%llx val=%llx",
                             bituser, n_tohll(bituser));
          } else {
            eos_static_err("msg=\"decoded base-64 uid/gid/sid too long\" len=%d", outlen);
            return;
          }

          bituser = n_tohll(bituser);

          if (out) {
            free(out);
          }

          if (startsWith(name, "*") || startsWith(name, "_")) {
            idp.reset(new id_pair((bituser >> 22) & 0xfffff, (bituser >> 6) & 0xffff));
            struct passwd* pwbufp = 0;

            if (getpwuid_r(idp->uid, &passwdinfo, buffer, buflen, &pwbufp) || (!pwbufp)) {
              return;
            }

            cacheUserIds(passwdinfo.pw_uid, passwdinfo.pw_name);
            vid.uid_string = passwdinfo.pw_name;

            if (idp->gid != passwdinfo.pw_gid) {
              // add the primary group if it is not the desired one
              vid.allowed_gids.insert(passwdinfo.pw_gid);
            }
          } else {
            // only user id got forwarded, we retrieve the corresponding group
            uid_t ruid = (bituser >> 6) & 0xfffffffff;
            struct passwd* pwbufp = 0;

            if (getpwuid_r(ruid, &passwdinfo, buffer, buflen, &pwbufp) || (!pwbufp)) {
              return;
            }

            idp.reset(new id_pair(passwdinfo.pw_uid, passwdinfo.pw_gid));
            vid.uid_string = passwdinfo.pw_name;
            cacheUserIds(passwdinfo.pw_uid, passwdinfo.pw_name);
          }

          eos_static_debug("using base64 mapping %s %d %d", name.c_str(), idp->uid,
                           idp->gid);
        } else {
          eos_static_err("msg=\"failed to decoded base-64 uid/gid/sid\" id=%s",
                         name.c_str());
          return;
        }
      }

      if (known_tident) {
        // unlikely as all the code paths here should have populated idp, but
        // just defensive programming
        if (!idp) {
          eos_static_err("msg=\"failed to retrieve id for\" name=%s",
                         name.c_str());
          return;
        }

        if (gRootSquash && idp && (!idp->uid || !idp->gid)) {
          return;
        }

        vid.uid = idp->uid;
        vid.gid = idp->gid;
        vid.allowed_uids.insert(vid.uid);
        vid.allowed_gids.insert(vid.gid);
        vid.allowed_uids.insert(99);
        vid.allowed_gids.insert(99);

        // If uid_string empty try best effort with the given "name"
        if (vid.uid_string.empty()) {
          addSecondaryGroups(vid, name, idp->gid);
        } else {
          addSecondaryGroups(vid, vid.uid_string, idp->gid);
        }

        auto gs = std::make_unique<gid_set>(vid.allowed_gids);
        eos_static_debug("adding to cache uid=%u gid=%u", idp->uid, idp->gid);
        gShardedPhysicalUidCache.store(name, std::move(idp));
        gShardedPhysicalGidCache.store(name, std::move(gs));
        return;
      }
    }

    if (use_pw) {
      if (auto ptr = gShardedNegativePhysicalUidCache.retrieve(name)) {
        eos_static_debug("msg=\"found in negative user name cache\" name=%s",
                         name.c_str());
        return;
      }

      struct passwd* pwbufp = 0;

      {
        if (getpwnam_r(name.c_str(), &passwdinfo, buffer, buflen, &pwbufp) ||
            (!pwbufp)) {
          gShardedNegativePhysicalUidCache.store(name, std::make_unique<bool>(true));
          return;
        }
      }

      idp.reset(new id_pair(passwdinfo.pw_uid, passwdinfo.pw_gid));
      vid.uid = idp->uid;
      vid.gid = idp->gid;
      vid.uid_string = passwdinfo.pw_name;
      cacheUserIds(passwdinfo.pw_uid, passwdinfo.pw_name);
    }
  }

  if (auto gv = gShardedPhysicalGidCache.retrieve(name)) {
    vid.allowed_uids.insert(idp->uid);
    vid.allowed_gids = *gv;
    vid.uid = idp->uid;
    vid.gid = idp->gid;
    eos_static_debug("msg=\"returning\" uid=%u gid=%u", idp->uid, idp->gid);

    if (!in_uid_cache) {
      eos_static_debug("msg=\"adding to cache\" uid=%u gid=%u", idp->uid, idp->gid);
      gShardedPhysicalUidCache.store(name, std::move(idp));
    }

    return;
  }

  // If uid_string empty try best effort with the given "name"
  if (vid.uid_string.empty()) {
    addSecondaryGroups(vid, name, idp->gid);
  } else {
    addSecondaryGroups(vid, vid.uid_string, idp->gid);
  }

  // add to the cache
  if (!in_uid_cache) {
    eos_static_debug("msg=\"adding to cache\" uid=%u gid=%u", idp->uid, idp->gid);
    gShardedPhysicalUidCache.store(name, std::move(idp));
  }

  gShardedPhysicalGidCache.store(name,
                                 std::make_unique<gid_set>(vid.allowed_gids));
  return;
}

void
Mapping::getPhysicalUids(const char* name, VirtualIdentity& vid)
{
  Mapping::getPhysicalIdShards(name, vid);
  vid.gid = VirtualIdentity::kNobodyGid;
  vid.allowed_gids.clear();
  vid.allowed_gids.insert(vid.gid);
}

void
Mapping::getPhysicalGids(const char* name, VirtualIdentity& vid)
{
  uid_t uid = vid.uid;
  Mapping::getPhysicalIdShards(name, vid);
  vid.uid = uid;
  vid.allowed_uids.clear();
  vid.allowed_uids.insert(uid);
  vid.allowed_uids.insert(VirtualIdentity::kNobodyUid);
}

void
Mapping::getPhysicalUidGids(const char* name, VirtualIdentity& vid)
{
  Mapping::getPhysicalIdShards(name, vid);
  vid.allowed_uids.clear();
  vid.allowed_gids.clear();
  vid.allowed_uids.insert(vid.uid);
  vid.allowed_gids.insert(vid.gid);
  vid.allowed_uids.insert(VirtualIdentity::kNobodyUid);
  vid.allowed_gids.insert(VirtualIdentity::kNobodyGid);
}

void
Mapping::cacheUserIds(uid_t uid, const std::string& username)
{
  std::scoped_lock lock(gPhysicalUserNameCacheMutex);
  gPhysicalUserIdCache[username] = uid;
  gPhysicalUserNameCache[uid] = username;
}

void
Mapping::cacheGroupIds(gid_t gid, const std::string& groupname)
{
  std::scoped_lock lock(gPhysicalGroupNameCacheMutex);
  gPhysicalGroupIdCache[groupname] = gid;
  gPhysicalGroupNameCache[gid] = groupname;
}

void
Mapping::HandleUidGidMapping(const char* name, VirtualIdentity& vid,
                             const std::string& uid_key_name,
                             const std::string& gid_key_name, bool force)
{
  eos_static_debug("msg=\"handle uid gid mapping\" name=%s prot=%s",
                   name, vid.prot.c_str());
  auto kv_uid = gVirtualUidMap.find(uid_key_name);
  auto kv_gid = gVirtualGidMap.find(gid_key_name);
  bool uid_mapped = kv_uid != gVirtualUidMap.end();
  bool gid_mapped = kv_gid != gVirtualGidMap.end();

  if (force || (uid_mapped && gid_mapped &&
                (kv_uid->second == 0) && (kv_gid->second == 0))) {
    eos_static_debug("msg=\"%s uid/gid mapping\"", vid.prot.c_str());
    Mapping::getPhysicalUidGids(name, vid);
    return;
  }

  if (uid_mapped) {
    if (kv_uid->second == 0) {
      eos_static_debug("msg=\"%s uid mapping\"", vid.prot.c_str());
      Mapping::getPhysicalUids(name, vid);
    } else {
      eos_static_debug("msg=\"%s uid forced mapping\"", vid.prot.c_str());
      vid.uid = kv_uid->second;
      vid.allowed_uids.clear();
      vid.allowed_uids.insert(vid.uid);
      vid.allowed_uids.insert(VirtualIdentity::kNobodyUid);
      vid.gid = VirtualIdentity::kNobodyGid;
      vid.allowed_gids.clear();
      vid.allowed_gids.insert(vid.gid);
    }
  }

  if (gid_mapped) {
    if (kv_gid->second == 0) {
      eos_static_debug("msg=\"%s gid mapping\"", vid.prot.c_str());
      Mapping::getPhysicalGids(name, vid);
    } else {
      eos_static_debug("msg=\"%s forced gid mapping\"", vid.prot.c_str());
      vid.allowed_gids.clear();
      vid.gid = kv_gid->second;
      vid.allowed_gids.insert(vid.gid);
    }
  }
}

//------------------------------------------------------------------------------
// Print JWT token
//------------------------------------------------------------------------------
std::string
Mapping::PrintJWT(const std::string accesstoken, bool dense)
{
  std::stringstream ss;

  try {
    auto decoded = jwt::decode(accesstoken);

    try {
      if (dense) {
        ss << "issuer:" << decoded.get_issuer() << ",";
      } else {
        ss << std::left << std::setw(20) << "issuer: " << decoded.get_issuer() <<
           std::endl;
      }
    } catch (...) {
      if (!dense) {
        ss << std::endl;
      }
    }

    try {
      if (dense) {
        ss << "subject:" << decoded.get_subject() << ",";
      } else {
        ss << std::left << std::setw(20) << "subject: " << decoded.get_subject() <<
           std::endl;
      }
    } catch (...) {
      if (!dense) {
        ss << std::endl;
      }
    }

    try {
      if (dense) {
        ss << "audience:[";

        for (auto i : decoded.get_audience()) {
          ss << i << ",";
        }

        ss.seekp(-1, ss.cur);
        ss << "],";
      } else {
        ss << std::setw(20) << "audience: ";
        ss << "[";

        for (auto i : decoded.get_audience()) {
          ss << i << ",";
        }

        ss.seekp(-1, ss.cur);
        ss << "]" << std::endl;
      }
    } catch (...) {
      if (!dense) {
        ss << std::endl;
      }
    }

    if (dense) {
      ss << "claims:[";

      try {
        for (auto& e : decoded.get_payload_json()) {
          ss << e.first << ":" << e.second << ",";
        }
      } catch (...) {}

      ss.seekp(-1, ss.cur);
      ss << "]";
    } else {
      ss << std::left << std::setw(20) << "claims: ";
      ss << "{" << std::endl;;

      try {
        for (auto& e : decoded.get_payload_json()) {
          ss << std::left << std::setw(22) << " " << e.first << ":" << e.second << "," <<
             std::endl;
        }
      } catch (...) {
        ss << std::endl;
      }

      ss.seekp(-1, ss.cur);
      ss << std::endl;
      ss << std::left << std::setw(20) << " " << "}" << std::endl;
    }
  } catch (...) {
    return "<!jwt>";
  }

  return ss.str();
}

EOSCOMMONNAMESPACE_END
