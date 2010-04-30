#ifndef __XRDCOMMON_MAPPING__
#define __XRDCOMMON_MAPPING__

#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysPthread.hh"

#include <pwd.h>
#include <grp.h>


class XrdCommonMappingGroupInfo {
public:
  XrdOucString DefaultGroup;
  XrdOucString AllGroups;
  struct passwd Passwd;
  int Lifetime;
  
  XrdCommonMappingGroupInfo(const char* defgroup="", const char* allgrps="", struct passwd *pw=0, int lf=60) { DefaultGroup = defgroup; AllGroups = allgrps; if (pw) memcpy(&Passwd,pw,sizeof(struct passwd)); Lifetime=lf;}
  virtual ~XrdCommonMappingGroupInfo() {}
};

class XrdCommonMapping {
private:
public:
  static void GetPhysicalGroups(const char* __name__, XrdOucString& __allgroups__, XrdOucString& __defaultgroup__);
  static void RoleMap(const XrdSecEntity* _client,const char* _env, XrdSecEntity &_mappedclient, const char* tident, uid_t &uid, gid_t &gid, uid_t &ruid, gid_t &rgid);
  static void GetId(XrdSecEntity &_client, uid_t &_uid, gid_t &gid);

  static  XrdSysMutex              gMapMutex;
  static  XrdSysMutex              gSecEntityMutex;
  static  XrdSysMutex              gVirtualMapMutex;

  // the format of the stored XrdOucString is: [static:]<role1>[:<role2>[:role3>]...]
  // if a wild card is used the value should be only one role !

  static  XrdOucHash<XrdOucString> gUserRoleTable;  // maps from user  string name -> list of user  roles 
  static  XrdOucHash<XrdOucString> gGroupRoleTable; // maps from group string name -> list of group roles

  static  XrdOucHash<XrdSecEntity> gSecEntityStore;

  static  XrdOucHash<long>         gVirtualUidMap;  // maps from string virtual user  name -> virtual UID
  static  XrdOucHash<long>         gVirtualGidMap;  // maps from string virtual group name -> virtual GID
  static  XrdOucHash<XrdOucString> gVirtualGroupMemberShip; // maps from string virtual name -> list of virtual group names (: separated)
  
  static  XrdOucHash<struct passwd> gPasswdStore;
  static  XrdOucHash<XrdCommonMappingGroupInfo>  gGroupInfoCache;
};

#endif
