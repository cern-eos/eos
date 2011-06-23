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
#include <vector>
#include <string>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class Mapping {
private:
public:

  typedef std::vector<uid_t> uid_vector;
  typedef std::vector<gid_t> gid_vector;
  typedef std::map<uid_t, uid_vector > UserRoleMap_t;
  typedef std::map<uid_t, gid_vector > GroupRoleMap_t;
  typedef std::map<std::string, uid_t> VirtualUserMap_t;
  typedef std::map<std::string, gid_t> VirtualGroupMap_t;
  typedef std::map<uid_t, bool > SudoerMap_t;

  class id_pair {
  public:
    uid_t uid;
    gid_t gid;
    id_pair(uid_t iuid, gid_t igid) { uid = iuid; gid = igid; }
    ~id_pair(){};
  };

  struct VirtualIdentity_t {
    uid_t uid;
    gid_t gid;
    uid_vector uid_list;
    gid_vector gid_list;
    XrdOucString tident;
    XrdOucString name;
    XrdOucString prot;
    std::string host;
    bool sudoer;
  };

  typedef struct VirtualIdentity_t VirtualIdentity;

  static void Nobody(VirtualIdentity &vid) {
    vid.uid=vid.gid=99; vid.uid_list.clear(); vid.gid_list.clear(); vid.uid_list.push_back(99);vid.gid_list.push_back(99); vid.sudoer =false;
  }

  static void Root(VirtualIdentity &vid) {
    vid.uid=vid.gid=0; vid.uid_list.clear(); vid.gid_list.clear(); vid.uid_list.push_back(0);vid.gid_list.push_back(0); vid.sudoer =false;
  }

  static void Copy(VirtualIdentity &vidin, VirtualIdentity &vidout) {
    vidout.uid = vidin.uid; vidout.gid = vidin.gid;
    vidout.sudoer = vidin.sudoer;
    vidout.name = vidin.name;
    vidout.tident = vidin.tident;
    vidout.prot = vidin.prot;
    vidout.uid_list.clear();
    vidout.gid_list.clear();
    vidout.host = vidin.host;

    for (unsigned int i=0; i< vidin.uid_list.size(); i++) vidout.uid_list.push_back(vidin.uid_list[i]);
    for (unsigned int i=0; i< vidin.gid_list.size(); i++) vidout.gid_list.push_back(vidin.gid_list[i]);
  }

  static void IdMap(const XrdSecEntity* client,const char* env, const char* tident, Mapping::VirtualIdentity &vid);

  static UserRoleMap_t  gUserRoleVector;  // describes which virtual user roles  a user with uid has
  static GroupRoleMap_t gGroupRoleVector; // describes which virtual group roles a user with uid has

  static VirtualUserMap_t  gVirtualUidMap;
  static VirtualGroupMap_t gVirtualGidMap;

  static SudoerMap_t gSudoerMap;

  static XrdOucHash<id_pair>    gPhysicalUidCache;
  static XrdOucHash<gid_vector> gPhysicalGidCache;

  static RWMutex gPhysicalIdMutex; // protects the physical ID cache

  static RWMutex gMapMutex;        // protects all global map hashes

  static XrdSysMutex ActiveLock;
  static void ActiveExpire(int interval=24*3600);
  static std::map<std::string, time_t> ActiveTidents;

  static bool gRootSquash;          // we never allow remote root mounts, this is statically set to true

  static  void KommaListToUidVector(const char* list, std::vector<uid_t> &vector_list) {
    XrdOucString slist = list;
    XrdOucString number="";
    int kommapos;
    if (!slist.endswith(",")) 
      slist += ",";
    do {
      kommapos = slist.find(",");
      if (kommapos != STR_NPOS) {
	number.assign(slist,0,kommapos-1);
	vector_list.push_back((uid_t)atoi(number.c_str()));
	slist.erase(0,kommapos+1);
      }
    } while (kommapos != STR_NPOS);
  }

  static  void KommaListToGidVector(const char* list, std::vector<gid_t> &vector_list) {
    XrdOucString slist = list;
    XrdOucString number="";
    int kommapos;
    if (!slist.endswith(",")) 
      slist += ",";
    do {
      kommapos = slist.find(",");
      if (kommapos != STR_NPOS) {
	number.assign(slist,0,kommapos-1);
	vector_list.push_back((gid_t)atoi(number.c_str()));
	slist.erase(0,kommapos+1);
      }
    } while (kommapos != STR_NPOS);
  }

  static  void Print(XrdOucString &stdOut, XrdOucString option="");

  static void getPhysicalIds(const char* name, VirtualIdentity &vid);

  static bool HasUid(uid_t uid, uid_vector vector) {
    uid_vector::const_iterator it;
    for (it = vector.begin(); it != vector.end(); ++it) {
      if ((*it) == uid)
	return true;
    }
    return false;
  }

  static bool HasGid(gid_t gid, gid_vector vector) {
    uid_vector::const_iterator it;
    for (it = vector.begin(); it != vector.end(); ++it) {
      if ((*it) == gid)
	return true;
    }
    return false;
  }

  static bool IsUid(XrdOucString idstring,uid_t &id) {
    id = strtoul(idstring.c_str(),0,10);
    char revid[1024];
    sprintf(revid,"%lu",(unsigned long)id);
    XrdOucString srevid=revid;
    if (idstring == srevid)
      return true;
    return false;
  }


  static bool IsGid(XrdOucString idstring,gid_t &id) {
    id = strtoul(idstring.c_str(),0,10);
    char revid[1024];
    sprintf(revid,"%lu",(unsigned long)id);
    XrdOucString srevid=revid;
    if (idstring == srevid)
      return true;
    return false;
  }

  static const char* ReduceTident(XrdOucString &tident, XrdOucString &wildcardtident, XrdOucString &mytident, XrdOucString &myhost) {
    int dotpos = tident.find(".");
    int addpos = tident.find("@");
    wildcardtident = tident;
    mytident = tident;
    mytident.erase(dotpos,addpos-dotpos);
    myhost = mytident;
    dotpos = mytident.find("@");
    myhost.erase(0,dotpos+1);
    wildcardtident = mytident;
    addpos = wildcardtident.find("@");
    wildcardtident.erase(0,addpos);
    wildcardtident = "*" + wildcardtident;
    return mytident.c_str();
  }

  static std::string UidToUserName(uid_t uid, int &errc);

  static std::string GidToGroupName(uid_t gid, int &errc);

  static uid_t UserNameToUid(std::string &username, int &errc);
  
  static gid_t GroupNameToGid(std::string &groupname, int &errc);

  static std::string UidAsString(uid_t uid) {
    std::string uidstring="";
    char suid[1024];
    snprintf(suid, sizeof(suid)-1,"%u", uid);
    uidstring = suid;
    return uidstring;
  }

  static std::string GidAsString(gid_t gid) {
    std::string gidstring="";
    char sgid[1024];
    snprintf(sgid, sizeof(sgid)-1,"%u", gid);
    gidstring = sgid;
    return gidstring;
  }

};

EOSCOMMONNAMESPACE_END

#endif
