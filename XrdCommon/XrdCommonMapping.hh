#ifndef __XRDCOMMON_MAPPING__
#define __XRDCOMMON_MAPPING__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"

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

#include <google/sparse_hash_map>

/*----------------------------------------------------------------------------*/
class XrdCommonMapping {
private:
public:

  typedef std::vector<uid_t> uid_vector;
  typedef std::vector<gid_t> gid_vector;
  typedef google::sparse_hash_map<uid_t, uid_vector > UserRoleMap;
  typedef google::sparse_hash_map<uid_t, gid_vector > GroupRoleMap;
  typedef google::sparse_hash_map<std::string, uid_t> VirtualUserMap;
  typedef google::sparse_hash_map<std::string, gid_t> VirtualGroupMap;
  typedef google::sparse_hash_map<uid_t, bool > SudoerMap;

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
    bool sudoer;
  };

  typedef struct VirtualIdentity_t VirtualIdentity;

  static void Nobody(VirtualIdentity &vid) {
    vid.uid=vid.gid=99; vid.uid_list.clear(); vid.gid_list.clear(); vid.uid_list.push_back(99);vid.gid_list.push_back(99); vid.sudoer =false;
  }

  static void Copy(VirtualIdentity &vidin, VirtualIdentity &vidout) {
    vidout.uid = vidin.uid; vidout.gid = vidin.gid;
    vidout.sudoer = vidin.sudoer;
    for (unsigned int i=0; i< vidin.uid_list.size(); i++) vidout.uid_list.push_back(vidin.uid_list[i]);
    for (unsigned int i=0; i< vidin.gid_list.size(); i++) vidout.gid_list.push_back(vidin.uid_list[i]);
  }

  static void IdMap(const XrdSecEntity* client,const char* env, const char* tident, XrdCommonMapping::VirtualIdentity &vid);

  static UserRoleMap  gUserRoleVector;  // describes which virtual user roles  a user with uid has
  static GroupRoleMap gGroupRoleVector; // describes which virtual group roles a user with uid has

  static VirtualUserMap  gVirtualUidMap;
  static VirtualGroupMap gVirtualGidMap;

  static SudoerMap gSudoerMap;
  
  static  XrdSysMutex gMapMutex; // protects all global vector & maps

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


  static XrdOucHash<id_pair> gPhysicalUidCache;
  static XrdOucHash<gid_vector> gPhysicalGidCache;
};

#endif
