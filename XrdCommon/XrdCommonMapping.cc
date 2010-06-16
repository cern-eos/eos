#include "XrdCommon/XrdCommonMapping.hh"
#include "XrdCommon/XrdCommonStringStore.hh"

/*----------------------------------------------------------------------------*/
XrdSysMutex                       XrdCommonMapping::gMapMutex;
XrdCommonMapping::UserRoleMap     XrdCommonMapping::gUserRoleVector;
XrdCommonMapping::GroupRoleMap    XrdCommonMapping::gGroupRoleVector;
XrdCommonMapping::VirtualUserMap  XrdCommonMapping::gVirtualUidMap;
XrdCommonMapping::VirtualGroupMap XrdCommonMapping::gVirtualGidMap;
XrdCommonMapping::SudoerMap       XrdCommonMapping::gSudoerMap;


XrdOucHash<XrdCommonMapping::id_pair>    XrdCommonMapping::gPhysicalUidCache;
XrdOucHash<XrdCommonMapping::gid_vector> XrdCommonMapping::gPhysicalGidCache;

/*----------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------*/
void 
XrdCommonMapping::IdMap(const XrdSecEntity* client,const char* env, const char* tident, XrdCommonMapping::VirtualIdentity &vid)
{

  eos_static_debug("name:%s role:%s group:%s", client->name, client->role, client->grps);

  // you first are 'nobody'
  Nobody(vid);
  XrdOucEnv Env(env);

  if (!client) 
    return;

  vid.name = client->name;
  vid.tident = tident;

  // first map by alias
  XrdOucString useralias = client->prot;
  useralias += ":";
  useralias += "\"";
  useralias += client->name;
  useralias += "\"";
  useralias += ":";
  XrdOucString groupalias=useralias;
  useralias += "uid";
  groupalias+= "gid";


  gMapMutex.Lock();

  if ((gVirtualUidMap.count("krb5:\"<pwd>\":uid")) || (gVirtualUidMap.count("ssl:\"<pwd>\":uid"))) {
    //    eos_static_debug("physical mapping");

    // use physical mapping for kerberos names
    XrdCommonMapping::getPhysicalIds(client->name, vid);
    vid.gid=99;
    vid.gid_list.clear();
  }
  
  if ((gVirtualGidMap.count("krb5:\"<pwd>\":gid")) || (gVirtualGidMap.count("ssl:\"<pwd>\":gid"))) {
    //    eos_static_debug("physical mapping");
    // use physical mapping for kerberos names
    uid_t uid = vid.uid;
    XrdCommonMapping::getPhysicalIds(client->name, vid);
    vid.uid = uid;
    vid.uid_list.clear();
    vid.uid_list.push_back(uid);
    vid.uid_list.push_back(99);
  }

  // tident mapping
  XrdOucString mytident="";
  XrdOucString stident = "tident:"; stident += "\""; stident += ReduceTident(vid.tident, mytident); 
  XrdOucString suidtident = stident; suidtident += "\":uid";
  XrdOucString sgidtident = stident; sgidtident += "\":gid";

  if ((gVirtualUidMap.count(suidtident.c_str()))) {
    //    eos_static_debug("tident mapping");
    vid.uid = gVirtualUidMap[suidtident.c_str()];
    if (!HasUid(vid.uid,vid.uid_list)) vid.uid_list.push_back(vid.uid);
    if (!HasUid(99, vid.uid_list)) vid.uid_list.push_back(99);
  }

  if ((gVirtualGidMap.count(sgidtident.c_str()))) {
    //    eos_static_debug("tident mapping");
    vid.gid = gVirtualGidMap[sgidtident.c_str()];
    if (!HasGid(vid.gid,vid.gid_list)) vid.gid_list.push_back(vid.gid);
    if (!HasGid(99, vid.gid_list)) vid.gid_list.push_back(99);
  }

  // the configuration door for localhost clients adds always the adm/adm vid's
  fprintf(stderr,"tident=%s\n", suidtident.c_str());
  if (suidtident == "tident:\"root@localhost.localdomain\":uid") {
    vid.sudoer = true;
    vid.uid = 3;
    vid.gid = 4;
    if (!HasUid(3,vid.uid_list)) vid.uid_list.push_back(vid.uid);
    if (!HasGid(4,vid.gid_list)) vid.gid_list.push_back(vid.gid);
  }

  // explicit virtual mapping overrules physical mappings - the second one comes from the physical mapping before
  vid.uid = (gVirtualUidMap.count(useralias.c_str())) ?gVirtualUidMap[useralias.c_str() ]:vid.uid;
  if (!HasUid(vid.uid,vid.uid_list)) vid.uid_list.insert(vid.uid_list.begin(),vid.uid);
  vid.gid = (gVirtualGidMap.count(groupalias.c_str()))?gVirtualGidMap[groupalias.c_str()]:vid.gid;

  // eos_static_debug("mapped %d %d", vid.uid,vid.gid);

  if (!HasGid(vid.gid,vid.gid_list)) vid.gid_list.insert(vid.gid_list.begin(),vid.gid);

  // add virtual user and group roles - if any 
  if (gUserRoleVector.count(vid.uid)) {
    uid_vector::const_iterator it;
    for (it = gUserRoleVector[vid.uid].begin(); it != gUserRoleVector[vid.uid].end(); ++it)
      if (!HasUid((*it),vid.uid_list)) vid.uid_list.push_back((*it));
  }

  if (gUserRoleVector.count(vid.gid)) {
    gid_vector::const_iterator it;
    for (it = gGroupRoleVector[vid.gid].begin(); it != gGroupRoleVector[vid.gid].end(); ++it)
      if (!HasGid((*it),vid.gid_list)) vid.gid_list.push_back((*it));
  }


  // select roles
  XrdOucString ruid = Env.Get("eos.ruid");
  XrdOucString rgid = Env.Get("eos.rgid");
  uid_t sel_uid = vid.uid;
  uid_t sel_gid = vid.gid;

  if (ruid.length()) {
    if (!IsUid(ruid,sel_uid)) {
      // try alias conversion
      sel_uid = (gVirtualUidMap.count(ruid.c_str())) ?gVirtualUidMap[ruid.c_str() ]:99;
    }
  }

  if (rgid.length()) {
    if (!IsGid(rgid,sel_gid)) {
      // try alias conversion
      sel_gid = (gVirtualGidMap.count(rgid.c_str()))?gVirtualGidMap[rgid.c_str()]:99;
    }
  }
    
  // set sudoer flag
  if (gSudoerMap.count(vid.uid)) {
    vid.sudoer = true;
  }

  // check if we are allowed to take sel_uid & sel_gid
  if (!vid.sudoer) {
    // if we are not a sudore, scan the allowed ids
    if (HasUid(sel_uid, vid.uid_list))
      vid.uid = sel_uid;
    else 
      vid.uid = 99;

    if (HasGid(sel_gid, vid.gid_list))
      vid.gid = sel_gid;
    else
      vid.gid = 99;
  } else {
    vid.uid = sel_uid;
    vid.gid = sel_gid;
  }

  eos_static_debug("selected %d %d [%s %s]", vid.uid,vid.gid, ruid.c_str(),rgid.c_str());
  
  gMapMutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void
XrdCommonMapping::Print(XrdOucString &stdOut, XrdOucString option)
{
  if ((!option.length()) || ( (option.find("u"))!=STR_NPOS)) {
    UserRoleMap::const_iterator it;
    for ( it = gUserRoleVector.begin(); it != gUserRoleVector.end(); ++it) {
      char iuid[4096];
      sprintf(iuid,"%d", it->first);
      char suid[4096];
      sprintf(suid,"%-6s",iuid);
      stdOut += "membership uid: ";stdOut += suid;
      stdOut += " => uids(";
      for ( unsigned int i=0; i< (it->second).size(); i++) {
	stdOut += (int) (it->second)[i];
	if (i < ((it->second).size()-1))
	  stdOut += ",";
      }
      stdOut += ")\n";
    }
  }

  if ((!option.length()) || ( (option.find("g"))!=STR_NPOS)) {
    UserRoleMap::const_iterator it;
    for ( it = gGroupRoleVector.begin(); it != gGroupRoleVector.end(); ++it) {
      char iuid[4096];
      sprintf(iuid,"%d", it->first);
      char suid[4096];
      sprintf(suid,"%-6s",iuid);
      stdOut += "membership uid: ";stdOut += suid;
      stdOut += " => gids(";
      for ( unsigned int i=0; i< (it->second).size(); i++) {
	stdOut += (int) (it->second)[i];
	if (i < ((it->second).size()-1))
	  stdOut += ",";
      }
      stdOut += ")\n";
    }
  }

  if ((!option.length()) || ( (option.find("s"))!=STR_NPOS)) {
    SudoerMap::const_iterator it;
    // print sudoer line
    stdOut += "sudoer                 => uids(";
    for ( it = gSudoerMap.begin() ;it != gSudoerMap.end(); ++it) {  
      if (it->second) {
	stdOut += (int) (it->first);
	stdOut += ",";
      }
    }
    if (stdOut.endswith(",")) {
      stdOut.erase(stdOut.length()-1);
    }
    stdOut += ")\n";
  }

  if ((!option.length()) || ( (option.find("U"))!=STR_NPOS)) {
    VirtualUserMap::const_iterator it;
    for ( it = gVirtualUidMap.begin(); it != gVirtualUidMap.end(); ++it) {
      stdOut += it->first.c_str(); stdOut += " => "; stdOut += (int)it->second; stdOut += "\n";
    }
  }

  if ((!option.length()) || ( (option.find("G"))!=STR_NPOS)) {
    VirtualGroupMap::const_iterator it;
    for ( it = gVirtualGidMap.begin(); it != gVirtualGidMap.end(); ++it) {
      stdOut += it->first.c_str(); stdOut += " => "; stdOut += (int)it->second; stdOut += "\n";
    }
  }
}

/*----------------------------------------------------------------------------*/
void
XrdCommonMapping::getPhysicalIds(const char* name, VirtualIdentity &vid)
{
  struct group* gr;
  struct passwd passwdinfo;
  char buffer[16384];

  if (!name)
    return;

  memset(&passwdinfo,0, sizeof(passwdinfo))
;
  gid_vector* gv;
  id_pair* id;

  eos_static_debug("find in uid cache");
  // cache short cut's
  if (!(id = gPhysicalUidCache.Find(name))) {
    eos_static_debug("not found in uid cache");
    struct passwd *pwbufp=0;
    
    if (getpwnam_r(name, &passwdinfo, buffer, 16384, &pwbufp)) 
      return;
    id = new id_pair(passwdinfo.pw_uid, passwdinfo.pw_gid);
    gPhysicalUidCache.Add(name, id, 60);
    eos_static_debug("adding to cache uid=%u gid=%u", id->uid,id->gid);
  };

  vid.uid = id->uid;
  vid.gid = id->gid;

  if ((gv = gPhysicalGidCache.Find(name))) {
    vid.uid_list.push_back(id->uid);
    vid.gid_list = *gv;
    vid.uid = id->uid;
    vid.gid = id->gid;
    eos_static_debug("returning uid=%u gid=%u", id->uid,id->gid);
    return; 
  }

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

  // add to the cache
  gid_vector* vec = new uid_vector;
  *vec = vid.gid_list;
  gPhysicalGidCache.Add(name,vec, 60);

  return ;
}
