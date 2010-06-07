#include "XrdCommon/XrdCommonMapping.hh"
#include "XrdCommon/XrdCommonStringStore.hh"

/*----------------------------------------------------------------------------*/
XrdSysMutex XrdCommonMapping::gMapMutex;
XrdSysMutex XrdCommonMapping::gSecEntityMutex;
XrdSysMutex XrdCommonMapping::gVirtualMapMutex;

XrdOucHash<XrdOucString> XrdCommonMapping::gUserRoleTable;
XrdOucHash<XrdOucString> XrdCommonMapping::gGroupRoleTable;
XrdOucHash<XrdSecEntity> XrdCommonMapping::gSecEntityStore;

XrdOucHash<long> XrdCommonMapping::gVirtualUidMap;
XrdOucHash<long> XrdCommonMapping::gVirtualGidMap;
XrdOucHash<XrdOucString> XrdCommonMapping::gVirtualGroupMemberShip;

XrdOucHash<struct passwd> XrdCommonMapping::gPasswdStore;
XrdOucHash<XrdCommonMappingGroupInfo> XrdCommonMapping::gGroupInfoCache;

/*----------------------------------------------------------------------------*/
void 
XrdCommonMapping::GetPhysicalGroups(const char* __name__, XrdOucString& __allgroups__, XrdOucString& __defaultgroup__) {        
  __allgroups__=":";                                                    
  __defaultgroup__="";                                                  
  XrdCommonMappingGroupInfo* ginfo=0;                                   
  
  if ((ginfo = gGroupInfoCache.Find(__name__))) {          
    __allgroups__ = ginfo->AllGroups;                                   
    __defaultgroup__ = ginfo->DefaultGroup;                             
    return;}                                                                     
  struct group* gr;                                                     
  struct passwd* passwdinfo = 0;                                     
  if (!(passwdinfo = gPasswdStore.Find(__name__))) {     
    passwdinfo = getpwnam(__name__);                                  
    if (passwdinfo) {                                                 
      struct passwd* pwdcpy = (struct passwd*) malloc(sizeof(struct passwd));
      memcpy(pwdcpy,passwdinfo,sizeof(struct passwd));                
      passwdinfo = pwdcpy;                                            
      gPasswdStore.Add(__name__,pwdcpy,60);            
    }                                                                 
  }        
  if (!passwdinfo)
    return;
  
  setgrent();                                                           
  while( (gr = getgrent() ) ) {                                         
    int cnt;                                                           
    cnt=0;                                                             
    if (gr->gr_gid == passwdinfo->pw_gid) {                            
	if (!__defaultgroup__.length()) __defaultgroup__+= gr->gr_name;  
	__allgroups__+= gr->gr_name; __allgroups__+=":";                 
    }                                                                  
    while (gr->gr_mem[cnt]) {                                          
      if (!strcmp(gr->gr_mem[cnt],__name__)) {                         
	__allgroups__+= gr->gr_name; __allgroups__+=":";              
      }                                                                
      cnt++;                                                           
    }                                                                  
  }     
  endgrent();                                                           
  ginfo = new XrdCommonMappingGroupInfo(__defaultgroup__.c_str(), __allgroups__.c_str(),passwdinfo);
  gGroupInfoCache.Add(__name__,ginfo, ginfo->Lifetime);  
}

/*----------------------------------------------------------------------------*/

void 
XrdCommonMapping::RoleMap(const XrdSecEntity* _client,const char* _env, XrdSecEntity &_mappedclient, const char* tident, uid_t &uid, gid_t &gid, uid_t &ruid, gid_t &rgid) {
  //  EPNAME("rolemap");
  XrdSecEntity* entity = 0;
  XrdOucEnv lenv(_env); 
  const char* role      = lenv.Get("role");		
  const char* suid = lenv.Get("suid");		
  
  gSecEntityMutex.Lock();
  char clientid[1024];
  sprintf(clientid,"%s:%llu",tident, (unsigned long long) _client);

  if ( (!suid) && (!role) && (entity = gSecEntityStore.Find(clientid))) {
    // find existing client rolemaps ....
    _mappedclient.name = entity->name;
    _mappedclient.role = entity->role;
    _mappedclient.host = entity->host;
    _mappedclient.vorg = entity->vorg;
    _mappedclient.grps = entity->grps;
    _mappedclient.endorsements = entity->endorsements;
    _mappedclient.tident = entity->endorsements;
     uid_t _client_uid;
     gid_t _client_gid;
     GetId(_mappedclient,_client_uid,_client_gid);                    
     uid = _client_uid;
     gid = _client_gid;
     ruid= uid;
     rgid= gid;

     gSecEntityMutex.UnLock();
     return;
  }
  gSecEntityMutex.UnLock();
     
  // wildcard mapping
  XrdOucString* wildcarduser;
  if ((wildcarduser = gUserRoleTable.Find("*"))){                    
    _mappedclient.name = XrdCommonStringStore::Store(wildcarduser->c_str());
  } else {
    _mappedclient.name = XrdCommonStringStore::Store(_client->name);
  }

  // suid switch
  XrdOucString* hisusers = gUserRoleTable.Find(_mappedclient.name); 
  XrdOucString match = ":";match+= suid; match+=":";		
  suid=0;
  if (hisusers) {							
    // we have a user role table entry
    if ((hisusers->find(match.c_str()))!=STR_NPOS) {		
      suid = lenv.Get("suid");		
      if (_client->name)_mappedclient.name = XrdCommonStringStore::Store(_client->name);
      // overwrite in case with suid
      if (suid) {
	_mappedclient.name = XrdCommonStringStore::Store(suid);
      }
      if (_client->host)_mappedclient.host = XrdCommonStringStore::Store(_client->host);	
      if (_client->vorg)_mappedclient.vorg = XrdCommonStringStore::Store(_client->vorg);	
      if (_client->role)_mappedclient.role = XrdCommonStringStore::Store(_client->role);	
      if (_client->grps)_mappedclient.grps = XrdCommonStringStore::Store(_client->grps);	
      if (_client->endorsements)_mappedclient.endorsements = XrdCommonStringStore::Store(_client->endorsements); 
      if (_client->tident)_mappedclient.tident = XrdCommonStringStore::Store(_client->tident); 

      // (static) user mapping with wildcard
      // this forces ALL people to the given identity! 

      // if no role has been selected, we set it to the role assigned in the SecEntity
      if (! role) 
	role = _mappedclient.role;
      
    }
  }
  // (re-)create client rolemaps
  do {
    XrdOucString allgroups="";
    XrdOucString defaultgroup="";
    gMapMutex.Lock();
    
    _mappedclient.name=(char*)"__noauth__";					
    _mappedclient.host=(char*)"";						
    _mappedclient.vorg=(char*)"";						
    _mappedclient.role=(char*)"";						
    _mappedclient.grps=(char*)"__noauth__";					
    _mappedclient.endorsements=(char*)"";					
    _mappedclient.tident=(char*)"";						

    if (_client) {							
      if (_client->prot)						
	strcpy(_mappedclient.prot,_client->prot);			

      // first take all the info delivered by the security plugins
      if (_client->name)_mappedclient.name = XrdCommonStringStore::Store(_client->name);
      // overwrite in case with suid
      if (suid) _mappedclient.name = XrdCommonStringStore::Store(suid);
      if (_client->host)_mappedclient.host = XrdCommonStringStore::Store(_client->host);	
      if (_client->vorg)_mappedclient.vorg = XrdCommonStringStore::Store(_client->vorg);	
      if (_client->role)_mappedclient.role = XrdCommonStringStore::Store(_client->role);	
      if (_client->grps)_mappedclient.grps = XrdCommonStringStore::Store(_client->grps);	
      if (_client->endorsements)_mappedclient.endorsements = XrdCommonStringStore::Store(_client->endorsements); 
      if (_client->tident)_mappedclient.tident = XrdCommonStringStore::Store(_client->tident); 

      // (static) user mapping with wildcard
      // this forces ALL people to the given identity! 

      XrdOucString* hisroles;                                                  
      XrdOucString FixedName="";
      
      if ((hisroles = gUserRoleTable.Find("*"))){                    
	FixedName = hisroles->c_str();
      }

      // if no role has been selected, we set it to the role assigned in the SecEntity
      if (! role) 
	role = _mappedclient.role;

      // look if the selected role is in the dynamic, static or sec entity role table
      hisroles = gGroupRoleTable.Find(_mappedclient.name); 

      XrdOucString match = ":";match+= role; match+=":";		
      if (hisroles) {							
	allgroups = hisroles->c_str();
	if (!defaultgroup.length()) {
	  defaultgroup = _mappedclient.grps;
	  int npos = defaultgroup.find(":",2);
	  if (npos != STR_NPOS) {
	    defaultgroup.erase(npos);
	  while( (defaultgroup.erase(":"))!=STR_NPOS) {defaultgroup.replace(":","");}         
	  }
	}
	// we have a group role table
	if ((hisroles->find(match.c_str()))!=STR_NPOS) {		
	  _mappedclient.role = XrdCommonStringStore::Store(role);			
	  _mappedclient.grps = XrdCommonStringStore::Store(allgroups.c_str());
	} else {							
	  if (hisroles->beginswith("static:")) {			
	    _mappedclient.role = XrdCommonStringStore::Store(hisroles->c_str()+7);	
	    _mappedclient.grps = _mappedclient.role;
	  } else { 					                
	    if ( (allgroups.find(match.c_str())) != STR_NPOS )          
	      _mappedclient.role = XrdCommonStringStore::Store(role);                   
	    else                                                        
	      _mappedclient.role = XrdCommonStringStore::Store(defaultgroup.c_str());			
	    _mappedclient.grps = XrdCommonStringStore::Store(allgroups.c_str());
	  }
	}
      }
      
      
      // if no role has been selected, we set it to the role assigned in the SecEntity
      if (! role) 
	role = _mappedclient.role;

      // if no role has been selected yet we assign the default group as role
      if ( ((! _mappedclient.role) || (!strlen(_mappedclient.role))) && (!role)) { role=XrdCommonStringStore::Store(defaultgroup.c_str()); }

      // look if the selected role is in the dynamic, static or sec entity role table
      hisroles = gGroupRoleTable.Find(_mappedclient.name); 
      match = ":";match+= role; match+=":";		
      if (hisroles) {							
	// we have a group role table
	if ((hisroles->find(match.c_str()))!=STR_NPOS) {		
	  _mappedclient.role = XrdCommonStringStore::Store(role);			
	} else {							
	  if (hisroles->beginswith("static:")) {			
	    _mappedclient.role = XrdCommonStringStore::Store(hisroles->c_str()+7);	
	  } else { 					                
	    if ( (allgroups.find(match.c_str())) != STR_NPOS )          
	      _mappedclient.role = XrdCommonStringStore::Store(role);                   
	    else                                                        
	      _mappedclient.role = XrdCommonStringStore::Store(defaultgroup.c_str());			
	}                                                                    
	while( (FixedName.find(":"))!=STR_NPOS) {FixedName.replace(":","");}         
	_mappedclient.name = XrdCommonStringStore::Store(FixedName.c_str());
	if (suid) _mappedclient.name = XrdCommonStringStore::Store(suid);                 
	GetPhysicalGroups(_mappedclient.name,allgroups,defaultgroup);             
	_mappedclient.grps=XrdCommonStringStore::Store(defaultgroup.c_str());                
	_mappedclient.role=XrdCommonStringStore::Store(defaultgroup.c_str());
	// leaving mapping loop here
	break;
      }
    }

    // there is no fixed mapping but 'some' authenticated name
    if (_client && _mappedclient.name) {				
      XrdOucString defaultgroup="";                                     
      XrdOucString allgroups="";
      // if there is no group information we try to obtain if from the password file
      if ((_client->grps==0)|| (!strlen(_client->grps))) {
	// get physical groups
	GetPhysicalGroups(_mappedclient.name,allgroups,defaultgroup); 
	_mappedclient.grps=XrdCommonStringStore::Store(allgroups.c_str());
	
	allgroups = _mappedclient.grps;
      }

      // if no role has been selected, we set it to the role assigned in the SecEntity
      if (! role) 
	role = _mappedclient.role;

      // if no role has been selected yet we assign the default group as role
      if ( ((! _mappedclient.role) || (!strlen(_mappedclient.role))) && (!role)) { role=XrdCommonStringStore::Store(defaultgroup.c_str()); }

      // look if the selected role is in the dynamic, static or sec entity role table
      XrdOucString* hisroles = gGroupRoleTable.Find(_mappedclient.name); 
      XrdOucString match = ":";match+= role; match+=":";		
      if (hisroles) {							
	// we have a group role table
	if ((hisroles->find(match.c_str()))!=STR_NPOS) {		
	  _mappedclient.role = XrdCommonStringStore::Store(role);			
	} else {							
	  if (hisroles->beginswith("static:")) {			
	    _mappedclient.role = XrdCommonStringStore::Store(hisroles->c_str()+7);	
	  } else { 					                
	    if ( (allgroups.find(match.c_str())) != STR_NPOS )          
	      _mappedclient.role = XrdCommonStringStore::Store(role);                   
	    else                                                        
	      _mappedclient.role = XrdCommonStringStore::Store(defaultgroup.c_str());			
	  }
	  // get virtual groups
	  gVirtualMapMutex.Lock();
	  XrdOucString* virtualgroups;
	  if ( (virtualgroups = gVirtualGroupMemberShip.Find(_mappedclient.name))) {
	    // store the virtual groups
	    _mappedclient.grps=XrdCommonStringStore::Store(virtualgroups->c_str());
	  }
	  gVirtualMapMutex.UnLock();
	} 
      }
      // if we use ssl authentication defaultgroup and allgroups are still empty
      if (!defaultgroup.length()) {
	defaultgroup = _mappedclient.grps;
	int npos = defaultgroup.find(":",2);
	if (npos != STR_NPOS) {
	  defaultgroup.erase(npos);
	  while( (defaultgroup.erase(":"))!=STR_NPOS) {defaultgroup.replace(":","");}         
	}
      }
      if (!allgroups.length()) {
	allgroups = _mappedclient.grps;
      }

      // if no role has been selected, we set it to the role assigned in the SecEntity
      if (! role) 
	role = _mappedclient.role;

      // if no role has been selected yet we assign the default group as role
      if ( ((! _mappedclient.role) || (!strlen(_mappedclient.role))) && (!role)) { role=XrdCommonStringStore::Store(defaultgroup.c_str()); }

      // look if the selected role is in the dynamic, static or sec entity role table
      hisroles = gGroupRoleTable.Find(_mappedclient.name); 
      match = ":";match+= role; match+=":";		
      if (hisroles) {							
	// we have a group role table
	if ((hisroles->find(match.c_str()))!=STR_NPOS) {		
	  _mappedclient.role = XrdCommonStringStore::Store(role);			
	} else {							
	  if (hisroles->beginswith("static:")) {			
	    _mappedclient.role = XrdCommonStringStore::Store(hisroles->c_str()+7);	
	  } else { 					                
	    if ( (allgroups.find(match.c_str())) != STR_NPOS )          
	      _mappedclient.role = XrdCommonStringStore::Store(role);                   
	    else                                                        
	      _mappedclient.role = XrdCommonStringStore::Store(defaultgroup.c_str());
	  }
	} 
      } else {
	// we have no group role table and take it from the sec entity definition
	if ((allgroups.find(match.c_str())) != STR_NPOS) {             
	  _mappedclient.role = XrdCommonStringStore::Store(role);                      
	} else {                                                       
	  _mappedclient.role = XrdCommonStringStore::Store(defaultgroup.c_str());	
	}                                                              
      }
      if ((!strlen(_mappedclient.role)) && (_client->grps))               
	_mappedclient.role = XrdCommonStringStore::Store(_client->grps);                  
    }
    {									
      // tident mapping
      XrdOucString reducedTident,user, stident;				
      reducedTident=""; user = ""; stident = tident;			
      int dotpos = stident.find(".");					
      reducedTident.assign(tident,0,dotpos-1);reducedTident += "@";	
      int adpos  = stident.find("@"); user.assign(tident,adpos+1); reducedTident += user; 
      XrdOucString* hisusers = gUserRoleTable.Find(reducedTident.c_str()); 
      XrdOucString match = ":";match+= suid; match+=":";		
      if (hisusers) {							
	if ((hisusers->find(match.c_str())) != STR_NPOS) {		
	  XrdOucString allgroups="";
	  XrdOucString defaultgroup="";
	  GetPhysicalGroups(_mappedclient.name,allgroups,defaultgroup); 
	  gVirtualMapMutex.Lock();
	  XrdOucString* virtualgroups;
	  if ( (virtualgroups = gVirtualGroupMemberShip.Find(_mappedclient.name))) {
	    // store the virtual groups
	    allgroups = XrdCommonStringStore::Store(virtualgroups->c_str());
	    defaultgroup = allgroups;
	    int npos = defaultgroup.find(":",2);
	    if (npos!=STR_NPOS)
	      defaultgroup.erase(npos);
	    while( (defaultgroup.erase(":"))!=STR_NPOS) {defaultgroup.replace(":","");}         
	  }
	  gVirtualMapMutex.UnLock();

	  _mappedclient.grps = XrdCommonStringStore::Store(allgroups.c_str());
	  _mappedclient.role = XrdCommonStringStore::Store(defaultgroup.c_str());
	  _mappedclient.role = XrdCommonStringStore::Store(suid);

	} else {							
	  if (hisusers->beginswith("static:")) {			
	    XrdOucString allgroups="";
	    XrdOucString defaultgroup="";
	    GetPhysicalGroups(_mappedclient.name,allgroups,defaultgroup); 
	    gVirtualMapMutex.Lock();
	    XrdOucString* virtualgroups;
	    if ( (virtualgroups = gVirtualGroupMemberShip.Find(_mappedclient.name))) {
	      // store the virtual groups
	      allgroups = XrdCommonStringStore::Store(virtualgroups->c_str());
	      defaultgroup = allgroups;
	      int npos = defaultgroup.find(":",2);
	      if (npos!=STR_NPOS)
		defaultgroup.erase(npos);
	      while( (defaultgroup.erase(":"))!=STR_NPOS) {defaultgroup.replace(":","");}         
	    }
	    gVirtualMapMutex.UnLock();
	    
	    _mappedclient.grps = XrdCommonStringStore::Store(allgroups.c_str());
	    _mappedclient.role = XrdCommonStringStore::Store(defaultgroup.c_str());
	    _mappedclient.name = XrdCommonStringStore::Store(hisroles->c_str()+7);	
	  }								
	}
      }
    }
    // root mapping to root/root/root
    if (_mappedclient.role && (!strcmp(_mappedclient.role,"root"))) {
      _mappedclient.name=XrdCommonStringStore::Store("root");                           
      _mappedclient.role=XrdCommonStringStore::Store("root");                           
      _mappedclient.grps=XrdCommonStringStore::Store("root");                           
    }
    }
    break;
  } while (0);

  gMapMutex.UnLock();

  gSecEntityMutex.Lock();
  XrdSecEntity* newentity = new XrdSecEntity();
  if (newentity) {
    newentity->name = _mappedclient.name;
    newentity->role = _mappedclient.role;
    newentity->host = _mappedclient.host;
    newentity->vorg = _mappedclient.vorg;
    newentity->grps = _mappedclient.grps;
    newentity->endorsements = _mappedclient.endorsements;
    newentity->tident = _mappedclient.tident;
    gSecEntityStore.Add(clientid, newentity, 60);
  }
  gSecEntityMutex.UnLock();

  uid_t _client_uid;
  gid_t _client_gid;

  GetId(_mappedclient,_client_uid,_client_gid);                     
  uid = _client_uid;
  gid = _client_gid;
  ruid = uid;
  rgid = gid;

  if (uid == 99) {
    // try virtual mapping for uids
    long* vuid=0;
    gVirtualMapMutex.Lock();
    if ((vuid = gVirtualUidMap.Find(newentity->name))) {
      uid = *vuid;
    }
    gVirtualMapMutex.UnLock();
  }

  if (gid == 99) {
    // try virtual mapping for gids
    long* vgid=0;
    gVirtualMapMutex.Lock();
    if ((vgid = gVirtualGidMap.Find(newentity->role))) {
      gid = *vgid;
    }
    gVirtualMapMutex.UnLock();
  }
  return;
}


/*----------------------------------------------------------------------------*/
void XrdCommonMapping::GetId(XrdSecEntity &_client, uid_t &_uid, gid_t &_gid) {                                    
  gMapMutex.Lock();                                      
  _uid=99;                                                             
  _gid=99;                                                             
  struct passwd* pw=0;                                      
  XrdCommonMappingGroupInfo* ginfo=0;
  if (_client.name) {
    if ((ginfo = gGroupInfoCache.Find(_client.name))) { 
      pw = &(ginfo->Passwd);                          
      if (pw) _uid=pw->pw_uid;
    } else {                                                           
      if (!(pw = gPasswdStore.Find(_client.name))) {    
	pw = getpwnam(_client.name);                                 
	if (pw) {
	  struct passwd* pwdcpy = (struct passwd*) malloc(sizeof(struct passwd));
	  memcpy(pwdcpy,pw,sizeof(struct passwd));                       
	  pw = pwdcpy; 
	  gPasswdStore.Add(_client.name,pwdcpy,60);       
	}
      }                                                                
      if (pw) _uid=pw->pw_uid;                                         
    }
  }  
  
  if (_client.role) {                                                  
    struct group* gr = getgrnam(_client.role);                       
    if (gr) _gid= gr->gr_gid;                                        
  } else {                                                           
    if (pw) _gid=pw->pw_gid;                                         
  }                                                                  
  if (_uid==0) {_gid=0;}                                             
  /*  if (GTRACE(authorize)) {						
    XrdOucString tracestring = "getgid ";				
    tracestring += (int)_uid;						
    tracestring += "/";						
    tracestring += (int)_gid;						
    XTRACE(authorize, _x, tracestring.c_str());			
    }*/									
  gMapMutex.UnLock();                                   
}
