/*----------------------------------------------------------------------------*/
#include "XrdMgmOfs/XrdMgmQuota.hh"
#include "XrdMgmOfs/XrdMgmFstNode.hh"
#include "XrdMgmOfs/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
XrdOucHash<XrdMgmSpaceQuota> XrdMgmQuota::gQuota;
XrdSysMutex XrdMgmQuota::gQuotaMutex;


/*----------------------------------------------------------------------------*/
void
XrdMgmSpaceQuota::RmQuota(unsigned long tag, unsigned long id, bool lock) 
{
  if (lock) Mutex.Lock();
  Quota.erase(Index(tag,id));
  eos_static_debug("rm quota tag=%lu id=%lu", tag, id);
  if (lock) Mutex.UnLock();
  return;
}

/*----------------------------------------------------------------------------*/
long long 
XrdMgmSpaceQuota::GetQuota(unsigned long tag, unsigned long id, bool lock) 
{
  long long ret;
  if (lock) Mutex.Lock();
  ret = Quota[Index(tag,id)];
  eos_static_debug("get quota tag=%lu id=%lu value=%lld", tag, id, ret);
  if (lock) Mutex.UnLock();
  return (unsigned long long)ret;
}

/*----------------------------------------------------------------------------*/
void
XrdMgmSpaceQuota::SetQuota(unsigned long tag, unsigned long id, unsigned long long value, bool lock) 
{
  if (lock) Mutex.Lock();
  eos_static_debug("set quota tag=%lu id=%lu value=%llu", tag, id, value);
  Quota[Index(tag,id)] = value;
  if (lock) Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void
XrdMgmSpaceQuota::AddQuota(unsigned long tag, unsigned long id, unsigned long long value, bool lock) 
{
  if (lock) Mutex.Lock();
  eos_static_debug("add quota tag=%lu id=%lu value=%llu", tag, id , value);
  Quota[Index(tag,id)] += value;
  eos_static_debug("sum quota tag=%lu id=%lu value=%llu", tag, id, Quota[Index(tag,id)]);
  if (lock) Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void
XrdMgmSpaceQuota::UpdateTargetSums() 
{
  Mutex.Lock();
  eos_static_debug("updating targets");

  ResetQuota(kAllUserBytesTarget,0, false);
  ResetQuota(kAllUserFilesTarget,0, false);
  ResetQuota(kAllGroupBytesTarget,0, false);
  ResetQuota(kAllGroupFilesTarget,0, false);

  google::dense_hash_map<long long, unsigned long long>::const_iterator it;

  for (it=Begin(); it != End(); it++) {
    if ( (UnIndex(it->first) == kUserBytesTarget)) 
      AddQuota(kAllUserBytesTarget,0, it->second, false);
    if ( (UnIndex(it->first) == kUserFilesTarget)) 
      AddQuota(kAllUserFilesTarget,0, it->second, false);
    if ( (UnIndex(it->first) == kGroupBytesTarget)) 
      AddQuota(kAllGroupBytesTarget,0, it->second, false);
    if ( (UnIndex(it->first) == kGroupFilesTarget)) 
      AddQuota(kAllGroupFilesTarget,0, it->second, false);
  }

  Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void
XrdMgmSpaceQuota::PrintOut(XrdOucString &output, long uid_sel, long gid_sel)
{
  char headerline[4096];
  eos_static_debug("called");

  google::dense_hash_map<long long, unsigned long long>::const_iterator it;

  
  int* sortuidarray = (int*) malloc ( sizeof(int) * (Quota.size()+1));
  int* sortgidarray = (int*) malloc ( sizeof(int) * (Quota.size()+1));

  int userentries=0;
  int groupentries=0;

  Quota.rehash(0);

  // make a map containing once all the defined uid's+gid's
  google::dense_hash_map<unsigned long, unsigned long > sortuidhash;
  google::dense_hash_map<unsigned long, unsigned long > sortgidhash;

  google::dense_hash_map<unsigned long, unsigned long >::const_iterator sortit;

  sortuidhash.set_empty_key(-1);
  sortgidhash.set_empty_key(-1);

  output+="# ====================================================================================\n";
  sprintf(headerline,"# ==> Space: %-16s\n", SpaceName.c_str());
  output+= headerline;

  for (it=Begin(); it != End(); it++) {
    if ( (UnIndex(it->first) >= kUserBytesIs) && (UnIndex(it->first) <= kUserFilesTarget)) {
      eos_static_debug("adding %llx to print list ", UnIndex(it->first));
      unsigned long ugid = (it->first)&0xffff;
      // uid selection filter
      if (uid_sel>=0) 
	if (ugid != (unsigned long)uid_sel) 
	  continue;

      // we don't print the users if a gid is selected
      if (gid_sel>=0)
	continue;
      
      sortuidhash[ugid] = ugid;
    }

    if ( (UnIndex(it->first) >= kGroupBytesIs) && (UnIndex(it->first) <= kGroupFilesTarget)) {
      unsigned long ugid = (it->first)&0xffff;
      // uid selection filter
      if (gid_sel>=0) 
	if (ugid != (unsigned long)gid_sel) 
	  continue;
      // we don't print the group if a uid is selected
      if (uid_sel>=0)
	continue;
      sortgidhash[ugid] = ugid;
    }
  }
  

  for (sortit=sortuidhash.begin(); sortit != sortuidhash.end(); sortit++) {
    sortuidarray[userentries] = (sortit->first);
    eos_static_debug("loop %d %d", userentries, sortuidarray[userentries]);
    userentries++;
  }

  for (sortit=sortgidhash.begin(); sortit != sortgidhash.end(); sortit++) {
    // sort only based on the user bytes entries
    sortgidarray[groupentries] = (sortit->first);
    eos_static_debug("loop %d %d", groupentries, sortgidarray[groupentries]);
    groupentries++;
  }

  sort(sortuidarray,sortuidarray+userentries);
  sort(sortgidarray,sortgidarray+groupentries);
  
  eos_static_debug("sorted");
  for (int k=0; k< userentries; k++) {
    eos_static_debug("sort %d %d", k, sortuidarray[k]);
  }
  
  for (int k=0; k< groupentries; k++) {
    eos_static_debug("sort %d %d", k, sortgidarray[k]);
  }
  

  if (userentries) {
    // user loop
    sprintf(headerline,"%-5s %-16s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kUserBytesIs), "SPACE", GetTagName(kUserBytesIs), GetTagName(kUserBytesIs+2), GetTagName(kUserBytesIs+1), GetTagName(kUserBytesIs+3), "FILLED[%]", "STATUS");
    output+= headerline;
  }
    
  for (int lid = 0; lid < userentries; lid++) {
    eos_static_debug("loop with id=%d",lid);
    XrdOucString value1="";
    XrdOucString value2="";
    XrdOucString value3="";
    XrdOucString value4="";
    
    XrdOucString id=""; id += sortuidarray[lid];
    XrdOucString percentage="";

    sprintf(headerline,"%-5s %-16s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str() , SpaceName.c_str(), 
	    XrdCommonFileSystem::GetReadableSizeString(value1, GetQuota(kUserBytesIs,sortuidarray[lid]),"B"), 
	    XrdCommonFileSystem::GetReadableSizeString(value2, GetQuota(kUserFilesIs,sortuidarray[lid]),""), 
	    XrdCommonFileSystem::GetReadableSizeString(value3, GetQuota(kUserBytesTarget,sortuidarray[lid]),"B"), 
	    XrdCommonFileSystem::GetReadableSizeString(value4, GetQuota(kUserFilesTarget,sortuidarray[lid]),""),
	    GetQuotaPercentage(GetQuota(kUserBytesIs,sortuidarray[lid]), GetQuota(kUserBytesTarget,sortuidarray[lid]), percentage),
	    GetQuotaStatus(GetQuota(kUserBytesIs,sortuidarray[lid]), GetQuota(kUserBytesTarget,sortuidarray[lid])));

    output += headerline;
  }
  
  if (groupentries) {
    // group loop
    sprintf(headerline,"%-5s %-16s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kGroupBytesIs), "SPACE", GetTagName(kGroupBytesIs), GetTagName(kGroupBytesIs+2), GetTagName(kGroupBytesIs+1), GetTagName(kGroupBytesIs+3), "FILLED[%]", "STATUS");
    output+= headerline;
  }

  for (int lid = 0; lid < groupentries; lid++) {
    eos_static_debug("loop with id=%d",lid);
    XrdOucString value1="";
    XrdOucString value2="";
    XrdOucString value3="";
    XrdOucString value4="";
    
    XrdOucString id=""; id += sortgidarray[lid];
    XrdOucString percentage="";

    sprintf(headerline,"%-5s %-16s %-10s %-10s %-10s %-10s %-10s %-10s \n", id.c_str() , SpaceName.c_str(),
	    XrdCommonFileSystem::GetReadableSizeString(value1, GetQuota(kGroupBytesIs,sortgidarray[lid]),"B"), 
	    XrdCommonFileSystem::GetReadableSizeString(value2, GetQuota(kGroupFilesIs,sortgidarray[lid]),""), 
	    XrdCommonFileSystem::GetReadableSizeString(value3, GetQuota(kGroupBytesTarget,sortgidarray[lid]),"B"), 
	    XrdCommonFileSystem::GetReadableSizeString(value4, GetQuota(kGroupFilesTarget,sortgidarray[lid]),""),
	    GetQuotaPercentage(GetQuota(kGroupBytesIs,sortgidarray[lid]), GetQuota(kGroupBytesTarget,sortgidarray[lid]), percentage),
	    GetQuotaStatus(GetQuota(kGroupBytesIs,sortgidarray[lid]), GetQuota(kGroupBytesTarget,sortgidarray[lid])));
    output += headerline;
  }

  if ( (uid_sel <0) && (gid_sel <0)) {
    output+="# ------------------------------------------------------------------------------------\n";
    output+="# ==> Summary\n";
    
    XrdOucString value1="";
    XrdOucString value2="";
    XrdOucString value3="";
    XrdOucString value4="";
    
    XrdOucString id="ALL";
    XrdOucString percentage="";
    XrdOucString percentage1="";
    XrdOucString percentage2="";
    
    sprintf(headerline,"%-5s %-16s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kAllUserBytesIs), "SPACE", GetTagName(kAllUserBytesIs), GetTagName(kAllUserBytesIs+2), GetTagName(kAllUserBytesIs+1), GetTagName(kAllUserBytesIs+3), "FILLED[%]", "STATUS");
    output += headerline;
    sprintf(headerline,"%-5s %-16s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str() , SpaceName.c_str(), 
	    XrdCommonFileSystem::GetReadableSizeString(value1, GetQuota(kAllUserBytesIs,0),"B"), 
	    XrdCommonFileSystem::GetReadableSizeString(value2, GetQuota(kAllUserFilesIs,0),""), 
	    XrdCommonFileSystem::GetReadableSizeString(value3, GetQuota(kAllUserBytesTarget,0),"B"), 
	    XrdCommonFileSystem::GetReadableSizeString(value4, GetQuota(kAllUserFilesTarget,0),""),
	    GetQuotaPercentage(GetQuota(kAllUserBytesIs,0), GetQuota(kAllUserBytesTarget,0), percentage),
	    GetQuotaStatus(GetQuota(kAllUserBytesIs,0), GetQuota(kAllUserBytesTarget,0)));
    output += headerline;
    sprintf(headerline,"%-5s %-16s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kAllGroupBytesIs), "SPACE", GetTagName(kAllGroupBytesIs), GetTagName(kAllGroupBytesIs+2), GetTagName(kAllGroupBytesIs+1), GetTagName(kAllGroupBytesIs+3), "FILLED[%]", "STATUS");
    output += headerline;
    sprintf(headerline,"%-5s %-16s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str() , SpaceName.c_str(), 
	    XrdCommonFileSystem::GetReadableSizeString(value1, GetQuota(kAllGroupBytesIs,0),"B"), 
	    XrdCommonFileSystem::GetReadableSizeString(value2, GetQuota(kAllGroupFilesIs,0),""), 
	    XrdCommonFileSystem::GetReadableSizeString(value3, GetQuota(kAllGroupBytesTarget,0),"B"), 
	    XrdCommonFileSystem::GetReadableSizeString(value4, GetQuota(kAllGroupFilesTarget,0),""),
	    GetQuotaPercentage(GetQuota(kAllGroupBytesIs,0), GetQuota(kAllGroupBytesTarget,0), percentage),
	    GetQuotaStatus(GetQuota(kAllGroupBytesIs,0), GetQuota(kAllGroupBytesTarget,0)));
    output += headerline;
    
    output+="# ------------------------------------------------------------------------------------\n";
    output+="# ==> Physical\n";
    sprintf(headerline,"%-5s %-16s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s \n", GetTagCategory(kGroupBytesIs), "SPACE", GetTagName(kGroupBytesIs), GetTagName(kGroupBytesIs+2), GetTagName(kGroupBytesIs+1), GetTagName(kGroupBytesIs+3), "VOLUME[%]", "STATUS-VOL", "INODES[%]","STATUS-INO"); 
    output+= headerline;
    sprintf(headerline,"PHYS  %-16s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", SpaceName.c_str(),
	    XrdCommonFileSystem::GetReadableSizeString(value1, PhysicalMaxBytes-PhysicalFreeBytes,"B"), 
	    XrdCommonFileSystem::GetReadableSizeString(value2, PhysicalMaxFiles-PhysicalFreeFiles,""), 
	    XrdCommonFileSystem::GetReadableSizeString(value3, PhysicalMaxBytes,"B"), 
	    XrdCommonFileSystem::GetReadableSizeString(value4, PhysicalMaxFiles,""),
	    GetQuotaPercentage(PhysicalMaxBytes-PhysicalFreeBytes,PhysicalMaxBytes, percentage1),
	    GetQuotaStatus(PhysicalMaxBytes-PhysicalFreeBytes,PhysicalMaxBytes),
	  GetQuotaPercentage(PhysicalMaxFiles-PhysicalFreeFiles,PhysicalMaxFiles, percentage2),
	    GetQuotaStatus(PhysicalMaxFiles-PhysicalFreeFiles,PhysicalMaxFiles));
    output+= headerline;
    output+="# ------------------------------------------------------------------------------------------------------------\n";
  }
  free(sortuidarray);
  free(sortgidarray);
}

/*----------------------------------------------------------------------------*/
XrdMgmSpaceQuota* 
XrdMgmQuota::GetSpaceQuota(const char* name, bool nocreate) 
{
  gQuotaMutex.Lock();
  XrdMgmSpaceQuota* spacequota=0;
  
  if ( (spacequota = gQuota.Find(name)) ) {
    
  } else {
    if (nocreate) {
      gQuotaMutex.UnLock();
      return NULL;
    }

    spacequota = new XrdMgmSpaceQuota(name);
    gQuota.Add(name,spacequota);
  }
  
  gQuotaMutex.UnLock();
  return spacequota;
}

/*----------------------------------------------------------------------------*/
int 
XrdMgmQuota::GetSpaceNameList(const char* key, XrdMgmSpaceQuota* spacequota, void *Arg)
{
  XrdOucString* spacestring = (XrdOucString*) Arg;
  (*spacestring) += spacequota->GetSpaceName();
  (*spacestring) += ",";
  return 0;
}

/*----------------------------------------------------------------------------*/
void
XrdMgmQuota::PrintOut(const char* space, XrdOucString &output, long uid_sel, long gid_sel)
{
  output="";
  XrdOucString spacenames="";
  gQuota.Apply(GetSpaceNameList, &spacenames);
  int kommapos=0;
  int spos=0;
  eos_static_debug("space=%s spacenames=%s",space, spacenames.c_str());
  if (space ==0) {
    while ((kommapos = spacenames.find(",", spos)) != STR_NPOS) {
      XrdOucString spacename;
      spacename.assign(spacenames,spos,kommapos-1);
      XrdMgmSpaceQuota* spacequota = GetSpaceQuota(spacename.c_str(),true);
      if (spacequota) {
	spacequota->PrintOut(output, uid_sel, gid_sel);
      }
      spos = kommapos+1;
    }
  } else {
    XrdMgmSpaceQuota* spacequota = GetSpaceQuota(space, true);
    if (spacequota) {
      spacequota->PrintOut(output, uid_sel, gid_sel);
    }
  }
}

/*----------------------------------------------------------------------------*/
void 
XrdMgmQuota::UpdateHint(unsigned int fsid) 
{
  // get the space this fsid points to ....
  XrdMgmFstFileSystem* filesystem=0;
  if ((filesystem = (XrdMgmFstFileSystem*) XrdMgmFstNode::gFileSystemById[fsid])) {
    const char* spacename = filesystem->GetSpaceName();
    eos_static_debug("filesystem for %lu belongs to space %s", fsid, spacename);

    XrdMgmSpaceQuota* spacequota = GetSpaceQuota(spacename);

    if (spacequota->NeedsRecalculation()) {
      eos_static_debug("space %s needs recomputation",spacename);
      // recalculate and lock everything!

      google::dense_hash_map<unsigned int, unsigned long long>::const_iterator it;
      
      bool firstinnerfs;
      firstinnerfs=true;

      spacequota->ResetPhysicalFreeBytes();
      spacequota->ResetPhysicalMaxBytes();
      spacequota->ResetPhysicalFreeFiles();
      spacequota->ResetPhysicalMaxFiles();

      for(it = XrdMgmFstNode::gFileSystemById.begin(); it != XrdMgmFstNode::gFileSystemById.end(); it++) {

	eos_static_debug("looping over all nodes fsid %lu %llu",(it->first), (it->second));
	// loop over all filesystems
	google::dense_hash_map<long, unsigned long long>::const_iterator idit;
	XrdMgmFstFileSystem* innerfilesystem = (XrdMgmFstFileSystem*)(it->second);

	if (!innerfilesystem)
	  continue;

	eos_static_debug("spacename is %s", innerfilesystem->GetSpaceName());

	// add physical bytes/files
	spacequota->AddPhysicalFreeBytes(innerfilesystem->GetStatfs()->f_bfree * 4096ll);
	spacequota->AddPhysicalMaxBytes (innerfilesystem->GetStatfs()->f_blocks * 4096ll)
;	spacequota->AddPhysicalFreeFiles(innerfilesystem->GetStatfs()->f_ffree * 1ll);
	spacequota->AddPhysicalMaxFiles (innerfilesystem->GetStatfs()->f_files * 1ll);

	// we recompute only the same space filessytems
	if ((strcmp(spacename, innerfilesystem->GetSpaceName()))) 
	  continue;

	// user/group occupation
	for (idit = innerfilesystem->UserBytes.begin(); idit != innerfilesystem->UserBytes.end(); idit++) {
	  eos_static_debug("looping over all user bytes uid %lu",(idit->first));
	  // loop over user byte values
	  if (firstinnerfs) {
	    spacequota->ResetQuota(XrdMgmSpaceQuota::kUserBytesIs, idit->first);
	    spacequota->ResetQuota(XrdMgmSpaceQuota::kAllUserBytesIs, 0);
	  }
	  spacequota->AddQuota(XrdMgmSpaceQuota::kUserBytesIs, idit->first, idit->second);
	  spacequota->AddQuota(XrdMgmSpaceQuota::kAllUserBytesIs, 0, idit->second);
	}
	for (idit = innerfilesystem->UserFiles.begin(); idit != innerfilesystem->UserFiles.end(); idit++) {
	  eos_static_debug("looping over all user files uid %lu",(idit->first));
	  // loop over user file values
	  if (firstinnerfs) {
	    spacequota->ResetQuota(XrdMgmSpaceQuota::kUserFilesIs, idit->first);
	    spacequota->ResetQuota(XrdMgmSpaceQuota::kAllUserFilesIs, 0);
	  }
	  spacequota->AddQuota(XrdMgmSpaceQuota::kUserFilesIs, idit->first, idit->second);
	  spacequota->AddQuota(XrdMgmSpaceQuota::kAllUserFilesIs, 0, idit->second);
	}
	for (idit = innerfilesystem->GroupBytes.begin(); idit != innerfilesystem->GroupBytes.end(); idit++) {
	  eos_static_debug("looping over all group bytes gid %lu",(idit->first));
	  // loop over group byte values
	  if (firstinnerfs) {
	    spacequota->ResetQuota(XrdMgmSpaceQuota::kGroupBytesIs, idit->first);
	    spacequota->ResetQuota(XrdMgmSpaceQuota::kAllGroupBytesIs, 0);
	  }
	  spacequota->AddQuota(XrdMgmSpaceQuota::kGroupBytesIs, idit->first, idit->second);
	  spacequota->AddQuota(XrdMgmSpaceQuota::kAllGroupBytesIs, 0, idit->second);
	}
	for (idit = innerfilesystem->GroupFiles.begin(); idit != innerfilesystem->GroupFiles.end(); idit++) {
	  eos_static_debug("looping over all group files gid %lu",(idit->first));
	  // loop over group files values
	  if (firstinnerfs) {
	    spacequota->ResetQuota(XrdMgmSpaceQuota::kGroupFilesIs, idit->first);
	    spacequota->ResetQuota(XrdMgmSpaceQuota::kAllGroupFilesIs, 0);
	  }
	  spacequota->AddQuota(XrdMgmSpaceQuota::kGroupFilesIs, idit->first, idit->second);
	  spacequota->AddQuota(XrdMgmSpaceQuota::kAllGroupFilesIs, 0,  idit->second);
	}
	firstinnerfs = false;
      }
    } else {
      eos_static_debug("space %s needs to recomputation",spacename);
    }

  }
}

/*----------------------------------------------------------------------------*/
bool 
XrdMgmQuota::SetQuota(XrdOucString space, long uid_sel, long gid_sel, long long bytes, long long files, XrdOucString &msg, int &retc) 
{
  eos_static_debug("space=%s",space.c_str());

  XrdMgmSpaceQuota* spacequota = 0;

  XrdOucString configstring="";
  char configvalue[1024];

  if (!space.length()) {
    spacequota = GetSpaceQuota("default", true);
    configstring += "default";
  } else {
    spacequota = GetSpaceQuota(space.c_str(), true);
    configstring += space.c_str();
  }
  
  configstring += ":";
  
  retc =  EINVAL;
	
  if (spacequota) {
    char printline[1024];
    XrdOucString sizestring;
    msg="";
    if ( (uid_sel >=0) && (bytes>=0) ) {
      spacequota->SetQuota(XrdMgmSpaceQuota::kUserBytesTarget, uid_sel, bytes);
      sprintf(printline, "success: updated quota for uid=%ld to %s\n", uid_sel,XrdCommonFileSystem::GetReadableSizeString(sizestring, bytes,"B"));
      configstring += "uid="; configstring += (int) uid_sel; configstring += ":";
      configstring += XrdMgmSpaceQuota::GetTagAsString(XrdMgmSpaceQuota::kUserBytesTarget);
      sprintf(configvalue,"%llu",bytes);
      msg+= printline;
      retc = 0;
    } 

    if ( (uid_sel >=0) && (files>=0) ) {
      spacequota->SetQuota(XrdMgmSpaceQuota::kUserFilesTarget, uid_sel, files);
      sprintf(printline, "success: updated quota for uid=%ld to %s files\n", uid_sel,XrdCommonFileSystem::GetReadableSizeString(sizestring, files,""));
      configstring += "uid="; configstring += (int) uid_sel; configstring += ":";
      configstring += XrdMgmSpaceQuota::GetTagAsString(XrdMgmSpaceQuota::kUserFilesTarget);
      sprintf(configvalue,"%llu",files);
      msg+= printline;
      retc = 0;
    }

    if ( (gid_sel >=0) && (bytes>=0) ) {
      spacequota->SetQuota(XrdMgmSpaceQuota::kGroupBytesTarget, gid_sel, bytes);
      sprintf(printline, "success: updated quota for gid=%ld to %s\n", gid_sel,XrdCommonFileSystem::GetReadableSizeString(sizestring, bytes,"B"));
      configstring += "gid="; configstring += (int) gid_sel; configstring += ":";
      configstring += XrdMgmSpaceQuota::GetTagAsString(XrdMgmSpaceQuota::kGroupBytesTarget);
      sprintf(configvalue,"%llu",bytes);
      msg+= printline;
      retc = 0;
    }

    if ( (gid_sel >=0) && (files>=0) ) {
      spacequota->SetQuota(XrdMgmSpaceQuota::kGroupFilesTarget, gid_sel, files);
      sprintf(printline, "success: updated quota for gid=%ld to %s files\n", gid_sel,XrdCommonFileSystem::GetReadableSizeString(sizestring, files,""));
      configstring += "gid="; configstring += (int) gid_sel; configstring += ":";
      configstring += XrdMgmSpaceQuota::GetTagAsString(XrdMgmSpaceQuota::kGroupFilesTarget);
      sprintf(configvalue,"%llu",files);
      msg+= printline;
      retc = 0;
    }

    spacequota->UpdateTargetSums();
    // store the setting into the config table
    gOFS->ConfigEngine->SetConfigValue("quota", configstring.c_str(), configvalue);
    return true;
  } else {
    msg = "error: no space defined with name ";msg += space;
    return false;
  }
}

/*----------------------------------------------------------------------------*/
bool 
XrdMgmQuota::RmQuota(XrdOucString space, long uid_sel, long gid_sel, XrdOucString &msg, int &retc) 
{
  eos_static_debug("space=%s",space.c_str());

  XrdMgmSpaceQuota* spacequota = 0;

  if (!space.length()) {
    spacequota = GetSpaceQuota("default", true);
  } else {
    spacequota = GetSpaceQuota(space.c_str(), true);
  }

  retc =  EINVAL;
	
  if (spacequota) {
    char printline[1024];
    XrdOucString sizestring;
    msg="";
    if ( (uid_sel >=0) ) {
      spacequota->RmQuota(XrdMgmSpaceQuota::kUserBytesTarget, uid_sel);
      spacequota->RmQuota(XrdMgmSpaceQuota::kUserBytesIs, uid_sel);
      sprintf(printline, "success: removed volume quota for uid=%ld\n", uid_sel);
      msg+= printline;
      retc = 0;
    } 

    if ( (uid_sel >=0) ) {
      spacequota->RmQuota(XrdMgmSpaceQuota::kUserFilesTarget, uid_sel);
      spacequota->RmQuota(XrdMgmSpaceQuota::kUserFilesIs, uid_sel);
      sprintf(printline, "success: removed inode quota for uid=%ld\n", uid_sel);
      msg+= printline;
      retc = 0;
    }

    if ( (gid_sel >=0) ) {
      spacequota->RmQuota(XrdMgmSpaceQuota::kGroupBytesTarget, gid_sel);
      spacequota->RmQuota(XrdMgmSpaceQuota::kGroupBytesIs, uid_sel);
      sprintf(printline, "success: removed volume quota for gid=%ld\n", gid_sel);
      msg+= printline;
      retc = 0;
    }

    if ( (gid_sel >=0) ) {
      spacequota->RmQuota(XrdMgmSpaceQuota::kGroupFilesTarget, gid_sel);
      spacequota->RmQuota(XrdMgmSpaceQuota::kGroupFilesIs, uid_sel);
      sprintf(printline, "success: removed inode quota for gid=%ld\n", gid_sel);
      msg+= printline;
      retc = 0;
    }
    
    spacequota->UpdateTargetSums();
    return true;
  } else {
    msg = "error: no space defined with name ";msg += space;
    return false;
  }
}
/*----------------------------------------------------------------------------*/
