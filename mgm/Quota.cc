/*----------------------------------------------------------------------------*/
#include "mgm/Quota.hh"
#include "mgm/FstNode.hh"
#include "mgm/XrdMgmOfs.hh"
#include "namespace/accounting/QuotaStats.hh"
/*----------------------------------------------------------------------------*/
#include <errno.h>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

XrdOucHash<SpaceQuota> Quota::gQuota;
eos::common::RWMutex Quota::gQuotaMutex;

#ifdef __APPLE__
#define ENONET 64
#endif

/*----------------------------------------------------------------------------*/
SpaceQuota::SpaceQuota(const char* name) {
  Quota.set_empty_key(-1);
  Quota.set_deleted_key(-2);
  SpaceName = name;
  LastCalculationTime = 0;
  LastEnableCheck = 0;
  PhysicalFreeBytes = PhysicalFreeFiles = PhysicalMaxBytes = PhysicalMaxFiles = 0;
  PhysicalTmpFreeBytes = PhysicalTmpFreeFiles = PhysicalTmpMaxBytes = PhysicalTmpMaxFiles = 0;

  gOFS->eosViewMutex.Lock();
  eos::ContainerMD *quotadir=0;
  try {
    quotadir = gOFS->eosView->getContainer(name );
  } catch( eos::MDException &e ) {
    quotadir = 0;
  }
  if (!quotadir) {
    try {
      quotadir = gOFS->eosView->createContainer(name, true );
    } catch( eos::MDException &e ) {
      eos_static_crit("Cannot create quota directory %s", name);
    }
  }

  if (quotadir) {
    try {
      QuotaNode = gOFS->eosView->getQuotaNode(quotadir);
    } catch( eos::MDException &e ) {
      QuotaNode = 0;
    }
    if (!QuotaNode) {
      try {
	QuotaNode = gOFS->eosView->registerQuotaNode( quotadir );
      } catch ( eos::MDException &e ) {
	QuotaNode = 0;
	eos_static_crit("Cannot register quota node %s", name);
      }
    }
  }
  gOFS->eosViewMutex.UnLock();
}


/*----------------------------------------------------------------------------*/
SpaceQuota::~SpaceQuota() {}


/*----------------------------------------------------------------------------*/
void
SpaceQuota::RmQuota(unsigned long tag, unsigned long id, bool lock) 
{
  if (lock) Mutex.Lock();
  Quota.erase(Index(tag,id));
  eos_static_debug("rm quota tag=%lu id=%lu", tag, id);
  if (lock) Mutex.UnLock();
  return;
}

/*----------------------------------------------------------------------------*/
long long 
SpaceQuota::GetQuota(unsigned long tag, unsigned long id, bool lock) 
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
SpaceQuota::SetQuota(unsigned long tag, unsigned long id, unsigned long long value, bool lock) 
{
  if (lock) Mutex.Lock();
  eos_static_debug("set quota tag=%lu id=%lu value=%llu", tag, id, value);
  Quota[Index(tag,id)] = value;
  if (lock) Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void
SpaceQuota::AddQuota(unsigned long tag, unsigned long id, long long value, bool lock) 
{
  if (lock) Mutex.Lock();
  eos_static_debug("add quota tag=%lu id=%lu value=%llu", tag, id , value);
  // fix for avoiding negative numbers
  if ( ( ((long long)Quota[Index(tag,id)]) + (long long)value) >=0)
    Quota[Index(tag,id)] += value;
  eos_static_debug("sum quota tag=%lu id=%lu value=%llu", tag, id, Quota[Index(tag,id)]);
  if (lock) Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void
SpaceQuota::UpdateTargetSums() 
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
SpaceQuota::UpdateIsSums() 
{
  Mutex.Lock();
  eos_static_debug("updating IS values");

  ResetQuota(kAllUserBytesIs,0, false);
  ResetQuota(kAllUserFilesIs,0, false);
  ResetQuota(kAllGroupBytesIs,0, false);
  ResetQuota(kAllGroupFilesIs,0, false);

  google::dense_hash_map<long long, unsigned long long>::const_iterator it;

  for (it=Begin(); it != End(); it++) {
    if ( (UnIndex(it->first) == kUserBytesIs)) 
      AddQuota(kAllUserBytesIs,0, it->second, false);
    if ( (UnIndex(it->first) == kUserLogicalBytesIs)) 
      AddQuota(kAllUserLogicalBytesIs,0, it->second, false);
    if ( (UnIndex(it->first) == kUserFilesIs)) 
      AddQuota(kAllUserFilesIs,0, it->second, false);
    if ( (UnIndex(it->first) == kGroupBytesIs)) 
      AddQuota(kAllGroupBytesIs,0, it->second, false);
    if ( (UnIndex(it->first) == kGroupLogicalBytesIs)) 
      AddQuota(kAllGroupLogicalBytesIs,0, it->second, false);
    if ( (UnIndex(it->first) == kGroupFilesIs)) 
      AddQuota(kAllGroupFilesIs,0, it->second, false);
  }

  Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void
SpaceQuota::PrintOut(XrdOucString &output, long uid_sel, long gid_sel, bool monitoring, bool translateids)
{
  char headerline[4096];
  eos_static_debug("called");

  google::dense_hash_map<long long, unsigned long long>::const_iterator it;

  UpdateIsSums();
  Quota::NodeToSpaceQuota(SpaceName.c_str(), true);

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

  if (!SpaceName.beginswith("/")) {
    if ( (uid_sel <0) && (gid_sel <0)) {
      XrdOucString value1="";
      XrdOucString value2="";
      XrdOucString value3="";
      XrdOucString value4="";
      
      XrdOucString percentage1="";
      XrdOucString percentage2="";

      // this is a virtual quota node
      if (!monitoring) {
	output+="# __________________________________________________________________________________________\n";
	sprintf(headerline,"# ==> Space     : %-16s\n", SpaceName.c_str());
	output+= headerline;
	output+="# ------------------------------------------------------------------------------------------\n";
	output+="# ==> Physical\n";
	sprintf(headerline,"     %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s \n", GetTagName(kGroupBytesIs), GetTagName(kGroupFilesIs), GetTagName(kGroupBytesTarget), GetTagName(kGroupFilesTarget), "volume[%]", "status-vol", "inodes[%]","status-ino"); 
	output+= headerline;
	sprintf(headerline,"PHYS %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n",
		eos::common::StringConversion::GetReadableSizeString(value1, PhysicalMaxBytes-PhysicalFreeBytes,"B"), 
  	        eos::common::StringConversion::GetReadableSizeString(value2, PhysicalMaxFiles-PhysicalFreeFiles,"-"), 
		eos::common::StringConversion::GetReadableSizeString(value3, PhysicalMaxBytes,"B"), 
		eos::common::StringConversion::GetReadableSizeString(value4, PhysicalMaxFiles,"-"),
		GetQuotaPercentage(PhysicalMaxBytes-PhysicalFreeBytes,PhysicalMaxBytes, percentage1),
		GetQuotaStatus(PhysicalMaxBytes-PhysicalFreeBytes,PhysicalMaxBytes),
		GetQuotaPercentage(PhysicalMaxFiles-PhysicalFreeFiles,PhysicalMaxFiles, percentage2),
		GetQuotaStatus(PhysicalMaxFiles-PhysicalFreeFiles,PhysicalMaxFiles));
	output+= headerline;
	output+="# ..........................................................................................\n";
      } else {
	sprintf(headerline,"quota=phys space=%s usedbytes=%llu usedfiles=%llu maxbytes=%llu maxfiles=%llu percentageusedbytes=%s statusbytes=%s percentageusedfiles=%s statusfiles=%s\n", SpaceName.c_str(),
		PhysicalMaxBytes-PhysicalFreeBytes,
		PhysicalMaxFiles-PhysicalFreeFiles,
		PhysicalMaxBytes,
		PhysicalMaxFiles,
		GetQuotaPercentage(PhysicalMaxBytes-PhysicalFreeBytes,PhysicalMaxBytes, percentage1),
		GetQuotaStatus(PhysicalMaxBytes-PhysicalFreeBytes,PhysicalMaxBytes),
		GetQuotaPercentage(PhysicalMaxFiles-PhysicalFreeFiles,PhysicalMaxFiles, percentage2),
		GetQuotaStatus(PhysicalMaxFiles-PhysicalFreeFiles,PhysicalMaxFiles));
	output+= headerline;
      }
    }    
  } else {
    // this is a virtual quota node
    if (!monitoring) {
      output+="# ____________________________________________________________________________________\n";
      sprintf(headerline,"# ==> Quota Node: %-16s\n", SpaceName.c_str());
      output+= headerline;
      output+="# ____________________________________________________________________________________\n";
    }
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
      if (!monitoring) {
	sprintf(headerline,"%-8s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kUserBytesIs), GetTagName(kUserBytesIs), GetTagName(kUserLogicalBytesIs), GetTagName(kUserFilesIs),GetTagName(kUserBytesTarget), GetTagName(kUserFilesTarget), "filled[%]", "status");
	output+= headerline;
      }
    }
    
    for (int lid = 0; lid < userentries; lid++) {
      eos_static_debug("loop with id=%d",lid);
      XrdOucString value1="";
      XrdOucString value2="";
      XrdOucString value3="";
      XrdOucString value4="";
      
      XrdOucString id=""; id += sortuidarray[lid];

      if (translateids) {
	// try to translate with password database
	char buffer[16384];
	int buflen = sizeof(buffer);
	struct passwd pwbuf;
	struct passwd *pwbufp=0;
	
	if (!getpwuid_r(sortuidarray[lid], &pwbuf, buffer, buflen, &pwbufp)) {
	  char uidlimit[16];
	  snprintf(uidlimit,8,"%s",pwbuf.pw_name);
	  id = uidlimit;
	}
      }

      XrdOucString percentage="";
      if (!monitoring) {
	sprintf(headerline,"%-8s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str() ,
		eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kUserBytesIs,sortuidarray[lid]),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kUserLogicalBytesIs,sortuidarray[lid]),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value2, GetQuota(kUserFilesIs,sortuidarray[lid]),"-"), 
		eos::common::StringConversion::GetReadableSizeString(value3, GetQuota(kUserBytesTarget,sortuidarray[lid]),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value4, GetQuota(kUserFilesTarget,sortuidarray[lid]),"-"),
		GetQuotaPercentage(GetQuota(kUserBytesIs,sortuidarray[lid]), GetQuota(kUserBytesTarget,sortuidarray[lid]), percentage),
		GetQuotaStatus(GetQuota(kUserBytesIs,sortuidarray[lid]), GetQuota(kUserBytesTarget,sortuidarray[lid])));
      } else {
	sprintf(headerline,"quota=node uid=%s space=%s usedbytes=%llu usedlogicalbytes=%lluusedfiles=%llu maxbytes=%llu maxfiles=%llu percentageusedbytes=%s statusbytes=%s\n", id.c_str() , SpaceName.c_str(), 
		GetQuota(kUserBytesIs,sortuidarray[lid]),
		GetQuota(kUserLogicalBytesIs,sortuidarray[lid]),
		GetQuota(kUserFilesIs,sortuidarray[lid]),
		GetQuota(kUserBytesTarget,sortuidarray[lid]),
		GetQuota(kUserFilesTarget,sortuidarray[lid]),
		GetQuotaPercentage(GetQuota(kUserBytesIs,sortuidarray[lid]), GetQuota(kUserBytesTarget,sortuidarray[lid]), percentage),
		GetQuotaStatus(GetQuota(kUserBytesIs,sortuidarray[lid]), GetQuota(kUserBytesTarget,sortuidarray[lid])));
      }
      
      output += headerline;
    }
    
    if (groupentries) {
      // group loop
      if (!monitoring) {
	output+="# ....................................................................................\n";
	sprintf(headerline,"%-8s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kGroupBytesIs), GetTagName(kGroupBytesIs), GetTagName(kGroupLogicalBytesIs), GetTagName(kGroupBytesIs+2), GetTagName(kGroupBytesIs+1), GetTagName(kGroupBytesIs+3), "filled[%]", "status");
	output+= headerline;
      }
    }
    
    for (int lid = 0; lid < groupentries; lid++) {
      eos_static_debug("loop with id=%d",lid);
      XrdOucString value1="";
      XrdOucString value2="";
      XrdOucString value3="";
      XrdOucString value4="";
      
      XrdOucString id=""; id += sortgidarray[lid];

      if (translateids) {
	// try to translate with password database
	char buffer[16384];
	int buflen = sizeof(buffer);
	struct group grbuf;
	struct group *grbufp=0;
	
	if (!getgrgid_r(sortgidarray[lid], &grbuf, buffer, buflen, &grbufp)) {
	  char gidlimit[16];
	  snprintf(gidlimit,8,"%s",grbuf.gr_name);
	  id = gidlimit;
	}
      }

      XrdOucString percentage="";
      if (!monitoring) {
	sprintf(headerline,"%-8s %-10s %-10s %-10s %-10s %-10s %-10s %-10s \n", id.c_str() ,
		eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kGroupBytesIs,sortgidarray[lid]),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kGroupLogicalBytesIs,sortgidarray[lid]),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value2, GetQuota(kGroupFilesIs,sortgidarray[lid]),"-"), 
		eos::common::StringConversion::GetReadableSizeString(value3, GetQuota(kGroupBytesTarget,sortgidarray[lid]),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value4, GetQuota(kGroupFilesTarget,sortgidarray[lid]),"-"),
		GetQuotaPercentage(GetQuota(kGroupBytesIs,sortgidarray[lid]), GetQuota(kGroupBytesTarget,sortgidarray[lid]), percentage),
		GetQuotaStatus(GetQuota(kGroupBytesIs,sortgidarray[lid]), GetQuota(kGroupBytesTarget,sortgidarray[lid])));
      } else {
	sprintf(headerline,"quota=node gid=%s space=%s usedbytes=%llu usedlogicalbytes=%llu usedfiles=%llu maxbytes=%llu maxfiles=%llu percentageusedbytes=%s statusbytes=%s\n", id.c_str() , SpaceName.c_str(),
		GetQuota(kGroupBytesIs,sortgidarray[lid]),
		GetQuota(kGroupLogicalBytesIs,sortgidarray[lid]),
		GetQuota(kGroupFilesIs,sortgidarray[lid]),
		GetQuota(kGroupBytesTarget,sortgidarray[lid]),
		GetQuota(kGroupFilesTarget,sortgidarray[lid]),
		GetQuotaPercentage(GetQuota(kGroupBytesIs,sortgidarray[lid]), GetQuota(kGroupBytesTarget,sortgidarray[lid]), percentage),
		GetQuotaStatus(GetQuota(kGroupBytesIs,sortgidarray[lid]), GetQuota(kGroupBytesTarget,sortgidarray[lid])));
      }
      output += headerline;
    }
    
    if ( (uid_sel <0) && (gid_sel <0)) {
      if (!monitoring) {
	output+="# ------------------------------------------------------------------------------------\n";
	output+="# ==> Summary\n";
      }

      XrdOucString value1="";
      XrdOucString value2="";
      XrdOucString value3="";
      XrdOucString value4="";
      
      XrdOucString id="ALL";
      XrdOucString percentage="";
      
      if (!monitoring) {
	sprintf(headerline,"%-8s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kAllUserBytesIs), GetTagName(kAllUserBytesIs), GetTagName(kAllUserLogicalBytesIs), GetTagName(kAllUserFilesIs), GetTagName(kAllUserBytesTarget), GetTagName(kAllUserFilesTarget), "filled[%]", "status");
	output += headerline;
	sprintf(headerline,"%-8s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str() ,
		eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kAllUserBytesIs,0),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kAllUserLogicalBytesIs,0),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value2, GetQuota(kAllUserFilesIs,0),"-"), 
		eos::common::StringConversion::GetReadableSizeString(value3, GetQuota(kAllUserBytesTarget,0),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value4, GetQuota(kAllUserFilesTarget,0),"-"),
		GetQuotaPercentage(GetQuota(kAllUserBytesIs,0), GetQuota(kAllUserBytesTarget,0), percentage),
		GetQuotaStatus(GetQuota(kAllUserBytesIs,0), GetQuota(kAllUserBytesTarget,0)));
      } else {
	sprintf(headerline,"quota=node uid=%s space=%s usedbytes=%llu usedlogicalbytes=%llu usedfiles=%llu maxbytes=%llu maxfiles=%llu percentageusedbytes=%s statusbytes=%s\n", id.c_str() , SpaceName.c_str(), 
		GetQuota(kAllUserBytesIs,0),
		GetQuota(kAllUserLogicalBytesIs,0),
		GetQuota(kAllUserFilesIs,0),
		GetQuota(kAllUserBytesTarget,0),
		GetQuota(kAllUserFilesTarget,0),
		GetQuotaPercentage(GetQuota(kAllUserBytesIs,0), GetQuota(kAllUserBytesTarget,0), percentage),
		GetQuotaStatus(GetQuota(kAllUserBytesIs,0), GetQuota(kAllUserBytesTarget,0)));
      }
      output += headerline;

      if (!monitoring) {
	sprintf(headerline,"%-8s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kAllGroupBytesIs), GetTagName(kAllGroupBytesIs), GetTagName(kAllGroupLogicalBytesIs), GetTagName(kAllGroupFilesIs), GetTagName(kAllGroupBytesTarget), GetTagName(kAllGroupFilesTarget), "filled[%]", "status");
	output += headerline;
	sprintf(headerline,"%-8s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str() , 
		eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kAllGroupBytesIs,0),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kAllGroupLogicalBytesIs,0),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value2, GetQuota(kAllGroupFilesIs,0),"-"), 
		eos::common::StringConversion::GetReadableSizeString(value3, GetQuota(kAllGroupBytesTarget,0),"B"), 
		eos::common::StringConversion::GetReadableSizeString(value4, GetQuota(kAllGroupFilesTarget,0),"-"),
		GetQuotaPercentage(GetQuota(kAllGroupBytesIs,0), GetQuota(kAllGroupBytesTarget,0), percentage),
		GetQuotaStatus(GetQuota(kAllGroupBytesIs,0), GetQuota(kAllGroupBytesTarget,0)));
      } else {
	sprintf(headerline,"quota=node gid=%s space=%s usedbytes=%llu usedlogicalbytes=%llu usedfiles=%llu maxbytes=%llu maxfiles=%llu percentageusedbytes=%s statusbytes=%s\n", id.c_str() , SpaceName.c_str(), 
		GetQuota(kAllGroupBytesIs,0),
		GetQuota(kAllGroupLogicalBytesIs,0),
		GetQuota(kAllGroupFilesIs,0),
		GetQuota(kAllGroupBytesTarget,0),
		GetQuota(kAllGroupFilesTarget,0),
		GetQuotaPercentage(GetQuota(kAllGroupBytesIs,0), GetQuota(kAllGroupBytesTarget,0), percentage),
		GetQuotaStatus(GetQuota(kAllGroupBytesIs,0), GetQuota(kAllGroupBytesTarget,0)));
      }
      output += headerline;
    }
  }
  free(sortuidarray);
  free(sortgidarray);
}



/*----------------------------------------------------------------------------*/
int 
SpaceQuota::FilePlacement(uid_t uid, gid_t gid, const char* grouptag, unsigned long lid, std::vector<unsigned int> &selectedfs, bool truncate, int forcedindex, unsigned long long bookingsize)
{
  // the caller routing has to lock via => eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex) !!!
  std::set<eos::common::FileSystem::fsid_t> fsidavoidlist;
  std::map<eos::common::FileSystem::fsid_t, float> availablefs;

  // fill the avoid list from the selectedfs input vector
  for (unsigned int i=0; i< selectedfs.size(); i++) {
    fsidavoidlist.insert(selectedfs[i]);
  }

  unsigned int nfilesystems = eos::common::LayoutId::GetStripeNumber(lid) + 1; // 0 = 1 replica !
  unsigned int nassigned = 0;
  bool hasquota = false;

  // first figure out how many filesystems we need
  eos_static_debug("uid=%u gid=%u grouptag=%s place filesystems=%u",uid,gid,grouptag, nfilesystems);
  
  std::string indextag = "";
  if (grouptag) {
    indextag = grouptag;
  } else {
    indextag += uid; 
    indextag += ":";
    indextag += gid;
  }
  
  // check if the uid/gid has enough quota configured to place in this space !
  // -> user quota

  eos_static_debug("%llu %llu",GetQuota(kUserBytesTarget,uid,false), GetQuota(kUserBytesIs,uid,false));

  if (Enabled()) {
    if ( ( ( (GetQuota(kUserBytesTarget,uid,false)) - (GetQuota(kUserBytesIs,uid,false)) ) > (long long)(1ll*nfilesystems*bookingsize) ) &&
	 ( ( (GetQuota(kUserFilesTarget,uid,false)) - (GetQuota(kUserFilesIs,uid,false)) ) > (1*nfilesystems ) ) ) {
      // the user has quota here!
      hasquota = true;
    } 
    
    // -> group quota
    if ( ( ( (GetQuota(kGroupBytesTarget,gid,false)) - (GetQuota(kGroupBytesIs,gid,false)) ) > (long long) (1ll*nfilesystems*bookingsize) ) &&
	 ( ( (GetQuota(kGroupFilesTarget,gid,false)) - (GetQuota(kGroupFilesIs,gid,false)) ) > (1*nfilesystems)  ) ) {
      // the group has quota here!
      hasquota = true;
    } 
    
    if (uid==0) {
      // root does not need any quota
      hasquota = true;
    }
    
    if (!hasquota) {
      eos_static_debug("uid=%u git=%u grouptag=%s place filesystems=%u has no quota left!",uid,gid,grouptag, nfilesystems);
      return ENOSPC;
    }
  } 
  
  unsigned int currentfsrandomoffset=0;

  std::string spacename = SpaceName.c_str();
  
  std::set<FsGroup*>::const_iterator git;

  // protects that filesystems don't change during this operation
  if (!FsView::gFsView.mSpaceGroupView.count(spacename)) {
    // there is no filesystem in that space
    selectedfs.clear();
    return ENOSPC;
  }
  
  // place the group iterator
  if (forcedindex>=0) {
    for (git = FsView::gFsView.mSpaceGroupView[spacename].begin(); git != FsView::gFsView.mSpaceGroupView[spacename].end(); git++) {
      if ((*git)->GetIndex() == (unsigned int)forcedindex)
	break;
    }
    if ((*git)->GetIndex() != (unsigned int)forcedindex) {
      selectedfs.clear();
      return ENOSPC;
    }
  } else {
    if (schedulingGroup.count(indextag)) {
      git = FsView::gFsView.mSpaceGroupView[spacename].find(schedulingGroup[indextag]);
    } else {
      git = FsView::gFsView.mSpaceGroupView[spacename].begin();
      schedulingGroup[indextag] = *git;
    }
  }
  
  // we can loop over all existing scheduling views
  for (unsigned int groupindex=0; groupindex < FsView::gFsView.mSpaceGroupView[spacename].size(); groupindex++) {
    eos_static_debug("scheduling group loop %d", forcedindex);
    selectedfs.clear();
    
    std::set<eos::common::FileSystem::fsid_t>::const_iterator fsit;
    eos::common::FileSystem::fsid_t fsid=0;
    // create the string map key for this group/index pair
    XrdOucString fsindextag="";
    fsindextag += (int)(*git)->GetIndex();
    // place the filesystem iterator
    fsindextag += "|";
    fsindextag += indextag.c_str();
    std::string sfsindextag = fsindextag.c_str();
    
    if (schedulingFileSystem.count(sfsindextag)) {
      //
      fsid = schedulingFileSystem[sfsindextag];
      fsit = (*git)->find(fsid);
      if (fsit == (*git)->end()) {
	// this filesystem is not anymore there, we start with the first one
	fsit = (*git)->begin();
	fsid = *fsit;
      }
    } else {
      fsit = (*git)->begin();
      fsid = *fsit;
    }
    
    // fake weight
    double weight = 1.0;

    currentfsrandomoffset = (unsigned int) (( 0.999999 * random()* (*git)->size() )/RAND_MAX);
    
    // we loop over all filesystems in that group
    for (unsigned int fsindex=0; fsindex < (*git)->size(); fsindex++) {
      eos_static_debug("checking scheduling group %d filesystem %d", (*git)->GetIndex(), *fsit);	

      // take filesystem snapshot
      eos::common::FileSystem::fs_snapshot_t snapshot;
      // we are already in a locked section
      FsView::gFsView.mIdView[fsid]->SnapShotFileSystem(snapshot,false);
      
      // check if this filesystem can be used (online, enough space etc...)
      if ( (snapshot.mStatus       == eos::common::FileSystem::kBooted) && 
	   (snapshot.mConfigStatus == eos::common::FileSystem::kRW) && 
	   (snapshot.mErrCode      == 0 ) && // this we probably don't need 
	   (FsView::gFsView.mNodeView[snapshot.mQueue]->GetConfigMember("status") == "on") && 
	   (FsView::gFsView.mGroupView[snapshot.mGroup]->GetConfigMember("status") == "on") ) {
	
	if (!fsidavoidlist.count(fsid)) {
	  availablefs[fsid] = weight;
	}
      } else {
	eos_static_err("%d %d %d\n", (snapshot.mStatus), (snapshot.mConfigStatus), (snapshot.mErrCode      == 0 ));
      }
      fsit++;
      schedulingFileSystem[sfsindextag] = *fsit;
      // create cycling
      if (fsit == (*git)->end()) {
	fsit = (*git)->begin();
      }
      fsid = *fsit;
      // evt. this has to be commented
      if ( (availablefs.size()>= nfilesystems) && (availablefs.size() > ((*git)->size()/2)) ) {
	// we stop if we have found enough ... atleast half of the scheduling group
	break;
      }
    }
    
    std::map<eos::common::FileSystem::fsid_t,float>::const_iterator ait=availablefs.begin();
    
    // check if there are atlast <nfilesystems> in the available map
    if (availablefs.size() >= nfilesystems) {
      for (unsigned int loop = 0; loop < 1000; loop++) {
	// we cycle over the available filesystems
	float randomacceptor = (0.999999 * random()/RAND_MAX);
	eos_static_debug("fs %u acceptor %f/%f for %d. replica [loop=%d] [avail=%d]", ait->first, randomacceptor, ait->second, nassigned+1, loop, availablefs.size());
	
	if ( (nassigned==0) ) {
	  if (ait->first<randomacceptor) {
	    ait++;
	    if (ait==availablefs.end())
	      ait = availablefs.begin();
	  } else {
            // push it on the selection list
	    selectedfs.push_back(ait->first);
	    eos_static_debug("fs %u selected for %d. replica", ait->first, nassigned+1);
	    
	    // remove it from the selection map
	    availablefs.erase(ait->first);
	    
	    // rotate scheduling view ptr
	    nassigned++;
	  }
	} else {
	  // we select a random one
	  unsigned int randomindex;
	  randomindex = (unsigned int) (( 0.999999 * random()* availablefs.size() )/RAND_MAX);
	  eos_static_debug("trying random index %d", randomindex);
	  
	  for (unsigned int i=0; i< randomindex; i++) {
	    ait++;
	    if (ait == availablefs.end())
	      ait = availablefs.begin();
	  }
	  
	  if (ait->second>randomacceptor) {
	    // push it on the selection list
	    selectedfs.push_back(ait->first);
	    eos_static_debug("fs %u selected for %d. replica", ait->first, nassigned+1);
	    
	    // remove it from the selection map
	    availablefs.erase(ait->first);
	    nassigned++;
	  }
	}
	if (nassigned >= nfilesystems)
	  break;
      } // leave the <loop> where filesystems get selected by weight
    } 
    
    git++;
    if ( git ==  FsView::gFsView.mSpaceGroupView[spacename].end() ) {
      git = FsView::gFsView.mSpaceGroupView[spacename].begin();
    }

    // remember the last group for that indextag
    schedulingGroup[indextag] = *git;

    if (nassigned >= nfilesystems) {
      // leave the group loop - we got enough
      break;
    }
    
    selectedfs.clear();
    nassigned = 0;
    
    if (forcedindex >=0) {
      // in this case we leave, the requested one was tried and we finish here
      break;
      }
  }
  
  if (nassigned == nfilesystems) {
    // now we reshuffle the order using a random number
    unsigned int randomindex;
    randomindex = (unsigned int) (( 0.999999 * random()* selectedfs.size() )/RAND_MAX);
    
    std::vector<unsigned int> randomselectedfs;
    randomselectedfs = selectedfs;
    
    selectedfs.clear();
    
    int rrsize=randomselectedfs.size();
    for (int i=0; i< rrsize; i++) {
      selectedfs.push_back(randomselectedfs[(randomindex+i)%rrsize]);
    }
    return 0;
  } else {
    selectedfs.clear();
    return ENOSPC;
  }
}


/*----------------------------------------------------------------------------*/
int SpaceQuota::FileAccess(uid_t uid, gid_t gid, unsigned long forcedfsid, const char* forcedspace, unsigned long lid, std::vector<unsigned int> &locationsfs, unsigned long &fsindex, bool isRW)
{
  // the caller routing has to lock via => eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex) !!!

  if (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kPlain) {
    // we have only a single replica ... so just check the state of the filesystem where that is located
    if (locationsfs.size() && locationsfs[0]) {
      eos::common::FileSystem* filesystem = 0;
      if (FsView::gFsView.mIdView.count(locationsfs[0])){
	filesystem = FsView::gFsView.mIdView[locationsfs[0]];
      }
      if (!filesystem)
	return ENODATA;

      // take filesystem snapshot
      eos::common::FileSystem::fs_snapshot_t snapshot;
      // we are already in a locked section
      FsView::gFsView.mIdView[locationsfs[0]]->SnapShotFileSystem(snapshot,false);

      if (isRW) {
	// FIX ME!
	if ( (snapshot.mStatus       == eos::common::FileSystem::kBooted) && 
	     (snapshot.mConfigStatus == eos::common::FileSystem::kRW) && 
	     (snapshot.mErrCode      == 0 ) && // this we probably don't need 
	     (FsView::gFsView.mNodeView[snapshot.mQueue]->GetConfigMember("status") == "on") && 
	     (FsView::gFsView.mGroupView[snapshot.mGroup]->GetConfigMember("status") == "on") ) {
          // perfect!
          fsindex = 0;
          eos_static_debug("selected plain file access via filesystem %u",locationsfs[0]);
          return 0;
        } else {

	  // FIX ME!
          // check if we are in any kind of no-update mode
	  if (0) {
            return EROFS;
          }

          // we are off the wire
          return ENONET;
        }
      } else {
	if ( (snapshot.mStatus       == eos::common::FileSystem::kBooted) && 
	     (snapshot.mConfigStatus >= eos::common::FileSystem::kRO) && 
	     (snapshot.mErrCode      == 0 ) && // this we probably don't need 
	     (FsView::gFsView.mNodeView[snapshot.mQueue]->GetConfigMember("status") == "on") && 
	     (FsView::gFsView.mGroupView[snapshot.mGroup]->GetConfigMember("status") == "on") ) {
          // perfect!
          fsindex = 0;
          return 0;
        } else {
          return ENONET;
        }
      }
    } else {
      return ENODATA;
    }
  }
  return EINVAL;
}


/*----------------------------------------------------------------------------*/
SpaceQuota* 
Quota::GetSpaceQuota(const char* name, bool nocreate) 
{
  // the caller has to Readlock gQuotaMutex
  SpaceQuota* spacequota=0;
  
  if ( (spacequota = gQuota.Find(name)) ) {
    
  } else {
    if (nocreate) {
      return NULL;
    }
    do {
      gQuotaMutex.UnLockRead();
      gQuotaMutex.LockWrite();    
      spacequota = new SpaceQuota(name);
      gQuota.Add(name,spacequota);
      gQuotaMutex.UnLockWrite();    
      gQuotaMutex.LockRead();
    } while (! (spacequota = gQuota.Find(name)));
  }
  
  return spacequota;
}

/*----------------------------------------------------------------------------*/
int 
Quota::GetSpaceNameList(const char* key, SpaceQuota* spacequota, void *Arg)
{
  XrdOucString* spacestring = (XrdOucString*) Arg;
  (*spacestring) += spacequota->GetSpaceName();
  (*spacestring) += ",";
  return 0;
}

/*----------------------------------------------------------------------------*/
void
Quota::PrintOut(const char* space, XrdOucString &output, long uid_sel, long gid_sel, bool monitoring, bool translateids)
{
  output="";
  XrdOucString spacenames="";
  gQuota.Apply(GetSpaceNameList, &spacenames);
  int kommapos=0;
  int spos=0;
  std::set<std::string> spacevector;

  eos_static_debug("space=%s spacenames=%s",space, spacenames.c_str());

  eos::common::RWMutexReadLock lock(gQuotaMutex);

  if (space ==0) {
    while ((kommapos = spacenames.find(",", spos)) != STR_NPOS) {
      XrdOucString spacename;
      spacename.assign(spacenames,spos,kommapos-1);
      std::string sspacename = spacename.c_str();
      spacevector.insert(sspacename);
      spos = kommapos+1;
    }
    
    std::set<std::string>::iterator it;
    for (it = spacevector.begin(); it != spacevector.end(); it++) {
      SpaceQuota* spacequota = GetSpaceQuota(it->c_str(),true);
      if (spacequota) {
	spacequota->PrintOut(output, uid_sel, gid_sel, monitoring, translateids);
      }
    }
  } else {
    SpaceQuota* spacequota = GetSpaceQuota(space, true);
    if (spacequota) {
      spacequota->PrintOut(output, uid_sel, gid_sel, monitoring, translateids);
    }
  }
}


/*----------------------------------------------------------------------------*/
bool 
Quota::SetQuota(XrdOucString space, long uid_sel, long gid_sel, long long bytes, long long files, XrdOucString &msg, int &retc) 
{
  eos_static_debug("space=%s",space.c_str());

  eos::common::RWMutexReadLock lock(gQuotaMutex);

  SpaceQuota* spacequota = 0;

  XrdOucString configstring="";
  XrdOucString configstringheader="";
  char configvalue[1024];

  if (!space.length()) {
    spacequota = GetSpaceQuota("/eos",false);
    configstringheader += "/eos";
  } else {
    spacequota = GetSpaceQuota(space.c_str(),false);
    configstringheader += space.c_str();
  }
  
  configstringheader += ":";
  
  retc =  EINVAL;
	
  if (spacequota) {
    char printline[1024];
    XrdOucString sizestring;
    msg="";
    if ( (uid_sel >=0) && (bytes>=0) ) {
      configstring = configstringheader;
      spacequota->SetQuota(SpaceQuota::kUserBytesTarget, uid_sel, bytes);
      sprintf(printline, "success: updated quota for uid=%ld to %s\n", uid_sel,eos::common::StringConversion::GetReadableSizeString(sizestring, bytes,"B"));
      configstring += "uid="; configstring += (int) uid_sel; configstring += ":";
      configstring += SpaceQuota::GetTagAsString(SpaceQuota::kUserBytesTarget);
      sprintf(configvalue,"%llu",bytes);
      msg+= printline;
      // store the setting into the config table
      gOFS->ConfEngine->SetConfigValue("quota", configstring.c_str(), configvalue);
     
      retc = 0;
    } 

    if ( (uid_sel >=0) && (files>=0) ) {
      configstring = configstringheader;
      spacequota->SetQuota(SpaceQuota::kUserFilesTarget, uid_sel, files);
      sprintf(printline, "success: updated quota for uid=%ld to %s files\n", uid_sel,eos::common::StringConversion::GetReadableSizeString(sizestring, files,"-"));
      configstring += "uid="; configstring += (int) uid_sel; configstring += ":";
      configstring += SpaceQuota::GetTagAsString(SpaceQuota::kUserFilesTarget);
      sprintf(configvalue,"%llu",files);
      msg+= printline;
      // store the setting into the config table
      gOFS->ConfEngine->SetConfigValue("quota", configstring.c_str(), configvalue);
      retc = 0;
    }

    if ( (gid_sel >=0) && (bytes>=0) ) {
      configstring = configstringheader;
      spacequota->SetQuota(SpaceQuota::kGroupBytesTarget, gid_sel, bytes);
      sprintf(printline, "success: updated quota for gid=%ld to %s\n", gid_sel,eos::common::StringConversion::GetReadableSizeString(sizestring, bytes,"B"));
      configstring += "gid="; configstring += (int) gid_sel; configstring += ":";
      configstring += SpaceQuota::GetTagAsString(SpaceQuota::kGroupBytesTarget);
      sprintf(configvalue,"%llu",bytes);
      msg+= printline;
      // store the setting into the config table
      gOFS->ConfEngine->SetConfigValue("quota", configstring.c_str(), configvalue);
      retc = 0;
    }

    if ( (gid_sel >=0) && (files>=0) ) {
      configstring = configstringheader;
      spacequota->SetQuota(SpaceQuota::kGroupFilesTarget, gid_sel, files);
      sprintf(printline, "success: updated quota for gid=%ld to %s files\n", gid_sel,eos::common::StringConversion::GetReadableSizeString(sizestring, files,"-"));
      configstring += "gid="; configstring += (int) gid_sel; configstring += ":";
      configstring += SpaceQuota::GetTagAsString(SpaceQuota::kGroupFilesTarget);
      sprintf(configvalue,"%llu",files);
      msg+= printline;
      // store the setting into the config table
      gOFS->ConfEngine->SetConfigValue("quota", configstring.c_str(), configvalue);
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
bool 
Quota::RmQuota(XrdOucString space, long uid_sel, long gid_sel, XrdOucString &msg, int &retc) 
{
  eos_static_debug("space=%s",space.c_str());

  eos::common::RWMutexReadLock lock(gQuotaMutex);

  SpaceQuota* spacequota = 0;

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
      spacequota->RmQuota(SpaceQuota::kUserBytesTarget, uid_sel);
      spacequota->RmQuota(SpaceQuota::kUserBytesIs, uid_sel);
      sprintf(printline, "success: removed volume quota for uid=%ld\n", uid_sel);
      msg+= printline;
      retc = 0;
    } 

    if ( (uid_sel >=0) ) {
      spacequota->RmQuota(SpaceQuota::kUserFilesTarget, uid_sel);
      spacequota->RmQuota(SpaceQuota::kUserFilesIs, uid_sel);
      sprintf(printline, "success: removed inode quota for uid=%ld\n", uid_sel);
      msg+= printline;
      retc = 0;
    }

    if ( (gid_sel >=0) ) {
      spacequota->RmQuota(SpaceQuota::kGroupBytesTarget, gid_sel);
      spacequota->RmQuota(SpaceQuota::kGroupBytesIs, uid_sel);
      sprintf(printline, "success: removed volume quota for gid=%ld\n", gid_sel);
      msg+= printline;
      retc = 0;
    }

    if ( (gid_sel >=0) ) {
      spacequota->RmQuota(SpaceQuota::kGroupFilesTarget, gid_sel);
      spacequota->RmQuota(SpaceQuota::kGroupFilesIs, uid_sel);
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

uint64_t 
Quota::MapSizeCB(const eos::FileMD *file)
{
  //------------------------------------------------------------------------
  //! Callback function for the namespace to calculate how much space a file occupies
  //------------------------------------------------------------------------

  if (!file)
    return 0;

  unsigned long long size=0;

  // plain layout
  eos::FileMD::layoutId_t lid = file->getLayoutId();
  if (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kPlain) {
    size = file->getSize();
  }

  // replica layout
  if (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReplica) {
    size = (file->getSize() * (file->getNumLocation()));
  }
  
  return size;
}

/*----------------------------------------------------------------------------*/
void
Quota::LoadNodes() 
{
  // iterate over the defined quota nodes and make them visible as SpaceQuota
  eos::common::RWMutexReadLock lock(gQuotaMutex);

  eos::QuotaStats::NodeMap::iterator it;
  for (it = gOFS->eosView->getQuotaStats()->nodesBegin(); it != gOFS->eosView->getQuotaStats()->nodesEnd(); it++) {
    eos::ContainerMD::id_t id = it->first;
    eos::ContainerMD* container = gOFS->eosDirectoryService->getContainerMD(id);
    std::string quotapath = gOFS->eosView->getUri( container);
    SpaceQuota* spacequota = Quota::GetSpaceQuota(quotapath.c_str(), false);
    if (spacequota) {
      eos_static_notice("Created space for quota node: %s", quotapath.c_str());
    } else {
      eos_static_err("Failed to create space for quota node: %s\n", quotapath.c_str());
    }
  }
}

/*----------------------------------------------------------------------------*/
void 
Quota::NodesToSpaceQuota() 
{
  gOFS->eosViewMutex.Lock();
  // inserts the current state of the quota nodes into SpaceQuota's 
  eos::QuotaStats::NodeMap::iterator it;
  for (it = gOFS->eosView->getQuotaStats()->nodesBegin(); it != gOFS->eosView->getQuotaStats()->nodesEnd(); it++) {
    eos::ContainerMD::id_t id = it->first;
    eos::ContainerMD* container = gOFS->eosDirectoryService->getContainerMD(id);
    std::string quotapath = gOFS->eosView->getUri( container);
    NodeToSpaceQuota(quotapath.c_str(), false);
  }
  gOFS->eosViewMutex.UnLock();
}

/*----------------------------------------------------------------------------*/


void 
Quota::NodeToSpaceQuota(const char* name, bool lock)
{
  if (!name)
    return;

  eos::common::RWMutexReadLock locker(gQuotaMutex);

  SpaceQuota* spacequota = Quota::GetSpaceQuota(name, false);

  if (lock) 
    gOFS->eosViewMutex.Lock();
  if (spacequota && spacequota->GetQuotaNode()) {
    // insert current state of a single quota node into aSpaceQuota
    eos::QuotaNode::UserMap::const_iterator itu;
    eos::QuotaNode::GroupMap::const_iterator itg;
    // loop over user
    for (itu = spacequota->GetQuotaNode()->userUsageBegin(); itu != spacequota->GetQuotaNode()->userUsageEnd(); itu++) {
      spacequota->SetQuota(SpaceQuota::kUserBytesIs, itu->first, itu->second.physical_space);
      spacequota->SetQuota(SpaceQuota::kUserFilesIs, itu->first, itu->second.files);
      spacequota->SetQuota(SpaceQuota::kUserLogicalBytesIs, itu->first, itu->second.space);
    }
    for (itg = spacequota->GetQuotaNode()->groupUsageBegin(); itg != spacequota->GetQuotaNode()->groupUsageEnd(); itg++) {
      spacequota->SetQuota(SpaceQuota::kGroupBytesIs, itg->first, itg->second.physical_space);
      spacequota->SetQuota(SpaceQuota::kGroupFilesIs, itg->first, itg->second.files);
      spacequota->SetQuota(SpaceQuota::kGroupLogicalBytesIs, itg->first, itg->second.space);
    }
  } else {
    eos_static_crit("Cannot get quota node extract quota map");
  }
  if (lock)
    gOFS->eosViewMutex.UnLock();
}

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_END
