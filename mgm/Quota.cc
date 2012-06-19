// ----------------------------------------------------------------------
// File: Quota.cc
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
#include "mgm/Quota.hh"
#include "mgm/Policy.hh"
#include "mgm/XrdMgmOfs.hh"
#include "namespace/accounting/QuotaStats.hh"
/*----------------------------------------------------------------------------*/
#include <errno.h>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

std::map<std::string, SpaceQuota*> Quota::gQuota;
eos::common::RWMutex Quota::gQuotaMutex;

#ifdef __APPLE__
#define ENONET 64
#endif

/*----------------------------------------------------------------------------*/
SpaceQuota::SpaceQuota(const char* name) {
  SpaceName = name;
  LastCalculationTime = 0;
  LastEnableCheck = 0;
  QuotaNode = 0;
  PhysicalFreeBytes = PhysicalFreeFiles = PhysicalMaxBytes = PhysicalMaxFiles = 0;
  PhysicalTmpFreeBytes = PhysicalTmpFreeFiles = PhysicalTmpMaxBytes = PhysicalTmpMaxFiles = 0;
  LayoutSizeFactor = 1.0;
  On = false;

  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  eos::ContainerMD *quotadir=0;

  std::string path = name;

  if (path[0] == '/') {
    if (path[path.length()-1] != '/') {
      path += "/";
    }
    SpaceName = path.c_str();

    try {
      quotadir = gOFS->eosView->getContainer(path.c_str());
    } catch( eos::MDException &e ) {
      quotadir = 0;
    }
    if (!quotadir) {
      try {
        quotadir = gOFS->eosView->createContainer(name, true );
        quotadir->setMode(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH );
        gOFS->eosView->updateContainerStore(quotadir);

      } catch( eos::MDException &e ) {
        eos_static_crit("Cannot create quota directory %s", name);
      }
    }
    
    if (quotadir) {
      try {
        QuotaNode = gOFS->eosView->getQuotaNode(quotadir, false);
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
  } else {
    QuotaNode = 0;
  }

}


/*----------------------------------------------------------------------------*/
SpaceQuota::~SpaceQuota() {}


/*----------------------------------------------------------------------------*/
void
SpaceQuota::RemoveQuotaNode(XrdOucString &msg, int &retc) 

{
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  eos::ContainerMD *quotadir=0;
  try {
    quotadir = gOFS->eosView->getContainer( SpaceName.c_str() );
    gOFS->eosView->removeQuotaNode( quotadir ) ;
    retc = 0;
    msg = "success: removed quota node "; msg += SpaceName.c_str();
  } catch( eos::MDException &e ) {
    quotadir = 0;
    retc = e.getErrno();
    msg = e.getMessage().str().c_str();
  }  
}



/*----------------------------------------------------------------------------*/
void SpaceQuota::UpdateLogicalSizeFactor()
{
  // ------------------------------------------------------------------------------------------------------
  // ! this routine calculates the default factor for a quota node to calculate the logical available bytes
  // ------------------------------------------------------------------------------------------------------

  if (!SpaceName.beginswith("/"))
    return;

  XrdOucErrInfo error;
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Root(vid);
  vid.sudoer=1;
  eos::ContainerMD::XAttrMap map;

  int retc = gOFS->_attr_ls(SpaceName.c_str(),
                            error, 
                            vid, 
                            0, 
                            map);

  if (!retc) {
    unsigned long layoutId;
    XrdOucEnv env;
    unsigned long forcedfsid;
    XrdOucString spn=SpaceName;
    // get the layout in this quota node
    Policy::GetLayoutAndSpace(SpaceName.c_str(), map, vid, layoutId, spn, env, forcedfsid);
    LayoutSizeFactor = eos::common::LayoutId::GetSizeFactor(layoutId);
  } else {
    LayoutSizeFactor = 1.0;
  }
  // just a /0 protection 
  if (LayoutSizeFactor <1.0)
    LayoutSizeFactor = 1.0;
}

/*----------------------------------------------------------------------------*/
bool
SpaceQuota::RmQuota(unsigned long tag, unsigned long id, bool lock) 
{
  bool removed=false;
  if (lock) Mutex.Lock();
  if (Quota.count(Index(tag,id)))
    removed=true;
  Quota.erase(Index(tag,id));
  eos_static_debug("rm quota tag=%lu id=%lu", tag, id);
  if (lock) Mutex.UnLock();
  return removed; 
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
  ResetQuota(kAllUserLogicalBytesTarget,0, false);
  ResetQuota(kAllGroupLogicalBytesTarget,0, false);

  std::map<long long, unsigned long long>::const_iterator it;

  for (it=Begin(); it != End(); it++) {
    if ( (UnIndex(it->first) == kUserBytesTarget)) {
      AddQuota(kAllUserBytesTarget,0, it->second, false);
      AddQuota(kAllUserLogicalBytesTarget,0, it->second/LayoutSizeFactor, false);
    }

    if ( (UnIndex(it->first) == kUserFilesTarget)) 
      AddQuota(kAllUserFilesTarget,0, it->second, false);

    if ( (UnIndex(it->first) == kGroupBytesTarget)) {
      AddQuota(kAllGroupBytesTarget,0, it->second, false);
      AddQuota(kAllGroupLogicalBytesTarget,0, it->second/LayoutSizeFactor, false);
    }

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
  ResetQuota(kAllUserLogicalBytesIs,0, false);
  ResetQuota(kAllUserFilesIs,0, false);
  ResetQuota(kAllGroupBytesIs,0, false);
  ResetQuota(kAllGroupFilesIs,0, false);
  ResetQuota(kAllGroupLogicalBytesIs,0, false);

  std::map<long long, unsigned long long>::const_iterator it;

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
SpaceQuota::UpdateFromQuotaNode(uid_t uid, gid_t gid) 
{
  Mutex.Lock();
  eos_static_debug("updating uid/gid values from quota node");
  if (QuotaNode) {
    SetQuota(kAllUserBytesIs        ,uid,QuotaNode->getPhysicalSpaceByUser(uid),false);
    SetQuota(kAllUserLogicalBytesIs ,uid,QuotaNode->getUsedSpaceByUser(uid),false);
    SetQuota(kAllUserFilesIs        ,uid,QuotaNode->getNumFilesByUser(uid),false);
    SetQuota(kAllGroupBytesIs       ,gid,QuotaNode->getPhysicalSpaceByGroup(gid),false);
    SetQuota(kAllGroupLogicalBytesIs,gid,QuotaNode->getUsedSpaceByUser(gid),false);
    SetQuota(kAllGroupFilesIs       ,gid,QuotaNode->getNumFilesByGroup(gid),false);
  }
  Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void
SpaceQuota::PrintOut(XrdOucString &output, long uid_sel, long gid_sel, bool monitoring, bool translateids)
{
  char headerline[4096];
  eos_static_debug("called");

  std::map<long long, unsigned long long>::const_iterator it;

  UpdateLogicalSizeFactor();
  UpdateIsSums();
  UpdateTargetSums();

  Quota::NodeToSpaceQuota(SpaceName.c_str(), true);

  int* sortuidarray = (int*) malloc ( sizeof(int) * (Quota.size()+1));
  int* sortgidarray = (int*) malloc ( sizeof(int) * (Quota.size()+1));

  int userentries=0;
  int groupentries=0;

  // make a map containing once all the defined uid's+gid's
  std::map<unsigned long, unsigned long > sortuidhash;
  std::map<unsigned long, unsigned long > sortgidhash;

  std::map<unsigned long, unsigned long >::const_iterator sortit;

  if (!SpaceName.beginswith("/")) {
    free(sortuidarray);
    free(sortgidarray);
    // we don't show them right now ... maybe if we put quota on physical spaces we will
    return ;

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
      output+="# _______________________________________________________________________________________________\n";
      sprintf(headerline,"# ==> Quota Node: %-16s\n", SpaceName.c_str());
      output+= headerline;
      output+="# _______________________________________________________________________________________________\n";
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
    
    std::vector <std::string> uidout;
    std::vector <std::string> gidout;
    
    if (userentries) {
      // user loop
      if (!monitoring) {
        sprintf(headerline,"%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kUserBytesIs), GetTagName(kUserBytesIs), GetTagName(kUserLogicalBytesIs), GetTagName(kUserFilesIs),GetTagName(kUserBytesTarget), GetTagName(kUserLogicalBytesTarget), GetTagName(kUserFilesTarget), "filled[%]", "vol-status","ino-status");
        output+= headerline;
      }
    }
    
    for (int lid = 0; lid < userentries; lid++) {
      eos_static_debug("loop with id=%d",lid);
      XrdOucString value1="";
      XrdOucString value2="";
      XrdOucString value3="";
      XrdOucString value4="";
      XrdOucString value5="";
      XrdOucString value6="";

      XrdOucString id=""; id += sortuidarray[lid];

      if (translateids) {
        // try to translate with password database
        char buffer[16384];
        int buflen = sizeof(buffer);
        struct passwd pwbuf;
        struct passwd *pwbufp=0;
        memset(&pwbuf, 0, sizeof(pwbuf)); 
        if (!getpwuid_r(sortuidarray[lid], &pwbuf, buffer, buflen, &pwbufp)) {
          char uidlimit[16];
          if (pwbuf.pw_name) {
            snprintf(uidlimit,11,"%s",pwbuf.pw_name);
            id = uidlimit;
          }
        }
      }

      XrdOucString percentage="";
      if (!monitoring) {
        sprintf(headerline,"%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str() ,
                eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kUserBytesIs,sortuidarray[lid]),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value2, GetQuota(kUserLogicalBytesIs,sortuidarray[lid]),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value3, GetQuota(kUserFilesIs,sortuidarray[lid]),"-"), 
                eos::common::StringConversion::GetReadableSizeString(value4, GetQuota(kUserBytesTarget,sortuidarray[lid]),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value5, (long long)(GetQuota(kUserBytesTarget,sortuidarray[lid])/LayoutSizeFactor),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value6, GetQuota(kUserFilesTarget,sortuidarray[lid]),"-"),
                GetQuotaPercentage(GetQuota(kUserBytesIs,sortuidarray[lid]), GetQuota(kUserBytesTarget,sortuidarray[lid]), percentage),
                GetQuotaStatus(GetQuota(kUserBytesIs,sortuidarray[lid]), GetQuota(kUserBytesTarget,sortuidarray[lid])),
                GetQuotaStatus(GetQuota(kUserFilesIs,sortuidarray[lid]), GetQuota(kUserFilesTarget,sortuidarray[lid])));
      } else {
        sprintf(headerline,"quota=node uid=%s space=%s usedbytes=%llu usedlogicalbytes=%llu usedfiles=%llu maxbytes=%llu maxlogicalbytes=%llu maxfiles=%llu percentageusedbytes=%s statusbytes=%s statusfiles=%s\n", id.c_str() , SpaceName.c_str(), 
                GetQuota(kUserBytesIs,sortuidarray[lid]),
                GetQuota(kUserLogicalBytesIs,sortuidarray[lid]),
                GetQuota(kUserFilesIs,sortuidarray[lid]),
                GetQuota(kUserBytesTarget,sortuidarray[lid]),
                (unsigned long long)(GetQuota(kUserBytesTarget,sortuidarray[lid])/LayoutSizeFactor),
                GetQuota(kUserFilesTarget,sortuidarray[lid]),
                GetQuotaPercentage(GetQuota(kUserBytesIs,sortuidarray[lid]), GetQuota(kUserBytesTarget,sortuidarray[lid]), percentage),
                GetQuotaStatus(GetQuota(kUserBytesIs,sortuidarray[lid]), GetQuota(kUserBytesTarget,sortuidarray[lid])),
                GetQuotaStatus(GetQuota(kUserFilesIs,sortuidarray[lid]), GetQuota(kUserFilesTarget,sortuidarray[lid])));
      }
      
      if (!translateids) {
	output += headerline;
      } else {
	uidout.push_back(headerline);
      }
    }
    
    if (translateids) {
      std::sort(uidout.begin(),uidout.end());
      for (size_t i=0; i<uidout.size(); i++) {
	output += uidout[i].c_str();
      }
    }

    if (groupentries) {
      // group loop
      if (!monitoring) {
        output+="# ...............................................................................................\n";
        sprintf(headerline,"%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kGroupBytesIs), GetTagName(kGroupBytesIs), GetTagName(kGroupLogicalBytesIs), GetTagName(kGroupFilesIs), GetTagName(kGroupBytesTarget), GetTagName(kGroupLogicalBytesTarget), GetTagName(kGroupFilesTarget), "filled[%]", "vol-status","ino-status");
        output+= headerline;
      }
    }
    
    for (int lid = 0; lid < groupentries; lid++) {
      eos_static_debug("loop with id=%d",lid);
      XrdOucString value1="";
      XrdOucString value2="";
      XrdOucString value3="";
      XrdOucString value4="";
      XrdOucString value5="";
      XrdOucString value6="";
      
      XrdOucString id=""; id += sortgidarray[lid];

      if (translateids) {
        // try to translate with password database
        char buffer[16384];
        int buflen = sizeof(buffer);
        struct group grbuf;
        struct group *grbufp=0;
        
        if (!getgrgid_r(sortgidarray[lid], &grbuf, buffer, buflen, &grbufp)) {
          char gidlimit[16];
          snprintf(gidlimit,11,"%s",grbuf.gr_name);
          id = gidlimit;
        }
      }

      XrdOucString percentage="";
      if (!monitoring) {
        sprintf(headerline,"%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str() ,
                eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kGroupBytesIs,sortgidarray[lid]),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value2, GetQuota(kGroupLogicalBytesIs,sortgidarray[lid]),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value3, GetQuota(kGroupFilesIs,sortgidarray[lid]),"-"), 
                eos::common::StringConversion::GetReadableSizeString(value4, GetQuota(kGroupBytesTarget,sortgidarray[lid]),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value5, (long long)(GetQuota(kGroupBytesTarget,sortgidarray[lid])/LayoutSizeFactor),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value6, GetQuota(kGroupFilesTarget,sortgidarray[lid]),"-"),
                GetQuotaPercentage(GetQuota(kGroupBytesIs,sortgidarray[lid]), GetQuota(kGroupBytesTarget,sortgidarray[lid]), percentage),
                GetQuotaStatus(GetQuota(kGroupBytesIs,sortgidarray[lid]), GetQuota(kGroupBytesTarget,sortgidarray[lid])),
                GetQuotaStatus(GetQuota(kGroupFilesIs,sortgidarray[lid]), GetQuota(kGroupFilesTarget,sortgidarray[lid])));
      } else {
        sprintf(headerline,"quota=node gid=%s space=%s usedbytes=%llu usedlogicalbytes=%llu usedfiles=%llu maxbytes=%llu maxlogicalbytes=%llu maxfiles=%llu percentageusedbytes=%s statusbytes=%s statusfiles=%s\n", id.c_str() , SpaceName.c_str(),
                GetQuota(kGroupBytesIs,sortgidarray[lid]),
                GetQuota(kGroupLogicalBytesIs,sortgidarray[lid]),
                GetQuota(kGroupFilesIs,sortgidarray[lid]),
                GetQuota(kGroupBytesTarget,sortgidarray[lid]),
                (unsigned long long)(GetQuota(kGroupBytesTarget,sortgidarray[lid])/LayoutSizeFactor),
                GetQuota(kGroupFilesTarget,sortgidarray[lid]),
                GetQuotaPercentage(GetQuota(kGroupBytesIs,sortgidarray[lid]), GetQuota(kGroupBytesTarget,sortgidarray[lid]), percentage),
                GetQuotaStatus(GetQuota(kGroupBytesIs,sortgidarray[lid]), GetQuota(kGroupBytesTarget,sortgidarray[lid])),
                GetQuotaStatus(GetQuota(kGroupFilesIs,sortgidarray[lid]), GetQuota(kGroupFilesTarget,sortgidarray[lid])));
      }
      if (!translateids) {
	output += headerline;
      } else {
	gidout.push_back(headerline);
      }
    }
    
    if (translateids) {
      std::sort(gidout.begin(),gidout.end());
      for (size_t i=0; i<gidout.size(); i++) {
	output += gidout[i].c_str();
      }
    }

    if ( (uid_sel <0) && (gid_sel <0)) {
      if (!monitoring) {
        output+="# ----------------------------------------------------------------------------------------------------------\n";
        output+="# ==> Summary\n";
      }

      XrdOucString value1="";
      XrdOucString value2="";
      XrdOucString value3="";
      XrdOucString value4="";
      XrdOucString value5="";
      XrdOucString value6="";

      XrdOucString id="ALL";
      XrdOucString percentage="";
      
      if (!monitoring) {
        sprintf(headerline,"%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kAllUserBytesIs), GetTagName(kAllUserBytesIs), GetTagName(kAllUserLogicalBytesIs), GetTagName(kAllUserFilesIs), GetTagName(kAllUserBytesTarget), GetTagName(kAllUserLogicalBytesTarget), GetTagName(kAllUserFilesTarget), "filled[%]", "vol-status", "ino-status");
        output += headerline;
        sprintf(headerline,"%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str() ,
                eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kAllUserBytesIs,0),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value2, GetQuota(kAllUserLogicalBytesIs,0),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value3, GetQuota(kAllUserFilesIs,0),"-"), 
                eos::common::StringConversion::GetReadableSizeString(value4, GetQuota(kAllUserBytesTarget,0),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value5, GetQuota(kAllUserLogicalBytesTarget,0),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value6, GetQuota(kAllUserFilesTarget,0),"-"),
                GetQuotaPercentage(GetQuota(kAllUserBytesIs,0), GetQuota(kAllUserBytesTarget,0), percentage),
                GetQuotaStatus(GetQuota(kAllUserBytesIs,0), GetQuota(kAllUserBytesTarget,0)),
                GetQuotaStatus(GetQuota(kAllUserFilesIs,0), GetQuota(kAllUserFilesTarget,0)));
      } else {
        sprintf(headerline,"quota=node uid=%s space=%s usedbytes=%llu usedlogicalbytes=%llu usedfiles=%llu maxbytes=%llu maxlogicalbytes=%llu maxfiles=%llu percentageusedbytes=%s statusbytes=%s statusfiles=%s\n", id.c_str() , SpaceName.c_str(), 
                GetQuota(kAllUserBytesIs,0),
                GetQuota(kAllUserLogicalBytesIs,0),
                GetQuota(kAllUserFilesIs,0),
                GetQuota(kAllUserBytesTarget,0),
                GetQuota(kAllUserLogicalBytesTarget,0),
                GetQuota(kAllUserFilesTarget,0),
                GetQuotaPercentage(GetQuota(kAllUserBytesIs,0), GetQuota(kAllUserBytesTarget,0), percentage),
                GetQuotaStatus(GetQuota(kAllUserBytesIs,0), GetQuota(kAllUserBytesTarget,0)),
                GetQuotaStatus(GetQuota(kAllUserFilesIs,0), GetQuota(kAllUserFilesTarget,0)));
      }
      output += headerline;

      if (!monitoring) {
        sprintf(headerline,"%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", GetTagCategory(kAllGroupBytesIs), GetTagName(kAllGroupBytesIs), GetTagName(kAllGroupLogicalBytesIs), GetTagName(kAllGroupFilesIs), GetTagName(kAllGroupBytesTarget), GetTagName(kAllGroupLogicalBytesTarget), GetTagName(kAllGroupFilesTarget), "filled[%]", "vol-status","ino-status");
        output += headerline;
        sprintf(headerline,"%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", id.c_str() , 
                eos::common::StringConversion::GetReadableSizeString(value1, GetQuota(kAllGroupBytesIs,0),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value2, GetQuota(kAllGroupLogicalBytesIs,0),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value3, GetQuota(kAllGroupFilesIs,0),"-"), 
                eos::common::StringConversion::GetReadableSizeString(value4, GetQuota(kAllGroupBytesTarget,0),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value5, GetQuota(kAllGroupLogicalBytesTarget,0),"B"), 
                eos::common::StringConversion::GetReadableSizeString(value6, GetQuota(kAllGroupFilesTarget,0),"-"),
                GetQuotaPercentage(GetQuota(kAllGroupBytesIs,0), GetQuota(kAllGroupBytesTarget,0), percentage),
                GetQuotaStatus(GetQuota(kAllGroupBytesIs,0), GetQuota(kAllGroupBytesTarget,0)),
                GetQuotaStatus(GetQuota(kAllGroupFilesIs,0), GetQuota(kAllGroupFilesTarget,0)));
      } else {
        sprintf(headerline,"quota=node gid=%s space=%s usedbytes=%llu usedlogicalbytes=%llu usedfiles=%llu maxbytes=%llu maxlogicalbytes=%llu maxfiles=%llu percentageusedbytes=%s statusbytes=%s statusfiles=%s\n", id.c_str() , SpaceName.c_str(), 
                GetQuota(kAllGroupBytesIs,0),
                GetQuota(kAllGroupLogicalBytesIs,0),
                GetQuota(kAllGroupFilesIs,0),
                GetQuota(kAllGroupBytesTarget,0),
                GetQuota(kAllGroupLogicalBytesTarget,0),
                GetQuota(kAllGroupFilesTarget,0),
                GetQuotaPercentage(GetQuota(kAllGroupBytesIs,0), GetQuota(kAllGroupBytesTarget,0), percentage),
                GetQuotaStatus(GetQuota(kAllGroupBytesIs,0), GetQuota(kAllGroupBytesTarget,0)),
                GetQuotaStatus(GetQuota(kAllGroupFilesIs,0), GetQuota(kAllGroupFilesTarget,0)));
      }
      output += headerline;
    }
  }
  free(sortuidarray);
  free(sortgidarray);
}


bool 
SpaceQuota::CheckWriteQuota(uid_t uid, gid_t gid, long long desiredspace, unsigned int inodes) 
{
  bool hasquota = false;

  // copy info from namespace Quota Node ...
  UpdateFromQuotaNode(uid,gid);
  eos_static_info("uid=%d gid=%d size=%llu quota=%llu", uid, gid, desiredspace, GetQuota(kUserBytesTarget,uid,false));

  bool userquota  = false;
  bool groupquota = false;
  bool hasuserquota = false;
  bool hasgroupquota = false;

  if (GetQuota(kUserBytesTarget,uid,false)>0) {
    userquota = true;
  }
    
  if (GetQuota(kGroupBytesTarget,gid,false)>0) {
    groupquota = true;
  }

  if ( ( ( (GetQuota(kUserBytesTarget,uid,false)) - (GetQuota(kUserBytesIs,uid,false)) ) > (long long)(desiredspace) ) &&
       ( ( (GetQuota(kUserFilesTarget,uid,false)) - (GetQuota(kUserFilesIs,uid,false)) ) > (inodes ) ) ) {
    // the user has quota here!
    hasuserquota = true;
  } 
  
  // -> group quota
  if ( ( ( (GetQuota(kGroupBytesTarget,gid,false)) - (GetQuota(kGroupBytesIs,gid,false)) ) > (long long) (desiredspace) ) &&
       ( ( (GetQuota(kGroupFilesTarget,gid,false)) - (GetQuota(kGroupFilesIs,gid,false)) ) > (inodes)  ) ) {
    // the group has quota here
    hasgroupquota = true;

  } 

  if ( (userquota) && (groupquota) ) {
    // both are defined, we need to have both
    hasquota = hasuserquota & hasgroupquota;
  } else {
    hasquota = hasuserquota || hasgroupquota;
  }
  
  if (uid==0) {
    // root does not need any quota
    hasquota = true;
  }
  
  return hasquota;
}

/*----------------------------------------------------------------------------*/
int 
SpaceQuota::FilePlacement(const char* path, uid_t uid, gid_t gid, const char* grouptag, unsigned long lid, std::vector<unsigned int> &selectedfs, bool truncate, int forcedindex, unsigned long long bookingsize)
{
  // the caller routing has to lock via => eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex) !!!
  std::set<eos::common::FileSystem::fsid_t> fsidavoidlist;
  std::map<eos::common::FileSystem::fsid_t, float> availablefs;
  std::list<eos::common::FileSystem::fsid_t> availablevector;

  // fill the avoid list from the selectedfs input vector
  for (unsigned int i=0; i< selectedfs.size(); i++) {
    fsidavoidlist.insert(selectedfs[i]);
  }

  unsigned int nfilesystems = eos::common::LayoutId::GetStripeNumber(lid) + 1; // 0 = 1 replica !
  unsigned int nassigned = 0;
  bool hasquota = false;

  // first figure out how many filesystems we need
  eos_static_debug("uid=%u gid=%u grouptag=%s place filesystems=%u",uid,gid,grouptag, nfilesystems);
  
  XrdOucString lindextag = "";
  if (grouptag) {
    lindextag = grouptag;
  } else {
    lindextag += (int)uid; 
    lindextag += ":";
    lindextag += (int)gid;
  }
  
  std::string indextag = lindextag.c_str();

  // check if the uid/gid has enough quota configured to place in this space !

  if (Enabled()) {
    // we have physical spacequota and namespace spacequota
    SpaceQuota* namespacequota = Quota::GetResponsibleSpaceQuota(path);
    if (namespacequota) {
      hasquota = namespacequota->CheckWriteQuota(uid, gid,1ll * nfilesystems * bookingsize, nfilesystems);
      if (!hasquota) {
        eos_static_debug("uid=%u gid=%u grouptag=%s place filesystems=%u has no quota left!",uid,gid,grouptag, nfilesystems);
        return EDQUOT;
      }
    } else {
      eos_static_err("no namespace quota found for path=%s", path);
      return EDQUOT;
    }
  } 
  
  std::string spacename = SpaceName.c_str();
  
  std::set<FsGroup*>::const_iterator git;

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
    if ( ( git != FsView::gFsView.mSpaceGroupView[spacename].end()) && ((*git)->GetIndex() != (unsigned int)forcedindex)) {
      selectedfs.clear();
      return ENOSPC;
    }
  } else {
    schedulingMutex.Lock();
    if (schedulingGroup.count(indextag)) {
      git = FsView::gFsView.mSpaceGroupView[spacename].find(schedulingGroup[indextag]);
    } else {
      git = FsView::gFsView.mSpaceGroupView[spacename].begin();
      schedulingGroup[indextag] = *git;
    }
    schedulingMutex.UnLock();
  }
  
  // we can loop over all existing scheduling views
  for (unsigned int groupindex=0; groupindex < FsView::gFsView.mSpaceGroupView[spacename].size(); groupindex++) {
    eos_static_debug("scheduling group loop %d", forcedindex);
    selectedfs.clear();
    availablefs.clear();

    std::set<eos::common::FileSystem::fsid_t>::const_iterator fsit;
    eos::common::FileSystem::fsid_t fsid=0;

    // create the string map key for this group/index pair
    XrdOucString fsindextag="";
    fsindextag += (int)(*git)->GetIndex();
    // place the filesystem iterator
    fsindextag += "|";
    fsindextag += indextag.c_str();
    std::string sfsindextag = fsindextag.c_str();
    
    schedulingMutex.Lock();
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
    schedulingMutex.UnLock();

    eos_static_debug("Enter %s points to %d", sfsindextag.c_str(), *fsit);

    // remember the one we started with ...

    // we loop over some filesystems in that group
    for (unsigned int fsindex=0; fsindex < (*git)->size(); fsindex++) {
      eos_static_debug("checking scheduling group %d filesystem %d", (*git)->GetIndex(), *fsit);        

      // take filesystem snapshot
      eos::common::FileSystem::fs_snapshot_t snapshot;
      // we are already in a locked section
      FileSystem* fs = 0;
      if (FsView::gFsView.mIdView.count(fsid))
        fs = FsView::gFsView.mIdView[fsid];
      else
        continue;

      fs->SnapShotFileSystem(snapshot,false);
      
      // the weight is given mainly by the disk performance and the network load has a weaker impact (sqrt)
      double weight    = (1.0 - snapshot.mDiskUtilization);
      double netweight = (1.0- ((snapshot.mNetEthRateMiB)?(snapshot.mNetInRateMiB/snapshot.mNetEthRateMiB):0.0));
      weight          *= ((netweight>0)?sqrt(netweight):0);
      if (weight < 0.1) {
	weight = 0.1;
      }
	
      // check if this filesystem can be used (online, enough space etc...)
      if ( (snapshot.mStatus       == eos::common::FileSystem::kBooted) && 
           (snapshot.mConfigStatus == eos::common::FileSystem::kRW) && 
           (snapshot.mErrCode      == 0 ) && // this we probably don't need 
           (fs->GetActiveStatus(snapshot)) && // this checks the heartbeat and the group & node are enabled
           (fs->ReserveSpace(snapshot,bookingsize)) ) {
        
        if (!fsidavoidlist.count(fsid)) {
          availablefs[fsid] = weight;
          availablevector.push_back(fsid);
        }
      } else {
        //      eos_static_err("%d %d %d\n", (snapshot.mStatus), (snapshot.mConfigStatus), (snapshot.mErrCode      == 0 ));
      }
      fsit++;

      // create cycling
      if (fsit == (*git)->end()) {
        fsit = (*git)->begin();
      }

      if (fsindex==0) {
        // we move the iterator only by one position
        schedulingMutex.Lock();
        schedulingFileSystem[sfsindextag] = *fsit;
        eos_static_debug("Exit %s points to %d", sfsindextag.c_str(), *fsit);
        schedulingMutex.UnLock();
      }


      fsid = *fsit;
      // evt. this has to be commented
      if ( (availablefs.size()>= nfilesystems) && (availablefs.size() > ((*git)->size()/2)) ) {
        // we stop if we have found enough ... atleast half of the scheduling group
        break;
      }
    }
    
    // check if there are atlast <nfilesystems> in the available map
    if (availablefs.size() >= nfilesystems) {
      std::list<eos::common::FileSystem::fsid_t>::iterator ait;
      ait = availablevector.begin();

      for (unsigned int loop = 0; loop < 1000; loop++) {
        // we cycle over the available filesystems
        float randomacceptor = (0.999999 * random()/RAND_MAX);
        eos_static_debug("fs %u acceptor %f/%f for %d. replica [loop=%d] [avail=%d]", *ait, randomacceptor, availablefs[*ait], nassigned+1, loop, availablevector.size());
        
        if ( (nassigned==0) ) {
          if (availablefs[*ait]<randomacceptor) {
            ait++;
            if (ait == availablevector.end()) 
              ait = availablevector.begin();
            continue;
          } else {
            // push it on the selection list
            selectedfs.push_back(*ait);
            eos_static_debug("fs %u selected for %d. replica", *ait, nassigned+1);
            
            // remove it from the selection map
            availablefs.erase(*ait);
            ait = availablevector.erase(ait);
            if (ait == availablevector.end()) 
              ait = availablevector.begin();
            
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
            if (ait == availablevector.end())
              ait = availablevector.begin();
          }
          
          if (availablefs[*ait]>randomacceptor) {
            // push it on the selection list
            selectedfs.push_back(*ait);
            eos_static_debug("fs %u selected for %d. replica", *ait, nassigned+1);
            
            // remove it from the selection map
            availablefs.erase(*ait);
            ait = availablevector.erase(ait);
            nassigned++;
            if (ait == availablevector.end()) 
              ait = availablevector.begin();
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
    schedulingMutex.Lock();
    schedulingGroup[indextag] = *git;
    schedulingMutex.UnLock();

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
int SpaceQuota::FileAccess(uid_t uid, gid_t gid, unsigned long forcedfsid, const char* forcedspace, unsigned long lid, std::vector<unsigned int> &locationsfs, unsigned long &fsindex, bool isRW, unsigned long long bookingsize, std::vector<unsigned int> &unavailfs, eos::common::FileSystem::fsstatus_t min_fsstatus)
{
  // the caller routing has to lock via => eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex) !!!
  
  int returnCode = 0;

  // --------------------------------------------------------------------------------
  // ! PLAIN Layout Scheduler
  // --------------------------------------------------------------------------------

  if (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kPlain) {
    // we have one or more replica's ... find the best place to schedule this IO
    if (locationsfs.size() && locationsfs[0]) {
      eos::common::FileSystem* filesystem = 0;
      if (FsView::gFsView.mIdView.count(locationsfs[0])){
        filesystem = FsView::gFsView.mIdView[locationsfs[0]];
      }

      std::set<eos::common::FileSystem::fsid_t> availablefs;

      if (!filesystem)
        return ENODATA;

      // take filesystem snapshot
      eos::common::FileSystem::fs_snapshot_t snapshot;
      // we are already in a locked section

      FileSystem* fs = FsView::gFsView.mIdView[locationsfs[0]];

      fs->SnapShotFileSystem(snapshot,false);

      if (isRW) {
        if ( (snapshot.mStatus       == eos::common::FileSystem::kBooted) && 
             (snapshot.mConfigStatus >= eos::common::FileSystem::kWO) && 
             (snapshot.mErrCode      == 0 ) && // this we probably don't need 
             (fs->GetActiveStatus(snapshot)) && // this checks the heartbeat and the group & node are enabled
             (fs->ReserveSpace(snapshot,bookingsize)) ) { 
          // perfect!
          fsindex = 0;
          eos_static_debug("selected plain file access via filesystem %u",locationsfs[0]);
          return returnCode;
        } else {
          // check if we are in any kind of no-update mode
          if ( (snapshot.mConfigStatus == eos::common::FileSystem::kRO) || 
               (snapshot.mConfigStatus == eos::common::FileSystem::kWO) ) {
            return EROFS;
          }

          // we are off the wire
          return ENONET;
        }
      } else {
        if ( (snapshot.mStatus       == eos::common::FileSystem::kBooted) && 
             (snapshot.mConfigStatus >= min_fsstatus) && 
             (snapshot.mErrCode      == 0 ) && // this we probably don't need 
             (fs->GetActiveStatus(snapshot)) ) {
          
          // perfect!
          fsindex = 0;
          return returnCode;
        } else {
          return ENONET;
        }
      }
    } else {
      return ENODATA;
    }
  }

  // --------------------------------------------------------------------------------
  // ! REPLICA/RAID Layout Scheduler
  // --------------------------------------------------------------------------------

  if ((eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReplica) || 
      (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kRaidDP) ||
      (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReedS) ) {
    std::set<eos::common::FileSystem::fsid_t> availablefs;
    std::multimap<double, eos::common::FileSystem::fsid_t> availablefsweightsort;

    double renorm = 0; // this is the sum of all weights, we renormalize each weight in the selection with this sum

    // -----------------------------------------------------------------------
    // check all the locations - for write we need all - for read atleast one
    // -----------------------------------------------------------------------
    for (size_t i=0; i< locationsfs.size();i++) {
      FileSystem* filesystem = 0;

      if (FsView::gFsView.mIdView.count(locationsfs[i])){
        filesystem = FsView::gFsView.mIdView[locationsfs[i]];
      }
      if (!filesystem) {
        if (isRW)
          return ENONET;
        else
          continue;
      }

      // take filesystem snapshot
      eos::common::FileSystem::fs_snapshot_t snapshot;
      // we are already in a locked section

      FileSystem* fs = FsView::gFsView.mIdView[locationsfs[i]];
      fs->SnapShotFileSystem(snapshot,false);

      if (isRW) {
        if ( (snapshot.mStatus       == eos::common::FileSystem::kBooted) && 
             (snapshot.mConfigStatus >= eos::common::FileSystem::kWO) && 
             (snapshot.mErrCode      == 0 ) && // this we probably don't need 
             (fs->GetActiveStatus(snapshot)) && // this checks the heartbeat and the group & node are enabled
             (fs->ReserveSpace(snapshot,bookingsize)) ) { 
          // perfect!
          availablefs.insert(snapshot.mId);
	  
          // the weight is given mainly by the disk performance and the network load has a weaker impact (sqrt)
          double weight    = (1.0 - snapshot.mDiskUtilization);
          double netweight = (1.0- ((snapshot.mNetEthRateMiB)?(snapshot.mNetInRateMiB/snapshot.mNetEthRateMiB):0.0));
          weight          *= ((netweight>0)?sqrt(netweight):0);

          availablefsweightsort.insert(std::pair<double,eos::common::FileSystem::fsid_t> (weight, snapshot.mId));
          renorm += weight;
        } else {
          // check if we are in any kind of no-update mode
          if ( (snapshot.mConfigStatus == eos::common::FileSystem::kRO) || 
               (snapshot.mConfigStatus == eos::common::FileSystem::kWO) ) {
            return EROFS;
          }
          
          // we are off the wire
          return ENONET;
        }
      } else {
        if ( (snapshot.mStatus       == eos::common::FileSystem::kBooted) && 
             (snapshot.mConfigStatus >= min_fsstatus) && 
             (snapshot.mErrCode      == 0 ) && // this we probably don't need 
             (snapshot.mActiveStatus) ) {
          availablefs.insert(snapshot.mId);
          
          // the weight is given mainly by the disk performance and the network load has a weaker impact (sqrt)
          double weight    = (1.0 - snapshot.mDiskUtilization);
          double netweight = (1.0- ((snapshot.mNetEthRateMiB)?(snapshot.mNetOutRateMiB/snapshot.mNetEthRateMiB):0.0));
          weight          *= ((netweight>0)?sqrt(netweight):0);
          // drain patch
          if (snapshot.mConfigStatus == eos::common::FileSystem::kDrain) {
            // we set a low weight for drain filesystems if there is more than one replica
            if (locationsfs.size() == 1) {
              weight = 1.0;
            } else {
	      // this is a protection to get atleast something selected even if the weights are small
              if (weight > 0.1)
                weight =0.1;
            }
          }
          availablefsweightsort.insert(std::pair<double,eos::common::FileSystem::fsid_t> (weight, snapshot.mId));          
          renorm += weight;
          eos_static_debug("weight = %f netweight = %f renorm = %f %d=>%f\n", weight, netweight, renorm, snapshot.mId, snapshot.mDiskUtilization);
        } else {
	  // -----------------------------------------------------------------------
	  // we store not available filesystems in the unavail vector 
	  // - this matters for RAID layouts because we have to remove there URLs to let the RAID driver use only online stripes
	  // -----------------------------------------------------------------------
	  unavailfs.push_back(snapshot.mId);
	}
      }
    }
    
    eos_static_debug("Requesting %d/%d replicas to be online\n", availablefs.size(),eos::common::LayoutId::GetMinOnlineReplica(lid));
    // -----------------------------------------------------------------------
    // check if there are enough stripes available for a read operation of the given layout
    // -----------------------------------------------------------------------
    if (availablefs.size() <  eos::common::LayoutId::GetMinOnlineReplica(lid)) {
      return ENONET;
    }

    if ( (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kRaidDP) ||
	 (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReedS) ) {
      if (availablefs.size() != eos::common::LayoutId::GetOnlineStripeNumber(lid)) {
	// -----------------------------------------------------------------------
	// check if there are enough stripes available for the layout type chosen
	// if not we set EXDEV as return code indicating that the caller needs
	// to place new stripes for each unavailfs entry
	// -----------------------------------------------------------------------
	returnCode = EXDEV;
      }
    }

    // -----------------------------------------------------------------------
    // for write we can just return if they are all available, otherwise we would never go here ....
    // -----------------------------------------------------------------------
    if (isRW) {
      fsindex = 0;
      return returnCode;
    }
    
    // -----------------------------------------------------------------------
    // if there was a forced one, see if it is there
    // -----------------------------------------------------------------------
    if (forcedfsid>0) {
      if (availablefs.count(forcedfsid)==1) {
        for (size_t i=0; i< locationsfs.size();i++) {
          if (locationsfs[i] == forcedfsid) {
            fsindex = i;
            return returnCode;
          }
        }
        // uuh! - this should NEVER happen!
        eos_static_crit("fatal inconsistency in scheduling - file system missing after selection of forced fsid");
        return EIO;
      }
      return ENONET;
    }

    if (!renorm) {
      renorm = 1.0;
    }
    
    // -----------------------------------------------------------------------
    // if there was none available, return
    // -----------------------------------------------------------------------
    if (!availablefs.size()) {
      return ENONET;
    }

    // -----------------------------------------------------------------------
    // if there was only one available, use that one
    // -----------------------------------------------------------------------
    if (availablefs.size()==1) {
      for (size_t i=0; i< locationsfs.size();i++) {
        if (locationsfs[i] == *(availablefs.begin())) {
          fsindex = i;
          return returnCode;
        }
      }
      // uuh! - this should NEVER happen!
      eos_static_crit("fatal inconsistency in scheduling - file system missing after selection of single replica");
      return EIO;
    }
    
    // -----------------------------------------------------------------------
    // now start with the one with the highest weight, but still use probabilty to select it
    // -----------------------------------------------------------------------
    std::multimap<double, eos::common::FileSystem::fsid_t>::reverse_iterator wit;
    for (wit = availablefsweightsort.rbegin(); wit != availablefsweightsort.rend(); wit++) {
      float randomacceptor = (0.999999 * random()/RAND_MAX);
      eos_static_debug("random acceptor=%.02f norm=%.02f weight=%.02f normweight=%.02f fsid=%u", randomacceptor, renorm, wit->first, wit->first/renorm, wit->second);
      
      if ( (wit->first/renorm)> randomacceptor) {
        // take this
        for (size_t i=0; i< locationsfs.size();i++) {
          if (locationsfs[i] == wit->second) {
            fsindex = i;
            return returnCode;
          }
        }
        // uuh! - this should NEVER happen!
        eos_static_crit("fatal inconsistency in scheduling - file system missing after selection in randomacceptor");
        return EIO;
      }
    }
    // -----------------------------------------------------------------------
    // if we don't succeed by the randomized weight, we return the one with the highest weight
    // -----------------------------------------------------------------------
    
    for (size_t i=0; i< locationsfs.size();i++) {
      if (locationsfs[i] == availablefsweightsort.begin()->second) {
        fsindex = i;
        return returnCode;
      }
    }
    // uuh! - this should NEVER happen!
    eos_static_crit("fatal inconsistency in scheduling - file system missing after selection");
    return EIO;
  }

  return EINVAL;
}


/*----------------------------------------------------------------------------*/
SpaceQuota* 
Quota::GetSpaceQuota(const char* name, bool nocreate) 
{
  // the caller has to Readlock gQuotaMutex
  SpaceQuota* spacequota=0;
  std::string sname = name;

  if (sname[0] == '/') {
    // allow sloppy guys to skip the trailing '/'
    if (sname[sname.length()-1] != '/') {
      sname += "/";
    }
  }
  
  if ( (gQuota.count(sname)) && (spacequota = gQuota[sname]) ) {
    
  } else {
    if (nocreate) {
      return NULL;
    }
    do {
      // this is a dangerous way if any other mutex was used from the caller after gQuotaMutex.UnLockRead() => take care not do to that!
      gQuotaMutex.UnLockRead();
      gQuotaMutex.LockWrite();    
      spacequota = new SpaceQuota(sname.c_str());
      gQuota[sname]=spacequota;
      gQuotaMutex.UnLockWrite();    
      gQuotaMutex.LockRead();
    } while ((!gQuota.count(sname) && (! (spacequota = gQuota[sname]))));
  }
  
  return spacequota;
}

/*----------------------------------------------------------------------------*/
SpaceQuota* 
Quota::GetResponsibleSpaceQuota(const char* path)
{
  // the caller has to Readlock gQuotaMutex
  SpaceQuota* spacequota=0;
  XrdOucString matchpath = path;
  std::map<std::string, SpaceQuota*>::const_iterator it;
  for (it=gQuota.begin(); it!=gQuota.end();it++) {
    if (matchpath.beginswith(it->second->GetSpaceName())) {
      if ( (!spacequota) || ( (strlen(it->second->GetSpaceName()) > strlen(spacequota->GetSpaceName())) ) ) {
        spacequota = it->second;
      }
    }
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
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock lock(gQuotaMutex);
  output="";
  XrdOucString spacenames="";
  if (space ==0) {
    // make sure all configured spaces exist In the quota views
    std::map<std::string, FsSpace*>::const_iterator sit;

    for (sit = FsView::gFsView.mSpaceView.begin(); sit != FsView::gFsView.mSpaceView.end(); sit++) {
      Quota::GetSpaceQuota(sit->second->GetMember("name").c_str());
    }

    std::map<std::string, SpaceQuota*>::const_iterator it;
    
    for (it = gQuota.begin(); it != gQuota.end(); it++) {
      it->second->PrintOut(output, uid_sel, gid_sel, monitoring, translateids);
    }
  } else {
    std::string sspace=space;
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

  if (!space.endswith("/")) {
    space += "/";
  }

  if (!space.length()) {
    spacequota = GetSpaceQuota("/eos/",false);
    configstringheader += "/eos/";
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
      if (bytes==0) {
      } else {
        gOFS->ConfEngine->DeleteConfigValue("quota", configstring.c_str());
        gOFS->ConfEngine->SetConfigValue("quota", configstring.c_str(), configvalue);
      }
     
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
      if (files == 0) {
        gOFS->ConfEngine->DeleteConfigValue("quota", configstring.c_str());
      } else {
        gOFS->ConfEngine->SetConfigValue("quota", configstring.c_str(), configvalue);
      }
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
      if (bytes == 0) {
        gOFS->ConfEngine->DeleteConfigValue("quota", configstring.c_str());
      } else {
        gOFS->ConfEngine->SetConfigValue("quota", configstring.c_str(), configvalue);
      }
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
      if (files == 0) {
        gOFS->ConfEngine->DeleteConfigValue("quota", configstring.c_str());
      } else {
        gOFS->ConfEngine->SetConfigValue("quota", configstring.c_str(), configvalue);
      }
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
  // -> this is not used anymore, the remove is done with SetQuota(files=0,vol=0)
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
      bool removed=false;
      removed |= spacequota->RmQuota(SpaceQuota::kUserBytesTarget, uid_sel);
      removed |= spacequota->RmQuota(SpaceQuota::kUserBytesIs, uid_sel);
      if (removed) {
        sprintf(printline, "success: removed volume quota for uid=%ld\n", uid_sel);
        msg+= printline;
        retc = 0;
      } else {
        sprintf(printline, "error: no volume quota removed for uid=%ld\n", uid_sel);
        msg+= printline;
        retc = ENOENT;
      }
    } 

    if ( (uid_sel >=0) ) {
      bool removed=false;
      removed |= spacequota->RmQuota(SpaceQuota::kUserFilesTarget, uid_sel);
      removed |= spacequota->RmQuota(SpaceQuota::kUserFilesIs, uid_sel);
      if (removed) {
        sprintf(printline, "success: removed inode quota for uid=%ld\n", uid_sel);
        msg+= printline;
        retc = 0;
      } else {
        sprintf(printline, "error: no inode quota removed for uid=%ld\n", uid_sel);
        msg+= printline;
        retc = ENOENT;
      }
    }

    if ( (gid_sel >=0) ) {
      bool removed=false;
      removed |= spacequota->RmQuota(SpaceQuota::kGroupBytesTarget, gid_sel);
      removed |= spacequota->RmQuota(SpaceQuota::kGroupBytesIs, uid_sel);
      if (removed) {
        sprintf(printline, "success: removed volume quota for gid=%ld\n", gid_sel);
        msg+= printline;
        retc = 0;
      } else {
        sprintf(printline, "error: not volume quota removed for gid=%ld\n", gid_sel);
        msg+= printline;
        retc = ENOENT;
      }
    }

    if ( (gid_sel >=0) ) {
      bool removed=false;
      removed |= spacequota->RmQuota(SpaceQuota::kGroupFilesTarget, gid_sel);
      removed |= spacequota->RmQuota(SpaceQuota::kGroupFilesIs, uid_sel);
      if (removed) {
        sprintf(printline, "success: removed inode quota for gid=%ld\n", gid_sel);
        msg+= printline;
        retc = 0;
      } else {
        sprintf(printline, "error: not inode quota removed for gid=%ld\n", gid_sel);
        msg+= printline;
        retc = ENOENT;
      }
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
Quota::RmSpaceQuota(XrdOucString space, XrdOucString &msg, int &retc) 
{
  eos_static_debug("space=%s",space.c_str());

  eos::common::RWMutexWriteLock lock(gQuotaMutex);
  
  SpaceQuota* spacequota = 0;
  
  spacequota = GetSpaceQuota(space.c_str(), true);
  
  if (!spacequota) {
    msg = "error: there is no quota node under this path";
    retc =  EINVAL;
    return false;
  } else {
    msg = "success: removed space quota for "; msg += space.c_str();
    spacequota->RemoveQuotaNode(msg, retc);
    gQuota.erase(space.c_str());
    return true;
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
  // this routine has to be called with eosViewMutex locked
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
  // this routine has to be called with eosViewMutex locked
  eos::common::RWMutexReadLock locker(gQuotaMutex);
  // inserts the current state of the quota nodes into SpaceQuota's 
  eos::QuotaStats::NodeMap::iterator it;
  for (it = gOFS->eosView->getQuotaStats()->nodesBegin(); it != gOFS->eosView->getQuotaStats()->nodesEnd(); it++) {
    eos::ContainerMD::id_t id = it->first;
    eos::ContainerMD* container = gOFS->eosDirectoryService->getContainerMD(id);
    std::string quotapath = gOFS->eosView->getUri( container);
    NodeToSpaceQuota(quotapath.c_str(), false);
  }
}

/*----------------------------------------------------------------------------*/


void 
Quota::NodeToSpaceQuota(const char* name, bool lock)
{
  // this routine has to be called with gQuotaMutex read-locked 
  if (!name)
    return;

  SpaceQuota* spacequota = Quota::GetSpaceQuota(name, false);

  if (lock) 
    gOFS->eosViewRWMutex.LockRead();
  if (spacequota && spacequota->GetQuotaNode()) {
    // insert current state of a single quota node into aSpaceQuota
    eos::QuotaNode::UserMap::const_iterator itu;
    eos::QuotaNode::GroupMap::const_iterator itg;
    // loop over user
    for (itu = spacequota->GetQuotaNode()->userUsageBegin(); itu != spacequota->GetQuotaNode()->userUsageEnd(); itu++) {
      spacequota->SetQuota(SpaceQuota::kUserBytesIs, itu->first, itu->second.physicalSpace);
      spacequota->SetQuota(SpaceQuota::kUserFilesIs, itu->first, itu->second.files);
      spacequota->SetQuota(SpaceQuota::kUserLogicalBytesIs, itu->first, itu->second.space);
    }
    for (itg = spacequota->GetQuotaNode()->groupUsageBegin(); itg != spacequota->GetQuotaNode()->groupUsageEnd(); itg++) {
      spacequota->SetQuota(SpaceQuota::kGroupBytesIs, itg->first, itg->second.physicalSpace);
      spacequota->SetQuota(SpaceQuota::kGroupFilesIs, itg->first, itg->second.files);
      spacequota->SetQuota(SpaceQuota::kGroupLogicalBytesIs, itg->first, itg->second.space);
    }
  }
  if (lock)
    gOFS->eosViewRWMutex.UnLockRead();
}

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_END
