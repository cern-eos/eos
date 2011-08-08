/*----------------------------------------------------------------------------*/
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "common/Mapping.hh"
#include "mgm/Fsck.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/

#include "XrdPosix/XrdPosixXrootd.hh"
/*----------------------------------------------------------------------------*/

#include <iostream>
#include <fstream>
#include <vector>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/* ------------------------------------------------------------------------- */
Fsck::Fsck() 
{
  mRunning = false;
  
  mErrorMapMutex.Lock();
  mTotalErrorMap["totalfiles"]=0;
  mErrorNames.resize(14);
  mErrorHelp["totalfiles"] = "Total number of replicas found";
  mErrorNames[0] = "totalfiles";

  mTotalErrorMap["diff_mgm_disk_size"] = 0;
  mErrorHelp["diff_mgm_disk_size"] = "The size registered in the namespace differs from the size of a replica on disk";
  mErrorNames[1] = "diff_mgm_disk_size";

  mTotalErrorMap["diff_fst_disk_fmd_size"] = 0;
  mErrorHelp["diff_fst_disk_fmd_size"] = "The size of a replica on disk differs from the size stored in the changelog on the FST.";
  mErrorNames[2] = "diff_fst_disk_fmd_size";
  
  mTotalErrorMap["diff_mgm_disk_checksum"] = 0;
  mErrorHelp["diff_mgm_disk_checksum"] = "The checksum registered in the namespace differs from the checksum of a replica on disk";
  mErrorNames[3] = "diff_mgm_disk_checksum";

  mTotalErrorMap["diff_fst_disk_fmd_checksum"] = 0;
  mErrorHelp["diff_fst_disk_fmd_checksum"] = "The checksum in the chenagelog of the FST differes from the checksum stored in the extended attributes on disk";
  mErrorNames[4] = "diff_fst_disk_fmd_checksum";

  mTotalErrorMap["diff_file_checksum_scan"] = 0;
  mErrorHelp["diff_file_checksum_scan"] = "A file checksum error has been detected during the file scan _ the computed checksum differes from the checksum stored in the extended attributes on disk";
  mErrorNames[5] = "diff_file_checksum_scan";

  mTotalErrorMap["diff_block_checksum_scan"] = 0;
  mErrorHelp["diff_block_checksum_scan"] = "A block checksum errors has been detected during the file scan";
  mErrorNames[6] = "diff_block_checksum_scan";

  mTotalErrorMap["scanned_files"] = 0;
  mErrorHelp["scanned_files"] = "Number of files scanned by the checksum scanner";
  mErrorNames[7] = "scanned_files";

  mTotalErrorMap["not_scanned_files"] = 0;
  mErrorHelp["not_scanned_files"] = "Number of files without checksum scan";
  mErrorNames[8] = "not_scanned_files";

  mTotalErrorMap["replica_not_registered"] = 0;
  mErrorHelp["replica_not_registered"] = "Replica not registered";
  mErrorNames[9] = "replica_not_registered";

  mTotalErrorMap["replica_orphaned"] = 0;
  mErrorHelp["replica_orphaned"] = "There is no file name anymore connected to that replica";
  mErrorNames[10] = "replica_orphaned";
 
  mTotalErrorMap["diff_replica_layout"] = 0;
  mErrorHelp["diff_replica_layout"] = "There is a different number of replica's existing than defined by the layout";
  mErrorNames[11] = "diff_replica_layout";

  mTotalErrorMap["replica_offline"] = 0;
  mErrorHelp["replica_offline"] = "Not all replicas are online";
  mErrorNames[12] = "replica_offline";

  mTotalErrorMap["file_offline"] = 0;
  mErrorHelp["file_offline"] = "No replica is accessible";
  mErrorNames[13] = "file_offline";

  mErrorMapMutex.UnLock(); 
} 

/* ------------------------------------------------------------------------- */
bool
Fsck::Start()
{
  if (!mRunning) {
    XrdSysThread::Run(&mThread, Fsck::StaticCheck, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "Report Receiver Thread");
    mRunning = true;
    return true;
  } else {
    return false;
  }
}

/* ------------------------------------------------------------------------- */
bool
Fsck::Stop()
{
  if (mRunning) {
    eos_static_info("cancel fsck thread");
    XrdSysThread::Cancel(mThread);
    XrdSysThread::Join(mThread,NULL);
    eos_static_info("joined fsck thread");
    mRunning = false;
    return true;
  } else {
    return false;
  }
}

/* ------------------------------------------------------------------------- */
Fsck::~Fsck() 
{
  if (mRunning)
    Stop();
}

/* ------------------------------------------------------------------------- */
void* 
Fsck::StaticCheck(void* arg){
  return reinterpret_cast<Fsck*>(arg)->Check();
}

/* ------------------------------------------------------------------------- */
void* 
Fsck::Check(void)
{
  XrdSysThread::SetCancelOn();
  XrdSysThread::SetCancelDeferred();
  while (1) {
    usleep(1000000);
    eos_static_debug("Started consistency checker thread");
    ClearLog();
    Log(false,"started check");

    // run through the fsts 
    // compare files on disk with files in the namespace

    size_t pos=0;
    size_t max=0;
    {
      {
	eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
	max = FsView::gFsView.mIdView.size();
      }
      Log(false,"Filesystems to check: %lu", max);
      eos_static_debug("filesystems to check: %lu",max);
    }
    
    std::map<eos::common::FileSystem::fsid_t, FileSystem*>::const_iterator it;

    unsigned long long totalfiles=0;
    unsigned long long nchecked=0;
    unsigned long long nunchecked=0;
    unsigned long long n_error_replica_not_registered=0;
    unsigned long long n_error_replica_orphaned=0;

    unsigned long long n_error_mgm_disk_size_differ = 0;
    unsigned long long n_error_fst_disk_fmd_size_differ = 0;
    unsigned long long n_error_mgm_disk_checksum_differ = 0;
    unsigned long long n_error_fst_disk_fmd_checksum_differ = 0;    
    unsigned long long n_error_fst_filechecksum = 0;
    unsigned long long n_error_fst_blockchecksum = 0;
    unsigned long long n_error_replica_layout = 0;
    unsigned long long n_error_replica_offline = 0;
    unsigned long long n_error_file_offline = 0;

    std::set<eos::common::FileSystem::fsid_t> scannedfsids;

    while (pos < max) {
      {
	eos::common::FileSystem::fsid_t fsid;
	std::string hostport="";
	std::string mountpoint="";
	bool active=false;
	{
	  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
	  it = FsView::gFsView.mIdView.begin();
	  for (size_t l=0; l < pos; l++) {
	    it++;
	  }
	  if (it != FsView::gFsView.mIdView.end()) {
	    fsid = it->first;
	    hostport  = it->second->GetString("hostport");
	    mountpoint = it->second->GetString("path");
	    if (it->second->GetActiveStatus() == eos::common::FileSystem::kOnline) {
	      active = true;
	    } else {
	      active = false;
	    }
	  } else {
	    fsid = 0;
	  }
	}
	
	if (fsid) {
	  // this remembers which fsids have been scanned ... we use this to remove old fsids from the global map (which don't exist anymore)

	  scannedfsids.insert(fsid);

	  // local accounting get's copy after the full loop in to the global accounting
	  std::map<std::string, unsigned long long> mLocalErrorMap; 
	  std::map<std::string, std::set<unsigned long long> > mLocalErrorFidSet;
	  std::set<unsigned long long> mSet;

	  // initialize local accounting
	  for (size_t i=0; i< mErrorNames.size(); i++) {
	    mLocalErrorMap[mErrorNames[i]] = 0;
	    mLocalErrorFidSet[mErrorNames[i]] = mSet;
	  }

	  if (!active) {
	    Log(true,"filesystem: %lu/%lu fsid=%05d hostport=%20s mountpoint=%s INACTIVE", pos+1,max, fsid, hostport.c_str(),mountpoint.c_str());
	    Log(false,"");
	  } else {
	    Log(true,"filesystem: %lu/%lu fsid=%05d hostport=%20s mountpoint=%s totalfiles=%lu", pos+1,max, fsid, hostport.c_str(),mountpoint.c_str(),totalfiles);
	    
	    eos_static_debug("checking filesystem: fsid=%lu hostport=%s mountpoint=%s",pos, hostport.c_str(),mountpoint.c_str());
	    XrdOucString url = "root://daemon@"; url += hostport.c_str(); url += "/"; url += mountpoint.c_str();

	    XrdSysThread::SetCancelOff();
	    DIR* dir = XrdPosixXrootd::Opendir(url.c_str());
	    unsigned long long nfiles=0;
	    if (dir) {
	      static struct dirent* dentry;
	      while ( (dentry = XrdPosixXrootd::Readdir(dir)) ) {
		//	      Log(true,"filesystem: %lu/%lu %s", pos,max, dentry->d_name);
		nfiles++;
		totalfiles++;
		mLocalErrorMap[mErrorNames[0]]++;

		Log(true,"filesystem: %lu/%lu fsid=%05d hostport=%20s mountpoint=%s totalfiles=%llu nfiles=%lu", pos+1,max, fsid, hostport.c_str(),mountpoint.c_str(), totalfiles, nfiles);
		// decode the entry
		std::vector<std::string> tokens;
		std::string delimiter=":";
		std::string token= dentry->d_name;
		eos::common::StringConversion::Tokenize(token, tokens, delimiter);
		unsigned long long fid = strtoull(tokens[0].c_str(),0,16);
		if (fid) {
		  eos::FileMD* fmd=0;
		  
		  //-------------------------------------------
		  gOFS->eosViewMutex.Lock();
		  try {
		    fmd = gOFS->eosFileService->getFileMD(fid);
		  } catch ( eos::MDException &e ) {
		    // nothing to catch
		  }
		 
		  // convert size & checksum into strings
		  XrdOucString sizestring="";
		  std::string mgm_size = "";
		  std::string mgm_checksum = "";
		  bool replicaexists = false;
		  bool lfnexists = false;
		  bool unlinkedlocation = false;

		  if (fmd) {
		    eos::FileMD fmdCopy(*fmd);
		    fmd = &fmdCopy;
		    gOFS->eosViewMutex.UnLock();
		    //-------------------------------------------
		    eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)fmd->getSize());
		  
		    mgm_size = sizestring.c_str();
		    for (unsigned int i=0; i< SHA_DIGEST_LENGTH; i++) {
		      unsigned int checksumtype = eos::common::LayoutId::GetChecksum(fmd->getLayoutId());
		      if ( ( (checksumtype == eos::common::LayoutId::kAdler) || 
			     (checksumtype == eos::common::LayoutId::kCRC32) || 
			     (checksumtype == eos::common::LayoutId::kCRC32C) ) && (i<4) ) {
			char hb[3]; sprintf(hb,"%02x", (unsigned char) (fmd->getChecksum().getDataPtr()[3-i]));
			mgm_checksum += hb;

		      } else {
			char hb[3]; sprintf(hb,"%02x", (unsigned char) (fmd->getChecksum().getDataPtr()[i]));
			mgm_checksum += hb;
		      }
		    }
		    if (fmd->hasLocation( (eos::FileMD::location_t) fsid))
		      replicaexists=true;

		    if (fmd->hasUnlinkedLocation( (eos::FileMD::location_t) fsid))
		      unlinkedlocation=true;

		    // check if we have != stripes than defined by the layout
		    if (fmd->getNumLocation() != (eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId())+1)) {
		      mLocalErrorMap[mErrorNames[11]]++;
		      mLocalErrorFidSet[mErrorNames[11]].insert(fid);
		      n_error_replica_layout++;
		    }

		    // check if locations are online
		    eos::FileMD::LocationVector::const_iterator lociter;
		    bool oneoffline=false;
		    size_t nonline=0;
		    for ( lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter) {
                        if (*lociter) {
                          if (FsView::gFsView.mIdView.count(*lociter)) {
			    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
			    if (FsView::gFsView.mIdView.count(*lociter)) {
			      if (FsView::gFsView.mIdView[*lociter]->GetActiveStatus() == eos::common::FileSystem::kOffline) {
				if (!oneoffline) {
				  n_error_replica_offline++;
				  oneoffline=true;
				  mLocalErrorMap[mErrorNames[12]]++;
				  mLocalErrorFidSet[mErrorNames[12]].insert(fid);
				}
			      } else {
				nonline++;
			      }
			    }
			  }
			}
		    }
		    if ((fmd->getNumLocation()) && (nonline < eos::common::LayoutId::GetMinOnlineReplica((fmd->getLayoutId())))) {
		      mLocalErrorMap[mErrorNames[13]]++;
		      mLocalErrorFidSet[mErrorNames[13]].insert(fid);
		      n_error_file_offline++;
		    }
		  } else {
		    gOFS->eosViewMutex.UnLock();
		    //-------------------------------------------
		  }
		  
		  
		  //		  if (!(nfiles%10)) 
		  //		    eos_static_info("%d tokens %s %s %s\n", tokens.size(), token.c_str(), mgm_size.c_str(), mgm_checksum.c_str());
		  
		  // now we have all the information and we compare all sizes, checksums and locations
		  // the mgm size differes from the size on disk
		  bool error_mgm_disk_size_differ = false;
		  // the changelog size on the FST differs from the size on disk
		  bool error_fst_disk_fmd_size_differ = false;
		  // the mgm checksum differs from the disk checksum
		  bool error_mgm_disk_checksum_differ = false;
		  // the changelog checksum on the FST differes from the checksum in the extended attributes on disk
		  bool error_fst_disk_fmd_checksum_differ = false;
		  // the scanned filechecksum does not agree with the extended attribute checksum
		  bool error_fst_filechecksum;
		  // the scanned blockchecksums are faulty
		  bool error_fst_blockchecksum;
		  
		  if (replicaexists) {
		    if (mgm_size != tokens[5]) {
		      error_mgm_disk_size_differ = true;
		      n_error_mgm_disk_size_differ++;
		      mLocalErrorMap[mErrorNames[1]]++;
		      mLocalErrorFidSet[mErrorNames[1]].insert(fid);
		    }
		    
		    if (tokens[5] != tokens[6]) {
		      error_fst_disk_fmd_size_differ = true;
		      n_error_fst_disk_fmd_size_differ++;
		      mLocalErrorMap[mErrorNames[2]]++;
		      mLocalErrorFidSet[mErrorNames[2]].insert(fid);
		    }
		    
		    if (mgm_checksum != tokens[7]) {
		      error_mgm_disk_checksum_differ = true;
		      n_error_mgm_disk_checksum_differ++;
		      mLocalErrorMap[mErrorNames[3]]++;
		      mLocalErrorFidSet[mErrorNames[3]].insert(fid);
		    }
		    
		    if (tokens[2] != tokens[7]) {
		      error_fst_disk_fmd_checksum_differ = true;
		      n_error_fst_disk_fmd_checksum_differ++;
		      mLocalErrorMap[mErrorNames[4]]++;
		      mLocalErrorFidSet[mErrorNames[4]].insert(fid);
		    }
		    
		    if (tokens[1] != "x") {
		      nchecked++;
		      mLocalErrorMap[mErrorNames[7]]++;
		      mLocalErrorFidSet[mErrorNames[7]].insert(fid);
		      if (tokens[3] == "1") {
			error_fst_filechecksum = true;
			n_error_fst_filechecksum++;
			mLocalErrorMap[mErrorNames[5]]++;
			mLocalErrorFidSet[mErrorNames[5]].insert(fid);
		      }
		      if (tokens[4] == "1") {
			error_fst_blockchecksum = true;
			n_error_fst_blockchecksum++;
			mLocalErrorMap[mErrorNames[6]]++;
			mLocalErrorFidSet[mErrorNames[6]].insert(fid);
		      }
		    } else {
		      nunchecked++;
		      mLocalErrorMap[mErrorNames[8]]++;
		    mLocalErrorFidSet[mErrorNames[8]].insert(fid);
		    }
		  } else {
		    if (lfnexists) {
		      if (!unlinkedlocation) {
			mLocalErrorMap[mErrorNames[9]]++;
			if (!mLocalErrorFidSet[mErrorNames[9]].count(fid))
			  mLocalErrorFidSet[mErrorNames[9]].insert(fid);
			n_error_replica_not_registered++;
		      }
		    } else {
		      if (!unlinkedlocation) {
			mLocalErrorMap[mErrorNames[10]]++;
			mLocalErrorFidSet[mErrorNames[10]].insert(fid);
			n_error_replica_orphaned++;
		      }
		    }
		  }
		}
	      }

	      XrdPosixXrootd::Closedir(dir);

	      XrdSysThread::SetCancelOn();
	    } else {
	      Log(false,"error: unable to open %s", url.c_str());
	      Log(false,"");
	    }
	  }
	  // copy local maps to global maps
	  mErrorMapMutex.Lock();
	  for (size_t i=0; i< mErrorNames.size(); i++) {
	    mFsidErrorMap[mErrorNames[i]][fsid] = mLocalErrorMap[mErrorNames[i]];
	    mFsidErrorFidSet[mErrorNames[i]][fsid] = mLocalErrorFidSet[mErrorNames[i]];
	  }
	  mErrorMapMutex.UnLock();
	}
      }
      pos++;
      XrdSysThread::CancelPoint();
    }

    mErrorMapMutex.Lock();

    mTotalErrorMap["totalfiles"]= totalfiles;
    mTotalErrorMap["diff_mgm_disk_size"]         = n_error_mgm_disk_size_differ;
    mTotalErrorMap["diff_fst_disk_fmd_size"]     = n_error_fst_disk_fmd_size_differ;
    mTotalErrorMap["diff_mgm_disk_checksum"]     = n_error_mgm_disk_checksum_differ;
    mTotalErrorMap["diff_fst_disk_fmd_checksum"] = n_error_fst_disk_fmd_checksum_differ;
    mTotalErrorMap["diff_file_checksum_scan"]    = n_error_fst_filechecksum;
    mTotalErrorMap["diff_block_checksum_scan"]   = n_error_fst_blockchecksum;
    mTotalErrorMap["scanned_files"]              = nchecked;
    mTotalErrorMap["not_scanned_files"]          = nunchecked;
    mTotalErrorMap["replica_not_registered"]     = n_error_replica_not_registered;
    mTotalErrorMap["replica_orphaned"]           = n_error_replica_orphaned;
    mTotalErrorMap["diff_replica_layout"]        = n_error_replica_layout;
    mTotalErrorMap["replica_offline"]            = n_error_replica_offline;
    mTotalErrorMap["file_offline"]               = n_error_file_offline;

    // remove not scanned fsids

    std::set<eos::common::FileSystem::fsid_t> fsidstodelete;
    std::map <eos::common::FileSystem::fsid_t, unsigned long long>::const_iterator fsit;
    for (fsit = mFsidErrorMap[mErrorNames[0]].begin(); fsit != mFsidErrorMap[mErrorNames[0]].end(); fsit++) {
      if (!scannedfsids.count(fsit->first)) {
	fsidstodelete.insert(fsit->first);
      }
    }

    std::set<eos::common::FileSystem::fsid_t>::const_iterator dit;
    for (dit = fsidstodelete.begin(); dit != fsidstodelete.end(); dit++) {
      for (size_t i=0; i< mErrorNames.size(); i++) {
	mFsidErrorMap[mErrorNames[i]].erase(*dit);
	mFsidErrorFidSet[mErrorNames[i]].erase(*dit);
      }
    }
    
    mErrorMapMutex.UnLock();

    Log(false,"N-TOTAL-FILES           = %llu", totalfiles);
    Log(false,"E-MGM-DISK-SIZE         = %llu", n_error_mgm_disk_size_differ);
    Log(false,"E-FST-DISK-FMD-SIZE     = %llu", n_error_fst_disk_fmd_size_differ);
    Log(false,"E-MGM-DISK-CHECKSUM     = %llu", n_error_mgm_disk_checksum_differ);
    Log(false,"E-FST-DISK-FMD-CHECKSUM = %llu", n_error_fst_disk_fmd_checksum_differ);
    Log(false,"E-FST-FILECHECKSUM      = %llu", n_error_fst_filechecksum);
    Log(false,"E-FST-BLOCKCHECKSUM     = %llu", n_error_fst_blockchecksum);
    Log(false,"N-FST-CHECKED           = %llu", nchecked);
    Log(false,"N-FST-UNCHECKED         = %llu", nunchecked);
    Log(false,"N-REPLICA_NOT_REGISTERED= %llu", n_error_replica_not_registered);
    Log(false,"N-REPLICA_ORPHANED      = %llu", n_error_replica_orphaned);
    Log(false,"N-REPLICA-LAYOUT        = %llu", n_error_replica_layout);
    Log(false,"N-REPLICA-OFFLINE       = %llu", n_error_replica_offline);
    Log(false,"N-FILE-OFFLINE          = %llu", n_error_file_offline);
    Log(false,"stopping check");
    
    XrdSysThread::CancelPoint();
    Log(false,"=> next run in 8 hours");
    sleep(3600*8);
  }
  return 0;
}

/* ------------------------------------------------------------------------- */
void 
Fsck::PrintOut(XrdOucString &out,  XrdOucString option)
{
  mLogMutex.Lock();
  out = mLog;
  mLogMutex.UnLock();
}

/* ------------------------------------------------------------------------- */
void 
Fsck::ClearLog() 
{
  mLogMutex.Lock();
  mLog="";
  mLogMutex.UnLock();
}

/* ------------------------------------------------------------------------- */
void
Fsck::Log(bool overwrite, const char* msg, ...)
{
  static time_t current_time;
  static struct timeval tv;
  static struct timezone tz;
  static struct tm *tm;
  
  va_list args;
  va_start (args, msg);
  char buffer[16384];
  char* ptr;
  time (&current_time);
  gettimeofday(&tv, &tz);

  tm = localtime (&current_time);
  sprintf (buffer, "%02d%02d%02d %02d:%02d:%02d %lu.%06lu ", tm->tm_year-100, tm->tm_mon+1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, current_time, (unsigned long)tv.tv_usec);
  ptr = buffer + strlen(buffer);

  vsprintf(ptr, msg, args);
  mLogMutex.Lock();
  if (overwrite) {
    int spos = mLog.rfind("\n",mLog.length()-2);
    if (spos>0) {
      mLog.erase(spos+1);
    }
  }
  mLog+=buffer;
  mLog+= "\n";
  va_end(args);
  mLogMutex.UnLock();
}


/* ------------------------------------------------------------------------- */
bool
Fsck::Report(XrdOucString &out,  XrdOucString &err, XrdOucString option, XrdOucString selection)
{

  std::string sel =selection.c_str();
  
  if (selection.length()) {
    bool found=false;
    for (size_t i=0; i< mErrorNames.size(); i++) {
      if (sel == mErrorNames[i]) {
	found = true;
	break;
      }
    }
    if (!found) {
      err += "error: there is no 'tag' named '"; err += selection; err += "'\n";
      return false;
    }
  }
  
  mErrorMapMutex.Lock();
  if ((option.find("g")!=STR_NPOS) || (option.length()==0) ) {
    // print global counts
    for (size_t i = 0; i < mErrorNames.size(); i++) {
      if ( (! sel.length()) || 
	   (sel == mErrorNames[i]) ) {
	char outline[4096];
	if (option.find("m")==STR_NPOS) {
	  snprintf(outline,sizeof(outline)-1,"ALL        %-32s %lld\n", mErrorNames[i].c_str(),mTotalErrorMap[mErrorNames[i]]);
	} else {
	  snprintf(outline,sizeof(outline)-1,"fsck_n_%s=%lld\n",mErrorNames[i].c_str(), mTotalErrorMap[mErrorNames[i]]);
	}
	out += outline;
      }
    }
  }
  if ((option.find("a")!=STR_NPOS)) {
    // print statistic for all filesystems having errors
    for (size_t i = 0; i < mErrorNames.size(); i++) {
      if ( (! sel.length()) || 
	   (sel == mErrorNames[i]) ) {
	if (mFsidErrorMap[mErrorNames[i]].size()) {
	  std::map<eos::common::FileSystem::fsid_t,unsigned long long>::const_iterator it;
	  for (it = mFsidErrorMap[mErrorNames[i]].begin(); it != mFsidErrorMap[mErrorNames[i]].end(); it ++) {
	    char outline[4096];
	    if (it->second) {
	      if (option.find("g")== STR_NPOS) {
		if (option.find("m")==STR_NPOS) {
		  XrdOucString sizestring;
		  snprintf(outline,sizeof(outline)-1,"%-12s%-32s %lld\n",eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long) it->first), mErrorNames[i].c_str(),it->second);
		} else {
		  snprintf(outline,sizeof(outline)-1,"fsck_fsid=%u fsck_n_%s=%lld\n",it->first, mErrorNames[i].c_str(), it->second);
		}
		out += outline;
	      }
	      if (option.find("i")!=STR_NPOS) {
		XrdOucString fidstring="";
		std::set<unsigned long long>::const_iterator fidit;
		for (fidit = mFsidErrorFidSet[mErrorNames[i]][it->first].begin(); fidit != mFsidErrorFidSet[mErrorNames[i]][it->first].end(); fidit++) {
		  XrdOucString sizestring;
		  XrdOucString fxid="";
		  eos::common::FileId::Fid2Hex(*fidit, fxid);
		  out += "fxid=";
		  out += fxid;
		  out += " e=";
		  out += mErrorNames[i].c_str();
		  out += "\n";
		}
	      }
	      
	      if (option.find("l")!=STR_NPOS) {
		std::set<unsigned long long>::const_iterator fidit;
		for (fidit = mFsidErrorFidSet[mErrorNames[i]][it->first].begin(); fidit != mFsidErrorFidSet[mErrorNames[i]][it->first].end(); fidit++) {
		  XrdOucString sizestring;
		  //-------------------------------------------
		  gOFS->eosViewMutex.Lock();
		  std::string path="";
		  try {
		    path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(*fidit));
		  } catch ( eos::MDException &e ) {
		    path ="EINVAL";
		  }
 		  gOFS->eosViewMutex.UnLock();

		  if (path.length()) {
		    out += "lfn=";
		    out += path.c_str();
		    out += " e="; out += mErrorNames[i].c_str();
		    out += "\n";
		  }

		  if ( (option.find("C")!=STR_NPOS) && 
		       (mErrorNames[i] == "diff_fst_disk_fmd_checksum") ) {
		    // send a verify to that file/filesystem
		    eos::common::Mapping::VirtualIdentity vid;
		    eos::common::Mapping::Root(vid);
		    XrdOucErrInfo error;
		    int lretc = gOFS->_verifystripe(path.c_str(), error, vid, it->first, "&mgm.verify.compute.checksum=1");
		    if (!lretc) {
		      out += "success: sending verify to fsid="; out += (int)it->first; out += " for path="; out += path.c_str(); out += "\n";
		    } else {
		      err += "error: sending verify to fsid=";   err += (int)it->first; err += " failed for path="; err += path.c_str(); err += "\n";
		    }
		  }

		  if ( (option.find("U")!=STR_NPOS) &&
		       (mErrorNames[i] == "replica_not_registered")) {
		    // remove unregistered replicas
		    if (gOFS->DeleteExternal(it->first, *fidit)) {
		      char outline[1024];
		      snprintf(outline,sizeof(outline)-1, "success: send unlink to fsid=%u fxid=%llx\n",it->first,*fidit);
		      out += outline;
		    } else {
		      char errline[1024];
		      snprintf(errline,sizeof(outline)-1, "err: unable to send unlink to fsid=%u fxid=%llx\n",it->first,*fidit);
		      err += outline;
		    }
		  }
		  
		  if ( (option.find("O")!=STR_NPOS) &&
		       (mErrorNames[i] == "replica_orphaned")) {
		    // remove orphan replicas
		    if (gOFS->DeleteExternal(it->first, *fidit)) {
		      char outline[1024];
		      snprintf(outline,sizeof(outline)-1, "success: send unlink to fsid=%u fxid=%llx\n",it->first,*fidit);
		      out += outline;
		    } else {
		      char errline[1024];
		      snprintf(errline,sizeof(outline)-1, "err: unable to send unlink to fsid=%u fxid=%llx\n",it->first,*fidit);
		      err += outline;
		    }
		  }
		  

		  if ( (option.find("A")!=STR_NPOS) && 
		       (mErrorNames[i] == "diff_replica_layout") ) {
		    // execute adjust replica
		    eos::common::Mapping::VirtualIdentity vid;
		    eos::common::Mapping::Root(vid);
		    XrdOucErrInfo error;

		    // execute a proc command
		    ProcCommand Cmd;
		    XrdOucString info="mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
		    info += path.c_str();

		    Cmd.open("/proc/user",info.c_str(), vid, &error);
		    Cmd.AddOutput(out,err);
		    Cmd.close();
		  }
		  
		  //-------------------------------------------
		}
	      }
	    }
	  }
	}
      }
    }
  }
  mErrorMapMutex.UnLock();
  return true;
}



EOSMGMNAMESPACE_END
