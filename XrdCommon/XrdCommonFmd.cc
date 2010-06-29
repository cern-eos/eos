/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonFmd.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <stdio.h>
#include <sys/mman.h>
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
XrdCommonFmdHandler gFmdHandler;
/*----------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------*/
bool 
XrdCommonFmdHeader::Read(int fd) 
{
  // the header is already in the beginning
  lseek(fd,0,SEEK_SET);
  int nread = read(fd,&fmdHeader, sizeof(struct FMDHEADER));
  if (nread != sizeof(struct FMDHEADER)) {
    eos_crit("unable to read fmd header");
    return false;
  }

  eos_info("fmd header version %s creation time is %u filesystem id %04d", fmdHeader.version, fmdHeader.ctime, fmdHeader.fsid);
  if (strcmp(fmdHeader.version, VERSION)) {
    eos_crit("fmd header contains version %s but this is version %s", fmdHeader.version, VERSION);
    return false;
  }
  if (fmdHeader.magic != XRDCOMMONFMDHEADER_MAGIC) {
    eos_crit("fmd header magic is wrong - found %x", fmdHeader.magic);
    return false;
  }
  return true;
}
 
/*----------------------------------------------------------------------------*/
bool 
XrdCommonFmdHeader::Write(int fd) 
{
  // the header is always in the beginning
  lseek(fd,0,SEEK_SET);
  int nwrite = write(fd,&fmdHeader, sizeof(struct FMDHEADER));
  if (nwrite != sizeof(struct FMDHEADER)) {
    eos_crit("unable to write fmd header");
    return false;
  }
  eos_debug("wrote fmd header version %s creation time %u filesystem id %04d", fmdHeader.version, fmdHeader.ctime, fmdHeader.fsid);
  return true;
}

/*----------------------------------------------------------------------------*/
bool 
XrdCommonFmd::Write(int fd) 
{
  // compute crc32
  fMd.crc32 = ComputeCrc32((char*)(&(fMd.fid)), sizeof(struct FMD) - sizeof(fMd.magic) - (2*sizeof(fMd.sequencetrailer)) - sizeof(fMd.crc32));
  eos_debug("computed meta CRC for fileid %d to %x", fMd.fid, fMd.crc32);
  if ( (write(fd, &fMd, sizeof(fMd))) != sizeof(fMd)) {
    eos_crit("failed to write fmd struct");
    return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool 
XrdCommonFmd::Read(int fd, off_t offset) 
{
  if ( (pread(fd, &fMd, sizeof(fMd),offset)) != sizeof(fMd)) {
    eos_crit("failed to read fmd struct");
    return false;
  }

  return true;
}


/*----------------------------------------------------------------------------*/
bool
XrdCommonFmdHandler::SetChangeLogFile(const char* changelogfilename, int fsid) 
{
  Mutex.Lock();
  bool isNew=false;

  char fsChangeLogFileName[1024];
  sprintf(fsChangeLogFileName,"%s.%04d.mdlog", ChangeLogFileName.c_str(),fsid);
  
  if (fdChangeLogRead[fsid]>0) {
    eos_info("closing changelog read file %s", ChangeLogFileName.c_str());
    close(fdChangeLogRead[fsid]);
  }
  if (fdChangeLogWrite[fsid]>0) {
    eos_info("closing changelog read file %s", ChangeLogFileName.c_str());
    close(fdChangeLogWrite[fsid]);
  }

  sprintf(fsChangeLogFileName,"%s.%04d.mdlog", changelogfilename,fsid);
  ChangeLogFileName = changelogfilename;
  eos_info("changelog file is now %s\n", ChangeLogFileName.c_str());

  // check if this is a new changelog file
  if ((access(fsChangeLogFileName,R_OK))) {
    isNew = true;
  }

  if ( (fdChangeLogWrite[fsid] = open(fsChangeLogFileName,O_CREAT| O_RDWR, 0600 )) <0) {
    eos_err("unable to open changelog file for writing %s",fsChangeLogFileName);
    fdChangeLogWrite[fsid] = fdChangeLogRead[fsid] = -1;
    Mutex.UnLock();
    return false;
  }

  // spool to the end
  lseek(fdChangeLogWrite[fsid], 0, SEEK_END);
 
  if ( (fdChangeLogRead[fsid] = open(fsChangeLogFileName,O_RDONLY)) < 0) {
    eos_err("unable to open changelog file for writing %s",fsChangeLogFileName);
    close(fdChangeLogWrite[fsid]);
    fdChangeLogWrite[fsid] = fdChangeLogRead[fsid] = -1;
    Mutex.UnLock();
    return false;
  }
  eos_info("opened changelog file %s for filesystem %04d", fsChangeLogFileName, fsid);

  if (isNew) {
    // try to write the header
    fmdHeader.SetId(fsid);
    fmdHeader.SetLogId("FmdHeader");
    if (!fmdHeader.Write(fdChangeLogWrite[fsid])) {
      // cannot write the header 
      isOpen=false;
      Mutex.UnLock();
      return isOpen;
    }
  }

  isOpen = ReadChangeLogHash(fsid);

  Mutex.UnLock();
  return isOpen;
}


int 
XrdCommonFmdHandler::CompareMtime(const void* a, const void *b) {
  struct filestat {
    struct stat buf;
    char filename[1024];
  };
  return ( (((struct filestat*)a)->buf.st_mtime) - ((struct filestat*)b)->buf.st_mtime);
}

/*----------------------------------------------------------------------------*/
bool XrdCommonFmdHandler::AttachLatestChangeLogFile(const char* changelogdir, int fsid) 
{
  DIR *dir;
  struct dirent *dp;
  struct filestat {
    struct stat buf;
    char filename[1024];
  };

  Fmd[fsid].set_empty_key(0xffffffffe);
  Fmd[fsid].set_deleted_key(0xffffffff);

  int nobjects=0;
  long tdp=0;

  struct filestat* allstat = 0;
  ChangeLogDir = changelogdir;
  ChangeLogDir += "/";
  while (ChangeLogDir.replace("//","/")) {};
  XrdOucString Directory = changelogdir;
  XrdOucString FileName ="";
  char fileend[1024];
  sprintf(fileend,".%04d.mdlog",fsid);
  if ((dir = opendir(Directory.c_str()))) {
    tdp = telldir(dir);
    while ((dp = readdir (dir)) != 0) {
      FileName = dp->d_name;
      if ( (!strcmp(dp->d_name,".")) || (!strcmp(dp->d_name,".."))
           || (strlen(dp->d_name) != strlen("fmd.1272892439.0000.mdlog")) || (strncmp(dp->d_name,"fmd.",4)) || (!FileName.endswith(fileend)))
        continue;
      
      nobjects++;
    }
    allstat = (struct filestat*) malloc(sizeof(struct filestat) * nobjects);
    if (!allstat) {
      eos_err("cannot allocate sorting array");
      return false;
    }
   
    eos_debug("found %d old changelog files\n", nobjects);
    // go back
    seekdir(dir,tdp);
    int i=0;
    while ((dp = readdir (dir)) != 0) {
      FileName = dp->d_name;
      if ( (!strcmp(dp->d_name,".")) || (!strcmp(dp->d_name,".."))
           || (strlen(dp->d_name) != strlen("fmd.1272892439.0000.mdlog")) || (strncmp(dp->d_name,"fmd.",4)) || (!FileName.endswith(fileend)))
        continue;
      char fullpath[8192];
      sprintf(fullpath,"%s/%s",Directory.c_str(),dp->d_name);

      sprintf(allstat[i].filename,"%s",dp->d_name);
      eos_debug("stat on %s\n", dp->d_name);
      if (stat(fullpath, &(allstat[i].buf))) {
        eos_err("cannot stat after readdir file %s", fullpath);
      }
      i++;
    }
    closedir(dir);
    // do the sorting
    qsort(allstat,nobjects,sizeof(struct filestat),XrdCommonFmdHandler::CompareMtime);
  } else {
    eos_err("cannot open changelog directory",Directory.c_str());
    return false;
  }
  
  XrdOucString changelogfilename = changelogdir;

  if (allstat && (nobjects>0)) {
    // attach an existing one
    changelogfilename += "/";
    while (changelogfilename.replace("//","/")) {};
    changelogfilename += allstat[0].filename;
    changelogfilename.replace(fileend,"");
    eos_info("attaching existing changelog file %s", changelogfilename.c_str());
    free(allstat);
  } else {
    // attach a new one
    CreateChangeLogName(changelogfilename.c_str(), changelogfilename);
    eos_info("creating new changelog file %s", changelogfilename.c_str());
  }
  // initialize sequence number
  fdChangeLogSequenceNumber[fsid]=0;
  return SetChangeLogFile(changelogfilename.c_str(),fsid);
}

/*----------------------------------------------------------------------------*/
bool XrdCommonFmdHandler::ReadChangeLogHash(int fsid) 
{
  if (!fmdHeader.Read(fdChangeLogRead[fsid])) {
    // failed to read header
    return false;
  }
  struct stat stbuf; 

  // create first empty root entries
  UserBytes [(((unsigned long long)fsid)<<32) | 0] = 0;
  GroupBytes[(((unsigned long long)fsid)<<32) | 0] = 0;
  UserFiles [(((unsigned long long)fsid)<<32) | 0] = 0;
  GroupFiles[(((unsigned long long)fsid)<<32) | 0] = 0;

  if (::fstat(fdChangeLogRead[fsid], &stbuf)) {
    eos_crit("unable to stat file size of changelog file - errc%d", errno);
    return false;
  }

  if (stbuf.st_size > (4 * 1000l*1000l*1000l)) {
    // we don't map more than 4 GB ... should first trim here
    eos_crit("changelog file exceeds memory limit of 4 GB for boot procedure");
    return false;
  }
  
  // mmap the complete changelog ... wow!

  if ( (unsigned long)stbuf.st_size <= sizeof(struct XrdCommonFmdHeader::FMDHEADER)) {
    eos_info("changelog is empty - nothing to check");
    return true;
  }

  char* changelogmap   =  (char*) mmap(0, stbuf.st_size, PROT_READ, MAP_SHARED, fdChangeLogRead[fsid],0);

  if (!changelogmap) {
    eos_crit("unable to mmap changelog file - errc=%d",errno);
    return false;
  }

  char* changelogstart =  changelogmap + sizeof(struct XrdCommonFmdHeader::FMDHEADER);;
  char* changelogstop  =  changelogmap + stbuf.st_size;
  struct XrdCommonFmd::FMD* pMd;
  bool success = true;
  unsigned long sequencenumber=0;
  int retc=0;
  int nchecked=0;

  eos_debug("memory mapped changelog file at %lu", changelogstart);

  while ( (changelogstart+sizeof(struct XrdCommonFmd::FMD)) <= changelogstop) {
    nchecked++;
    if (!(nchecked%1000)) {
      eos_info("checking SEQ# %d # %d", sequencenumber, nchecked);
    } else {
      eos_debug("checking SEQ# %d # %d", sequencenumber, nchecked);
    }

    pMd = (struct XrdCommonFmd::FMD*) changelogstart;
    eos_debug("%llx %llx %ld %llx %lu %llu %lu %x", pMd, &(pMd->fid), sizeof(*pMd), pMd->magic, pMd->sequenceheader, pMd->fid, pMd->fsid, pMd->crc32);
    if (!(retc = XrdCommonFmd::IsValid(pMd, sequencenumber))) {
      // good meta data block
    } else {
      // illegal meta data block
      if (retc == EINVAL) {
	eos_crit("Block is neither creation/update or deletion block %u offset %llu", sequencenumber, ((char*)pMd) - changelogmap);
      }
      if (retc == EILSEQ) {
	eos_crit("CRC32 error in meta data block sequencenumber %u offset %llu", sequencenumber, ((char*)pMd) - changelogmap);
      }
      if (retc == EOVERFLOW) {
	eos_crit("SEQ# error in meta data block sequencenumber %u offset %llu", sequencenumber, ((char*)pMd) - changelogmap);
      }
      if (retc == EFAULT) {
	eos_crit("SEQ header/trailer mismatch in meta data block sequencenumber %u/%u offset %llu", pMd->sequenceheader,pMd->sequencetrailer, ((char*)pMd) - changelogmap);
      }
      success = false;
    }

    if (pMd->sequenceheader > (unsigned long long) fdChangeLogSequenceNumber[fsid]) {
      fdChangeLogSequenceNumber[fsid] = pMd->sequenceheader;
    }

    // setup the hash entries
    Fmd[fsid].insert(make_pair(pMd->fid, (unsigned long long) (changelogstart-changelogmap)));
    // do quota hashs
    if (XrdCommonFmd::IsCreate(pMd)) {
      long long exsize = -1;
      if (FmdSize.count(pMd->fid)>0) {
	// exists
	exsize = FmdSize[pMd->fid];
      }

      //      eos_debug("fid %lu psize %lld", pMd->fid, FmdSize[pMd->fid]);
      if (exsize>=0) {
	// substract old size
	UserBytes [(((unsigned long long)pMd->fsid)<<32) | pMd->uid]  -= exsize;
	GroupBytes[(((unsigned long long)pMd->fsid)<<32) | pMd->gid]  -= exsize;
	UserFiles [(((unsigned long long)pMd->fsid)<<32) | pMd->uid]--;
	GroupFiles[(((unsigned long long)pMd->fsid)<<32) | pMd->gid]--;
      }
      // store new size
      FmdSize[pMd->fid] = pMd->size;

      // add new size
      UserBytes [(pMd->fsid<<32) | pMd->uid]  += pMd->size;
      GroupBytes[(pMd->fsid<<32) | pMd->gid] += pMd->size;
      UserFiles [(pMd->fsid<<32) | pMd->uid]++;
      GroupFiles[(pMd->fsid<<32) | pMd->gid]++;
    }
    if (XrdCommonFmd::IsDelete(pMd)) {
      FmdSize[pMd->fid] = 0;
      UserBytes [(pMd->fsid<<32) | pMd->uid]  -= pMd->size;
      GroupBytes[(pMd->fsid<<32) | pMd->gid] -= pMd->size;
      UserFiles [(pMd->fsid<<32) | pMd->uid]--;
      GroupFiles[(pMd->fsid<<32) | pMd->gid]--;
    }
    eos_debug("userbytes %llu groupbytes %llu userfiles %llu groupfiles %llu",  UserBytes [(pMd->fsid<<32) | pMd->uid], GroupBytes[(pMd->fsid<<32) | pMd->gid], UserFiles [(pMd->fsid<<32) | pMd->uid],GroupFiles[(pMd->fsid<<32) | pMd->gid]);
    pMd++;
    changelogstart += sizeof(struct XrdCommonFmd::FMD);
  }
  munmap(changelogmap, stbuf.st_size);
  eos_debug("checked %d FMD entries",nchecked);
  return success;
}

/*----------------------------------------------------------------------------*/
XrdCommonFmd*
XrdCommonFmdHandler::GetFmd(unsigned long long fid, unsigned int fsid, uid_t uid, gid_t gid, unsigned int layoutid, bool isRW) 
{
  Mutex.Lock();
  if (fdChangeLogRead[fsid]>0) {
    if (Fmd[fsid][fid] != 0) {
      // this is to read an existing entry
      XrdCommonFmd* fmd = new XrdCommonFmd();
      if (!fmd->Read(fdChangeLogRead[fsid],Fmd[fsid][fid])) {
	eos_crit("unable to read block for fid %d on fs %d", fid, fsid);
	Mutex.UnLock();
	return 0;
      }
      if ( fmd->fMd.fid != fid) {
	// fatal this is somehow a wrong record!
	eos_crit("unable to get fmd for fid %d on fs %d - file id mismatch in meta data block", fid, fsid);
	Mutex.UnLock();
	return 0;
      }
      if ( fmd->fMd.fsid != fsid) {
	// fatal this is somehow a wrong record!
	eos_crit("unable to get fmd for fid %d on fs %d - filesystem id mismatch in meta data block", fid, fsid);
	Mutex.UnLock();
	return 0;
      }
      // return the new entry
      Mutex.UnLock();	
      return fmd;
    }
    if (isRW) {
      // make a new record
      XrdCommonFmd* fmd = new XrdCommonFmd(fid, fsid);
      fmd->MakeCreationBlock();
      
      if (fdChangeLogWrite[fsid]>0) {
	off_t position = lseek(fdChangeLogWrite[fsid],0,SEEK_CUR);

	fdChangeLogSequenceNumber[fsid]++;
	// set sequence number
	fmd->fMd.uid = uid;
	fmd->fMd.gid = gid;
	fmd->fMd.lid = layoutid;
	fmd->fMd.sequenceheader=fmd->fMd.sequencetrailer = fdChangeLogSequenceNumber[fsid];
	fmd->fMd.ctime = fmd->fMd.mtime = time(0);
	// get micro seconds
	struct timeval tv;
	struct timezone tz;
	
	gettimeofday(&tv, &tz);
	fmd->fMd.ctime_ns = fmd->fMd.mtime_ns = tv.tv_usec * 1000;
	
	// write this block
	if (!fmd->Write(fdChangeLogWrite[fsid])) {
	  // failed to write
	  eos_crit("failed to write new block for fid %d on fs %d", fid, fsid);
	  Mutex.UnLock();
	  return 0;
	}
	// add to the in-memory hashes
	Fmd[fsid][fid]     = position;
	FmdSize[fid] = 0;

	// add new file counter
	UserFiles [(((unsigned long long) fsid)<<32) | fmd->fMd.uid] ++;
	GroupFiles[(((unsigned long long) fsid)<<32) | fmd->fMd.gid] ++;
  
	
	eos_debug("returning meta data block for fid %d on fs %d", fid, fsid);
	// return the mmaped meta data block
	Mutex.UnLock();
	return fmd;
      } else {
	eos_crit("unable to write new block for fid %d on fs %d - no changelog file open for writing", fid, fsid);
	Mutex.UnLock();
	return 0;
      }
    } else {
      eos_err("unable to get fmd for fid %d on fs %d - record not found", fid, fsid);
      Mutex.UnLock();
      return 0;
    }
  } else {
    eos_crit("unable to get fmd for fid %d on fs %d - there is no changelog file open for that file system id", fid, fsid);
    Mutex.UnLock();
    return 0;
  }
}


/*----------------------------------------------------------------------------*/
bool
XrdCommonFmdHandler::DeleteFmd(unsigned long long fid, unsigned int fsid) 
{
  bool rc = true;
  Mutex.Lock();
  
  XrdCommonFmd* fmd = XrdCommonFmdHandler::GetFmd(fid,fsid, 0,0,0,false);\
  if (!fmd)
    rc = false;
  else {
    fmd->fMd.magic = XRDCOMMONFMDDELETE_MAGIC;
    rc = Commit(fmd);
    delete fmd;
  }

  // erase the has entries
  Fmd[fsid].erase(fid);
  FmdSize.erase(fid);

  Mutex.UnLock();
  return rc;
}


/*----------------------------------------------------------------------------*/
bool
XrdCommonFmdHandler::Commit(XrdCommonFmd* fmd)
{
  if (!fmd)
    return false;

  int fsid = fmd->fMd.fsid;
  int fid  = fmd->fMd.fid;

  Mutex.Lock();
  
  // get file position
  off_t position = lseek(fdChangeLogWrite[fsid],0,SEEK_CUR);
  fdChangeLogSequenceNumber[fsid]++;
  // set sequence number
  fmd->fMd.sequenceheader=fmd->fMd.sequencetrailer = fdChangeLogSequenceNumber[fsid];

  // put modification time
  fmd->fMd.mtime = time(0);
  // get micro seconds
  struct timeval tv;
  struct timezone tz;
  
  gettimeofday(&tv, &tz);
  fmd->fMd.mtime_ns = tv.tv_usec * 1000;
  
  // write this block
  if (!fmd->Write(fdChangeLogWrite[fsid])) {
    // failed to write
    eos_crit("failed to write commit block for fid %d on fs %d", fid, fsid);
    Mutex.UnLock();
    return false;
  }

  // store present size
  unsigned long long oldsize = FmdSize[fid];
  // add to the in-memory hashes
  Fmd[fsid][fid]     = position;
  FmdSize[fid] = fmd->fMd.size;


  // adjust the quota accounting of the update
  eos_debug("booking %d new bytes on quota %d/%d", (fmd->fMd.size-oldsize), fmd->fMd.uid, fmd->fMd.gid);
  UserBytes [(fmd->fMd.fsid<<32) | fmd->fMd.uid] += (fmd->fMd.size-oldsize);
  GroupBytes[(fmd->fMd.fsid<<32) | fmd->fMd.gid] += (fmd->fMd.size-oldsize);

  Mutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
bool
XrdCommonFmdHandler::TrimLogFile(int fsid) {
  bool rc=true;
  char newfilename[1024];
  sprintf(newfilename,".%04d.mdlog", fsid);
  XrdOucString NewChangeLogFileName;
  CreateChangeLogName(ChangeLogDir.c_str(),NewChangeLogFileName);
  NewChangeLogFileName += newfilename;

  int newfd = open(NewChangeLogFileName.c_str(),O_CREAT|O_TRUNC| O_RDWR, 0600);

  if (!newfd) 
    return false;

  int newrfd = open(NewChangeLogFileName.c_str(),O_RDONLY);
  
  if (!newrfd) {
    close(newfd);
    return false;
  }

  // write new header
  if (!fmdHeader.Write(newfd))
    return false;

  std::vector <unsigned long long> alloffsets;
  google::dense_hash_map <unsigned long long, unsigned long long> offsetmapping;
  offsetmapping.set_empty_key(0xffffffff);

  eos_static_info("trimming step 1");
  Mutex.Lock();

  google::dense_hash_map<unsigned long long, unsigned long long>::const_iterator it;
  for (it = Fmd[fsid].begin(); it != Fmd[fsid].end(); it++) {
    alloffsets.push_back(it->second);
  }
  eos_static_info("trimming step 2");

  // sort the offsets
  std::sort(alloffsets.begin(), alloffsets.end());
  eos_static_info("trimming step 3");

  // write the new file
  std::vector<unsigned long long>::const_iterator fmdit;
  XrdCommonFmd fmdblock;
  int rfd = dup(fdChangeLogRead[fsid]);

  eos_static_info("trimming step 4");

  if (rfd > 0) {
    for (fmdit = alloffsets.begin(); fmdit != alloffsets.end(); ++fmdit) {
      if (!fmdblock.Read(rfd,*fmdit)) {
	eos_static_crit("fatal error reading active changelog file at position %llu",*fmdit);
	rc = false;
	break;
      } else {
	off_t newoffset = lseek(newfd,0,SEEK_CUR);
	offsetmapping[*fmdit] = newoffset;
	if (!fmdblock.Write(newfd)) {
	  rc = false;
	  break; 
	}
      }
      
    }
  } else {
    eos_static_crit("fatal error duplicating read file descriptor");
    rc = false;
  }

  eos_static_info("trimming step 5");

  if (rc) {
    // now we take a lock, copy the latest changes since the trimming to the new file and exchange the current filedescriptor
    unsigned long long oldtailoffset = lseek(fdChangeLogWrite[fsid],0,SEEK_CUR);
    unsigned long long newtailoffset = lseek(newfd,0,SEEK_CUR);
    unsigned long long offset = oldtailoffset;

    ssize_t tailchange = oldtailoffset-newtailoffset;
    eos_static_info("tail length is %llu [ %llu %llu %llu ] ", tailchange, oldtailoffset, newtailoffset, offset);
    char copybuffer[128*1024];
    int nread=0;
    do {
      nread = pread(rfd, copybuffer, sizeof(copybuffer), offset);
      if (nread>0) {
	offset += nread;
	int nwrite = write(newfd,copybuffer,nread);
	if (nwrite != nread) {
	  eos_static_crit("fatal error doing last recent change copy");
	  rc = false;
	  break;
	}
      }
    } while (nread>0);

    
    // now adjust the in-memory map
    // clean up erased
    Fmd[fsid].resize(0);
    FmdSize.resize(0);

    google::dense_hash_map<unsigned long long, unsigned long long>::iterator it;
    for (it = Fmd[fsid].begin(); it != Fmd[fsid].end(); it++) {
      if (it->second >= oldtailoffset) {
	// this has just to be adjusted by the trim length
	it->second -= tailchange;
      } else {
	if (offsetmapping.count(it->second)) {
	  // that is what we expect
	  it->second = offsetmapping[it->second];
	} else {
	  // that should never happen!
	  eos_static_crit("fatal error found not mapped offset position during trim procedure!");
	  rc = false;
	}
      }      
    }
    // now high-jack the old write and read filedescriptor;
    close(fdChangeLogWrite[fsid]);
    close(fdChangeLogRead[fsid]);
    fdChangeLogWrite[fsid] = newfd;
    fdChangeLogRead[fsid] = newrfd;
  }
  eos_static_info("trimming step 6");  
  close(rfd);
  Mutex.UnLock();
  return rc;
}

/*----------------------------------------------------------------------------*/
XrdOucEnv*
XrdCommonFmd::FmdToEnv() 
{
  char serialized[1024*64];
  XrdOucString base64checksum;

  // base64 encode the checksum
  XrdCommonSymKey::Base64Encode(fMd.checksum, SHA_DIGEST_LENGTH, base64checksum);

  sprintf(serialized,"mgm.fmd.magic=%llu&mgm.fmd.sequenceheader=%lu&mgm.fmd.fid=%llu&mgm.fmd.cid=%llu&mgm.fmd.fsid=%lu&mgm.fmd.ctime=%lu&mgm.fmd.ctime_ns=%lu&mgm.fmd.mtime=%lu&mgm.fmd.mtime_ns=%lu&mgm.fmd.size=%llu&mgm.fmd.checksum64=%s&mgm.fmd.lid=%lu&mgm.fmd.uid=%u&mgm.fmd.gid=%u&mgm.fmd.name=%s&mgm.fmd.container=%s&mgm.fmd.crc32=%lu&mgm.fmd.sequencetrailer=%lu",
	  fMd.magic,fMd.sequenceheader,fMd.fid,fMd.cid,fMd.fsid,fMd.ctime,fMd.ctime_ns,fMd.mtime,fMd.mtime_ns,fMd.size,base64checksum.c_str(),fMd.lid,fMd.uid,fMd.gid,fMd.name,fMd.container,fMd.crc32,fMd.sequencetrailer);
  return new XrdOucEnv(serialized);
};

/*----------------------------------------------------------------------------*/
bool 
XrdCommonFmd::EnvToFmd(XrdOucEnv &env, struct XrdCommonFmd::FMD &fmd)
{
  // check that all tags are present
  if ( !env.Get("mgm.fmd.magic") ||
       !env.Get("mgm.fmd.sequenceheader") ||
       !env.Get("mgm.fmd.fid") ||
       !env.Get("mgm.fmd.cid") ||
       !env.Get("mgm.fmd.fsid") ||
       !env.Get("mgm.fmd.ctime") ||
       !env.Get("mgm.fmd.ctime_ns") ||
       !env.Get("mgm.fmd.mtime") ||
       !env.Get("mgm.fmd.mtime_ns") ||
       !env.Get("mgm.fmd.size") ||
       !env.Get("mgm.fmd.checksum64") ||
       !env.Get("mgm.fmd.lid") ||
       !env.Get("mgm.fmd.uid") ||
       !env.Get("mgm.fmd.gid") ||
       !env.Get("mgm.fmd.name") ||
       !env.Get("mgm.fmd.container") ||
       !env.Get("mgm.fmd.crc32") ||
       !env.Get("mgm.fmd.sequencetrailer"))
    return false;

  // base64 decode
  XrdOucString checksum64 = env.Get("mgm.fmd.checksum64");
  unsigned int checksumlen;
  memset(fmd.checksum, 0, SHA_DIGEST_LENGTH);

  char* decodebuffer=0;
  if (!XrdCommonSymKey::Base64Decode(checksum64, decodebuffer, checksumlen))
    return false;

  memcpy(fmd.checksum,decodebuffer,SHA_DIGEST_LENGTH);
  free(decodebuffer);

  fmd.magic           = strtoull(env.Get("mgm.fmd.magic"),NULL,0);
  fmd.sequencetrailer = strtoul(env.Get("mgm.fmd.sequenceheader"),NULL,0);
  fmd.fid             = strtoull(env.Get("mgm.fmd.fid"),NULL,0);
  fmd.cid             = strtoull(env.Get("mgm.fmd.cid"),NULL,0);
  fmd.fsid            = strtoul(env.Get("mgm.fmd.fsid"),NULL,0);
  fmd.ctime           = strtoul(env.Get("mgm.fmd.ctime"),NULL,0);
  fmd.ctime_ns        = strtoul(env.Get("mgm.fmd.ctime_ns"),NULL,0);
  fmd.mtime           = strtoul(env.Get("mgm.fmd.mtime"),NULL,0);
  fmd.mtime_ns        = strtoul(env.Get("mgm.fmd.mtime_ns"),NULL,0);
  fmd.size            = strtoull(env.Get("mgm.fmd.size"),NULL,0);
  fmd.lid             = strtoul(env.Get("mgm.fmd.lid"),NULL,0);
  fmd.uid             = (uid_t) strtoul(env.Get("mgm.fmd.uid"),NULL,0);
  fmd.gid             = (gid_t) strtoul(env.Get("mgm.fmd.gid"),NULL,0);
  strncpy(fmd.name,      env.Get("mgm.fmd.name"),255);
  strncpy(fmd.container, env.Get("mgm.fmd.container"),255);
  fmd.crc32           = strtoul(env.Get("mgm.fmd.crc32"),NULL,0);
  fmd.sequencetrailer = strtoul(env.Get("mgm.fmd.sequencetrailer"),NULL,0);
  
  return true;
}
