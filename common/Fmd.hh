// ----------------------------------------------------------------------
// File: Fmd.hh
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

/**
 * @file   Fmd.hh
 * 
 * @brief  Classes for FST File Meta Data Handling.
 * 
 * 
 */

#ifndef __EOSCOMMON_FMD_HH__
#define __EOSCOMMON_FMD_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Logging.hh"
#include "common/SymKeys.hh"
#include "common/ClientAdmin.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
// this is needed because of some openssl definition conflict!
#undef des_set_key
#include <google/dense_hash_map>
#include <google/sparse_hash_map>
#include <google/sparsehash/densehashtable.h>
#include <sys/time.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <zlib.h>
#include <openssl/sha.h>


#ifdef __APPLE__
#define ECOMM 70
#endif

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
#define EOSCOMMONFMDHEADER_MAGIC 0xabcdabcdabcdabcdULL
#define EOSCOMMONFMDCREATE_MAGIC 0xffffffffffffffffULL
#define EOSCOMMONFMDDELETE_MAGIC 0xddddddddddddddddULL

#define FMDVERSION "1.0.0"

/*----------------------------------------------------------------------------*/
//! Class implementing the file meta header stored in changelog files on FSTs.
/*----------------------------------------------------------------------------*/
class FmdHeader : public LogId {
public:

  // ---------------------------------------------------------------------------
  //! Struct of the header found at the beginning of each FMD changelog file
  // ---------------------------------------------------------------------------
  struct FMDHEADER {
    //! magic to indentify changelog files
    unsigned long long magic;
    //! version string of the changelog file format
    char version[10];
    //! creation time as unix timestamp
    unsigned long long ctime;
    //! filesystem id stored in this changelog file
    int fsid;
  } ;

  // ---------------------------------------------------------------------------
  //! FMD header object
  // ---------------------------------------------------------------------------
  struct FMDHEADER fmdHeader;
  
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  FmdHeader() {fmdHeader.magic = EOSCOMMONFMDHEADER_MAGIC; strncpy(fmdHeader.version,FMDVERSION,10); fmdHeader.ctime=time(0); fmdHeader.fsid=0;}
  
  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~FmdHeader() {};

  // ---------------------------------------------------------------------------
  //! Set the filesystem id into the header
  // ---------------------------------------------------------------------------
  void SetId(int infsid) { fmdHeader.fsid = infsid;} 

  // ---------------------------------------------------------------------------
  //! Read a header from an open file
  // ---------------------------------------------------------------------------
  bool Read(int fd, bool ignoreversion=false);

  // ---------------------------------------------------------------------------
  //! Write a header to an open file
  // ---------------------------------------------------------------------------
  bool Write(int fd);

  // ---------------------------------------------------------------------------
  //! Dump a header
  // ---------------------------------------------------------------------------
  static void Dump(struct FMDHEADER* header);
};

// ---------------------------------------------------------------------------
//! Class implementing file meta data
// ---------------------------------------------------------------------------
class Fmd : public LogId {

public:
  // ---------------------------------------------------------------------------
  //! Changelog entry struct
  // ---------------------------------------------------------------------------
  struct FMD {
    unsigned long long magic;     //< EOSCOMMONFMDCREATE_MAGIC | EOSCOMMONFMDDELETE_MAGIC
    unsigned long sequenceheader; //< must be equal to sequencetrailer
    unsigned long long fid;       //< fileid
    unsigned long long cid;       //< container id (e.g. directory id)
    unsigned long fsid;           //< filesystem id
    unsigned long ctime;          //< creation time 
    unsigned long ctime_ns;       //< ns of creation time
    unsigned long mtime;          //< modification time | deletion time
    unsigned long mtime_ns;       //< ns of modification time
    unsigned long long size;      //< size
    char checksum[SHA_DIGEST_LENGTH]; //< checksum field
    unsigned long lid ;           //< layout id
    uid_t uid;                    //< creation|deletion user  id
    gid_t gid;                    //< creation|deletion group id
    char name[256];               //< name
    char container[256];          //< container name
    unsigned long crc32;          //< crc32 from fid to cgid including
    unsigned long sequencetrailer;//< must be equal to sequenceheader
  };

  // ---------------------------------------------------------------------------
  //! File Metadata object 
  struct FMD fMd;
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  //! File meta data object replication function (copy constructor)
  // ---------------------------------------------------------------------------

  void Replicate(struct FMD &fmd) {
    fMd.magic = fmd.magic;
    fMd.fid   = fmd.fid;
    fMd.cid   = fmd.cid;
    fMd.ctime = fmd.ctime;
    fMd.ctime_ns = fmd.ctime_ns;
    fMd.mtime = fmd.mtime;
    fMd.mtime_ns = fmd.mtime_ns;
    fMd.size  = fmd.size;
    memcpy(fMd.checksum, fmd.checksum, SHA_DIGEST_LENGTH);
    fMd.lid   = fmd.lid;
    fMd.uid   = fmd.uid;
    fMd.gid   = fmd.gid;
    strncpy(fMd.name, fmd.name, 255);
    strncpy(fMd.container, fmd.container,255);
  }

  // ---------------------------------------------------------------------------
  //! return the crc32 for the given ptr/size
  // ---------------------------------------------------------------------------
  static unsigned long ComputeCrc32(char* ptr, unsigned long size) {
    uLong crc = crc32(0L, Z_NULL, 0);
    return (unsigned long) crc32(crc,(const Bytef*) ptr, size);
  }

  // ---------------------------------------------------------------------------
  //! Check if the given FMD is a creation block
  // ---------------------------------------------------------------------------
  static bool IsCreate(struct FMD* pMd) {
    return (pMd->magic == EOSCOMMONFMDCREATE_MAGIC);
  }
  
  // ---------------------------------------------------------------------------
  //! Check if the given FMD is a deletion block
  // ---------------------------------------------------------------------------
  static bool IsDelete(struct FMD* pMd) {
    return (pMd->magic == EOSCOMMONFMDDELETE_MAGIC);
  }

  // ---------------------------------------------------------------------------
  //! Check if the given FMD is valid and the sequence number is ascending
  // ---------------------------------------------------------------------------
  static int IsValid(struct FMD* pMd, unsigned long &sequencenumber) {
    if ( (!IsCreate(pMd)) && (!IsDelete(pMd)) )           {return EINVAL;}
    if (pMd->sequenceheader != pMd->sequencetrailer) {return EFAULT;}
    if (pMd->sequenceheader <= sequencenumber)       {return EOVERFLOW;}
    //    fprintf(stderr,"computed CRC is %lx vs %lx", ComputeCrc32((char*)(&(pMd->fid)), sizeof(struct FMD) - sizeof(pMd->magic) - (2*sizeof(pMd->sequencetrailer)) - sizeof(pMd->crc32)), pMd->crc32);
    if (pMd->crc32 != ComputeCrc32((char*)(&(pMd->fid)), sizeof(struct FMD) - sizeof(pMd->magic) - (2*sizeof(pMd->sequencetrailer)) - sizeof(pMd->crc32) )) {return EILSEQ;}
    sequencenumber = pMd->sequenceheader;
    return 0;
  }
  
  // ---------------------------------------------------------------------------
  //! Write FMD to an open file
  // ---------------------------------------------------------------------------
  bool Write(int fd);
  
  // ---------------------------------------------------------------------------
  //! Re-Write FMD to an open file
  // ---------------------------------------------------------------------------
  bool ReWrite(int fd);

  // ---------------------------------------------------------------------------
  //! Read filemeta data from open file at offset
  // ---------------------------------------------------------------------------
  bool Read(int fd, off_t offset);

  // ---------------------------------------------------------------------------
  //! Make FMD a creation block
  // ---------------------------------------------------------------------------
  void MakeCreationBlock() { fMd.magic =  EOSCOMMONFMDCREATE_MAGIC;}

  // ---------------------------------------------------------------------------
  //! Make FMD a deletion bock
  // ---------------------------------------------------------------------------
  void MakeDeletionBlock() { fMd.magic =  EOSCOMMONFMDDELETE_MAGIC;}
  
  // ---------------------------------------------------------------------------
  //! Dump FMD
  // ---------------------------------------------------------------------------
  static void Dump(struct FMD* fmd);
  
  // ---------------------------------------------------------------------------
  //! Convert FMD into env representation
  // ---------------------------------------------------------------------------
  XrdOucEnv* FmdToEnv();

  // ---------------------------------------------------------------------------
  //! Conver env representation into FMD
  // ---------------------------------------------------------------------------
  static bool EnvToFmd(XrdOucEnv &env, struct Fmd::FMD &fmd);

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  Fmd(int fid=0, int fsid=0) {memset(&fMd,0, sizeof(struct FMD)); LogId();fMd.fid=fid; fMd.fsid=fsid;fMd.cid=0;};

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~Fmd() {};
};

// ---------------------------------------------------------------------------
//! Class handling many FMD changelog files at a time
// ---------------------------------------------------------------------------

class FmdHandler : public LogId {
private:
  bool isOpen;
public:
  google::sparse_hash_map<int,int> fdChangeLogRead;  //< hash storing the file descriptor's for read by filesystem id
  google::sparse_hash_map<int,int> fdChangeLogWrite; //< hash storing the file descriptor's for write by filesystem id
  google::sparse_hash_map<int,int> fdChangeLogSequenceNumber; //< hash storing the last sequence number by filesystem id

  XrdOucString ChangeLogFileName;//< buffer variable for the changelog file name - not useful since we have many
  XrdOucString ChangeLogDir;     //< path to the directory with changelog files
  RWMutex Mutex;                 //< Mutex protecting the FMD handler
  FmdHeader fmdHeader;           //< buffer variable to store a header

  // ---------------------------------------------------------------------------
  //! Define a changelog file for a filesystem id
  // ---------------------------------------------------------------------------
  bool SetChangeLogFile(const char* changelogfile, int fsid, XrdOucString option="") ;

  // ---------------------------------------------------------------------------
  //! Attach to an existing changelog file
  // ---------------------------------------------------------------------------
  bool AttachLatestChangeLogFile(const char* changelogdir, int fsid) ;

  // ---------------------------------------------------------------------------
  //! Read all FMD entries from a changelog file
  // ---------------------------------------------------------------------------
  bool ReadChangeLogHash(int fsid, XrdOucString option="");

  // ---------------------------------------------------------------------------
  //! Trim a changelog file
  // ---------------------------------------------------------------------------
  bool TrimLogFile(int fsid, XrdOucString option="");

  // the meta data handling functions
  
  // ---------------------------------------------------------------------------
  //! attach or create a fmd record
  // ---------------------------------------------------------------------------
  Fmd* GetFmd(unsigned long long fid, unsigned int fsid, uid_t uid, gid_t gid, unsigned int layoutid, bool isRW=false);

  // ---------------------------------------------------------------------------
  //! Create a deletion fmd record
  // ---------------------------------------------------------------------------
  bool DeleteFmd(unsigned long long fid, unsigned int fsid);

  // ---------------------------------------------------------------------------
  //! Commit a modified fmd record
  // ---------------------------------------------------------------------------
  bool Commit(Fmd* fmd);

  // ---------------------------------------------------------------------------
  //! Initialize the changelog hash
  // ---------------------------------------------------------------------------
  void Reset(int fsid) {
    FmdMap[fsid].clear();
  }
    
  // ---------------------------------------------------------------------------
  //! Comparison function for modification times
  // ---------------------------------------------------------------------------
  static int CompareMtime(const void* a, const void *b);

  // that is all we need for meta data handling

  // ---------------------------------------------------------------------------
  //! Hash map pointing from fid to offset in changelog file
  // ---------------------------------------------------------------------------
  google::sparse_hash_map<unsigned long long, google::dense_hash_map<unsigned long long, unsigned long long> > FmdMap;

  // ---------------------------------------------------------------------------
  //! Hash map with fid file sizes
  google::dense_hash_map<long long, unsigned long long> FmdSize;

  // ---------------------------------------------------------------------------
  //! Create a new changelog filename in 'dir' (the fsid suffix is not added!)
  // ---------------------------------------------------------------------------
  const char* CreateChangeLogName(const char* cldir, XrdOucString &clname) {
    clname = cldir; clname += "/"; clname += "fmd."; char now[1024]; sprintf(now,"%u",(unsigned int) time(0)); clname += now;
    return clname.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Retrieve FMD from a remote machine
  // ---------------------------------------------------------------------------
  int GetRemoteFmd(ClientAdmin* admin, const char* serverurl, const char* shexfid, const char* sfsid, struct Fmd::FMD &fmd);
  int GetRemoteAttribute(ClientAdmin* admin, const char* serverurl, const char* key, const char* path, XrdOucString& attribute);

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  FmdHandler() {
    SetLogId("CommonFmdHandler"); isOpen=false;
    FmdSize.set_empty_key(0xfffffffe);
    FmdSize.set_deleted_key(0xffffffff);
    ChangeLogFileName="";
    ChangeLogDir="";
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~FmdHandler() {};

};


// ---------------------------------------------------------------------------
extern FmdHandler gFmdHandler;

EOSCOMMONNAMESPACE_END

#endif
