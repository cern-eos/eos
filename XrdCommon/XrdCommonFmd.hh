#ifndef __XRDCOMMON_FMD_HH__
#define __XRDCOMMON_FMD_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
// this is needed because of some openssl definition conflict!
#undef des_set_key
#include <google/dense_hash_map>
#include <google/sparsehash/densehashtable.h>
#include <sys/time.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <zlib.h>
#include <openssl/sha.h>

/*----------------------------------------------------------------------------*/
#define XRDCOMMONFMDHEADER_MAGIC 0xabcdabcdabcdabcd
#define XRDCOMMONFMDCREATE_MAGIC 0xffffffffffffffff
#define XRDCOMMONFMDDELETE_MAGIC 0xdddddddddddddddd

/*----------------------------------------------------------------------------*/
class XrdCommonFmdHeader : public XrdCommonLogId {
public:
  struct FMDHEADER {
    unsigned long long magic;
    char version[10];
    unsigned long long ctime;
    int fsid;
  } ;


  struct FMDHEADER fmdHeader;

  XrdCommonFmdHeader() {fmdHeader.magic = XRDCOMMONFMDHEADER_MAGIC; strncpy(fmdHeader.version,VERSION,10); fmdHeader.ctime=time(0);}
  ~XrdCommonFmdHeader() {};

  void SetId(int infsid) { fmdHeader.fsid = infsid;} 

  bool Read(int fd);
  bool Write(int fd);
};


class XrdCommonFmd : public XrdCommonLogId {

public:
  struct FMD {
    unsigned long long magic;     // XRDCOMMONFMDCREATE_MAGIC | XRDCOMMONFMDDELETE_MAGIC
    unsigned long sequenceheader; // must be equal to sequencetrailer
    unsigned long long fid;       // fileid
    unsigned long long cid;       // container id (e.g. directory id)
    unsigned long fsid;           // filesystem id
    unsigned long ctime;          // creation time 
    unsigned long ctime_ns;       // ns of creation time
    unsigned long mtime;          // modification time | deletion time
    unsigned long mtime_ns;       // ns of modification time
    unsigned long long size;      // size
    char checksum[SHA_DIGEST_LENGTH];            // checksum field
    unsigned long lid ;           // layout id
    uid_t uid;                    // creation|deletion user  id
    gid_t gid;                    // creation|deletion group id
    char name[256];               // name
    char container[256];          // container name
    unsigned long crc32;          // crc from fid to cgid including
    unsigned long sequencetrailer;// must be equal to sequenceheader
  };

  struct FMD fMd;

  static unsigned long ComputeCrc32(char* ptr, unsigned long size) {
    uLong crc = crc32(0L, Z_NULL, 0);
    return (unsigned long) crc32(crc,(const Bytef*) ptr, size);
  }

  static bool IsCreate(struct FMD* pMd) {
    return (pMd->magic == XRDCOMMONFMDCREATE_MAGIC);
  }
  static bool IsDelete(struct FMD* pMd) {
    return (pMd->magic == XRDCOMMONFMDDELETE_MAGIC);
  }

  static int IsValid(struct FMD* pMd, unsigned long &sequencenumber) {
    if ( (!IsCreate(pMd)) && (!IsDelete(pMd)) )           {return EINVAL;}
    if (pMd->sequenceheader != pMd->sequencetrailer) {return EFAULT;}
    if (pMd->sequenceheader <= sequencenumber)       {return EOVERFLOW;}
    fprintf(stderr,"computed CRC is %lx vs %lx", ComputeCrc32((char*)(&(pMd->fid)), sizeof(struct FMD) - sizeof(pMd->magic) - (2*sizeof(pMd->sequencetrailer)) - sizeof(pMd->crc32)), pMd->crc32);
    if (pMd->crc32 != ComputeCrc32((char*)(&(pMd->fid)), sizeof(struct FMD) - sizeof(pMd->magic) - (2*sizeof(pMd->sequencetrailer)) - sizeof(pMd->crc32) )) {return EILSEQ;}
    sequencenumber = pMd->sequenceheader;
    return 0;
  }
  
  bool Write(int fd);
  bool ReWrite(int fd);
  bool Read(int fd, off_t offset);
  void MakeCreationBlock() { fMd.magic =  XRDCOMMONFMDCREATE_MAGIC;}
  void MakeDeletionBlock() { fMd.magic =  XRDCOMMONFMDDELETE_MAGIC;}

  XrdCommonFmd(int fid=0, int fsid=0) {memset(&fMd,0, sizeof(struct FMD)); XrdCommonLogId();fMd.fid=fid; fMd.fsid=fsid;fMd.cid=0;};
  ~XrdCommonFmd() {};
};

/*----------------------------------------------------------------------------*/
class XrdCommonFmdHandler : public XrdCommonLogId {
private:
  bool isOpen;
public:
  google::dense_hash_map<int,int> fdChangeLogRead;
  google::dense_hash_map<int,int> fdChangeLogWrite;
  google::dense_hash_map<int,int> fdChangeLogSequenceNumber;

  XrdOucString ChangeLogFileName;
  XrdSysMutex Mutex;
  XrdCommonFmdHeader fmdHeader;
  
  bool SetChangeLogFile(const char* changelogfile, int fsid) ;
  bool AttachLatestChangeLogFile(const char* changelogdir, int fsid) ;
  bool ReadChangeLogHash(int fsid);

  // the meta data handling functions

  // attach or create a fmd record
  XrdCommonFmd* GetFmd(unsigned long long fid, unsigned int fsid, uid_t uid, gid_t gid, unsigned int layoutid, bool isRW=false);

  // create a deletion fmd record
  bool DeleteFmd(unsigned long long fid, unsigned int fsid);

  // commit a modified fmd record
  bool Commit(XrdCommonFmd* fmd);

  // initialize the changelog hash
  void Reset() {
    Fmd.clear();
  }
    
  static int CompareMtime(const void* a, const void *b);

  // that is all we need for meta data handling
  // hash map pointing from fid to offset in changelog file
  google::dense_hash_map<unsigned long long, google::dense_hash_map<unsigned long long, unsigned long long> > Fmd;
  // hash map with fid file sizes
  google::dense_hash_map<long long, unsigned long long> FmdSize;

  // that is all we need for quota
  google::dense_hash_map<long long, unsigned long long> UserBytes; // the key is encoded as (fsid<<32) | uid 
  google::dense_hash_map<long long, unsigned long long> GroupBytes;// the key is encoded as (fsid<<32) | gid
  google::dense_hash_map<long long, unsigned long long> UserFiles; // the key is encoded as (fsid<<32) | uid
  google::dense_hash_map<long long, unsigned long long> GroupFiles;// the key is encoded as (fsid<<32) | gid


  XrdCommonFmdHandler() {
    SetLogId("CommonFmdHandler"); isOpen=false;
    Fmd.set_empty_key(0);
    FmdSize.set_empty_key(-1);

    UserBytes.set_empty_key(-1);
    GroupBytes.set_empty_key(-1);
    UserFiles.set_empty_key(-1);
    GroupFiles.set_empty_key(-1);

    fdChangeLogRead.set_empty_key(0);
    fdChangeLogWrite.set_empty_key(0);
    fdChangeLogSequenceNumber.set_empty_key(0);
    ChangeLogFileName="";
  }
  ~XrdCommonFmdHandler() {};

};


/*----------------------------------------------------------------------------*/
extern XrdCommonFmdHandler gFmdHandler;

#endif
