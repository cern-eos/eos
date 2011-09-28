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
class FmdHeader : public LogId {
public:
  struct FMDHEADER {
    unsigned long long magic;
    char version[10];
    unsigned long long ctime;
    int fsid;
  } ;


  struct FMDHEADER fmdHeader;

  FmdHeader() {fmdHeader.magic = EOSCOMMONFMDHEADER_MAGIC; strncpy(fmdHeader.version,FMDVERSION,10); fmdHeader.ctime=time(0);}
  ~FmdHeader() {};

  void SetId(int infsid) { fmdHeader.fsid = infsid;} 

  bool Read(int fd, bool ignoreversion=false);
  bool Write(int fd);

  static void Dump(struct FMDHEADER* header);
};


class Fmd : public LogId {

public:
  struct FMD {
    unsigned long long magic;     // EOSCOMMONFMDCREATE_MAGIC | EOSCOMMONFMDDELETE_MAGIC
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

  static unsigned long ComputeCrc32(char* ptr, unsigned long size) {
    uLong crc = crc32(0L, Z_NULL, 0);
    return (unsigned long) crc32(crc,(const Bytef*) ptr, size);
  }

  static bool IsCreate(struct FMD* pMd) {
    return (pMd->magic == EOSCOMMONFMDCREATE_MAGIC);
  }
  static bool IsDelete(struct FMD* pMd) {
    return (pMd->magic == EOSCOMMONFMDDELETE_MAGIC);
  }

  static int IsValid(struct FMD* pMd, unsigned long &sequencenumber) {
    if ( (!IsCreate(pMd)) && (!IsDelete(pMd)) )           {return EINVAL;}
    if (pMd->sequenceheader != pMd->sequencetrailer) {return EFAULT;}
    if (pMd->sequenceheader <= sequencenumber)       {return EOVERFLOW;}
    //    fprintf(stderr,"computed CRC is %lx vs %lx", ComputeCrc32((char*)(&(pMd->fid)), sizeof(struct FMD) - sizeof(pMd->magic) - (2*sizeof(pMd->sequencetrailer)) - sizeof(pMd->crc32)), pMd->crc32);
    if (pMd->crc32 != ComputeCrc32((char*)(&(pMd->fid)), sizeof(struct FMD) - sizeof(pMd->magic) - (2*sizeof(pMd->sequencetrailer)) - sizeof(pMd->crc32) )) {return EILSEQ;}
    sequencenumber = pMd->sequenceheader;
    return 0;
  }
  
  bool Write(int fd);
  bool ReWrite(int fd);
  bool Read(int fd, off_t offset);
  void MakeCreationBlock() { fMd.magic =  EOSCOMMONFMDCREATE_MAGIC;}
  void MakeDeletionBlock() { fMd.magic =  EOSCOMMONFMDDELETE_MAGIC;}
  
  static void Dump(struct FMD* fmd);
  
  XrdOucEnv* FmdToEnv();
  static bool EnvToFmd(XrdOucEnv &env, struct Fmd::FMD &fmd);

  Fmd(int fid=0, int fsid=0) {memset(&fMd,0, sizeof(struct FMD)); LogId();fMd.fid=fid; fMd.fsid=fsid;fMd.cid=0;};
  ~Fmd() {};
};

/*----------------------------------------------------------------------------*/
class FmdHandler : public LogId {
private:
  bool isOpen;
public:
  google::sparse_hash_map<int,int> fdChangeLogRead;
  google::sparse_hash_map<int,int> fdChangeLogWrite;
  google::sparse_hash_map<int,int> fdChangeLogSequenceNumber;

  XrdOucString ChangeLogFileName;
  XrdOucString ChangeLogDir;
  RWMutex Mutex;
  FmdHeader fmdHeader;
  
  bool SetChangeLogFile(const char* changelogfile, int fsid, XrdOucString option="") ;
  bool AttachLatestChangeLogFile(const char* changelogdir, int fsid) ;
  bool ReadChangeLogHash(int fsid, XrdOucString option="");

  bool TrimLogFile(int fsid, XrdOucString option="");

  // the meta data handling functions

  // attach or create a fmd record
  Fmd* GetFmd(unsigned long long fid, unsigned int fsid, uid_t uid, gid_t gid, unsigned int layoutid, bool isRW=false);

  // create a deletion fmd record
  bool DeleteFmd(unsigned long long fid, unsigned int fsid);

  // commit a modified fmd record
  bool Commit(Fmd* fmd);

  // initialize the changelog hash
  void Reset(int fsid) {
    FmdMap[fsid].clear();
  }
    
  static int CompareMtime(const void* a, const void *b);

  // that is all we need for meta data handling
  // hash map pointing from fid to offset in changelog file
  google::sparse_hash_map<unsigned long long, google::dense_hash_map<unsigned long long, unsigned long long> > FmdMap;
  // hash map with fid file sizes
  google::dense_hash_map<long long, unsigned long long> FmdSize;

  // create a new changelog filename in 'dir' (the fsid suffix is not added!)
  const char* CreateChangeLogName(const char* cldir, XrdOucString &clname) {
    clname = cldir; clname += "/"; clname += "fmd."; char now[1024]; sprintf(now,"%u",(unsigned int) time(0)); clname += now;
    return clname.c_str();
  }

  // remote fmd
  int GetRemoteFmd(ClientAdmin* admin, const char* serverurl, const char* shexfid, const char* sfsid, struct Fmd::FMD &fmd);


  FmdHandler() {
    SetLogId("CommonFmdHandler"); isOpen=false;
    FmdSize.set_empty_key(0xfffffffe);
    FmdSize.set_deleted_key(0xffffffff);
    ChangeLogFileName="";
    ChangeLogDir="";
  }
  ~FmdHandler() {};

};


/*----------------------------------------------------------------------------*/
extern FmdHandler gFmdHandler;

EOSCOMMONNAMESPACE_END

#endif
