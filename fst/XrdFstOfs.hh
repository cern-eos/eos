#ifndef __XRDFSTOFS_FSTOFS_HH__
#define __XRDFSTOFS_FSTOFS_HH__

/*----------------------------------------------------------------------------*/
#include "authz/XrdCapability.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include "common/Fmd.hh"
#include "common/ClientAdmin.hh"
#include "common/StringConversion.hh"
#include "fst/Namespace.hh"
#include "fst/storage/Storage.hh"
#include "fst/Config.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/layout/LayoutPlugins.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/Lock.hh"
#include "fst/Messaging.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqSharedObject.hh"

/*----------------------------------------------------------------------------*/
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "Xrd/XrdScheduler.hh"
/*----------------------------------------------------------------------------*/
#include <sys/mman.h>
#include <fts.h>
#include <queue>
/*----------------------------------------------------------------------------*/

#ifdef __APPLE__
#define EREMOTEIO 121
#endif

EOSFSTNAMESPACE_BEGIN

class XrdFstOfsDirectory : public XrdSfsDirectory, public eos::common::LogId {
private:
  FTS *fts_tree;
  char **fts_paths;
  XrdOucString entry;
  XrdOucString dirname;
  eos::common::FileSystem::fsid_t fsid;
  
public:
  XrdFstOfsDirectory(const char *user) : XrdSfsDirectory(user){eos::common::LogId();fts_tree=0;fts_paths=0;fsid=0;};
  virtual            ~XrdFstOfsDirectory() {
    close();
  }

  
  int         open(const char              *dirName,
                   const XrdSecClientName  *client = 0,
                   const char              *opaque = 0); 

  const char *nextEntry();

  const   char       *FName() {return (const char *)dirname.c_str();}

  int         close();
};

/*----------------------------------------------------------------------------*/
class XrdFstOfs : public XrdOfs, public eos::common::LogId {
  friend class XrdFstOfsDirectory;
  friend class XrdFstOfsFile;
  friend class eos::fst::Layout;
  friend class eos::fst::ReplicaLayout;
  friend class eos::fst::ReplicaParLayout;
  friend class eos::fst::PlainLayout;
  friend class eos::fst::Raid5Layout;
private:

public:
  XrdSfsDirectory *newDir(char *user=0) {return (XrdSfsDirectory *) new XrdFstOfsDirectory(user);}
  XrdSfsFile *newFile(char *user=0) {return (XrdSfsFile *) new XrdFstOfsFile(user);}
 
  int Configure(XrdSysError &error);

  XrdFstOfs() {eos::common::LogId(); Eroute = 0; Messaging = 0; Storage = 0; TransferScheduler = 0;}

  XrdSysError*        Eroute;          // used by the 

  // here we mask all illegal operations
  int            chmod(const char             *Name,
                       XrdSfsMode        Mode,
                       XrdOucErrInfo    &out_error,
                       const XrdSecEntity     *client,
                       const char             *opaque = 0) { return SFS_OK;}
  
  int            exists(const char                *fileName,
                        XrdSfsFileExistence &exists_flag,
                        XrdOucErrInfo       &out_error,
                        const XrdSecEntity        *client,
                        const char                *opaque = 0) { return SFS_OK;}


  
  int            fsctl(const int               cmd,
                       const char             *args,
                       XrdOucErrInfo    &out_error,
                       const XrdSecEntity     *client);

  int            mkdir(const char             *dirName,
                       XrdSfsMode        Mode,
                       XrdOucErrInfo    &out_error,
                       const XrdSecEntity     *client,
                       const char             *opaque = 0) { return SFS_OK;}
  
  int            prepare(      XrdSfsPrep       &pargs,
                               XrdOucErrInfo    &out_error,
                               const XrdSecEntity     *client = 0) { return SFS_OK;}
  
  
  int            rem(const char             *path,
                     XrdOucErrInfo    &out_error,
                     const XrdSecEntity     *client,
                     const char             *info = 0) ;

  int            _rem(const char             *path,
                      XrdOucErrInfo           &out_error,
                      const XrdSecEntity     *client,
                      XrdOucEnv              *info = 0, 
                      const char*               fstPath=0, 
                      unsigned long long      fid=0,
                      unsigned long           fsid=0) ;
  
  int            remdir(const char             *dirName,
                        XrdOucErrInfo    &out_error,
                        const XrdSecEntity     *client,
                        const char             *info = 0) { return SFS_OK;}
  
  int            rename(const char             *oldFileName,
                        const char             *newFileName,
                        XrdOucErrInfo    &out_error,
                        const XrdSecEntity     *client,
                        const char             *infoO = 0,
                        const char            *infoN = 0) {return SFS_OK;}

  int            stat(  const char             *path,
                        struct stat      *buf,
                        XrdOucErrInfo    &out_error,
                        const XrdSecEntity     *client,
                        const char             *opaque = 0);

  int            CallManager(XrdOucErrInfo *error, const char* path, const char* manager, XrdOucString &capOpaqueFile);


  // this function deals with plugin calls
  int            FSctl(int, XrdSfsFSctl&, XrdOucErrInfo&, const XrdSecEntity*);

  void           SetDebug(XrdOucEnv &env);
  void           SendRtLog(XrdMqMessage* message);

  int            Stall(XrdOucErrInfo &error, int stime, const char *msg);
  int            Redirect(XrdOucErrInfo &error, const char* host, int &port);

  eos::fst::LockManager LockManager;
 
  eos::common::ClientAdminManager ClientAdminManager;
  eos::fst::Messaging* Messaging;      // -> messaging interface class
  eos::fst::Storage* Storage;          // -> Meta data & filesytem store object

  XrdSysMutex OpenFidMutex;
  //  std::map<eos::common::FileSystem::fsid_t, std::map<unsigned long long, unsigned int> > WOpenFid;
  //  std::map<eos::common::FileSystem::fsid_t, std::map<unsigned long long, unsigned int> > ROpenFId;
  google::sparse_hash_map<eos::common::FileSystem::fsid_t, google::sparse_hash_map<unsigned long long, unsigned int> > WOpenFid;
  google::sparse_hash_map<eos::common::FileSystem::fsid_t, google::sparse_hash_map<unsigned long long, unsigned int> > ROpenFid;

  XrdSysMutex XSLockFidMutex;
  google::sparse_hash_map<eos::common::FileSystem::fsid_t, google::sparse_hash_map<unsigned long long, unsigned int> > XSLockFid;


  XrdSysMutex ReportQueueMutex;
  std::queue <XrdOucString> ReportQueue;

  XrdSysMutex ErrorReportQueueMutex;
  std::queue <std::string> ErrorReportQueue;

  XrdMqSharedObjectManager ObjectManager;// -> managing shared objects

  void OpenFidString(unsigned long fsid, XrdOucString &outstring);

  XrdScheduler* TransferScheduler;      // -> TransferScheduler
  XrdSysMutex   TransferSchedulerMutex; // -> protecting the TransferScheduler

  virtual ~XrdFstOfs() {};
};

/*----------------------------------------------------------------------------*/
extern XrdFstOfs gOFS;

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END

#endif


