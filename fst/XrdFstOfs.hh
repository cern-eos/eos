// ----------------------------------------------------------------------
// File: XrdFstOfs.hh
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

#ifndef __EOSFST_FSTOFS_HH__
#define __EOSFST_FSTOFS_HH__

/*----------------------------------------------------------------------------*/
#include "authz/XrdCapability.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include "common/Fmd.hh"
#include "common/StringConversion.hh"
#include "fst/Namespace.hh"
#include "fst/storage/Storage.hh"
#include "fst/Config.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/layout/LayoutPlugin.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/Lock.hh"
#include "fst/Messaging.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqSharedObject.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdOfs/XrdOfs.hh"
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
  XrdFstOfsDirectory(const char *user, int MonID=0) : XrdSfsDirectory(user,MonID){eos::common::LogId();fts_tree=0;fts_paths=0;fsid=0;};
  virtual            ~XrdFstOfsDirectory() {
    close();
  }

  
  int         open(const char              *dirName,
                   const XrdSecClientName  *client = 0,
                   const char              *opaque = 0); 

  const char* nextEntry();

  const char* FName() {return (const char *)dirname.c_str();}

  int         close();
};


/*----------------------------------------------------------------------------*/
class XrdFstOfs : public XrdOfs, public eos::common::LogId {
  friend class XrdFstOfsDirectory;
  friend class XrdFstOfsFile;
  friend class eos::fst::Layout;
  //  friend class eos::fst::ReplicaLayout;
  // friend class eos::fst::ReplicaParLayout;
  friend class eos::fst::PlainLayout;
  // friend class eos::fst::RaidDpLayout;
  //friend class eos::fst::ReedSLayout;

 private:

 public:
  XrdSfsDirectory *newDir(char *user=0, int MonID=0) {return (XrdSfsDirectory *) new XrdFstOfsDirectory(user,MonID);}
  XrdSfsFile *newFile(char *user=0,int MonID=0) {return (XrdSfsFile *) new XrdFstOfsFile(user,MonID);}
 
  int Configure(XrdSysError &error);

  static void xrdfstofs_shutdown(int sig);

  XrdFstOfs() {
    eos::common::LogId(); Eroute = 0; Messaging = 0; Storage = 0; TransferScheduler = 0; 
    (void) signal(SIGINT,xrdfstofs_shutdown);
    (void) signal(SIGTERM,xrdfstofs_shutdown);
    (void) signal(SIGQUIT,xrdfstofs_shutdown);
    Simulate_IO_read_error = Simulate_IO_write_error = Simulate_XS_read_error = Simulate_XS_write_error = false;
  }

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
                      unsigned long           fsid=0, 
		      bool     ignoreifnotexist=false) ;
  
  int            remdir(const char             *dirName,
			XrdOucErrInfo          &out_error,
			const XrdSecEntity     *client,
			const char             *info = 0) { return SFS_OK;}
  
  int            rename(const char             *oldFileName,
			const char             *newFileName,
			XrdOucErrInfo          &out_error,
			const XrdSecEntity     *client,
			const char             *infoO = 0,
			const char            *infoN = 0) {return SFS_OK;}

  int            stat(  const char             *path,
			struct stat            *buf,
			XrdOucErrInfo          &out_error,
			const XrdSecEntity     *client,
			const char             *opaque = 0);

  int            CallManager(XrdOucErrInfo *error, const char* path, const char* manager, XrdOucString &capOpaqueFile, XrdOucString* return_result=0);


  // this function deals with plugin calls
  int            FSctl(int, XrdSfsFSctl&, XrdOucErrInfo&, const XrdSecEntity*);

  void           SetDebug(XrdOucEnv &env);
  void           SendRtLog(XrdMqMessage* message);
  void           SendFsck(XrdMqMessage* message);

  int            Stall(XrdOucErrInfo &error, int stime, const char *msg);
  int            Redirect(XrdOucErrInfo &error, const char* host, int &port);

  eos::fst::LockManager LockManager;
 
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
  std::queue <XrdOucString> ReportQueue; // queue where file transaction reports get stored and picked up by a thread running in Storage
  
  XrdSysMutex ErrorReportQueueMutex;
  std::queue <XrdOucString> ErrorReportQueue; // queue where log error are stored and picked up by a thread running in Storage
  
  XrdSysMutex WrittenFilesQueueMutex;
  std::queue <struct FmdSqlite::FMD> WrittenFilesQueue;// queue where modified/written files get stored and picked up by a thread running in Storage


  XrdMqSharedObjectManager ObjectManager;// -> managing shared objects

  void OpenFidString(unsigned long fsid, XrdOucString &outstring);

  XrdScheduler* TransferScheduler;      // -> TransferScheduler
  XrdSysMutex   TransferSchedulerMutex; // -> protecting the TransferScheduler

  void SetSimulationError(const char* tag); // -> allows to switch on error simulation in the OfsFile stack
  bool Simulate_IO_read_error;          // -> simulate an IO error on read
  bool Simulate_IO_write_error;         // -> simulate an IO error on write
  bool Simulate_XS_read_error;          // -> simulate a checksum error on read
  bool Simulate_XS_write_error;         // -> simulate a checksum error on write

  virtual ~XrdFstOfs() {};
};

/*----------------------------------------------------------------------------*/
extern XrdFstOfs gOFS;

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END

#endif


