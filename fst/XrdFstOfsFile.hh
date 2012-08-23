// ----------------------------------------------------------------------
// File: XrdFstOfsFile.hh
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

#ifndef __XRDFSTOFS_FSTOFSFILE_HH__
#define __XRDFSTOFS_FSTOFSFILE_HH__

/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <vector>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/Fmd.hh"
#include "common/ClientAdmin.hh"
#include "common/SecEntity.hh"
#include "fst/Namespace.hh"
#include "fst/layout/Layout.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/FmdSqlite.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
class XrdFstOfsFile : public XrdOfsFile, public eos::common::LogId {
  friend class Layout;
  friend class PlainLayout;
  friend class ReplicaLayout;
  friend class ReplicaParLayout;
  friend class Raid5Layout;

public:
  int          openofs(const char                *fileName,
		       XrdSfsFileOpenMode   openMode,
		       mode_t               createMode,
		       const XrdSecEntity        *client,
		       const char                *opaque = 0, 
		       bool openBlockXS=false,
		       unsigned long lid=0 );
  
  int          open(const char                *fileName,
                    XrdSfsFileOpenMode   openMode,
                    mode_t               createMode,
                    const XrdSecEntity        *client,
                    const char                *opaque = 0);
  
  int          closeofs();

  int          close();

  int          read(XrdSfsFileOffset   fileOffset,   // Preread only
                    XrdSfsXferSize     amount);
  
  XrdSfsXferSize read(XrdSfsFileOffset   fileOffset,
                      char              *buffer,
                      XrdSfsXferSize     buffer_size);


  XrdSfsXferSize readofs(XrdSfsFileOffset   fileOffset,
                         char              *buffer,
                         XrdSfsXferSize     buffer_size);
  
  int          read(XrdSfsAio *aioparm);
  
  XrdSfsXferSize write(XrdSfsFileOffset   fileOffset,
                       const char        *buffer,
                       XrdSfsXferSize     buffer_size);

  XrdSfsXferSize writeofs(XrdSfsFileOffset   fileOffset,
                          const char        *buffer,
                          XrdSfsXferSize     buffer_size);
  
  int          write(XrdSfsAio *aioparm);

  int          stat(struct stat *buf);
  bool         verifychecksum();
  int          sync();
  int          syncofs();

  int          sync(XrdSfsAio *aiop);

  int          truncate(XrdSfsFileOffset   fileOffset);
  int          truncateofs(XrdSfsFileOffset   fileOffset);

  XrdFstOfsFile(const char* user, int MonID=0) : XrdOfsFile(user,MonID){openOpaque = 0; capOpaque = 0; fstPath=""; fstBlockXS=0; fstBlockSize=0; eos::common::LogId(); closed=false; opened=false; haswrite=false; fMd = 0;checkSum = 0; layOut = 0; isRW= 0; isCreation = 0; srBytes=swBytes=rOffset=wOffset=0; rTime.tv_sec=wTime.tv_sec=lrTime.tv_sec=lwTime.tv_sec=rTime.tv_usec=wTime.tv_usec=lrTime.tv_usec=lwTime.tv_usec=cTime.tv_sec=cTime.tv_usec=0;fileid=0;fsid=0;lid=0;cid=0;rCalls=wCalls=0; localPrefix="";maxOffsetWritten=0;openSize=0;closeSize=0;isReplication=false; deleteOnClose=false; closeTime.tv_sec = closeTime.tv_usec = openTime.tv_sec = openTime.tv_usec = tz.tz_dsttime = tz.tz_minuteswest = 0;viaDelete=false;SecString="";}
  virtual ~XrdFstOfsFile() {
    viaDelete = true;
    if (!closed) {
      close();
    }
    if (openOpaque) {delete openOpaque; openOpaque=0;}
    if (capOpaque)  {delete capOpaque;  capOpaque =0;}
    // unmap the MD record
    if (fMd) {delete fMd; fMd = 0;}
    if (checkSum) { delete checkSum; checkSum = 0;}
    if (layOut) { delete layOut; layOut = 0;}
    if (fstBlockXS) { fstBlockXS->CloseMap(); delete fstBlockXS; fstBlockXS=0; }
  }

protected:
  XrdOucEnv*   openOpaque;
  XrdOucEnv*   capOpaque;
  XrdOucString fstPath;
  CheckSum*    fstBlockXS;
  off_t        bookingsize;
  off_t        targetsize;
  bool         viaDelete;

  unsigned long long fstBlockSize;

  
  XrdOucString Path;
  XrdOucString localPrefix;
  XrdOucString RedirectManager; // -> host where we bounce back 
  XrdOucString SecString;       // -> authentication/application information
  XrdSysMutex  BlockXsMutex;
  XrdSysMutex  ChecksumMutex;

  unsigned long long fileid; // file id
  unsigned long fsid;        // file system id
  unsigned long lid;         // layout id
  unsigned long long cid;    // container id

  XrdOucString hostName;

  bool         closed;
  bool         opened;
  bool         haswrite;
  bool         isRW;
  bool         isCreation;
  bool         isReplication;
  bool         deleteOnClose;
  FmdSqlite*   fMd;
  eos::fst::CheckSum* checkSum;
  eos::fst::Layout*  layOut;
  
  unsigned long long maxOffsetWritten; // largest byte position written of a new created file

  off_t        openSize;
  off_t        closeSize;

  ///////////////////////////////////////////////////////////
  // file statistics
  struct timeval openTime;
  struct timeval closeTime;
  struct timezone tz;
  XrdSysMutex vecMutex;                 // protecting the rvec/wvec variables
  std::vector<unsigned long long> rvec; // vector with all read  sizes -> to compute sigma,min,max,total
  std::vector<unsigned long long> wvec; // vector with all write sizes -> to compute sigma,min,max,total
  unsigned long long srBytes;// sum bytes seeked
  unsigned long long swBytes;// sum bytes seeked
  unsigned long rCalls;      // number of read calls
  unsigned long wCalls;      // number of write calls
  unsigned long long rOffset;// offset since last read operation on this file
  unsigned long long wOffset;// offset since last write operation on this file

  struct timeval cTime;      // current time
  struct timeval lrTime;     // last read time
  struct timeval lwTime;     // last write time
  struct timeval rTime;      // sum time to serve read requests in ms
  struct timeval wTime;      // sum time to serve write requests in ms
  XrdOucString   tIdent;     // tident

  void AddReadTime() {
    unsigned long mus = ((lrTime.tv_sec-cTime.tv_sec)*1000000) + lrTime.tv_usec - cTime.tv_usec;
    rTime.tv_sec  += (mus/1000000);
    rTime.tv_usec += (mus%1000000);
  }

  void AddWriteTime() {
    unsigned long mus = ((lwTime.tv_sec-cTime.tv_sec)*1000000) + lwTime.tv_usec - cTime.tv_usec;
    wTime.tv_sec  += (mus/1000000);
    wTime.tv_usec += (mus%1000000);
  }
  
  void MakeReportEnv(XrdOucString &reportString);
};

EOSFSTNAMESPACE_END

#endif
