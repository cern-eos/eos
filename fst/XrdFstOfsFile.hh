#ifndef __XRDFSTOFS_FSTOFSFILE_HH__
#define __XRDFSTOFS_FSTOFSFILE_HH__

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/Fmd.hh"
#include "common/ClientAdmin.hh"
#include "fst/Namespace.hh"
#include "fst/layout/Layout.hh"
#include "fst/checksum/CheckSum.hh"
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
		    const char                *opaque = 0);

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

  int          sync();
  int          syncofs();

  int          sync(XrdSfsAio *aiop);

  int          truncate(XrdSfsFileOffset   fileOffset);
  int          truncateofs(XrdSfsFileOffset   fileOffset);

  XrdFstOfsFile(const char* user) : XrdOfsFile(user){openOpaque = 0; capOpaque = 0; fstPath=""; eos::common::LogId(); closed=false; opened=false; haswrite=false; fMd = 0;checkSum = 0; layOut = 0; isRW= 0; rBytes=wBytes=srBytes=swBytes=rOffset=wOffset=0; rTime.tv_sec=wTime.tv_sec=lrTime.tv_sec=lwTime.tv_sec=rTime.tv_usec=wTime.tv_usec=lrTime.tv_usec=lwTime.tv_usec=cTime.tv_sec=cTime.tv_usec=0;fileid=0;fsid=0;lid=0;cid=0;rCalls=wCalls=0; localPrefix="";}
  virtual ~XrdFstOfsFile() {
    close();
    if (openOpaque) {delete openOpaque; openOpaque=0;}
    if (capOpaque)  {delete capOpaque;  capOpaque =0;}
    // unmap the MD record
    if (fMd) {delete fMd; fMd = 0;}
    if (checkSum) { delete checkSum;}
    if (layOut) { delete layOut;}
  }

protected:
  XrdOucEnv*   openOpaque;
  XrdOucEnv*   capOpaque;
  XrdOucString fstPath;
  XrdOucString Path;
  XrdOucString localPrefix;

  unsigned long long fileid; // file id
  unsigned long fsid;        // file system id
  unsigned long lid;         // layout id
  unsigned long long cid;    // container id

  XrdOucString hostName;

  bool         closed;
  bool         opened;
  bool         haswrite;
  bool         isRW;
  eos::common::Fmd* fMd;
  eos::fst::CheckSum* checkSum;
  eos::fst::Layout*  layOut;
  

  ///////////////////////////////////////////////////////////
  // file statistics
  struct timeval openTime;
  struct timeval closeTime;
  struct timezone tz;
  unsigned long long rBytes; // sum bytes read
  unsigned long long wBytes; // sum bytes written
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
  
  void MakeReportEnv(XrdOucString &reportString) {
    char report[16384];
    sprintf(report,"log=%s&path=%s&ruid=%u&rgid=%u&td=%s&host=%s&lid=%lu&fid=%llu&fsid=%lu&ots=%lu&otms=%lu&&cts=%lu&ctms=%lu&rb=%llu&wb=%llu&srb=%llu&swb=%llu&nrc=%lu&nwc=%lu&rt=%.02f&wt=%.02f",this->logId,Path.c_str(),this->vid.uid,this->vid.gid, tIdent.c_str(), hostName.c_str(),lid, fileid, fsid, openTime.tv_sec, (unsigned long)openTime.tv_usec/1000,closeTime.tv_sec,(unsigned long)closeTime.tv_usec/1000,rBytes,wBytes,srBytes,swBytes,rCalls, wCalls,((rTime.tv_sec*1000.0)+(rTime.tv_usec/1000.0)), ((wTime.tv_sec*1000.0) + (wTime.tv_usec/1000.0)));
    reportString = report;
  }

};

EOSFSTNAMESPACE_END

#endif
