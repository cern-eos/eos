#ifndef __XRDFSTOFS_FSTOFSFILE_HH__
#define __XRDFSTOFS_FSTOFSFILE_HH__

class XrdFstOfsFile;

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdCommon/XrdCommonFmd.hh"
#include "XrdFstOfs/XrdFstOfsClientAdmin.hh"
#include "XrdFstOfs/XrdFstOfsLayout.hh"
#include "XrdFstOfs/XrdFstOfsChecksum.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
class XrdFstOfsFile : public XrdOfsFile, public XrdCommonLogId {
  friend class XrdFstOfsLayout;
  friend class XrdFstOfsPlainLayout;
  friend class XrdFstOfsReplicaLayout;

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


  XrdFstOfsFile(const char* user) : XrdOfsFile(user){openOpaque = 0; capOpaque = 0; fstPath=""; XrdCommonLogId(); closed=false; haswrite=false; fMd = 0;checkSum = 0; layOut = 0; isRW= 0;}
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
  unsigned long fileId;
  bool         closed;
  bool         haswrite;
  bool         isRW;
  XrdCommonFmd* fMd;
  XrdFstOfsChecksum* checkSum;
  XrdFstOfsLayout*  layOut;
};

#endif
