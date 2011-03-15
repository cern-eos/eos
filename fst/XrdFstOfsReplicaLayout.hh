#ifndef __XRDFSTOFS_REPLICALAYOUT_HH__
#define __XRDFSTOFS_REPLICALAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdFstOfs/XrdFstOfsFile.hh"
#include "XrdFstOfs/XrdFstOfsLayout.hh"
/*----------------------------------------------------------------------------*/
#include "XrdClient/XrdClient.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOfs/XrdOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

class XrdFstOfsReplicaLayout : public XrdFstOfsLayout {
private:
  int nReplica;
  int replicaIndex;
  XrdClient* replicaClient;
  XrdOucString replicaUrl;
  bool ioLocal;

public:

  XrdFstOfsReplicaLayout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *error);

  virtual int open(const char                *path,
		   XrdSfsFileOpenMode   open_mode,
		   mode_t               create_mode,
		   const XrdSecEntity        *client,
		   const char                *opaque);

  virtual int read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length);
  virtual int write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length);
  virtual int truncate(XrdSfsFileOffset offset);
  virtual int sync();
  virtual int close();

  virtual ~XrdFstOfsReplicaLayout();
};

#endif
