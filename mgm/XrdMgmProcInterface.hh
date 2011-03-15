#ifndef __XRDMGMOFS_PROCINTERFACE__HH__
#define __XRDMGMOFS_PROCINTERFACE__HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdCommon/XrdCommonMapping.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/

class XrdMgmProcCommand : public XrdCommonLogId {
private:
  XrdOucString path;
  XrdCommonMapping::VirtualIdentity* pVid;
  XrdOucString cmd;
  XrdOucString subcmd;
  XrdOucString args;

  XrdOucString stdOut;;
  XrdOucString stdErr;
  int retc;
  XrdOucString resultStream;

  XrdOucErrInfo*   error;

  size_t len;
  off_t  offset;
  void MakeResult(bool dosort=false);

  bool adminCmd;
  bool userCmd; 

public:

  int open(const char* path, const char* info, XrdCommonMapping::VirtualIdentity &vid, XrdOucErrInfo *error);
  int read(XrdSfsFileOffset offset, char *buff, XrdSfsXferSize blen);
  int stat(struct stat* buf);
  int close();

  XrdMgmProcCommand();
  ~XrdMgmProcCommand();
}; 

class XrdMgmProcInterface {
private:

public:

  static bool IsProcAccess(const char* path);
  static bool Authorize(const char* path, const char* info, XrdCommonMapping::VirtualIdentity &vid, const XrdSecEntity* entity);

  XrdMgmProcInterface();
  ~XrdMgmProcInterface();
};

#endif
