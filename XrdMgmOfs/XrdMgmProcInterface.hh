#ifndef __XRDMGMOFS_PROCINTERFACE__HH__
#define __XRDMGMOFS_PROCINTERFACE__HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/

class XrdMgmProcCommand : public XrdCommonLogId {
private:
  XrdOucString path;
  uid_t uid;
  gid_t gid;
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
  bool projectAdminCmd;

public:

  int open(const char* path, const char* info, uid_t uid, gid_t gid, XrdOucErrInfo *error);
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
  static bool Authorize(const char* path, const char* info, uid_t uid, gid_t gid, const XrdSecEntity* entity);

  XrdMgmProcInterface();
  ~XrdMgmProcInterface();
};

#endif
