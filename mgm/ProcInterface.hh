#ifndef __EOSMGM_PROCINTERFACE__HH__
#define __EOSMGM_PROCINTERFACE__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class ProcCommand : public eos::common::LogId {
private:
  XrdOucString path;
  eos::common::Mapping::VirtualIdentity* pVid;
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

  int open(const char* path, const char* info, eos::common::Mapping::VirtualIdentity &vid, XrdOucErrInfo *error);
  int read(XrdSfsFileOffset offset, char *buff, XrdSfsXferSize blen);
  int stat(struct stat* buf);
  int close();

  ProcCommand();
  ~ProcCommand();
}; 

class ProcInterface {
private:

public:

  static bool IsProcAccess(const char* path);
  static bool Authorize(const char* path, const char* info, eos::common::Mapping::VirtualIdentity &vid, const XrdSecEntity* entity);

  ProcInterface();
  ~ProcInterface();
};

EOSMGMNAMESPACE_END

#endif
