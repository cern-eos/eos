#ifndef __XRDFSTOFS_TRANSFER_HH__
#define __XRDFSTOFS_TRANSFER_HH__

/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
class XrdFstTransfer {
private:
  unsigned long long fId;
  unsigned long fsId;
  XrdOucString localPrefix;
  XrdOucString managerId;
  XrdOucString remoteUrl;

public:
  XrdFstTransfer(const char* remoteurl, unsigned long long fid, unsigned long fsid, const char* localprefix, const char* managerid) {
    fId = fid; fsId = fsid; localPrefix = localprefix; managerId = managerid; remoteUrl = remoteurl;
  }
  ~XrdFstTransfer() {};
};


#endif
