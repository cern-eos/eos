#ifndef __XRDCOMMON_STRINGSTORE__
#define __XRDCOMMON_STRINGSTORE__

#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSys/XrdSysPthread.hh"

class XrdCommonStringStore {
private:
  static XrdSysMutex StringMutex;
public:
  static XrdOucHash<XrdOucString> theStore;
  static char* Store(const char* charstring , int lifetime=0);

  XrdCommonStringStore() {};
  ~XrdCommonStringStore() {};
};

#endif
