#ifndef __EOSCOMMON_STRINGSTORE__
#define __EOSCOMMON_STRINGSTORE__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class StringStore {
private:
  static XrdSysMutex StringMutex;
public:
  static XrdOucHash<XrdOucString> theStore;
  static char* Store(const char* charstring , int lifetime=0);

  StringStore() {};
  ~StringStore() {};
};

EOSCOMMONNAMESPACE_END

#endif
