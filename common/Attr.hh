#ifndef __EOSCOMMON_ATTR__HH__
#define __EOSCOMMON_ATTR__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <sys/types.h>
#include <attr/xattr.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class Attr {
private:
  std::string fName;

public:
  bool Set(const char* name, const char* value, size_t len); // set a binary attribute (name has to start with 'user.' !!!)
  bool Set(std::string key, std::string value);              // set a string attribute (name has to start with 'user.' !!!)
  bool Get(const char* name, char* value, size_t &size); // get a binary attribute
  std::string Get(std::string name);                     // get a strnig attribute
  
  static Attr* OpenAttr(const char* file);            // factory function
  Attr(const char* file);
  ~Attr();


};

EOSCOMMONNAMESPACE_END
#endif

