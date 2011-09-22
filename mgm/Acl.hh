#ifndef __EOSMGM_ACL__HH__
#define __EOSMGM_ACL__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Acl {
  bool canRead;
  bool canWrite;
  bool canWriteOnce;
  bool canBrowse;
  bool hasAcl;
  bool hasEgroup;

public:

  Acl() { canRead = false; canWrite = false; canWriteOnce = false; canBrowse = false; hasAcl = false;}

  Acl(std::string sysacl, std::string useracl, eos::common::Mapping::VirtualIdentity &vid);
  ~Acl(){};
  
  void Set(std::string sysacl, std::string useracl, eos::common::Mapping::VirtualIdentity &vid);

  bool CanRead()      {return canRead;}
  bool CanWrite()     {return canWrite;}
  bool CanWriteOnce() {return canWriteOnce;}
  bool CanBrowse()   {return canBrowse;}
  bool HasAcl()       {return hasAcl;}
  bool HasEgroup()    {return hasEgroup;}
};

EOSMGMNAMESPACE_END

#endif
