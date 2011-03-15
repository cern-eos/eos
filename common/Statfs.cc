/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Statfs.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

XrdSysMutex Statfs::gMutex;
XrdOucHash<Statfs> Statfs::gStatfs;

EOSCOMMONNAMESPACE_END
