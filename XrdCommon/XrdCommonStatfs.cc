/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonStatfs.hh"
/*----------------------------------------------------------------------------*/

XrdSysMutex XrdCommonStatfs::gMutex;
XrdOucHash<XrdCommonStatfs> XrdCommonStatfs::gStatfs;
