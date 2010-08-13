#ifndef __XRDCOMMON_XRDCLIENTLOCK__HH__
#define __XRDCOMMON_XRDCLIENTLOCK__HH__

/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

// this lock is NOT YET in use because it seems it is not needed to lock XrdClient::Open calls!

extern XrdSysMutex gXrdClientLock;

/*----------------------------------------------------------------------------*/
#endif

  

