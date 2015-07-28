#ifndef __XRDGSIBACKENDMAPPER_HH__
#define __XRDGSIBACKENDMAPPER_HH__
/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

//------------------------------------------------------------------------------
//! @file XrdBackendMapper.hh
//! @author Geoffray Adde - CERN
//! @brief An helper class in charge of keeping the list of backend gridtp server
//!        up-to-date
//!        it includes an auto-discovery feature
//!        it uses a map in shared memory protected by a semaphore
//!        it allows the forked processes to read and update the shared map
//!        on each request to be served
//------------------------------------------------------------------------------
// ####################################################################################
// ######### VERY IMPORTANT INFORMATION TO UNDERSTAND THE CODE IN THIS CLASS ##########
// ####################################################################################
// The trick is to know that the globus_gridftp_server process forks many time
// Sometimes it activates the DSI plugin, sometimes it does not.
// When the plugin gets activated, sometimes it unactivates it, sometimes it does not.
// When the plugin gets activated and when the process forks, the plugin is reloaded.
// and the plugin is activated. As a consequence, all the static data is reinitialized
// BUT when the pthread_atfork hooks are being executed (especially the post fork ones)
// the plugin is not reloaded yet and we use this to clean-up a few things for the
// management of the shared map/semaphore
// ####################################################################################
extern "C"
{
#include "globus_gridftp_server.h"
}
#include <fcntl.h>
#include <vector>
#include <set>
#include <map>
#include <algorithm>
#include <string>
#include <functional>
#include <utility>
// boost headers for managed shared memory
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include "XrdCl/XrdClStatus.hh"
#include "XrdSys/XrdSysPthread.hh"

//#define FORKDEBUGGING

#ifdef FORKDEBUGGING

#define dbgprintf(...) \
  do { \
    char buffer[1024]; \
    timespec ts; \
    clock_gettime(CLOCK_MONOTONIC, &ts); \
    int fd=open("/tmp/globus_alternate_log.txt",O_WRONLY | O_APPEND | O_CREAT,0777); \
    int buflen = 1024 - snprintf(buffer,1024,"%d,%d | %d | %s :  ", (int)ts.tv_sec,(int)ts.tv_nsec,(int)getpid(),__PRETTY_FUNCTION__ ); \
    buflen-=snprintf(buffer+1024-buflen,buflen,__VA_ARGS__); \
    write(fd,buffer,1024-buflen); \
    close(fd); } \
  while(0)

#define dbgprintf2(...) \
  do { \
    char buffer[1024]; \
    timespec ts; \
    clock_gettime(CLOCK_MONOTONIC, &ts); \
    int fd=open("/tmp/globus_alternate_log.txt",O_WRONLY | O_APPEND | O_CREAT,0777); \
    int buflen = 1024 - snprintf(buffer,1024,"%d,%d | %d | %s at line %d :  ", (int)ts.tv_sec,(int)ts.tv_nsec,(int)getpid(),__PRETTY_FUNCTION__ , __LINE__ ); \
    buflen-=snprintf(buffer+1024-buflen,buflen,__VA_ARGS__); \
    write(fd,buffer,1024-buflen); \
    close(fd); } \
  while(0)

#define mysem_wait( sem ) \
do { \
  int sval = -1; \
  sem_getvalue (sSemaphore, &sval); \
  dbgprintf2("mysem_wait before, value is %d\n",sval); \
  sem_wait(sem); \
  sem_getvalue (sSemaphore, &sval); \
  dbgprintf2("mysem_wait after, value is %d\n",sval); }\
  while(0)

#define mysem_post( sem ) \
do { \
  int sval = -1; \
  sem_getvalue (sSemaphore, &sval); \
  dbgprintf2("mysem_post before, value is %d\n",sval); \
  sem_post(sem); \
  sem_getvalue (sSemaphore, &sval); \
  dbgprintf2("mysem_post after, value is %d\n",sval); }\
  while(0)

#else

#define dbgprintf(...) do{}while(0)

#define dbgprintf2(...) do{}while(0)

#define mysem_wait( sem ) sem_wait( sem )

#define mysem_post( sem ) sem_post( sem )

#endif


struct XrdGsiBackendItemShm
{
  enum gsiProbeStatus_e
  {
    unprobed, // was never probed
    pending,  // async probing triggered
    started,  // probing started
    failed,   // last probe is over and failed
    completed // last probe is over and was completed
  };

  bool gsiFtpAvailable;
  gsiProbeStatus_e probeStatus;
  time_t lastUpdate;
  time_t nextUpdate;

  XrdGsiBackendItemShm () :
      gsiFtpAvailable (false), probeStatus (unprobed), lastUpdate (0), nextUpdate (0)
  {
  }

  void CopyFrom (const XrdGsiBackendItemShm& item)
  {
    gsiFtpAvailable = item.gsiFtpAvailable;
    probeStatus = item.probeStatus;
    lastUpdate = item.lastUpdate;
    nextUpdate = item.nextUpdate;
  }

  static std::string enumStatusToStr (gsiProbeStatus_e st)
  {
    switch (st)
    {
      case unprobed:
        return "unprobed";
        break;
      case pending:
        return "pending";
        break;
      case started:
        return "started";
        break;
      case failed:
        return "failed";
        break;
      case completed:
        return "completed";
        break;
      default:
        ;
    }
    return "";
  }
};

class XrdGsiBackendMapper
{
  struct probeinfo
  {
    std::string url;
    XrdGsiBackendMapper *This;
    int sfd;
    addrinfo *result;
    probeinfo () :
        url (), This (NULL), sfd (-1), result (NULL)
    {
    }
    ~probeinfo ()
    {
      if (result) freeaddrinfo (result);
      if (sfd != -1) close (sfd);
    }
  };
  // threading stuff for autodiscovery
  static pthread_t sDiscoverThread;
  static bool sDiscovery;
  // keep tracks of the urls being discovered in the current process and its thread
  static std::map<pthread_t, std::string> sProbeThreadsUrls;
  static XrdSysRWLock sProbeThreadsLock;
public:
  static XrdGsiBackendMapper* This;
  // we need this lock because concurrent destructions might be tried because of our
  // forkhandler and the exit/termination of the program
  static XrdSysRWLock sDestructLock;
protected:

  // configuration of autodiscovery
  time_t pRefreshInterval;
  time_t pAvailGsiTtl;
  time_t pUnavailGsiRetryInterval;
  std::string pGsiBackendPort;

  // backend map in shared memory
public:
  typedef boost::interprocess::managed_shared_memory::segment_manager segment_manager_t;
  typedef boost::interprocess::allocator<void, segment_manager_t> void_allocator;
  typedef boost::interprocess::allocator<char, segment_manager_t> char_allocator;
  typedef boost::interprocess::basic_string<char, std::char_traits<char>, char_allocator> char_string;
  typedef char_string KeyType;
  typedef XrdGsiBackendItemShm MappedType;
  typedef std::pair<const KeyType, MappedType> ValueType;
  typedef boost::interprocess::allocator<ValueType, boost::interprocess::managed_shared_memory::segment_manager> ShmemAllocator;
  typedef boost::interprocess::allocator<char_string, boost::interprocess::managed_shared_memory::segment_manager> ShmemAllocatorStr;
  typedef boost::interprocess::map<KeyType, MappedType, std::less<KeyType>, ShmemAllocator> MyMap;
  typedef boost::interprocess::vector<KeyType, ShmemAllocatorStr> MyVect;
protected:
  static sem_t *sSemaphore;
  ShmemAllocator *alloc_inst;
  MyMap *pBackendMapIpc;
  MyVect *pActiveBackend;
  boost::interprocess::managed_shared_memory *segment;

public:
  // constructor/destr and configuration
  XrdGsiBackendMapper ();
  ~XrdGsiBackendMapper ();
  void Reset ();

  void SetGsiBackendPort (const std::string & port)
  {
    pGsiBackendPort = port;
  }
  void SetBackendServers (const std::vector<std::string> urls);
  void SetRefreshInterval (time_t interval)
  {
    pRefreshInterval = interval;
  }
  void SetAvailGsiTtl (time_t ttl)
  {
    pAvailGsiTtl = ttl;
  }
  void SetUnavailGsiRetryInterval (time_t interval)
  {
    pUnavailGsiRetryInterval = interval;
  }

  // access
  void LockBackendServers ()
  {
    mysem_wait(sSemaphore);
  }
  // any use of the resulting pointer should be surrounded
  // by calls to LockBackEndServers and UnLockBackEndServers
  const MyMap * GetBackEndMap ()
  {
    return pBackendMapIpc;
  }
  const MyVect * GetActiveBackEnd ()
  {
    return pActiveBackend;
  }
  void UnLockBackendServers ()
  {
    mysem_post(sSemaphore);
  }
  std::string DumpBackendMap (const std::string &sep = "\n");
  std::string DumpActiveBackend (const std::string &sep = "\n");
  KeyType Key (const char *s)
  {
    return KeyType (s, *alloc_inst);
  }

  // updater
  static void StartUpdater ();
  static void StopUpdater ();
  static void* StartUpdaterStatic (void* param);

  // probing
  void AsyncProbe (const std::string &url, bool unlockSemIfCanceled);
  static void* TestSocket (void *);
  static void TestSocketCleaner (void *);

  // extern update
  bool AddToProbeList (const std::string & url);
  bool MarkAsDown (const std::string & url);

  // fork handling
  static void PreFork ();
  static void PostForkChild ();
  static void PostForkParent ();
};

#endif
