// this is for debuuging only
// it logs some messages in /tmp/globus_alternate_log.txt
// and in /mnt/rd/globus_alternate_log.txt

#include "XrdGsiBackendMapper.hh"
#include "XrdUtils.hh"
//to probe background servers
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

bool XrdGsiBackendMapper::sDiscovery = false;
pthread_t XrdGsiBackendMapper::sDiscoverThread = 0;
XrdGsiBackendMapper* XrdGsiBackendMapper::This;
std::map<pthread_t, std::string> XrdGsiBackendMapper::sProbeThreadsUrls;
XrdSysRWLock XrdGsiBackendMapper::sProbeThreadsLock;
sem_t *XrdGsiBackendMapper::sSemaphore;
XrdSysRWLock XrdGsiBackendMapper::sDestructLock;

void XrdGsiBackendMapper::StartUpdater ()
{
  dbgprintf("starting the updater \n");
  if (!sDiscovery)
  {
    pthread_create (&XrdGsiBackendMapper::sDiscoverThread, NULL, XrdGsiBackendMapper::StartUpdaterStatic,
                    (void*) XrdGsiBackendMapper::This);
    sDiscovery = true;
  }
}

void XrdGsiBackendMapper::StopUpdater ()
{
  dbgprintf("stopping the updater \n");
  if (sDiscovery)
  {
    pthread_cancel (sDiscoverThread);
    pthread_join (sDiscoverThread, NULL);
    sDiscovery = false;
  }
}

void* XrdGsiBackendMapper::StartUpdaterStatic (void *param)
{
  //pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS,NULL);
  pthread_setcanceltype (PTHREAD_CANCEL_DEFERRED, NULL);
  XrdGsiBackendMapper* This = static_cast<XrdGsiBackendMapper*> (param);
  while (true)
  {
    time_t now = time (0);
    // first we try to remove the finished probes
    This->sProbeThreadsLock.WriteLock ();
    std::vector<std::map<pthread_t, std::string>::iterator> entriesToRm;
    for (auto it = This->sProbeThreadsUrls.begin (); it != This->sProbeThreadsUrls.end (); ++it)
    {
      if (!pthread_tryjoin_np (it->first, NULL)) entriesToRm.push_back (it);
    }
    for (auto it = entriesToRm.begin (); it != entriesToRm.end (); ++it)
      This->sProbeThreadsUrls.erase (*it);
    This->sProbeThreadsLock.UnLock ();

    pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);
    mysem_wait(sSemaphore);
    for (auto it = This->pBackendMapIpc->begin (); it != This->pBackendMapIpc->end (); ++it)
    {
      const auto &url = it->first;
      auto &iteminfo = it->second;
      if (iteminfo.nextUpdate < now)
      {
        // need to update this one
        switch (iteminfo.probeStatus)
        {
          case XrdGsiBackendItemShm::completed:
          case XrdGsiBackendItemShm::unprobed:
            This->AsyncProbe (url.c_str (), true);
            iteminfo.probeStatus = XrdGsiBackendItemShm::pending;
            //iteminfo.lastUpdate  iteminfo.nextUpdate are set by the callback
            break;
          case XrdGsiBackendItemShm::started:
            globus_gfs_log_message (GLOBUS_GFS_LOG_WARN,
                                    "cannot update info about backend server %s : previous query is still running! Will try again later.\n",
                                    url.c_str ());
            break;
          case XrdGsiBackendItemShm::failed:
            globus_gfs_log_message (GLOBUS_GFS_LOG_WARN, "cannot update info about backend server %s : last probe failed.\n", url.c_str ());
            break;
          default:
            ;
        }
      }
    }
    mysem_post(sSemaphore);
    pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);

    sleep (This->pRefreshInterval);
  }
  return NULL;
}

// if used on an existing entry, it forces an update
bool XrdGsiBackendMapper::AddToProbeList (const std::string & url)
{
  if (!(pAvailGsiTtl > 0)) return true;

  std::string myurl = url;
  int oldstate;

  if (url.compare (0, 11, "eos_node_ls"))
  {
    // if the URL doesn't already has a port, add the default discovery port
    if (myurl.rfind (':') == std::string::npos) myurl += (":" + this->pGsiBackendPort);
  }
  {
    pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, &oldstate);
    mysem_wait(sSemaphore);
    KeyType myurl2 (myurl.c_str (), *this->alloc_inst);
    bool newentry = (this->pBackendMapIpc->count (myurl2) == 0);
    if (!newentry)
    {
      mysem_post(sSemaphore);
      pthread_setcancelstate (oldstate, NULL);
      return false;
    }
    // well, we disable the refresh function for now

    this->AsyncProbe (myurl, true);

    XrdGsiBackendItemShm item;
    item.probeStatus = XrdGsiBackendItemShm::pending;
    if (newentry)
      (*pBackendMapIpc)[myurl2] = item;
    else
      (*pBackendMapIpc)[myurl2].CopyFrom (item);

    mysem_post(sSemaphore);
    pthread_setcancelstate (oldstate, NULL);
  }

  return true;
}

bool XrdGsiBackendMapper::MarkAsDown (const std::string & url)
{
  if (!(pAvailGsiTtl > 0)) return true;

  std::string myurl = url;

  if (url.compare (0, 11, "eos_node_ls"))
  {
    // if the URL doesn't already has a port, add the default discovery port
    if (myurl.rfind (':') == std::string::npos) myurl += (":" + this->pGsiBackendPort);
  }
  {
    int oldstate;
    pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, &oldstate);
    mysem_wait(sSemaphore);
    KeyType myurl2 (myurl.c_str (), *this->alloc_inst);
    bool newentry = (this->pBackendMapIpc->count (myurl2) == 0);
    if (newentry)
    {
      mysem_post(sSemaphore);
      pthread_setcancelstate (oldstate, NULL);
      return false;
    }

    XrdGsiBackendItemShm &item = (*pBackendMapIpc)[myurl2];
    // if there is no probe going on
    if (item.probeStatus == XrdGsiBackendItemShm::completed || item.probeStatus == XrdGsiBackendItemShm::unprobed
        || item.probeStatus == XrdGsiBackendItemShm::failed)
    {
      item.probeStatus = XrdGsiBackendItemShm::completed;
      item.gsiFtpAvailable = false;
      item.nextUpdate = time (0) + this->pUnavailGsiRetryInterval;
    }
    else
    {
      // there is a probe going on, just mark it as unavailable waiting for the result
      item.gsiFtpAvailable = false;
    }
    mysem_post(sSemaphore);
    pthread_setcancelstate (oldstate, NULL);
  }

  return true;
}

// probing
void XrdGsiBackendMapper::AsyncProbe (const std::string &url, bool unlockSemIfCanceled)
{
  probeinfo *pi = new probeinfo;
  pi->This = this;
  pi->url = url;
  // if the URL doesn't already has a port, add the default discovery port
  if (url.compare (0, 11, "eos_node_ls") && pi->url.rfind (':') == std::string::npos) pi->url.append (":" + this->pGsiBackendPort);
  pthread_t thread;

  sProbeThreadsLock.WriteLock ();

  pthread_create (&thread, NULL, XrdGsiBackendMapper::TestSocket, (void*) pi);
  sProbeThreadsUrls.insert (std::make_pair (thread, url));
  sProbeThreadsLock.UnLock ();
}

void XrdGsiBackendMapper::TestSocketCleaner (void* thr_probeinfo)
{
  probeinfo* pi = static_cast<probeinfo*> (thr_probeinfo);
  delete pi;
}

void*
XrdGsiBackendMapper::TestSocket (void* thr_probeinfo)
{
  probeinfo* pi = static_cast<probeinfo*> (thr_probeinfo);
  pthread_cleanup_push(XrdGsiBackendMapper::TestSocketCleaner,thr_probeinfo);
    pthread_setcanceltype (PTHREAD_CANCEL_DEFERRED, NULL);
    pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);
    XrdGsiBackendMapper *This = pi->This;

    bool isEosNodeLs = (pi->url.compare (0, 11, "eos_node_ls") == 0);
    bool connectOK = false;
    bool probeFailed = true;
    addrinfo *rp = NULL;

    std::string sport;
    KeyType urlAndPort2 ("", *(This->alloc_inst));
    // split url and port
    if (!isEosNodeLs)
    {
      auto colpos = pi->url.rfind (':');
      sport = pi->url.substr (pi->url.rfind (':') + 1, std::string::npos);
      pi->url.resize (colpos);
      urlAndPort2.append ((pi->url + ":" + sport).c_str ());
    }
    else
      urlAndPort2.append (pi->url.c_str ());

    pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);
    mysem_wait(sSemaphore);
    auto entry = This->pBackendMapIpc->find (urlAndPort2);
    if (entry != This->pBackendMapIpc->end ()) // should definitely exist
    entry->second.probeStatus = XrdGsiBackendItemShm::started;
    mysem_post(sSemaphore);
    pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);

    // using
    if (isEosNodeLs)
    {
      pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);
      std::string HeadServersList = pi->url.substr (11, std::string::npos);
      while (true)
      {
        auto pos = HeadServersList.find ("|");
        std::string url; // ("root://");
        url += HeadServersList.substr (0, pos);
        //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "url is %s\n",url.c_str());
        HeadServersList.erase (0, pos == std::string::npos ? pos : pos + 1);

        std::vector<std::string> servers;
        // get the servers
        XrdUtils::ListFstEos (servers, url);

        //run async probes on each
        for (auto it = servers.begin (); it != servers.end (); ++it)
        {
          //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "From EosNodeLs, triggering AsyncProb on %s\n",it->c_str());
          pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);
          This->AddToProbeList (*it);
          pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);
        }

        if (HeadServersList.size () == 0) break;
      }

      pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);
      mysem_wait(sSemaphore);
      auto entry = This->pBackendMapIpc->find (urlAndPort2);
      if (entry != This->pBackendMapIpc->end ()) // should definitely exist
      {
        time_t now = time (0);
        entry->second.gsiFtpAvailable = false;
        entry->second.lastUpdate = now;
        entry->second.nextUpdate = now + This->pAvailGsiTtl;
        entry->second.probeStatus = XrdGsiBackendItemShm::completed;
      }
      mysem_post(sSemaphore);
      goto myexit;
    }

    do
    {
      if (getaddrinfo (pi->url.c_str (), sport.c_str (), NULL, &pi->result))
      {
        //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "lookup failed %d\n");
        probeFailed = true;
        break;
      }

      for (rp = pi->result; rp != NULL; rp = rp->ai_next)
      {
        //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "try socket %d %d %d\n", rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        pi->sfd = socket (rp->ai_family, SOCK_STREAM, 0);
        if (pi->sfd == -1) continue;
        //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "try connecting\n");
        int ret;
        if ((ret = connect (pi->sfd, rp->ai_addr, rp->ai_addrlen)) == 0) break;
        //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "connecting retcode is %d\n", ret);
        close (pi->sfd);
        pi->sfd = -1;
      }

      probeFailed = false;

      if (rp == NULL)
      {
        //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "connecting failed\n");
        break;
      }

      if (write (pi->sfd, (const void*) "test", 4) < 0)
      {
        //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "write to socket failed\n");
        break;
      }
      connectOK = true;
    }
    while (false);

    {
      pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);
      mysem_wait(sSemaphore);
      auto urlAndPort (pi->url + ":" + sport);
      KeyType urlAndPort2 ((pi->url + ":" + sport).c_str (), *(This->alloc_inst));
      auto entry = This->pBackendMapIpc->find (urlAndPort2);
      bool modified = false;
      if (entry != This->pBackendMapIpc->end ()) // should definitely exist
      {
        time_t now = time (0);
        modified = (entry->second.gsiFtpAvailable != connectOK);
        entry->second.gsiFtpAvailable = connectOK;
        entry->second.lastUpdate = now;
        entry->second.nextUpdate = now + (connectOK ? This->pAvailGsiTtl : This->pUnavailGsiRetryInterval);
        entry->second.probeStatus = probeFailed ? XrdGsiBackendItemShm::failed : XrdGsiBackendItemShm::completed;
      }
      if (modified)
      {
        if (connectOK)
          This->pActiveBackend->push_back (urlAndPort2);
        else
          This->pActiveBackend->erase (std::find (This->pActiveBackend->begin (), This->pActiveBackend->end (), urlAndPort2));
      }
      mysem_post(sSemaphore);
      //pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);
    }

    //globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "probing gsi backend on host %s and port %s : connect is %d and failed is %d\n",
    //			  pi->url.c_str (), sport.c_str (), connectOK, probeFailed);
    myexit:
    do{}while(0); // this is to avoid a strange compilation error on SLC5
    pthread_cleanup_pop(1);
    //pthread_cleanup_pop(1);
  pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);
  return NULL;
}

std::string XrdGsiBackendMapper::DumpBackendMap (const std::string &sep)
{
  std::stringstream ss;
  pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);
  mysem_wait(sSemaphore);
  for (auto it = pBackendMapIpc->begin (); it != pBackendMapIpc->end (); ++it)
  {
    if (it != pBackendMapIpc->begin ()) ss << sep;
    ss << it->first << " => " << "GFTP=" << it->second.gsiFtpAvailable << "," << "STATUS="
        << XrdGsiBackendItemShm::enumStatusToStr (it->second.probeStatus) << "," << "LASTUD=" << it->second.lastUpdate << "," << "NEXTUD="
        << it->second.nextUpdate;
  }
  mysem_post(sSemaphore);
  pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);
  return ss.str ();
}

std::string XrdGsiBackendMapper::DumpActiveBackend (const std::string &sep)
{
  std::stringstream ss;
  pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);
  mysem_wait(sSemaphore);
  for (auto it = pActiveBackend->begin (); it != pActiveBackend->end (); ++it)
  {
    if (it != pActiveBackend->begin ()) ss << sep;
    ss << it->c_str ();
  }
  mysem_post(sSemaphore);
  pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);
  return ss.str ();
}

void XrdGsiBackendMapper::PreFork ()
{
  pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);
  dbgprintf("My PID is %d\n", (int)getpid());

  sDestructLock.WriteLock ();
  if (This) This->Reset ();
  sProbeThreadsLock.WriteLock ();
  pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);

}

void XrdGsiBackendMapper::PostForkChild ()
{
  pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);
  dbgprintf("My PID is %d\n", (int)getpid());

  sProbeThreadsLock.UnLock ();
  if (This)
  {
    delete This;
    This = 0;
  }
  sDestructLock.UnLock ();

  pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);

  /* #### WARNING ###
   * DON'T EVER emit some log at this point because
   * the GLOBUS logging system might deadlock!
   * Probably this is called before the logging system has a chance to get reinitialized after the fork
   */
}

void XrdGsiBackendMapper::PostForkParent ()
{
  pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);

  dbgprintf("My PID is %d\n", (int)getpid());

  globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "starting postfork \n");
  /* #### WARNING ###
   * DON'T EVER emit some log at this point because
   * the GLOBUS logging system would deadlock!
   * Probably this is called before the logging system has a chance to get reinitialized after the fork
   */

  sProbeThreadsLock.UnLock ();
  if (This && This->sDiscoverThread) This->StartUpdater ();
  sDestructLock.UnLock ();

  pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);
}

XrdGsiBackendMapper::~XrdGsiBackendMapper ()
{
  pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, NULL);
  dbgprintf("My PID is %d\n", (int)getpid());
  /* #### WARNING ###
   * DON'T EVER emit some log at this point because
   * the GLOBUS logging system would deadlock as it does!
   * Probably, this is called after the the logging system shuts down
   */
  // stop the updater
  Reset ();

  if (sSemaphore) sem_close (sSemaphore);
  sSemaphore = 0;

  delete alloc_inst;
  delete segment;
  This = NULL;
  pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);

}

void XrdGsiBackendMapper::Reset ()
{
  int oldstate;
  pthread_setcancelstate (PTHREAD_CANCEL_DISABLE, &oldstate);
  dbgprintf("My PID is %d\n", (int)getpid());
  /* #### WARNING ###
   * DON'T EVER emit some log att this point because
   * the GLOBUS logging system would deadlock as it does!
   * Probably, this is called after the the logging system shuts down
   */
  // stop the updater
  if (sDiscoverThread != 0) StopUpdater ();

  // we dot it two times since while they terminate, one specific thread (dealing with eosnodels)
  // could spawn others threads (that will NOT spawn other threads)
  for (int i = 0; i < 2; i++)
  {
    sProbeThreadsLock.WriteLock ();
    auto probeThreadsUrls (sProbeThreadsUrls);
    sProbeThreadsLock.UnLock ();
    for (auto it = probeThreadsUrls.begin (); it != probeThreadsUrls.end (); it++)
      pthread_cancel (it->first);
    for (auto it = probeThreadsUrls.begin (); it != probeThreadsUrls.end (); it++)
    {
      pthread_join (it->first, NULL);
      mysem_wait(sSemaphore);
      KeyType urlAndPort2 (it->second.c_str (), *(This->alloc_inst));
      auto entry = This->pBackendMapIpc->lower_bound (urlAndPort2);
      if (entry != This->pBackendMapIpc->end ()) // should definitely exist
        if (entry->first == urlAndPort2)
          if (entry->second.probeStatus == XrdGsiBackendItemShm::started || entry->second.probeStatus == XrdGsiBackendItemShm::pending)
          {
            entry->second.probeStatus = XrdGsiBackendItemShm::completed;
            dbgprintf("saving %s\n", urlAndPort2.c_str ());
          }
      mysem_post(sSemaphore);
      // we did not touch the time stamps so the it will be probed on the next restart
    }

    sProbeThreadsLock.WriteLock ();
    for (auto it = probeThreadsUrls.begin (); it != probeThreadsUrls.end (); ++it)
      sProbeThreadsUrls.erase (it->first);
    sProbeThreadsLock.UnLock ();
  }
  pthread_setcancelstate (oldstate, NULL);
}

XrdGsiBackendMapper::XrdGsiBackendMapper () :
    pRefreshInterval (60), pAvailGsiTtl (3600), pUnavailGsiRetryInterval (3600), pGsiBackendPort ("7001")
{
  globus_gfs_log_message (GLOBUS_GFS_LOG_INFO, "%s: My PID is %d\n", "XrdGsiBackendMapper::XrdGsiBackendMapper", (int) getpid ());
  sDestructLock.WriteLock ();

  This = this;
  pthread_atfork (XrdGsiBackendMapper::PreFork, XrdGsiBackendMapper::PostForkParent, XrdGsiBackendMapper::PostForkChild);

  // protects the shared memory
  sSemaphore = sem_open ("/xrootd-gridft", O_CREAT, 600, 1);
  if (sSemaphore == SEM_FAILED)
  {
    dbgprintf("semaphore open failed in pid %d, error is %d", (int)getpid(), (int)errno);
    abort ();
  }

  mysem_wait(sSemaphore);
  segment = new boost::interprocess::managed_shared_memory (boost::interprocess::open_or_create, "xrootd-gridftp-shm" //segment name
                                                            , 16777216);          //segment size in bytes

  alloc_inst = new ShmemAllocator (segment->get_segment_manager ());

  pBackendMapIpc = segment->find_or_construct<MyMap> ("MyMap") (std::less<KeyType> (), *alloc_inst);
  pActiveBackend = segment->find_or_construct<MyVect> ("MyVect") (*alloc_inst);

  mysem_post(sSemaphore);
  int sval = -1;
  sem_getvalue (sSemaphore, &sval);

  sDestructLock.UnLock ();
  globus_gfs_log_message (GLOBUS_GFS_LOG_DUMP, "constructor over %d %d %d %d\n", segment->get_size (), segment->check_sanity (),
                          segment->get_num_named_objects (), sval);
}
