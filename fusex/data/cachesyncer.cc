/*
 * cachesyncer.cc
 *
 *  Created on: May 10, 2017
 *      Author: simonm
 */

#include "cachesyncer.hh"
#include "bufferll.hh"
#include <unistd.h>

#include <XrdCl/XrdClXRootDResponses.hh>
#include <XrdSys/XrdSysPthread.hh>

#include <algorithm>
#include <vector>

class CollectiveHandler : public XrdCl::ResponseHandler
{
public:

  CollectiveHandler(size_t count) : count(count), sem(0), result(true)
  {
  }

  virtual void HandleResponse(XrdCl::XRootDStatus* status,
                              XrdCl::AnyObject* response)
  {
    Report(status);
  }

  void Wait()
  {
    sem.Wait();
  }

  void Report(XrdCl::XRootDStatus* status)
  {
    XrdSysMutexHelper scope(mtx);
    result &= status->IsOK();
    delete status;
    --count;

    if (count == 0) {
      scope.UnLock();
      sem.Post();
    }
  }

  bool WasSuccessful()
  {
    return result;
  }

private:

  size_t count;
  XrdSysMutex mtx;
  XrdSysSemaphore sem;
  bool result;
};

int cachesyncer::sync(int fd, interval_tree<uint64_t,
                      uint64_t>& journal,
                      size_t offshift,
                      off_t truncatesize)
{
  if (!journal.size() && (truncatesize == -1)) {
    return 0;
  }

  const size_t ntot = journal.size();
  const size_t nbatch = 256;
  size_t nsub = 0;
  auto itr = journal.begin();
  bool needtrunc = (truncatesize != -1);

  while(nsub<ntot || needtrunc) {
    const size_t n = std::min(ntot - nsub, nbatch);
    const bool dotrunc = (n < nbatch && needtrunc);
    CollectiveHandler handler(n + (dotrunc ? 1 : 0));

    std::map<size_t, bufferll> bufferm;
    size_t i = 0;
    while(i<n) {
      off_t cacheoff = itr->value + offshift;
      size_t size = itr->high - itr->low;
      bufferm[i].resize(size);
      int bytesRead = pread(fd, bufferm[i].ptr(), size, cacheoff);

      if (bytesRead < 0) {
        // TODO handle error
        return -1;
      }

      if (bytesRead < (int) size) {
        // TODO handle error
      }

      // do async write
      XrdCl::XRootDStatus st = file.Write(itr->low, size, bufferm[i].ptr(), &handler);
      ++nsub;

      if (!st.IsOK()) {
        handler.Report(new XrdCl::XRootDStatus(st));
      }

      ++i;
      ++itr;
    }

    // there might be a truncate call after the writes to be applied
    if (dotrunc) {
      XrdCl::XRootDStatus st = file.Truncate(truncatesize);
      handler.Report(new XrdCl::XRootDStatus(st));
      needtrunc = false;
    }

    handler.Wait();
    int rc = handler.WasSuccessful() ? 0 : -1;
    if (rc) return rc;
  }

  return 0;
}
