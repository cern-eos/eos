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

#include <vector>

class CollectiveHandler : public XrdCl::ResponseHandler
{
public:

  CollectiveHandler( size_t count ) : count( count ), sem( 0 ), result( true )
  {
  }

  virtual void HandleResponse( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response )
  {
    Report( status );
  }

  void Wait()
  {
    sem.Wait();
  }

  void Report( XrdCl::XRootDStatus *status)
  {
    XrdSysMutexHelper scope( mtx );
    result &= status->IsOK();
    delete status;
    --count;
    if ( count == 0 ) {
      scope.UnLock();
      sem.Post();
    }
  }

  bool WasSuccessful()
  {
    return result;
  }

private:

  size_t          count;
  XrdSysMutex     mtx;
  XrdSysSemaphore sem;
  bool            result;
} ;

int cachesyncer::sync( int fd, interval_tree<uint64_t,
                      uint64_t> &journal,
                      size_t offshift,
                      off_t truncatesize)
{
  if (!journal.size() && (truncatesize == -1))
    return 0;

  CollectiveHandler handler( journal.size() + ((truncatesize != -1)?1:0));

  std::map<size_t, bufferll> bufferm;

  size_t i=0;
  for ( auto itr = journal.begin(); itr != journal.end(); ++itr )
  {
    off_t  cacheoff = itr->value + offshift;
    size_t size   = itr->high - itr->low;
    bufferm[i].resize(size);
    int bytesRead = pread( fd, bufferm[i].ptr(), size, cacheoff );

    if ( bytesRead < 0 )
    {
      // TODO handle error
      return -1;
    }

    if ( bytesRead < (int) size )
    {
      // TODO handle error
    }
    // do async write
    XrdCl::XRootDStatus st = file.Write( itr->low, size, bufferm[i].ptr(), &handler );
    if ( !st.IsOK() )
    {
      handler.Report( new XrdCl::XRootDStatus( st ) );
    }
    i++;
  }

  // there might be a truncate call after the writes to be applied
  if (truncatesize != -1)
  {
    XrdCl::XRootDStatus st = file.Truncate( truncatesize );
    if( !st.IsOK())
    {
      handler.Report ( new XrdCl::XRootDStatus( st ) );
    }
  }

  handler.Wait();

  return handler.WasSuccessful() ? 0 : -1;
}
