/*
 * cachesyncer.cc
 *
 *  Created on: May 10, 2017
 *      Author: simonm
 */

#include "cachesyncer.hh"

#include <unistd.h>

#include <XrdCl/XrdClXRootDResponses.hh>
#include <XrdSys/XrdSysPthread.hh>

#include <vector>

class CollectiveHandler : public XrdCl::ResponseHandler
{
  public:

    CollectiveHandler( size_t count ) : count( count ), sem( 0 ), result( true ) { }

    virtual void HandleResponse( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response )
    {
      Report( status );
    }

    void Wait()
    {
      sem.Wait();
    }

    void Report( XrdCl::XRootDStatus *status )
    {
      XrdSysMutexHelper scope( mtx );
      result &= status->IsOK();
      delete status;
      --count;
      if( count == 0 )
        sem.Post();
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
};

int cachesyncer::sync( int fd, interval_tree<uint64_t, uint64_t> &journal, size_t offshift )
{
  CollectiveHandler handler( journal.size() );

  for( auto itr = journal.begin(); itr != journal.end(); ++itr )
  {
    off_t  cacheoff = itr->value + offshift;
    size_t size   = itr->high - itr->low;
    char  *buffer = new char[size];
    int bytesRead = pread( fd, buffer, size, cacheoff );

    if( bytesRead < 0 )
    {
      // TODO handle error
      delete[] buffer;
      return -1;
    }

    if( bytesRead < (int)size )
    {
      // TODO handle error
    }

    // do async write
    XrdCl::XRootDStatus st = file.Write( itr->low, size, buffer, &handler );
    delete[] buffer;
    if( !st.IsOK() )
    {
      handler.Report( new XrdCl::XRootDStatus( st ) );
    }
  }

  handler.Wait();

  return handler.WasSuccessful() ? 0 : -1;
}
