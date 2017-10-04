//------------------------------------------------------------------------------
// Copyright (c) 2013 by European Organization for Nuclear Research (CERN)
// Author: Lukasz Janyst <ljanyst@cern.ch>
//------------------------------------------------------------------------------
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//------------------------------------------------------------------------------

#ifndef __XRD_CL_SYNC_QUEUE_HH__
#define __XRD_CL_SYNC_QUEUE_HH__

#include "FuseException.hh"

#include <queue>

#include <XrdSys/XrdSysPthread.hh>

//----------------------------------------------------------------------------
//! A synchronized queue
//----------------------------------------------------------------------------
template <typename Item>
class SyncQueue
{
  public:
    //------------------------------------------------------------------------
    //! Constructor
    //------------------------------------------------------------------------
    SyncQueue()
    {
      if( sem_init( &sem, 0, 0 ) )
      {
       throw FuseException( errno );
      }
    };

    //------------------------------------------------------------------------
    //! Destructor
    //------------------------------------------------------------------------
    ~SyncQueue()
    {
      sem_destroy( &sem );

      while( !items.empty() )
      {
        Item *i = items.front();
        items.pop();
        delete i;
      }
    }

    //------------------------------------------------------------------------
    //! Put the item in the queue
    //------------------------------------------------------------------------
    void Put( Item *item )
    {
      XrdSysMutexHelper scopedLock( mutex );
      items.push( item );
      if( sem_post( &sem ) )
      {
        throw FuseException( errno );
      }
    }

    //------------------------------------------------------------------------
    //! Get the item from the front of the queue
    //------------------------------------------------------------------------
    bool Get( Item *&i, time_t timeout = 5 * 60 )
    {
      timespec ts;
      ts.tv_nsec = 0;
      ts.tv_sec  = ::time( 0 ) + timeout;
      if( sem_timedwait( &sem, &ts ) )
      {
        if( errno == ETIMEDOUT )
          return false;

        throw FuseException( errno );
      }

      XrdSysMutexHelper scopedLock( mutex );
      // this is not possible, so when it happens we commit a suicide
      if( items.empty() )
        throw FuseException( ENOENT );

      i = items.front();
      items.pop();
      return true;
    }

  private:

    std::queue<Item*>   items;
    mutable XrdSysMutex mutex;
    sem_t               sem;
};


#endif // __XRD_CL_ANY_OBJECT_HH__
