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

#include <queue>

#include "XrdSys/XrdSysPthread.hh"

namespace XrdCl
{
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
        pSem = new XrdSysSemaphore(0);
      };

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      ~SyncQueue()
      {
        delete pSem;
      }

      //------------------------------------------------------------------------
      //! Put the item in the queue
      //------------------------------------------------------------------------
      void Put( const Item &item )
      {
        XrdSysMutexHelper scopedLock( pMutex );
        pQueue.push( item );
        pSem->Post();
      }

      //------------------------------------------------------------------------
      //! Get the item from the front of the queue
      //------------------------------------------------------------------------
      Item Get()
      {
        pSem->Wait();
        XrdSysMutexHelper scopedLock( pMutex );

        // this is not possible, so when it happens we commit a suicide
        if( pQueue.empty() )
          abort();

        Item i = pQueue.front();
        pQueue.pop();
        return i;
      }

      //------------------------------------------------------------------------
      //! Clear the queue
      //------------------------------------------------------------------------
      void Clear()
      {
        XrdSysMutexHelper scopedLock( pMutex );
        while( !pQueue.empty() )
          pQueue.pop();
        delete pSem;
        pSem = new XrdSysSemaphore(0);
      }

      //------------------------------------------------------------------------
      //! Check if the queue is empty
      //------------------------------------------------------------------------
      bool IsEmpty()
      {
        XrdSysMutexHelper scopedLock( pMutex );
        return pQueue.empty();
      }

    protected:
      std::queue<Item>  pQueue;
      XrdSysMutex       pMutex;
      XrdSysSemaphore  *pSem;
  };
}

#endif // __XRD_CL_ANY_OBJECT_HH__
