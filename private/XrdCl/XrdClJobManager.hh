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

#ifndef __XRD_CL_JOB_MANAGER_HH__
#define __XRD_CL_JOB_MANAGER_HH__

#include <cstdint>
#include <vector>
#include <algorithm>
#include <pthread.h>
#include "XrdCl/XrdClSyncQueue.hh"

namespace XrdCl
{
  //----------------------------------------------------------------------------
  //! Interface for a job to be run by the job manager
  //----------------------------------------------------------------------------
  class Job
  {
    public:
      //------------------------------------------------------------------------
      //! Virtual destructor
      //------------------------------------------------------------------------
      virtual ~Job() {};

      //------------------------------------------------------------------------
      //! The job logic
      //------------------------------------------------------------------------
      virtual void Run( void *arg ) = 0;
  };

  //----------------------------------------------------------------------------
  //! A synchronized queue
  //----------------------------------------------------------------------------
  class JobManager
  {
    public:
      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      JobManager( uint32_t workers )
      {
        pRunning = false;
        pWorkers.resize( workers );
      }

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      ~JobManager()
      {
      }

      //------------------------------------------------------------------------
      //! Initialize the job manager
      //------------------------------------------------------------------------
      bool Initialize();

      //------------------------------------------------------------------------
      //! Finalize the job manager, clear the queues
      //------------------------------------------------------------------------
      bool Finalize();

      //------------------------------------------------------------------------
      //! Start the workers
      //------------------------------------------------------------------------
      bool Start();

      //------------------------------------------------------------------------
      //! Stop the workers
      //------------------------------------------------------------------------
      bool Stop();

      //------------------------------------------------------------------------
      //! Add a job to be run
      //------------------------------------------------------------------------
      void QueueJob( Job *job, void *arg = 0 )
      {
        pJobs.Put( JobHelper( job, arg ) );
      }

      //------------------------------------------------------------------------
      //! Run the jobs
      //------------------------------------------------------------------------
      void RunJobs();

      bool IsWorker()
      {
        pthread_t thread = pthread_self();
        std::vector<pthread_t>::iterator itr =
            std::find( pWorkers.begin(), pWorkers.end(), thread );
        return itr != pWorkers.end();
      }

    private:
      //------------------------------------------------------------------------
      //! Stop all workers up to n'th
      //------------------------------------------------------------------------
      void StopWorkers( uint32_t n );

      struct JobHelper
      {
        JobHelper( Job *j = 0, void *a = 0 ): job(j), arg(a) {}
        Job  *job;
        void *arg;
      };

      std::vector<pthread_t> pWorkers;
      SyncQueue<JobHelper>   pJobs;
      XrdSysMutex            pMutex;
      bool                   pRunning;
  };
}

#endif // __XRD_CL_ANY_OBJECT_HH__
