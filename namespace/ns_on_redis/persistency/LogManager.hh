/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Manager for change log files
//------------------------------------------------------------------------------

#ifndef EOS_NS_LOG_MANAGER_HH
#define EOS_NS_LOG_MANAGER_HH

#include "namespace/MDException.hh"
#include <string>
#include <ctime>
#include <stdint.h>

namespace eos
{
  //----------------------------------------------------------------------------
  //! Placeholder for log compacting stats
  //----------------------------------------------------------------------------
  struct LogCompactingStats
  {
    LogCompactingStats(): recordsUpdated(0), recordsDeleted(0), recordsTotal(0),
                          recordsKept(0), recordsWritten(0), timeElapsed(0) {}

    uint64_t recordsUpdated;
    uint64_t recordsDeleted;
    uint64_t recordsTotal;
    uint64_t recordsKept;
    uint64_t recordsWritten;
    time_t   timeElapsed;
  };

  //----------------------------------------------------------------------------
  //! Feedback from the changelog compacting process
  //----------------------------------------------------------------------------
  class ILogCompactingFeedback
  {
    public:
      enum Stage
      {
        InitialScan     = 1,
        CopyPreparation = 2,
        RecordCopying   = 3
      };

      //------------------------------------------------------------------------
      //! Called to report progress to the outside world
      //------------------------------------------------------------------------
      virtual void reportProgress( LogCompactingStats &stats, Stage stage ) = 0;
  };

  //----------------------------------------------------------------------------
  //! Manage change log files
  //----------------------------------------------------------------------------
  class LogManager
  {
    public:
      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      LogManager() {}

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~LogManager() {}

      //------------------------------------------------------------------------
      //! Compact the old log and write a new one, this works only for logs
      //! containing eos file and container metadata and assumes that
      //! first 8 bytes of each record containes the file or container
      //! identifier
      //------------------------------------------------------------------------
      static void compactLog( const std::string      &oldLogName,
                              const std::string      &newLogName,
                              LogCompactingStats     &stats,
                              ILogCompactingFeedback *feedback )
        throw( MDException );
  };
}

#endif // EOS_NS_LOG_MANAGER_HH
