//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Manager for change log files
//------------------------------------------------------------------------------

#ifndef EOS_LOG_MANAGER_HH
#define EOS_LOG_MANAGER_HH

#include "Namespace/MDException.hh"
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

#endif // EOS_LOG_MANAGER_HH
