//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Change Log compacting utility
//------------------------------------------------------------------------------

#include <iostream>
#include <sstream>
#include <string>
#include "Namespace/persistency/LogManager.hh"
#include "Namespace/utils/DisplayHelper.hh"
#include "Namespace/utils/DataHelper.hh"

//------------------------------------------------------------------------------
// Report feedback from the compacting procedure
//------------------------------------------------------------------------------
class Feedback: public eos::ILogCompactingFeedback
{
  public:
    //--------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------
    Feedback(): pPrevSize( 0 ), pLastUpdated( 0 ) {}

    //--------------------------------------------------------------------------
    // Report progress
    //--------------------------------------------------------------------------
    virtual void reportProgress( eos::LogCompactingStats            &stats,
                                 eos::ILogCompactingFeedback::Stage stage )
    {
      std::stringstream o;

      if( stage == eos::ILogCompactingFeedback::InitialScan )
      {
        if( pLastUpdated == stats.timeElapsed )
          return;
        pLastUpdated = stats.timeElapsed;

        o << "\r";
        o << "Elapsed time: ";
        o << eos::DisplayHelper::getReadableTime( stats.timeElapsed ) << " ";
        o << "Records processed: " << stats.recordsTotal << " (u:";
        o << stats.recordsUpdated << "/d:" << stats.recordsDeleted << ")";
      }
      else if( stage == eos::ILogCompactingFeedback::CopyPreparation )
      {
        std::cerr << std::endl;
        std::cerr << "Records kept: " << stats.recordsKept << " out of ";
        std::cerr << stats.recordsTotal << std::endl;
        return;
      }
      else if( stage == eos::ILogCompactingFeedback::RecordCopying )
      {
        if( pLastUpdated == stats.timeElapsed &&
            stats.recordsWritten != stats.recordsKept )
          return;
        pLastUpdated = stats.timeElapsed;

        o << "\r";
        o << "Elapsed time: ";
        o << eos::DisplayHelper::getReadableTime( stats.timeElapsed ) << " ";
        o << "Records written: " << stats.recordsWritten << " out of ";
        o << stats.recordsKept;
      }

      //------------------------------------------------------------------------
      // Avoid garbage on the screen by overwriting it with spaces
      //------------------------------------------------------------------------
      int thisSize = o.str().size();
      for( int i = thisSize; i <= pPrevSize; ++i )
        o << " ";
      pPrevSize = thisSize;

      std::cerr << o.str() << std::flush;

      if( stage == eos::ILogCompactingFeedback::RecordCopying &&
          stats.recordsWritten == stats.recordsKept )
        std::cerr << std::endl;
    }

  private:
    int    pPrevSize;
    time_t pLastUpdated;
};

//------------------------------------------------------------------------------
// Here we go
//------------------------------------------------------------------------------
int main( int argc, char **argv )
{
  //----------------------------------------------------------------------------
  // Check the commandline parameters
  //----------------------------------------------------------------------------
  if( argc != 3 )
  {
    std::cerr << "Usage:" << std::endl;
    std::cerr << "  " << argv[0] << " old_log_file new_log_file";
    std::cerr << std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // Repair the log
  //----------------------------------------------------------------------------
  Feedback                feedback;
  eos::LogCompactingStats stats;

  try
  {
    eos::LogManager::compactLog( argv[1], argv[2], stats, &feedback );
    eos::DataHelper::copyOwnership( argv[2], argv[1] );
  }
  catch( eos::MDException &e )
  {
    std::cerr << std::endl;
    std::cerr << "Error: " << e.what() << std::endl;
    return 2;
  }

  //----------------------------------------------------------------------------
  // Display the stats
  //----------------------------------------------------------------------------
  std::cerr << "Records updated         " << stats.recordsUpdated     << std::endl;
  std::cerr << "Records deleted:        " << stats.recordsDeleted     << std::endl;
  std::cerr << "Records total:          " << stats.recordsTotal       << std::endl;
  std::cerr << "Records kept:           " << stats.recordsKept        << std::endl;
  std::cerr << "Records written:        " << stats.recordsWritten     << std::endl;
  std::cerr << "Elapsed time:           ";
  std::cerr << eos::DisplayHelper::getReadableTime(stats.timeElapsed);
  std::cerr << std::endl;

  return 0;
}
