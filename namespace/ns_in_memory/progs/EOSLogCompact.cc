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
// desc:   Change Log compacting utility
//------------------------------------------------------------------------------

#include <iostream>
#include <sstream>
#include <string>
#include "namespace/utils/DisplayHelper.hh"
#include "namespace/utils/DataHelper.hh"
#include "namespace/ns_in_memory/persistency/LogManager.hh"

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
