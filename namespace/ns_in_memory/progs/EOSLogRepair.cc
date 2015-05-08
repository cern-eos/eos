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
// desc:   Change Log reparation utility
//------------------------------------------------------------------------------

#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include "namespace/utils/DisplayHelper.hh"
#include "namespace/utils/DataHelper.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogFile.hh"

//------------------------------------------------------------------------------
// Report feedback from the reparation procedure
//------------------------------------------------------------------------------
class Feedback: public eos::ILogRepairFeedback
{
  public:
    //--------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------
    Feedback(): pPrevSize( 0 ), pLastUpdated( 0 ) {}

    //--------------------------------------------------------------------------
    // Report progress
    //--------------------------------------------------------------------------
    virtual void reportProgress( eos::LogRepairStats &stats )
    {
      uint64_t          sum = stats.bytesAccepted + stats.bytesDiscarded;
      std::stringstream o;

      if( pLastUpdated == stats.timeElapsed  && sum != stats.bytesTotal )
        return;
      pLastUpdated = stats.timeElapsed;

      o << "\r";
      o << "Elapsed time: ";
      o << eos::DisplayHelper::getReadableTime( stats.timeElapsed ) << " ";
      o << "Progress: " << eos::DisplayHelper::getReadableSize( sum ) << " / ";
      o << eos::DisplayHelper::getReadableSize( stats.bytesTotal );

      //------------------------------------------------------------------------
      // Avoid garbage on the screen by overwriting it with spaces
      //------------------------------------------------------------------------
      int thisSize = o.str().size();
      for( int i = thisSize; i <= pPrevSize; ++i )
        o << " ";
      pPrevSize = thisSize;

      std::cerr << o.str() << std::flush;

      //------------------------------------------------------------------------
      // Go to the next line
      //------------------------------------------------------------------------
      if( sum == stats.bytesTotal )
        std::cerr << std::endl;
    }

    //--------------------------------------------------------------------------
    // Check the header
    //--------------------------------------------------------------------------
    virtual void reportHeaderStatus( bool               isOk,
                                     const std::string &message,
                                     uint8_t            version,
                                     uint16_t           contentFlag )
    {
      std::cerr << "Header status: ";
      if( isOk )
      {
        std::cerr << "OK (version: 0x" << std::setbase(16) << (int)version;
        std::cerr << ", content: 0x" << std::setbase(16) << contentFlag;
        std::cerr << ")" << std::setbase(10) << std::endl;
      }
      else
        std::cerr << "broken (" << message << ")" << std::endl;
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
    std::cerr << "  " << argv[0] << " broken_log_file new_log_file";
    std::cerr << std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // Repair the log
  //----------------------------------------------------------------------------
  Feedback            feedback;
  eos::LogRepairStats stats;

  try
  {
    eos::ChangeLogFile::repair( argv[1], argv[2], stats, &feedback );
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
  std::cerr << "Scanned:                " << stats.scanned            << std::endl;
  std::cerr << "Healthy:                " << stats.healthy            << std::endl;
  std::cerr << "Bytes total:            " << stats.bytesTotal         << std::endl;
  std::cerr << "Bytes accepted:         " << stats.bytesAccepted      << std::endl;
  std::cerr << "Bytes discarded:        " << stats.bytesDiscarded     << std::endl;
  std::cerr << "Not fixed:              " << stats.notFixed           << std::endl;
  std::cerr << "Fixed (wrong magic):    " << stats.fixedWrongMagic    << std::endl;
  std::cerr << "Fixed (wrong checksum): " << stats.fixedWrongChecksum << std::endl;
  std::cerr << "Fixed (wrong size):     " << stats.fixedWrongSize     << std::endl;
  std::cerr << "Elapsed time:           ";
  std::cerr << eos::DisplayHelper::getReadableTime(stats.timeElapsed);
  std::cerr << std::endl;

  return 0;
}
