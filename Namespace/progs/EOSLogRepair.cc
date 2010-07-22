//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Change Log reparation utility
//------------------------------------------------------------------------------

#include <iostream>
#include <sstream>
#include <string>
#include "Namespace/persistency/ChangeLogFile.hh"

//------------------------------------------------------------------------------
// Beautify time
//------------------------------------------------------------------------------
std::string readableTime( time_t t )
{
  int mins = t / 60;
  int secs = t % 60;
  std::ostringstream o;
  o << mins << " m. " << secs << " s.";
  return o.str();
}

//------------------------------------------------------------------------------
// Beautify size
//------------------------------------------------------------------------------
std::string units[] = {"KB", "MB", "GB" };

std::string readableSize( uint64_t size )
{
  std::ostringstream o;
  std::string unit = "B";

  for( int i = 0; i < 3; ++i )
  {
    if( size < 1024 )
      break;

    size /= 1024;
    unit = units[i];
  }

  o << size << " " << unit;
  return o.str();
}

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
      o << "Elapsed time: " << readableTime( stats.timeElapsed ) << " ";
      o << "Progress: " << readableSize( sum ) << " / ";
      o << readableSize( stats.bytesTotal );

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
  std::cerr << "Elapsed time:           " << readableTime(stats.timeElapsed);
  std::cerr << std::endl;

  return 0;
}
