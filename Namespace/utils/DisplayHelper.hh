//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   A couple of small functions that help displaying data
//------------------------------------------------------------------------------

#ifndef EOS_DISPLAY_HELPER_HH
#define EOS_DISPLAY_HELPER_HH

#include <sstream>
#include <string>

namespace eos
{
  std::string units[] = {"KB", "MB", "GB" };
  class DisplayHelper
  {
    public:

      //------------------------------------------------------------------------
      // Beautify time
      //------------------------------------------------------------------------
      static std::string getReadableTime( time_t t )
      {
        int mins = t / 60;
        int secs = t % 60;
        std::ostringstream o;
        o << mins << " m. " << secs << " s.";
        return o.str();
      }

      //------------------------------------------------------------------------
      // Beautify size
      //------------------------------------------------------------------------
      static std::string getReadableSize( uint64_t size )
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
  };
}

#endif // EOS_DISPLAY_HELPER_HH
