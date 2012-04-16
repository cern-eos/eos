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
// desc:   A couple of small functions that help displaying data
//------------------------------------------------------------------------------

#ifndef EOS_NS_DISPLAY_HELPER_HH
#define EOS_NS_DISPLAY_HELPER_HH

#include <sstream>
#include <string>
#include <iomanip>

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
        o << mins << " m. ";
        o << std::setw(2) << std::setfill('0') << secs << " s.";
        return o.str();
      }

      //------------------------------------------------------------------------
      // Beautify size
      //------------------------------------------------------------------------
      static std::string getReadableSize( uint64_t size )
      {
        std::ostringstream o;
        std::string unit  = "B";
        uint32_t reminder = 0;

        int i;
        for( i = 0; i < 3; ++i )
        {
          if( size < 1024 )
            break;

          reminder = size % 1024;
          size /= 1024;
          unit = units[i];
        }

        o << size;
        if( reminder )
        {
          o << "." << std::setw(3) << std::setfill('0');
          o << reminder*1000/1024;
        }
        o << " " << unit;
        return o.str();
      }
  };
}

#endif // EOS_NS_DISPLAY_HELPER_HH
