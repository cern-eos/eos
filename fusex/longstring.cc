//------------------------------------------------------------------------------
//! @file longstring.cc
//! @author Andreas-Joachim Peters CERN
//! @brief Class implementing fast number to string conversions
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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


#include "longstring.hh"
/*----------------------------------------------------------------------------*/
#include <limits> // std::numeric_limits
#include <algorithm> // std::reverse
/*----------------------------------------------------------------------------*/

using std::numeric_limits;
using std::reverse;

/*----------------------------------------------------------------------------*/
/**
 */

/*----------------------------------------------------------------------------*/


char*
longstring::unsigned_to_decimal (uint64_t number, char* buffer)
{
  char* sbuffer = buffer;
  if (number == 0)
  {
    *buffer++ = '0';
  }
  else
  {
    char* p_first = buffer;
    while (number != 0)
    {
      *buffer++ = '0' + number % 10;
      number /= 10;
    }
    reverse(p_first, buffer);
  }
  *buffer = '\0';
  return sbuffer;
}

/*----------------------------------------------------------------------------*/
/**
 */

/*----------------------------------------------------------------------------*/

char*
longstring::to_decimal (int64_t number, char* buffer)
{
  if (number < 0)
  {
    buffer[0] = '-';
    unsigned_to_decimal(-number, buffer + 1);
  }
  else
  {
    unsigned_to_decimal(number, buffer);
  }
  return buffer;
}

/*----------------------------------------------------------------------------*/


