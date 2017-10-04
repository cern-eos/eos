//------------------------------------------------------------------------------
//! @file longstring.hh
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


#ifndef FUSE_LONGSTRING_HH_
#define FUSE_LONGSTRING_HH_

#include <stdint.h>

class longstring
{
public:
  static char*
  unsigned_to_decimal(uint64_t number, char* buffer);

  static char*
  to_decimal(int64_t number, char* buffer);
};


#endif
