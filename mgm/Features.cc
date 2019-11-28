// ----------------------------------------------------------------------
// File: ZMQ.hh
// Author: Geoffray Adde - CERN
// ----------------------------------------------------------------------

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
/*----------------------------------------------------------------------------*/
#include "mgm/Features.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

static std::string checkInodeScheme() {
  if(getenv("EOS_USE_NEW_INODES") != nullptr && getenv("EOS_USE_NEW_INODES")[0] == '1') {
    return "1";
  }

  return "0";
}

const std::map< const std::string, const std::string> Features::sMap =
{
  { "eos.encodepath", "curl" },
  { "eos.lazyopen",   "true" },
  { "eos.inodeencodingscheme", checkInodeScheme() }
};

EOSMGMNAMESPACE_END
