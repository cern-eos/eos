//------------------------------------------------------------------------------
//! @file Utils.cc
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "common/utils/Utils.hh"
#include <uuid/uuid.h>

EOSCOMMONNAMESPACE_BEGIN

const std::string Utils::generateTimeBasedUuid()
{
  uuid_t uuid;
  uuid_generate_time(uuid);
  //The uuid_unparse function converts the supplied UUID from the binary representation into a 36-byte string + '\0' trailing
  char uuid_str[37];
  uuid_unparse(uuid,uuid_str);
  return std::string(uuid_str);
}

EOSCOMMONNAMESPACE_END
