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

//------------------------------------------------------------------------------
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Function for doing fast convertion to string
//------------------------------------------------------------------------------

#ifndef __EOS_NS_STRINGCONVERTION_HH__
#define __EOS_NS_STRINGCONVERTION_HH__

#include "namespace/Namespace.hh"
#include "fmt/format.h"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Fast convert element to string representation
//!
//! @param elem element to be converted
//!
//! @return string representation
//------------------------------------------------------------------------------
template <typename T>
std::string stringify(const T& elem)
{
  fmt::MemoryWriter out;
  out << elem;
  return out.str();
}

EOSNSNAMESPACE_END
#endif // __EOS_NS_STRINGCONVERTION_HH__
