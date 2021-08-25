/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Helper class to generate locality hints
//------------------------------------------------------------------------------

#ifndef EOS_NS_LOCALITY_HINT_HH
#define EOS_NS_LOCALITY_HINT_HH

#include <iostream>
#include "namespace/interface/Identifiers.hh"
#include "namespace/Namespace.hh"

EOSNSNAMESPACE_BEGIN

#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#define htobe64 OSSwapHostToBigInt64
#endif

class LocalityHint {
public:

  static std::string build(ContainerIdentifier parent, const std::string &name) {
    std::ostringstream ss;
    ss << unsignedIntToBinaryString(parent.getUnderlyingUInt64());
    ss << ":" << name;

    return ss.str();
  }

private:
  static inline void unsignedIntToBinaryString(uint64_t num, char* buff) {
    uint64_t be = htobe64(num);
    memcpy(buff, &be, sizeof(be));
  }

  static std::string unsignedIntToBinaryString(uint64_t num) {
    char buff[sizeof(num)];
    unsignedIntToBinaryString(num, buff);
    return std::string(buff, sizeof(num));
  }

};


EOSNSNAMESPACE_END

#endif
