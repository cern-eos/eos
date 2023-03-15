//------------------------------------------------------------------------------
//! @file BackOffInvoker.hh
//! @author Abhishek Lekshmanan <abhishek.lekshmanan@cern.ch>
//-----------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                           *
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

#pragma once

#include <limits>
#include <cstdint>
#include <type_traits>

namespace eos::common {

template <typename int_type = uint16_t,
          bool wrap_around=true>
class BackOffInvoker {
public:
  static constexpr auto LIMIT_BY_2 =
      (std::numeric_limits<int_type>::max() >> 1) + 1;

  template <typename Fn>
  bool
  invoke(Fn&& fn)
  {
    bool status = ++mCounter == mLimit;
    if (status) {
      fn();

      mLimit = mLimit == LIMIT_BY_2 ? wrap_around : mLimit << 1;
    }
    return status;
  }

  BackOffInvoker() : mCounter(0), mLimit(1) {}

  static_assert(std::is_unsigned<int_type>::value, "int_type must be unsigned");

private:
  int_type mCounter;
  int_type mLimit;
};

} // eos::common
