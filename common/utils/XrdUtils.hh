//------------------------------------------------------------------------------
//! @file XrdUtils.hh
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

#ifndef EOS_XRDUTILS_HH
#define EOS_XRDUTILS_HH

#include <XrdOuc/XrdOucTList.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <type_traits>
#include "common/StringUtils.hh"
#include "common/Namespace.hh"

EOSCOMMONNAMESPACE_BEGIN

/**
 * Utility class linked to Xrootd objects and containers
 */
class XrdUtils
{
public:
  /**
   * Counts the number of elements contained in a XrdOucTList
   * @param listPtr the pointer pointing to the XrdOucTList which we want to count the number of elements
   * @return The number of elements the list listPtr contains
   */
  static unsigned int countNbElementsInXrdOucTList(const XrdOucTList* listPtr);


  // While we don't modify XrdOucEnv, since Get isn't const marked, we've to use
  // a mutable ref
  static std::string
  GetEnv(XrdOucEnv& env, const char* key,
         std::string_view default_str = {});

  template <typename T>
  static auto
  GetEnv(XrdOucEnv& env, const char* key,
         T& out, T default_val={})
    -> std::enable_if_t<std::is_arithmetic_v<T>, bool> {
    char* val = 0;
    if ((val = env.Get(key))) {
      return eos::common::StringToNumeric(std::string_view(val), out, default_val);
    }

    return false;
  }

  // Variant for GetEnv for getting from system environment variables
  template <typename T>
  static auto
  GetEnv(const char* key, T& out, T default_val={})
    -> std::enable_if_t<std::is_arithmetic_v<T>, bool> {
    char *val = nullptr;
    if ((val = getenv(key))) {
      return common::StringToNumeric(std::string_view(val), out, default_val);
    }
    return false;
  }
};

EOSCOMMONNAMESPACE_END
#endif // EOS_XRDUTILS_HH
