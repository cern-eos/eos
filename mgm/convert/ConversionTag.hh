//------------------------------------------------------------------------------
// File: ConversionTag.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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
#include "mgm/Namespace.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/convert/ConversionInfo.hh"

#include <string>

EOSMGMNAMESPACE_BEGIN

class ConversionTag
{
public:
  static std::string
  Get(unsigned long long fid, std::string space, unsigned int layoutid,
      std::string plctplcy, bool ctime_update = true)
  {
    char conversion[1024];
    snprintf(conversion, sizeof(conversion) - 1, "%08x", layoutid);
    return Get(fid, space, conversion, plctplcy, ctime_update);
  }

  static std::string
  Get(unsigned long long fid, std::string space, std::string conversion,
      std::string plctplcy, bool ctime_update = true)
  {
    char conversiontagfile[4096];

    if (plctplcy.length()) {
      // requires a ~ separator
      plctplcy.insert(0, "~");
    }

    snprintf(conversiontagfile, sizeof(conversiontagfile) - 1,
             "%016llx:%s#%s%s", fid,
             space.c_str(), conversion.c_str(), plctplcy.c_str());
    std::string conv_tag = conversiontagfile;

    if (ctime_update) {
      conv_tag += eos::mgm::ConversionInfo::UPDATE_CTIME;
    }

    return conv_tag;
  }
};

EOSMGMNAMESPACE_END
