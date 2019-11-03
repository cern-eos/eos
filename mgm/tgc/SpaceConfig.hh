// ----------------------------------------------------------------------
// File: SpaceConfig.hh
// Author: Steven Murray - CERN
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

#ifndef __EOSMGMTGC_SPACECONFIG_HH__
#define __EOSMGMTGC_SPACECONFIG_HH__

#include "mgm/Namespace.hh"
#include "mgm/tgc/Constants.hh"

#include <cstdint>
#include <ctime>

/*----------------------------------------------------------------------------*/
/**
 * @file SpaceConfig.hh
 *
 * @brief The configuration of a tape-aware garbage collector for a specific EOS
 * space.
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! The configuration of a tape-aware garbage collector for a specific EOS
//! space.
//------------------------------------------------------------------------------
struct SpaceConfig {
  std::time_t queryPeriodSecs;
  std::uint64_t availBytes;
  std::uint64_t totalBytes;

  SpaceConfig():
    queryPeriodSecs(TGC_DEFAULT_QRY_PERIOD_SECS),
    availBytes(TGC_DEFAULT_AVAIL_BYTES),
    totalBytes(TGC_DEFAULT_TOTAL_BYTES)
  {
  }
};

EOSTGCNAMESPACE_END

#endif
