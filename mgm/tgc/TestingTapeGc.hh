// ----------------------------------------------------------------------
// File: TestingTapeGc.hh
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

#ifndef __EOSMGM_TESTINGTAPEGC_HH__
#define __EOSMGM_TESTINGTAPEGC_HH__

#include "mgm/tgc/TapeGc.hh"

#include <atomic>
#include <ctime>
#include <mutex>
#include <stdexcept>
#include <thread>

/*----------------------------------------------------------------------------*/
/**
 * @file TestingTapeGc.hh
 *
 * @brief Facilitates the unit testing of the TapeGc class
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Facilitates the unit testing of the TapeGc class
//------------------------------------------------------------------------------
class TestingTapeGc: public TapeGc
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param mgm interface to the EOS MGM
  //! @param space name of the EOS space that this garbage collector will work
  //! on
  //! @param maxConfigCacheAgeSecs maximum age in seconds of a tape-ware garbage
  //! collector's cached configuration
  //----------------------------------------------------------------------------
  TestingTapeGc(
    ITapeGcMgm &mgm,
    const std::string &space,
    const std::time_t maxConfigCacheAgeSecs
  ): TapeGc(mgm, space, maxConfigCacheAgeSecs)
  {
  }

  //----------------------------------------------------------------------------
  //! Make tryToGarbageCollectASingleFile() public so it can be unit tested
  //----------------------------------------------------------------------------
  using TapeGc::tryToGarbageCollectASingleFile;
};

EOSTGCNAMESPACE_END

#endif
