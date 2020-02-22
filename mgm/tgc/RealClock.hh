// ----------------------------------------------------------------------
// File: RealClock.hh
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

#ifndef __EOSMGMTGC_REALCLOCK_HH__
#define __EOSMGMTGC_REALCLOCK_HH__

#include "mgm/Namespace.hh"
#include "mgm/tgc/IClock.hh"


/*----------------------------------------------------------------------------*/
/**
 * @file RealClock.hh
 *
 * @brief Provides the current time using std::time().
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Provides the current time using std::time()
//------------------------------------------------------------------------------
class RealClock: public IClock {
public:

  //------------------------------------------------------------------------------
  //! @return Number of seconds since the Epoch, 1970-01-01 00:00:00 +0000 (UTC)
  //------------------------------------------------------------------------------
  std::time_t getTime() override;
};

EOSTGCNAMESPACE_END

#endif
