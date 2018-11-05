// ----------------------------------------------------------------------
// File: TapeAwareGc.hh
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

#ifndef __EOSMGM_TAPEAWAREGCCONSTANTS_HH__
#define __EOSMGM_TAPEAWAREGCCONSTANTS_HH__

#include <stdint.h>
/*----------------------------------------------------------------------------*/
/**
 * @file TapeAwareGcConstants.hh
 *
 * @brief Constants used by the tape aware garbage collector code
 *
 */
/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

/// Default value for the minimum number of free bytes an EOS space should have
static const uint64_t TAPEAWAREGC_MINFREEBYTES{10000000000ULL};

EOSMGMNAMESPACE_END

#endif
