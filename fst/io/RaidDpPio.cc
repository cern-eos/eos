//------------------------------------------------------------------------------
// File: RaidDpPio.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

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

/*----------------------------------------------------------------------------*/
#include <cmath>
/*----------------------------------------------------------------------------*/
#include "fst/io/RaidDpPio.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RaidDpPio::RaidDpPio( std::vector<std::string> stripeUrl,
                      int                      numParity,
                      bool                     storeRecovery,
                      bool                     isStreaming,
                      off_t                    stripeWidth, 
                      off_t                    targetSize,
                      std::string              bookingOpaque ) :
  RaidMetaLayout( NULL, 0, NULL, NULL, storeRecovery, isStreaming, targetSize, bookingOpaque ),
  RaidMetaPio( stripeUrl, numParity, storeRecovery, isStreaming, stripeWidth, targetSize, bookingOpaque ),
  RaidDpLayout( NULL, 0, NULL, NULL, storeRecovery, isStreaming, targetSize, bookingOpaque )
{
  // empty
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RaidDpPio::~RaidDpPio()
{
  // empty
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
RaidDpPio::Truncate( XrdSfsFileOffset offset ) {
  return RaidMetaPio::Truncate( offset );
}

EOSFSTNAMESPACE_END

