//------------------------------------------------------------------------------
//! @file ReedSPio.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Implementation of the Reed-Solomon layout using parallel IO
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

#ifndef __EOSFST_REEDSPIO_HH__
#define __EOSFST_REEDSPIO_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/io/RaidMetaPio.hh"
#include "fst/layout/ReedSLayout.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN


//------------------------------------------------------------------------------
//! Implementation of the Reed-Solomon layout using parallel IO
//------------------------------------------------------------------------------
class ReedSPio : public RaidMetaPio, public ReedSLayout
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param stripeUrl urls of the stripes
    //! @param numParity number of parity stripes
    //! @param storeRecovery if true write back the recovered blocks to file
    //! @param isStreaming file is written in streaming mode
    //! @param targetSize expected final size
    //! @param bookingOpaque opaque information
    //!
    //--------------------------------------------------------------------------
    ReedSPio( std::vector<std::string> stripeUrl,
              int                      numParity,
              bool                     storeRecovery,
              bool                     isStreaming,
              off_t                    stripeWidth, 
              off_t                    targetSize = 0,
              std::string              bookingOpaque = "oss.size" );
  

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~ReedSPio();


    //--------------------------------------------------------------------------
    //! Truncate
    //!
    //! @param offset truncate file to this value
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Truncate( XrdSfsFileOffset offset );

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_REEDSPIO_HH__

