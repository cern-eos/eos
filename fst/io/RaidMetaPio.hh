//------------------------------------------------------------------------------
//! @file RaidMetaPio.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Generic class to read/write RAID-like layout files using parallel IO
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

#ifndef __EOSFST_RAIDMETAPIO_HH__
#define __EOSFST_RAIDMETAPIO_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/layout/RaidMetaLayout.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Generic class to read/write different RAID-like layout files using parallel IO
//------------------------------------------------------------------------------
class RaidMetaPio : virtual public RaidMetaLayout
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
    RaidMetaPio( std::vector<std::string>& stripeUrl,
                 unsigned int              nbParity,
                 bool                      storeRecovery,
                 bool                      isStreaming,
                 off_t                     stripeWidth,
                 off_t                     targetSize = 0,
                 std::string               bookingOpaque = "oss.size" );
  

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~RaidMetaPio();


    //--------------------------------------------------------------------------
    //! Open file
    //!
    //! @param flags flags O_RDWR/O_RDONLY/O_WRONLY
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Open( XrdSfsFileOpenMode flags );


    //--------------------------------------------------------------------------
    //! Remove file
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Remove();


    //--------------------------------------------------------------------------
    //! Sync file to disk
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Sync();


    //--------------------------------------------------------------------------
    //! Close file
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Close();


    //--------------------------------------------------------------------------
    //! Get stats about the file
    //!
    //! @param buf stat buffer
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Stat( struct stat* buf );


    //--------------------------------------------------------------------------
    //! Truncate
    //!
    //! @param offset truncate file to this value
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Truncate( XrdSfsFileOffset offset );

 private:

  std::vector<std::string> mStripeUrls;  ///< the urls of the files
 
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_RAIDMETAPIO_HH__

