// -----------------------------------------------------------------------------
// File: RaidDpFile.hh
// Author: Elvin-Alin Sindrilaru - CERN
// -----------------------------------------------------------------------------

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

#ifndef __EOSFST_RAIDDPFILE_HH__
#define __EOSFST_RAIDDPFILE_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/io/RaidIO.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

#define VECTOR_SIZE 16    //used for computing XOR or 128 bits = 8 * 16

class RaidDpFile : public eos::fst::RaidIO
{
  public:

    // -------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param stripeurl vector containing the urls of the stripe files
    //! @param nparity number of parity stripes
    //! @param storerecovery if true write back the recovered blocks to file
    //! @param isstreaming file is written in streaming mode
    //! @param targetsize expected final size
    //! @param bookingpaque opaque information
    //!
    // -------------------------------------------------------------------------
    RaidDpFile( std::vector<std::string> stripeurl,
                int                      nparity,
                bool                     storerecovery,
                bool                     isstreaming,
                off_t                    targetsize = 0,
                std::string              bookingopaque = "oss.size" );

    // -------------------------------------------------------------------------
    //! Truncate file
    //!
    //! @param offset truncate value
    //!
    //! @return 0 if successful, otherwise error
    //!
    // -------------------------------------------------------------------------
    virtual int truncate( off_t offset );

    // -------------------------------------------------------------------------
    //! Destructor
    // -------------------------------------------------------------------------
    virtual ~RaidDpFile();

  private:

    // -------------------------------------------------------------------------
    //! Recover pieces of corrupted data
    //!
    //! @param offsetInit file offset corresponding to byte 0 from the buffer
    //! @param buffer place where to save the recovered piece
    //! @mapPiece map of pieces to be recovered <offset in file, length>
    //!
    //! @return true if recovery was successful, otherwise false
    //!
    // -------------------------------------------------------------------------
    virtual bool RecoverPieces( off_t offsetInit,
                                char* buffer,
                                std::map<off_t, size_t>& mapPiece );

    // -------------------------------------------------------------------------
    //! Add data block to compute parity stripes for current group of blocks
    //!  - used for the streaming mode
    //!
    //! @param offset block offset
    //! @param buffer data buffer
    //! @param length data length
    //!
    // -------------------------------------------------------------------------
    virtual void AddDataBlock( off_t offset, char* buffer, size_t length );

    // -------------------------------------------------------------------------
    //! Compute parity information
    // -------------------------------------------------------------------------
    virtual void ComputeParity();

    // -------------------------------------------------------------------------
    //! Write parity information corresponding to a group to files
    //!
    //! @param offsetGroup offset of the group of blocks
    //!
    //! @return 0 if successful, otherwise error
    //!
    // -------------------------------------------------------------------------
    virtual int WriteParityToFiles( off_t offsetGroup );

    // -------------------------------------------------------------------------
    //! Compute XOR operation for two blocks of any size
    //!
    //! @param block1 first input block
    //! @param block2 second input block
    //! @param result result of XOR operation
    //! @param totalBytes size of input blocks
    //!
    // -------------------------------------------------------------------------
    void OperationXOR( char*  block1,
                       char*  block2,
                       char*  result,
                       size_t totalBytes );

    // -------------------------------------------------------------------------
    //! Do recovery using simple parity - NOT USED!!
    //!
    //! @param buffer
    //! @param offset
    //! @param length
    //! @blockCorrupted
    //!
    //! @return true if successful, otherwise false
    //!
    // -------------------------------------------------------------------------
    bool SimpleParityRecover( char*  buffer,
                              off_t  offset,
                              size_t length,
                              int&   blockCorrupted );

    // -------------------------------------------------------------------------
    //! Do recovery using simple parity
    //!
    //! @param offsetInit file offset corresponding to byte 0 from the buffer
    //! @param buffer buffer where to save the recovered data
    //! @param mapPieces map containing corrupted pieces
    //!
    //! @return true if successful, otherwise error
    //!
    // -------------------------------------------------------------------------
    /*
    bool SimpleParityRecover( off_t                    offsetInit,
                              char*                    buffer,
                              std::map<off_t, size_t>& mapPieces,
                              unsigned int&            blocksCorrupted);
    */

    // -------------------------------------------------------------------------
    //! Do recovery using simple and/or double parity
    //!
    //! @param offsetInit file offset corresponding to byte 0 from the buffer
    //! @param buffer buffer where to save the recovered data
    //! @param mapPieces map containing corrupted pieces
    //!
    //! @return true if successful, otherwise error
    //!
    // -------------------------------------------------------------------------
    bool DoubleParityRecover( off_t                    offsetInit,
                              char*                    buffer,
                              std::map<off_t, size_t>& mapPieces );

    // -------------------------------------------------------------------------
    //! Return diagonal stripe corresponding to current block
    //!
    //! @param blockId block id
    //!
    //! @return vector containing the blocks on the diagonal stripe
    //!
    // -------------------------------------------------------------------------
    std::vector<unsigned int> GetDiagonalStripe( unsigned int blockId );

    // -------------------------------------------------------------------------
    //! Validate horizontal stripe for a block index
    //!
    //! @param horizStripe horizontal stripe for current block id
    //! @param statusBlock status of the blocks
    //! @param blockId current block index
    //!
    //! @return true if successful, otherwise false
    //!
    // -------------------------------------------------------------------------
    bool ValidHorizStripe( std::vector<unsigned int>& horizStripe,
                           bool*                      statusBlock,
                           unsigned int               blockId );

    // -------------------------------------------------------------------------
    //! Validate diagonal stripe for a block index
    //!
    //! @param diagStripe horizontal stripe for current block id
    //! @param statusBlock status of the blocks
    //! @param blockId current block index
    //!
    //! @return true if successful, otherwise false
    //!
    // -------------------------------------------------------------------------
    bool ValidDiagStripe( std::vector<unsigned int>& diagStripe,
                          bool*                      statusBlock,
                          unsigned int               blockId );

    // -------------------------------------------------------------------------
    //! Get indices of the simple parity blocks
    //!
    //! @return vecttor containing the values of the simple parity indices
    //!
    // -------------------------------------------------------------------------
    std::vector<unsigned int> GetSimpleParityIndices();

    // -------------------------------------------------------------------------
    //! Get indices of the double parity blocks
    //!
    //! @return vector containing the values of the double parity indices
    //!
    // -------------------------------------------------------------------------
    std::vector<unsigned int> GetDoubleParityIndices();

    // -------------------------------------------------------------------------
    //! Get simple parity block corresponding to current block
    //!
    //! @param elemFromStripe any element from the current stripe
    //!
    //! @return value of the simple parity index
    //!
    // -------------------------------------------------------------------------
    unsigned int GetSParityBlock( unsigned int elemFromStripe );

    // -------------------------------------------------------------------------
    //! Get double parity blocks corresponding to current stripe
    //!
    //! @param stripe elements from the current stripe
    //!
    //! @return value of the double parity block
    //!
    // -------------------------------------------------------------------------
    unsigned int GetDParityBlock( std::vector<unsigned int> stripe );

    // -------------------------------------------------------------------------
    //! Map index from nTotalBlocks representation to nDataBlocks
    //!
    //! @param idBig with values between 0 ans 23
    //!
    //! @return index with values between 0 and 15, -1 if error
    //!
    // -------------------------------------------------------------------------
    unsigned int MapBigToSmall( unsigned int idBig );

    // -------------------------------------------------------------------------
    //! Map index from nDataBlocks representation to nTotalBlocks
    //!
    //! @param idSmall with values between 0 and 15
    //!
    //! @return index with values between 0 and 23, -1 if error
    //!
    // -------------------------------------------------------------------------
    virtual unsigned int MapSmallToBig( unsigned int idSmall );

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_RAIDDPFILE_HH__

