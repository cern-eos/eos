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
typedef uint32_t u32;

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
    // -------------------------------------------------------------------------
    virtual int truncate( off_t offset );

    // -------------------------------------------------------------------------
    //! Destructor
    // -------------------------------------------------------------------------
    virtual ~RaidDpFile();

  private:

    unsigned int nDataBlocks;    //< no. data blocks in a group = nDataStripes^2
    unsigned int nTotalBlocks;   //< no. data and parity blocks in a group

    // virtual int updateParityForGroups(off_t offsetStart, off_t offsetEnd);
    // virtual bool recoverPieces( char* buffer, off_t offset, size_t length );

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
    virtual bool recoverPieces( off_t offsetInit,
                                char* buffer,
                                std::map<off_t, size_t>& mapPiece );

    // -------------------------------------------------------------------------
    //! Add data block to compute parity stripes for current group of blocks
    //!
    //! @param offset block offset
    //! @param buffer data buffer
    //! @param length data length
    //!
    // -------------------------------------------------------------------------
    virtual void addDataBlock( off_t offset, char* buffer, size_t length );

    // -------------------------------------------------------------------------
    //! Compute and write parity blocks to files
    //!
    //! @param offsetGroup offset of group
    //!
    // -------------------------------------------------------------------------
    virtual void doBlockParity( off_t offsetGroup );

    // -------------------------------------------------------------------------
    //! Compute parity information
    // -------------------------------------------------------------------------
    void computeParity();

    // -------------------------------------------------------------------------
    //! Compute XOR operation for two blocks of any size
    //! @param input1 first input block
    //! @param input2 second input block
    //! @param result result of XOR operation
    //! @param size size of input blocks
    //!
    // -------------------------------------------------------------------------
    void operationXOR( char* /*block1*/,
                       char* /*block2*/,
                       char* /*result*/,
                       size_t /*totalBytes*/ );

    // -------------------------------------------------------------------------
    //! Write parity inforamtion corresponding to a group to files
    //!
    //! @param offsetGroup offset of the group of blocks
    //!
    //! @return 0 if successful, otherwise error
    // -------------------------------------------------------------------------
    int writeParityToFiles( off_t offsetGroup );

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
    bool simpleParityRecover( char*  buffer,
                              off_t  offset,
                              size_t length,
                              int&   blockCorrupted );

    // -------------------------------------------------------------------------
    //! Do recovery using simple or doube parity
    //!
    //! @param offsetInit file offset corresponding to byte 0 from the buffer
    //! @param buffer buffer where to save the recovered data
    //! @param mapPieces map containing corrupted pieces
    //!
    //! @return true if successful, otherwise error
    //!
    // -------------------------------------------------------------------------
    bool doubleParityRecover( off_t                    offsetInit,
                              char*                    buffer,
                              std::map<off_t, size_t>& mapPieces );

    // -------------------------------------------------------------------------
    //! Return diagonal stripe corresponding to current block
    //!
    //! id block id
    //!
    //! @return vector containg the blocks on the diagonal stripe
    //!
    // -------------------------------------------------------------------------
    std::vector<unsigned int> getDiagonalStripe( unsigned int blockId );

    // -------------------------------------------------------------------------
    //! Validate horizontal stripe for a block index
    //!
    //! @param horizStripe horizontal stripe for current block id
    //! @param statusBlock status of the blocks
    //! @param blockId current block index
    //!
    //! @return true if successfull, otherwise false
    //!
    // -------------------------------------------------------------------------
    bool validHorizStripe( std::vector<unsigned int>& horizStripe,
                           bool*                      statusBlock,
                           unsigned int               blockId );

    // -------------------------------------------------------------------------
    //! Validate diagonal stripe for a block index
    //!
    //! @param diagStripe horizontal stripe for current block id
    //! @param statusBlock status of the blocks
    //! @param blockId current block index
    //!
    //! @return true if successfull, otherwise false
    //!
    // -------------------------------------------------------------------------
    bool validDiagStripe( std::vector<unsigned int>& diagStripe,
                          bool*                      statusBlock,
                          unsigned int               blockId );

    // -------------------------------------------------------------------------
    //! Get indices of the simple parity blocks
    // -------------------------------------------------------------------------
    std::vector<unsigned int> getSimpleParityIndices();

    // -------------------------------------------------------------------------
    //! Get indices of the double parity blocks
    // -------------------------------------------------------------------------
    std::vector<unsigned int> getDoubleParityIndices();

    // -------------------------------------------------------------------------
    //! Get simple parity blocks corresponding to current block
    // -------------------------------------------------------------------------
    unsigned int getParityBlockId( unsigned int );

    // -------------------------------------------------------------------------
    //! Get double parity blocks corresponding to current block
    // -------------------------------------------------------------------------
    unsigned int getDParityBlockId( std::vector<unsigned int> );

    // -------------------------------------------------------------------------
    //! Map index from nTotalBlocks representation to nDataBlocks
    //!
    //! @param idBig with values between 0 ans 23
    //!
    //! @return index with values between 0 and 15, -1 if error
    //!
    // -------------------------------------------------------------------------
    unsigned int mapBigToSmall( unsigned int idBig );

    // -------------------------------------------------------------------------
    //! Map index from nDataBlocks representation to nTotalBlocks
    //!
    //! @param idSmall with values between 0 and 15
    //!
    //! @return index with values between 0 and 23, -1 if error
    //!
    // -------------------------------------------------------------------------
    unsigned int mapSmallToBig( unsigned int idSmall );

    //--------------------------------------------------------------------------
    //! Non-streaming operation 
    //! Get a set of the group offsets for which we can compute the parity info
    //!
    //! @param offGroups set of offsets of the groups for which we can
    //!                  compute the parity
    //! @param forceAll  get also the offsets of the groups for which
    //!                  we don't have all the data
    //!
    //--------------------------------------------------------------------------
    virtual void GetOffsetGroups(std::set<off_t>& offGroups, bool forceAll);

    //--------------------------------------------------------------------------
    //! Non-streaming operation 
    //! Read data from the current group ofr parity computation
    //!
    //! @param offsetGroup offset of the grou about to be read
    //!
    //! @return true if operation successful, otherwise error
    //--------------------------------------------------------------------------
    virtual bool ReadGroup(off_t offsetGroup);

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_RAIDDPFILE_HH__

