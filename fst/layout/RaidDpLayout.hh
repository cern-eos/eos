//------------------------------------------------------------------------------
//! @file RaidDpLayout.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Implementation of the RAID-double parity layout
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

#ifndef __EOSFST_RAIDDPLAYOUT_HH__
#define __EOSFST_RAIDDPLAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/layout/RaidMetaLayout.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//! Used for computing XOR or 128 bits = 8 * 16
#define VECTOR_SIZE 16


//------------------------------------------------------------------------------
//! Implementation of the RAID-double parity layout
//------------------------------------------------------------------------------
class RaidDpLayout : public RaidMetaLayout
{
public:

  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param file handler to current file
  //! @param lid layout id
  //! @param client security information
  //! @param outError error information
  //! @param io access type 
  //! @param storeRecovery if true write back the recovered blocks to file
  //! @param targetSize expected final size
  //! @param bookingOpaque opaque information
  //!
  //--------------------------------------------------------------------------
  RaidDpLayout (XrdFstOfsFile* file,
                int lid,
                const XrdSecEntity* client,
                XrdOucErrInfo* outError,
                eos::common::LayoutId::eIoType io,
                bool storeRecovery = false,
                off_t targetSize = 0,
                std::string bookingOpaque = "oss.size");


  //--------------------------------------------------------------------------
  //! Truncate file
  //!
  //! @param offset truncate value
  //!
  //! @return 0 if successful, otherwise error
  //!
  //--------------------------------------------------------------------------
  virtual int Truncate (XrdSfsFileOffset offset);


  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~RaidDpLayout ();


private:

  //--------------------------------------------------------------------------
  //! Add data block to compute parity stripes for current group of blocks
  //! - used for the streaming mode
  //!
  //! @param offset block offset
  //! @param buffer data buffer
  //! @param length data length
  //!
  //--------------------------------------------------------------------------
  virtual void AddDataBlock (off_t offset, const char* buffer, size_t length);


  //--------------------------------------------------------------------------
  //! Compute parity information
  //--------------------------------------------------------------------------
  virtual void ComputeParity ();


  //--------------------------------------------------------------------------
  //! Write parity information corresponding to a group to files
  //!
  //! @param offsetGroup offset of the group of blocks
  //!
  //! @return 0 if successful, otherwise error
  //!
  //--------------------------------------------------------------------------
  virtual int WriteParityToFiles (off_t offsetGroup);


  //--------------------------------------------------------------------------
  //! Compute XOR operation for two blocks of any size
  //!
  //! @param pBlock1 first input block
  //! @param pBlock2 second input block
  //! @param pResult result of XOR operation
  //! @param totalBytes size of input blocks
  //!
  //--------------------------------------------------------------------------
  void OperationXOR (char* pBlock1,
                     char* pBlock2,
                     char* pResult,
                     size_t totalBytes);


  //--------------------------------------------------------------------------
  //! Do recovery in the current group using simple and/or double parity
  //!
  //! @param offsetInit file offset corresponding to byte 0 from the buffer
  //! @param pBuffer buffer where to save the recovered data
  //! @param rMapPieces map containing corrupted pieces only from a group
  //!
  //! @return true if successful, otherwise error
  //!
  //--------------------------------------------------------------------------
  bool RecoverPiecesInGroup (off_t offsetInit,
                             char* pBuffer,
                             std::map<off_t, size_t>& rMapPieces);


  //--------------------------------------------------------------------------
  //! Return diagonal stripe corresponding to current block
  //!
  //! @param blockId block id
  //!
  //! @return vector containing the blocks on the diagonal stripe
  //!
  //--------------------------------------------------------------------------
  std::vector<unsigned int> GetDiagonalStripe (unsigned int blockId);


  //--------------------------------------------------------------------------
  //! Validate horizontal stripe for a block index
  //!
  //! @param rStripes horizontal stripe for current block id
  //! @param pStatusBlock status of the blocks
  //! @param blockId current block index
  //!
  //! @return true if successful, otherwise false
  //!
  //--------------------------------------------------------------------------
  bool ValidHorizStripe (std::vector<unsigned int>& rStripes,
                         bool* pStatusBlock,
                         unsigned int blockId);


  //--------------------------------------------------------------------------
  //! Validate diagonal stripe for a block index
  //!
  //! @param rStripes diagonal stripe for current block id
  //! @param pStatusBlock vector of block's status
  //! @param blockId current block index
  //!
  //! @return true if successful, otherwise false
  //!
  //--------------------------------------------------------------------------
  bool ValidDiagStripe (std::vector<unsigned int>& rStripes,
                        bool* pStatusBlock,
                        unsigned int blockId);


  //--------------------------------------------------------------------------
  //! Get indices of the simple parity blocks
  //!
  //! @return vecttor containing the values of the simple parity indices
  //!
  //--------------------------------------------------------------------------
  std::vector<unsigned int> GetSimpleParityIndices ();


  //--------------------------------------------------------------------------
  //! Get indices of the double parity blocks
  //!
  //! @return vector containing the values of the double parity indices
  //!
  //--------------------------------------------------------------------------
  std::vector<unsigned int> GetDoubleParityIndices ();


  //--------------------------------------------------------------------------
  //! Get simple parity block corresponding to current block
  //!
  //! @param elemFromStripe any element from the current stripe
  //!
  //! @return value of the simple parity index
  //!
  //--------------------------------------------------------------------------
  unsigned int GetSParityBlock (unsigned int elemFromStripe);


  //--------------------------------------------------------------------------
  //! Get double parity blocks corresponding to current stripe
  //!
  //! @param rStripe elements from the current stripe
  //!
  //! @return value of the double parity block
  //!
  //--------------------------------------------------------------------------
  unsigned int GetDParityBlock (std::vector<unsigned int>& rStripe);


  //--------------------------------------------------------------------------
  //! Map index from nTotalBlocks representation to nDataBlocks
  //!
  //! @param idBig with values between 0 ans 23
  //!
  //! @return index with values between 0 and 15, -1 if error
  //!
  //--------------------------------------------------------------------------
  unsigned int MapBigToSmall (unsigned int idBig);


  //--------------------------------------------------------------------------
  //! Map index from nDataBlocks representation to nTotalBlocks
  //!
  //! @param idSmall with values between 0 and 15
  //!
  //! @return index with values between 0 and 23, -1 if error
  //!
  //--------------------------------------------------------------------------
  virtual unsigned int MapSmallToBig (unsigned int idSmall);


  //--------------------------------------------------------------------------
  //! Disable copy constructor
  //--------------------------------------------------------------------------
  RaidDpLayout (const RaidDpLayout&) = delete;


  //--------------------------------------------------------------------------
  //! Disable assign operator
  //--------------------------------------------------------------------------
  RaidDpLayout& operator = (const RaidDpLayout&) = delete;


  //--------------------------------------------------------------------------
  //! Do recovery using simple parity
  //!
  //! @param offsetInit file offset corresponding to byte 0 from the buffer
  //! @param buffer buffer where to save the recovered data
  //! @param mapPieces map containing corrupted pieces
  //!
  //! @return true if successful, otherwise error
  //!
  //--------------------------------------------------------------------------
  /*
  bool SimpleParityRecover( off_t                    offsetInit,
                            char*                    buffer,
                            std::map<off_t, size_t>& mapPieces,
                            unsigned int&            blocksCorrupted);
   */


};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_RAIDDPLAYOUT_HH__

