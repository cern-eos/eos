//------------------------------------------------------------------------------
//! @file ReedSLayout.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Implementation of the Reed-Solomon layout
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

#ifndef __EOSFST_REEDSFILE_HH__
#define __EOSFST_REEDSFILE_HH__

/*----------------------------------------------------------------------------*/
#include "fst/layout/RaidMetaLayout.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Implementation of the Reed-Solomon layout
//------------------------------------------------------------------------------
class ReedSLayout : public RaidMetaLayout
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
  //! @param timeout timeout value 
  //! @param storeRecovery if true write back the recovered blocks to file
  //! @param targetSize expected final size
  //! @param bookingOpaque opaque information
  //!
  //--------------------------------------------------------------------------
  ReedSLayout (XrdFstOfsFile* file,
               int lid,
               const XrdSecEntity* client,
               XrdOucErrInfo* outError,
               eos::common::LayoutId::eIoType io,
               uint16_t timeout = 0,
               bool storeRecovery = false,
               off_t targetSize = 0,
               std::string bookingOpaque = "oss.size");


  //--------------------------------------------------------------------------
  //! Truncate file
  //!
  //! @param offset truncate size value
  //!
  //! @return 0 if successful, otherwise error
  //!
  //--------------------------------------------------------------------------
  virtual int Truncate (XrdSfsFileOffset offset);


  //--------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Fallocate (XrdSfsFileOffset lenght);


  //--------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Fdeallocate (XrdSfsFileOffset fromOffset,
                           XrdSfsFileOffset toOffset);


  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~ReedSLayout ();

private:

  //--------------------------------------------------------------------------
  //! Compute error correction blocks
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
  //! Recover pieces of corrupted data in the current group
  //!
  //! @param offset file offset corresponding to byte 0 from the buffer
  //! @param pBuffer place where to save the recovered piece
  //! @param rMapPiece map of pieces to be recovered <offset in file, length>
  //!                  which belong to the same group
  //!
  //! @return true if recovery was successful, otherwise false
  //!
  //--------------------------------------------------------------------------
  virtual bool RecoverPiecesInGroup (off_t offset,
                                     char* pBuffer,
                                     std::map<off_t, size_t>& rMapPieces);


  //--------------------------------------------------------------------------
  //! Add data block to compute parity stripes for current group of blocks
  //!
  //! @param offset block offset
  //! @param pBuffer data buffer
  //! @param length data length
  //!
  //--------------------------------------------------------------------------
  virtual void AddDataBlock (off_t offset, const char* pBuffer, size_t length);


  //--------------------------------------------------------------------------
  //! Map index from nDataBlocks representation to nTotalBlocks
  //!
  //! @param idSmall with values between 0 and nDataBlocks
  //!
  //! @return index with the same values as idSmall, identical function
  //!
  //--------------------------------------------------------------------------
  virtual unsigned int MapSmallToBig (unsigned int idSmall);


  //--------------------------------------------------------------------------
  //! Get backtracking solution
  //--------------------------------------------------------------------------
  bool SolutionBkt (unsigned int k,
                    unsigned int* pIndexes,
                    std::vector<unsigned int>& validId);


  //--------------------------------------------------------------------------
  //! Validate backtracking solution
  //--------------------------------------------------------------------------
  bool ValidBkt (unsigned int k,
                 unsigned int* pIndexes,
                 std::vector<unsigned int>& validId);


  //--------------------------------------------------------------------------
  //! Backtracking method for getting the indices used in the recovery process
  //--------------------------------------------------------------------------
  bool Backtracking (unsigned int k,
                     unsigned int* pIndexes,
                     std::vector<unsigned int>& validId);


  //--------------------------------------------------------------------------
  //! Disable copy constructor
  //--------------------------------------------------------------------------
  ReedSLayout (const ReedSLayout&) = delete;


  //--------------------------------------------------------------------------
  //! Disable assign operator
  //--------------------------------------------------------------------------
  ReedSLayout& operator = (const ReedSLayout&) = delete;

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_REEDSLAYOUT_HH__
