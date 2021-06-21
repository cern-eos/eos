//------------------------------------------------------------------------------
//! @file ReedSLayout.hh
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
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

#pragma once
#include "fst/layout/RainMetaLayout.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Implementation of the Reed-Solomon layout - this uses the Jerasure code
//! for implementing Cauchy Reed-Solomon
//------------------------------------------------------------------------------
class ReedSLayout : public RainMetaLayout
{
public:

  //----------------------------------------------------------------------------
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
  //----------------------------------------------------------------------------
  ReedSLayout(XrdFstOfsFile* file, unsigned long lid,
              const XrdSecEntity* client, XrdOucErrInfo* outError,
              const char* path, uint16_t timeout = 0,
              bool storeRecovery = false, off_t targetSize = 0,
              std::string bookingOpaque = "oss.size");

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ReedSLayout()
  {
    FreeJerasure();
  }

  //----------------------------------------------------------------------------
  //! Truncate file
  //!
  //! @param offset truncate size value
  //!
  //! @return 0 if successful, otherwise error
  //----------------------------------------------------------------------------
  virtual int Truncate(XrdSfsFileOffset offset);

  //----------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int Fallocate(XrdSfsFileOffset lenght);

  //----------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int Fdeallocate(XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset);

private:
  //----------------------------------------------------------------------------
  //! Disable copy/move assign/constructor operators
  //----------------------------------------------------------------------------
  ReedSLayout& operator = (const ReedSLayout&) = delete;
  ReedSLayout(const ReedSLayout&) = delete;
  ReedSLayout& operator = (ReedSLayout&&) = delete;
  ReedSLayout(ReedSLayout&&) = delete;

  //! Values use by Jerasure codes
  unsigned int w;           ///< word size for Jerasure
  unsigned int mPacketSize; ///< packet size for Jerasure
  int* matrix;
  int* bitmatrix;
  int** schedule;
  std::atomic<bool> mDoneInit {false}; ///< Mark Jerasure initialization

  //----------------------------------------------------------------------------
  //! Initialise the Jerasure structures used for encoding and decoding
  //----------------------------------------------------------------------------
  void InitialiseJerasure();

  //----------------------------------------------------------------------------
  //! Deallocated any Jerasure structures used for encoding and decoding
  //----------------------------------------------------------------------------
  void FreeJerasure();

  //------------------------------------------------------------------------------
  //! Compute error correction blocks
  //!
  //! @param grp group object for parity computation
  //!
  //! @return true if parity info computed successfully, otherwise false
  //------------------------------------------------------------------------------
  virtual bool ComputeParity(std::shared_ptr<eos::fst::RainGroup>& grp);

  //----------------------------------------------------------------------------
  //! Write parity information corresponding to a group to files
  //!
  //! @param grp group object
  //!
  //! @return 0 if successful, otherwise error
  //----------------------------------------------------------------------------
  virtual int WriteParityToFiles(std::shared_ptr<eos::fst::RainGroup>& grp);

  //--------------------------------------------------------------------------
  //! Recover corrupted chunks from the current group
  //!
  //! @param grp_errs chunks to be recovered
  //!
  //! @return true if recovery successful, false otherwise
  //--------------------------------------------------------------------------
  virtual bool RecoverPiecesInGroup(XrdCl::ChunkList& grp_errs);

  //--------------------------------------------------------------------------
  //! Map index from nDataBlocks representation to nTotalBlocks
  //!
  //! @param idSmall with values between 0 and nDataBlocks
  //!
  //! @return index with the same values as idSmall, identical function
  //--------------------------------------------------------------------------
  virtual unsigned int MapSmallToBig(unsigned int idSmall);

  //--------------------------------------------------------------------------
  //! Convert a global offset (from the inital file) to a local offset within
  //! a stripe data file. The initial block does *NOT* span multiple chunks
  //! (stripes) therefore if the original length is bigger than one chunk the
  //! splitting must be done before calling this method.
  //!
  //! @param global_off initial offset
  //!
  //! @return tuple made up of the logical index of the stripe data file the
  //!         piece belongs to and the local offset within that file.
  //--------------------------------------------------------------------------
  virtual std::pair<int, uint64_t>
  GetLocalPos(uint64_t global_off);

  //--------------------------------------------------------------------------
  //! Convert a local position (from a stripe data file) to a global position
  //! within the initial file file. Note that the local offset has to come
  //! from a stripe data file since there is no corresponde in the original
  //! file for a piece which is in the parity stripe.
  //!
  //! @param stripe_id logical stripe index
  //! @param local_off local offset
  //!
  //! @return offset in the initial file of the local given piece
  //--------------------------------------------------------------------------
  virtual uint64_t
  GetGlobalOff(int stripe_id, uint64_t local_off);
};

EOSFSTNAMESPACE_END
