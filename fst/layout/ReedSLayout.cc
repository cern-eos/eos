//------------------------------------------------------------------------------
// File: ReedSLayout.cc
// Author: Elvin-Alin Sindrilaru - CERN
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
#include "common/Timing.hh"
#include "fst/layout/ReedSLayout.hh"
#include "fst/io/AsyncMetaHandler.hh"
/*----------------------------------------------------------------------------*/
#include <cmath>
#include <map>
#include <set>
#include <algorithm>
#include "fst/layout/jerasure/jerasure.hh"
#include "fst/layout/jerasure/reed_sol.hh"
#include "fst/layout/jerasure/galois.hh"
#include "fst/layout/jerasure/cauchy.hh"
#include "fst/layout/jerasure/liberation.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ReedSLayout::ReedSLayout(XrdFstOfsFile* file,
                         int lid,
                         const XrdSecEntity* client,
                         XrdOucErrInfo* outError,
                         eos::common::LayoutId::eIoType io,
                         uint16_t timeout,
                         bool storeRecovery,
                         off_t targetSize,
                         std::string bookingOpaque) :
  RaidMetaLayout(file, lid, client, outError, io, timeout,
                 storeRecovery, targetSize, bookingOpaque),
  mDoneInitialisation(false)
{
  mNbDataBlocks = mNbDataFiles;
  mNbTotalBlocks = mNbDataFiles + mNbParityFiles;
  mSizeGroup = mNbDataFiles * mStripeWidth;
  mSizeLine = mSizeGroup;

  // Set the parameters for the Jerasure codes
  mTech = Cauchy_Good;   // type of error correction code
  w = 8;                 // "word size" this can be adjusted between 4..32
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

ReedSLayout::~ReedSLayout()
{
  // empty
}


//------------------------------------------------------------------------------
// Initialise the Jerasure library
//------------------------------------------------------------------------------
bool
ReedSLayout::InitialiseJerasure()
{
  mPacketSize = mSizeLine / (mNbDataBlocks * w * sizeof(int));
  eos_info("mStripeWidth=%zu, mSizeLine=%zu, mNbDataBlocks=%u, mNbParityFiles=%u,"
           " w=%u, mPacketSize=%u", mStripeWidth, mSizeLine, mNbDataBlocks,
           mNbParityFiles, w, mPacketSize);

  if (mSizeLine % mPacketSize != 0)
  {
    eos_err("error=packet size could not be computed correctly");
    return false;
  }

  // Do error checking for coding technique
  if (mTech == Reed_Sol_Van)
  {
    if (w != 8 && w != 16 && w != 32)
    {
      eos_err("error=w must be one of {8, 16, 32}");
      return false;
    }
  }
  else if (mTech == Reed_Sol_R6_Op)
  {
    if (mNbParityFiles != 2)
    {
      eos_err("error=m must be equal to 2");
      return false;
    }

    if (w != 8 && w != 16 && w != 32)
    {
      eos_err("error=w must be one of {8, 16, 32}");
      return false;
    }
  }
  else if (mTech == Cauchy_Orig)
  {
    if (mPacketSize == 0)
    {
      eos_err("error=must include packetsize");
      return false;
    }
  }
  else if (mTech == Cauchy_Good)
  {
    if (mPacketSize == 0)
    {
      eos_err("error=must include packetsize");
      return false;
    }
  }
  else if (mTech == Liberation)
  {
    if (mNbDataBlocks > w)
    {
      eos_err("error= mNbDataBlocks must be less than or equal to w");
      return false;
    }

    if (w <= 2 || !(w % 2) || !IsPrime(w))
    {
      eos_err("error=w must be greater than two and w must be prime");
      return false;
    }

    if (mPacketSize == 0)
    {
      eos_err("error=must include packetsize");
      return false;
    }

    if ((mPacketSize % (sizeof(int))) != 0)
    {
      eos_err("error=packetsize must be a multiple of sizeof(int)");
      return false;
    }
  }
  else if (mTech == Blaum_Roth)
  {
    if (mNbDataBlocks > w)
    {
      eos_err("error=mNbDataBlocks must be less than or equal to w");
      return false;
    }

    if (w <= 2 || !((w + 1) % 2) || !IsPrime(w + 1))
    {
      eos_err("error=w must be greater than two and w+1 must be prime");
      return false;
    }

    if (mPacketSize == 0)
    {
      eos_err("error=must include packetsize");
      return false;
    }

    if ((mPacketSize % (sizeof(int))) != 0)
    {
      eos_err("error=packetsize must be a multiple of sizeof(int)");
      return false;
    }
  }
  else if (mTech == Liber8tion)
  {
    if (mPacketSize == 0)
    {
      eos_err("error=must include packetsize");
      return false;
    }

    if (w != 8)
    {
      eos_err("error=w must equal 8");
      return false;
    }

    if (mNbParityFiles != 2)
    {
      eos_err("error=mNbParityBlocks must equal 2");
      return false;
    }

    if (mNbDataBlocks > w)
    {
      eos_err("error=mNbDataBlocks must be less than or equal to w");
      return false;
    }
  }
  else
  {
    eos_err("error=not a valid coding technique. Set one of the following: "
            "reed_sol_van, reed_sol_r6_op, cauchy_orig, cauchy_good, liberation,"
            " blaum_roth, liber8tion, no_coding");
    return false;
  }

  switch (mTech)
  {
  case No_Coding:
    break;

  case Reed_Sol_Van:
    matrix = reed_sol_vandermonde_coding_matrix(mNbDataBlocks, mNbParityFiles, w);
    break;

  case Cauchy_Orig:
    matrix = cauchy_original_coding_matrix(mNbDataBlocks, mNbParityFiles, w);
    bitmatrix = jerasure_matrix_to_bitmatrix(mNbDataBlocks, mNbParityFiles, w, matrix);
    schedule = jerasure_smart_bitmatrix_to_schedule(mNbDataBlocks, mNbParityFiles, w, bitmatrix);
    break;

  case Cauchy_Good:
    matrix = cauchy_good_general_coding_matrix(mNbDataBlocks, mNbParityFiles, w);
    bitmatrix = jerasure_matrix_to_bitmatrix(mNbDataBlocks, mNbParityFiles, w, matrix);
    schedule = jerasure_smart_bitmatrix_to_schedule(mNbDataBlocks, mNbParityFiles, w, bitmatrix);
    break;

  case Liberation:
    bitmatrix = liberation_coding_bitmatrix(mNbDataBlocks, w);
    schedule = jerasure_smart_bitmatrix_to_schedule(mNbDataBlocks, mNbParityFiles, w, bitmatrix);
    break;

  case Blaum_Roth:
    bitmatrix = blaum_roth_coding_bitmatrix(mNbDataBlocks, w);
    schedule = jerasure_smart_bitmatrix_to_schedule(mNbDataBlocks, mNbParityFiles, w, bitmatrix);
    break;

  case Liber8tion:
    bitmatrix = liber8tion_coding_bitmatrix(mNbDataBlocks);
    schedule = jerasure_smart_bitmatrix_to_schedule(mNbDataBlocks, mNbParityFiles, w, bitmatrix);
    break;

  default:
    eos_err("error=unsupported coding technique");
    break;
  }

  return true;
}


//------------------------------------------------------------------------------
// Compute the error correction blocks
//------------------------------------------------------------------------------
bool
ReedSLayout::ComputeParity()
{
  // Initialise Jerasure structures if not done already
  if (!mDoneInitialisation)
  {
    if (!InitialiseJerasure())
    {
      eos_err("error=failed to initialise Jerasure library");
      return false;
    }
    
    mDoneInitialisation = true;
  }
  
  // Get pointers to data and parity information
  char* coding[mNbParityFiles];
  char* data[mNbDataFiles];

  for (unsigned int i = 0; i < mNbDataFiles; i++)
  {
    data[i] = (char*) mDataBlocks[i];
  }

  for (unsigned int i = 0; i < mNbParityFiles; i++)
  {
    coding[i] = (char*) mDataBlocks[mNbDataFiles + i];
  }
    
  // Encode according to coding method
  switch(mTech) {	
    case No_Coding:
      break;
    case Reed_Sol_Van:
      jerasure_matrix_encode(mNbDataBlocks, mNbParityFiles, w, matrix, data,
                             coding, mStripeWidth);
      break;
    case Reed_Sol_R6_Op:
      reed_sol_r6_encode(mNbDataBlocks, mNbParityFiles, data, coding, mStripeWidth);
      break;
    case Cauchy_Orig:
      jerasure_schedule_encode(mNbDataBlocks, mNbParityFiles, w, schedule, data,
                               coding, mStripeWidth, mPacketSize);
      break;
    case Cauchy_Good:
      jerasure_schedule_encode(mNbDataBlocks, mNbParityFiles, w, schedule, data,
                               coding, mStripeWidth, mPacketSize);
      break;
    case Liberation:
      jerasure_schedule_encode(mNbDataBlocks, mNbParityFiles, w, schedule, data,
                               coding, mStripeWidth, mPacketSize);
      break;
    case Blaum_Roth:
      jerasure_schedule_encode(mNbDataBlocks, mNbParityFiles, w, schedule, data,
                               coding, mStripeWidth, mPacketSize);
      break;
    case Liber8tion:
      jerasure_schedule_encode(mNbDataBlocks, mNbParityFiles, w, schedule, data,
                               coding, mStripeWidth, mPacketSize);
      break;
    default:
      fprintf(stderr,  "unsupported coding technique used\n");
      break;
  }

  return true;

}


//------------------------------------------------------------------------------
// Recover corrupted pieces in the current group, all errors in the map
// belonging to the same group
//------------------------------------------------------------------------------

bool
ReedSLayout::RecoverPiecesInGroup(off_t offsetInit,
                                  char* pBuffer,
                                  std::map<off_t, size_t>& rMapErrors)
{
  // Initialise Jerasure structures if not done already
  if (!mDoneInitialisation)
  {
    if (!InitialiseJerasure())
    {
      eos_err("error=failed to initialise Jerasure library");
      return false;
    }
    
    mDoneInitialisation = true;
  }
  
  //............................................................................
  // Obs: RecoverPiecesInGroup also checks the parity blocks
  //............................................................................
  bool ret = true;
  int64_t nread = 0;
  int64_t nwrite = 0;
  unsigned int num_blocks_corrupted;
  unsigned int physical_id;
  vector<unsigned int> valid_ids;
  vector<unsigned int> invalid_ids;
  off_t offset = rMapErrors.begin()->first;
  off_t offset_local = (offset / mSizeGroup) * mStripeWidth;
  off_t offset_group = (offset / mSizeGroup) * mSizeGroup;
  size_t length = 0;
  AsyncMetaHandler* ptr_handler = 0;
  num_blocks_corrupted = 0;
  offset_local += mSizeHeader;

  for (unsigned int i = 0; i < mNbTotalFiles; i++)
  {
    physical_id = mapLP[i];

    //........................................................................
    // Read data from stripe
    //........................................................................
    if (mStripeFiles[physical_id])
    {
      ptr_handler = static_cast<AsyncMetaHandler*>
                    (mStripeFiles[physical_id]->GetAsyncHandler());

      if (ptr_handler)
        ptr_handler->Reset();

      //enable readahead
      nread = mStripeFiles[physical_id]->ReadAsync(offset_local, mDataBlocks[i],
              mStripeWidth, true, mTimeout);

      if (nread != mStripeWidth)
      {
        eos_err("error=local block corrupted id=%i.", i);
        invalid_ids.push_back(i);
        num_blocks_corrupted++;
      }
    }
    else
    {
      //........................................................................
      // File not opened, register it as an error
      //........................................................................
      invalid_ids.push_back(i);
      num_blocks_corrupted++;
    }
  }

  //............................................................................
  // Wait for read responses and mark corrupted blocks
  //............................................................................
  for (unsigned int i = 0; i < mStripeFiles.size(); i++)
  {
    physical_id = mapLP[i];

    if (mStripeFiles[physical_id])
    {
      ptr_handler = static_cast<AsyncMetaHandler*>
                    (mStripeFiles[physical_id]->GetAsyncHandler());

      if (ptr_handler)
      {
        uint16_t error_type = ptr_handler->WaitOK();

        if (error_type != XrdCl::errNone)
        {
          std::pair< uint16_t, std::map<uint64_t, uint32_t> > pair_err;
          eos_err("error=remote block corrupted id=%i", i);
          invalid_ids.push_back(i);
          num_blocks_corrupted++;

          if (error_type == XrdCl::errOperationExpired)
          {
            mStripeFiles[physical_id]->Close(mTimeout);
            delete mStripeFiles[physical_id];
            mStripeFiles[physical_id] = NULL;
          }
        }
      }
    }
  }

  //............................................................................
  // Get the rest of the valid ids
  //............................................................................
  for (unsigned int i = 0; i < mNbTotalFiles; i++)
  {
    if (find(invalid_ids.begin(), invalid_ids.end(), i) == invalid_ids.end())
    {
      valid_ids.push_back(i);
    }
  }

  if (num_blocks_corrupted == 0)
  {
    return true;
  }
  else if (num_blocks_corrupted > mNbParityFiles)
  {
    eos_err("error=more blocks corrupted than the maximum number supported");
    return false;
  }

  // Get pointers to data and parity information
  char* coding[mNbParityFiles];
  char* data[mNbDataFiles];

  for (unsigned int i = 0; i < mNbDataFiles; i++)
  {
    data[i] = (char*) mDataBlocks[i];
  }

  for (unsigned int i = 0; i < mNbParityFiles; i++)
  {
    coding[i] = (char*) mDataBlocks[mNbDataFiles + i];
  }

  // Array of ids of erased pieces
  int *erasures = new int[invalid_ids.size() + 1];
  int index = 0;
  
  for (auto iter = invalid_ids.begin();
       iter != invalid_ids.end(); ++iter, ++index)
  {
    erasures[index] = *iter;
  }

  erasures[invalid_ids.size()] = -1;

  //............................................................................
  // ******* DECODE ******
  //............................................................................
  int decode;
  
  if (mTech == Reed_Sol_Van || mTech == Reed_Sol_R6_Op) {
    decode = jerasure_matrix_decode(mNbDataBlocks, mNbParityFiles, w, matrix, 1,
                                    erasures, data, coding, mStripeWidth);
  }
  else if (mTech == Cauchy_Orig || mTech == Cauchy_Good ||
           mTech == Liberation || mTech == Blaum_Roth || mTech == Liber8tion)
  {
    decode  = jerasure_schedule_decode_lazy(mNbDataBlocks, mNbParityFiles, w, bitmatrix,
                                            erasures, data, coding, mStripeWidth, mPacketSize, 1);
  }
  else {
    eos_err("error=not a valid coding technique");
    delete[] erasures;
    return false;
  }

  // Free memory
  delete[] erasures;
  
  if (decode == -1) {
    eos_err("error=decoding was unsuccessful");
    return false;
  }  
  
  //............................................................................
  // Update the files in which we found invalid blocks
  //............................................................................
  char* pBuff;
  unsigned int stripe_id;

  for (vector<unsigned int>::iterator iter = invalid_ids.begin();
       iter != invalid_ids.end();
       ++iter)
  {
    stripe_id = *iter;
    physical_id = mapLP[stripe_id];

    if (mStoreRecovery && mStripeFiles[physical_id])
    {
      ptr_handler =
        static_cast<AsyncMetaHandler*>(mStripeFiles[physical_id]->GetAsyncHandler());

      if (ptr_handler)
        ptr_handler->Reset();

      nwrite = mStripeFiles[physical_id]->WriteAsync(offset_local,
                                                     mDataBlocks[stripe_id],
                                                     mStripeWidth,
                                                     mTimeout);

      if (nwrite != mStripeWidth)
      {
        eos_err("error=while doing local write operation offset=%lli", offset_local);
        ret = false;
        break;
      }
    }

    //..........................................................................
    // Write the correct block to the reading buffer, if it is not parity info
    //..........................................................................
    if (stripe_id < mNbDataFiles)
    {
      //if one of the data blocks
      for (std::map<off_t, size_t>::iterator itPiece = rMapErrors.begin();
           itPiece != rMapErrors.end();
           itPiece++)
      {
        offset = itPiece->first;
        length = itPiece->second;

        if ((offset >= (off_t)(offset_group + stripe_id * mStripeWidth)) &&
            (offset < (off_t)(offset_group + (stripe_id + 1) * mStripeWidth)))
        {
          pBuff = pBuffer + (offset - offsetInit);
          pBuff = static_cast<char*>(memcpy(pBuff,
                                            mDataBlocks[stripe_id] + (offset % mStripeWidth),
                                            length));
        }
      }
    }
  }

  //............................................................................
  // Wait for write responses
  //............................................................................
  for (vector<unsigned int>::iterator iter = invalid_ids.begin();
       iter != invalid_ids.end();
       ++iter)
  {
    physical_id = mapLP[*iter];

    if (mStripeFiles[physical_id])
    {
      ptr_handler = static_cast<AsyncMetaHandler*>
                    (mStripeFiles[physical_id]->GetAsyncHandler());

      if (ptr_handler)
      {
        uint16_t error_type = ptr_handler->WaitOK();

        if (error_type != XrdCl::errNone)
        {
          eos_err("ReedSRecovery - write stripe failed");
          ret = false;

          if (error_type == XrdCl::errOperationExpired)
          {
            mStripeFiles[physical_id]->Close(mTimeout);
            delete mStripeFiles[physical_id];
            mStripeFiles[physical_id] = NULL;
          }
        }
      }
    }
  }

  mDoneRecovery = true;
  return ret;
}


//------------------------------------------------------------------------------
// Writing a file in streaming mode
// Add a new data used to compute parity block
//------------------------------------------------------------------------------

void
ReedSLayout::AddDataBlock(off_t offset, const char* pBuffer, size_t length)
{
  int indx_block;
  size_t nwrite;
  off_t offset_in_block;
  off_t offset_in_group = offset % mSizeGroup;

  //............................................................................
  // In case the file is smaller than mSizeGroup, we need to force it to compute
  // the parity blocks
  //............................................................................
  if ((mOffGroupParity == -1) && (offset < mSizeGroup))
  {
    mOffGroupParity = 0;
  }

  if (offset_in_group == 0)
  {
    mFullDataBlocks = false;

    for (unsigned int i = 0; i < mNbDataFiles; i++)
    {
      memset(mDataBlocks[i], 0, mStripeWidth);
    }
  }

  char* ptr;
  size_t availableLength;

  while (length)
  {
    offset_in_block = offset_in_group % mStripeWidth;
    availableLength = mStripeWidth - offset_in_block;
    indx_block = offset_in_group / mStripeWidth;
    nwrite = (length > availableLength) ? availableLength : length;
    ptr = mDataBlocks[indx_block];
    ptr += offset_in_block;
    ptr = static_cast<char*>(memcpy(ptr, pBuffer, nwrite));
    offset += nwrite;
    length -= nwrite;
    pBuffer += nwrite;
    offset_in_group = offset % mSizeGroup;

    if (offset_in_group == 0)
    {
      //........................................................................
      // We completed a group, we can compute parity
      //........................................................................
      mOffGroupParity = ((offset - 1) / mSizeGroup) * mSizeGroup;
      mFullDataBlocks = true;
      DoBlockParity(mOffGroupParity);
      mOffGroupParity = (offset / mSizeGroup) * mSizeGroup;

      for (unsigned int i = 0; i < mNbDataFiles; i++)
      {
        memset(mDataBlocks[i], 0, mStripeWidth);
      }
    }
  }
}


//------------------------------------------------------------------------------
// Write the parity blocks from mDataBlocks to the corresponding file stripes
//------------------------------------------------------------------------------

int
ReedSLayout::WriteParityToFiles(off_t offsetGroup)
{
  int ret = SFS_OK;
  int64_t nwrite = 0;
  unsigned int physical_id;
  off_t offset_local = (offsetGroup / mNbDataFiles);
  offset_local += mSizeHeader;

  for (unsigned int i = mNbDataFiles; i < mNbTotalFiles; i++)
  {
    physical_id = mapLP[i];

    //..........................................................................
    // Write parity block
    //..........................................................................
    if (mStripeFiles[physical_id])
    {
      nwrite = mStripeFiles[physical_id]->WriteAsync(offset_local, mDataBlocks[i],
               mStripeWidth, mTimeout);

      if (nwrite != mStripeWidth)
      {
        eos_err("error=while doing local write operation offset=%lli",
                offset_local);
        ret = SFS_ERROR;
        break;
      }
    }
  }

  //.............................................................................
  // We collect the write responses either the next time we do a read like in
  // ReadGroups or in the Close method for the whole file.
  //.............................................................................
  return ret;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
ReedSLayout::Truncate(XrdSfsFileOffset offset)
{
  int rc = SFS_OK;
  off_t truncate_offset = 0;
  truncate_offset = ceil((offset * 1.0) / mSizeGroup) * mStripeWidth;
  truncate_offset += mSizeHeader;
  eos_debug("Truncate local stripe to file_offset = %lli, stripe_offset = %zu",
            offset, truncate_offset);

  if (mStripeFiles[0])
    mStripeFiles[0]->Truncate(truncate_offset, mTimeout);

  if (mIsEntryServer)
  {
    if (!mIsPio)
    {
      //........................................................................
      // In non PIO access each stripe will compute its own truncate value
      //........................................................................
      truncate_offset = offset;
    }

    for (unsigned int i = 1; i < mStripeFiles.size(); i++)
    {
      eos_debug("Truncate stripe %i, to file_offset = %lli, stripe_offset = %zu",
                i, offset, truncate_offset);

      if (mStripeFiles[i])
      {
        if (mStripeFiles[i]->Truncate(truncate_offset, mTimeout))
        {
          eos_err("error=error while truncating");
          return SFS_ERROR;
        }
      }
    }
  }

  //............................................................................
  // *!!!* Reset the maxOffsetWritten from XrdFstOfsFile to logical offset
  //............................................................................
  mFileSize = offset;

  if (!mIsPio)
  {
    mOfsFile->maxOffsetWritten = offset;
  }

  return rc;
}


//------------------------------------------------------------------------------
// Return the same index in the Reed-Solomon case
//------------------------------------------------------------------------------

unsigned int
ReedSLayout::MapSmallToBig(unsigned int idSmall)
{
  if (idSmall >= mNbDataBlocks)
  {
    eos_err("error=idSmall bigger than expected");
    return -1;
  }

  return idSmall;
}


//--------------------------------------------------------------------------
// Allocate file space ( reserve )
//--------------------------------------------------------------------------
int
ReedSLayout::Fallocate(XrdSfsFileOffset length)
{
  int64_t size = ceil((1.0 * length) / mSizeGroup) * mStripeWidth + mSizeHeader;
  return mStripeFiles[0]->Fallocate(size);
}


//--------------------------------------------------------------------------
// Deallocate file space
//--------------------------------------------------------------------------
int
ReedSLayout::Fdeallocate(XrdSfsFileOffset fromOffset,
                         XrdSfsFileOffset toOffset)
{
  int64_t from_size = ceil((1.0 * fromOffset) / mSizeGroup) * mStripeWidth +
                      mSizeHeader;
  int64_t to_size = ceil((1.0 * toOffset) / mSizeGroup) * mStripeWidth +
                    mSizeHeader;
  return mStripeFiles[0]->Fdeallocate(from_size, to_size);
}


//------------------------------------------------------------------------------
// Check if a number is prime
//------------------------------------------------------------------------------
bool
ReedSLayout::IsPrime(int w)
{
  int prime55[] = {2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,
                   83,89,97,101,103,107,109,113,127,131,137,139,149,151,157,163,167,
                   173,179,181,191,193,197,199,211,223,227,229,233,239,241,251,257};

  for (int i = 0; i < 55; i++) {
    if (w % prime55[i] == 0) {
      if (w == prime55[i]) return true;
      else return false; 
    }
  }

  return false;;
}


EOSFSTNAMESPACE_END

