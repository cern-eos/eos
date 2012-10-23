// -----------------------------------------------------------------------------
// File: RaidMetaLayout.hh
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

#ifndef __EOSFST_IO_RAIDMETALAYOUT_HH__
#define __EOSFST_IO_RAIDMETALAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include <vector>
#include <string>
#include <list>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "fst/layout/Layout.hh"
#include "fst/io/HeaderCRC.hh"
#include "fst/io/AsyncReadHandler.hh"
#include "fst/io/AsyncWriteHandler.hh"
#include "fst/XrdFstOfsFile.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
//! @file RaidMetaLayout.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Generic class to read/write different layout files
//------------------------------------------------------------------------------

EOSFSTNAMESPACE_BEGIN

using namespace XrdCl;

//------------------------------------------------------------------------------
//! Generic class to read/write different layout files
//------------------------------------------------------------------------------
class RaidMetaLayout : public Layout
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param algorithm type of layout used
    //! @param stripeUrl vector containing the location of the stripe files
    //! @param nbParity number of stripes used for parity
    //! @param storeRecovery force writing back the recovered blocks to the files
    //! @param isStreaming file is written in streaming mode
    //! @param targetSize exepected size (?!)
    //! @param bookingOpaque opaque information
    //!
    //--------------------------------------------------------------------------
    RaidMetaLayout(  XrdFstOfsFile*      file,
                     int                 lid,
                     const XrdSecEntity* client,
                     XrdOucErrInfo*      outError,
                     bool                storeRecovery,
                     bool                isStreaming,
                     off_t               targetSize,
                     std::string         bookingOpaque );

    //--------------------------------------------------------------------------
    //! Open file
    //!
    //! @param flags flags O_RDWR/O_RDONLY/O_WRONLY
    //!
    //! @return 0 if successful, otherwise error
    //!
    //--------------------------------------------------------------------------
    virtual int Open( const std::string& path,
                      XrdSfsFileOpenMode flags,
                      mode_t             mode,
                      const char*        opaque );

    //--------------------------------------------------------------------------
    //! Read from file
    //!
    //! @param offset file offset
    //! @param buffer data to be read
    //! @param length length of the data
    //!
    //! @return length of data read
    //!
    //--------------------------------------------------------------------------
    virtual int64_t Read( XrdSfsFileOffset offset,
                          char*            buffer,
                          XrdSfsXferSize   length );


    //--------------------------------------------------------------------------
    //! Write to file
    //!
    //! @param offset file offset
    //! @param buffer data to be written
    //! @param length length of the data
    //!
    //! @return length of data written
    //!
    //--------------------------------------------------------------------------
    virtual int64_t Write( XrdSfsFileOffset offset,
                           char*            buffer,
                           XrdSfsXferSize   length );
        

    //--------------------------------------------------------------------------
    //! Truncate file
    //!
    //! @param offset size to truncate
    //!
    //! @return 0 if successful, otherwise error
    //!
    //--------------------------------------------------------------------------
    virtual int Truncate( XrdSfsFileOffset offset ) = 0;

    //--------------------------------------------------------------------------
    //! Unlink all connected pieces
    //!
    //! @return 0 if successful, otherwise error
    //!
    //--------------------------------------------------------------------------
    virtual int Remove();

    //--------------------------------------------------------------------------
    //! Sync all connected pieces to disk
    //!
    //! @return 0 if successful, otherwise error
    //!
    //--------------------------------------------------------------------------
    virtual int Sync();

    //--------------------------------------------------------------------------
    //! Close file
    //!
    //! @return 0 if successful, otherwise error
    //!
    //--------------------------------------------------------------------------
    virtual int Close();


    //--------------------------------------------------------------------------
    //! Allocate file space
    //!
    //! @param length space to be allocated
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Fallocate( XrdSfsFileOffset lenght ); 


    //--------------------------------------------------------------------------
    //! Deallocate file space
    //!
    //! @param fromOffset offset start
    //! @param toOffset offset end
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Fdeallocate( XrdSfsFileOffset fromOffset,
                             XrdSfsFileOffset toOffset );
  
    //--------------------------------------------------------------------------
    //! Get stats about the file
    //!
    //! @param buf stat structure for the file
    //!
    //! @return 0 if successful, otherwise error
    //!
    //--------------------------------------------------------------------------
    virtual int Stat( struct stat* buf );

    //--------------------------------------------------------------------------
    //! Get size of file
    //--------------------------------------------------------------------------
    virtual uint64_t Size(); // returns the total size of the file

    //--------------------------------------------------------------------------
    //! Get size of the stripe
    //--------------------------------------------------------------------------
    static const int GetSizeStripe();

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~RaidMetaLayout();

  protected:

    bool mIsRw;                  ///< mark for writing
    bool mIsOpen;                ///< mark if open
    bool mDoTruncate;            ///< mark if there is a need to truncate
    bool mUpdateHeader;          ///< mark if header updated
    bool mDoneRecovery;          ///< mark if recovery done
    bool mFullDataBlocks;        ///< mark if we have all data blocks to compute parity
    bool mStoreRecovery;         ///< set if recovery also triggers writing back to the
                                 ///< files, this also means that all files must be available
    bool mIsStreaming;           ///< file is written in streaming mode

    unsigned int mStripeHead;    ///< head stripe value
    unsigned int mPhysicalStripeIndex; ///< physical index of the current stripe
    unsigned int mLogicalStripeIndex;
    unsigned int mNbParityFiles; ///< number of parity files
    unsigned int mNbDataFiles;   ///< number of data files
    unsigned int mNbTotalFiles;  ///< total number of files ( data + parity )

    unsigned int mNbDataBlocks;  ///< no. data blocks in a group
    unsigned int mNbTotalBlocks; ///< no. data and parity blocks in a group

    off_t mStripeWidth;         ///< stripe width
    off_t mSizeHeader;           ///< size of header = 4KB
    off_t mFileSize;             ///< total size of current file
    off_t mTargetSize;           ///< expected final size (?!)
    off_t mOffGroupParity;       ///< offset of the last group for which we
                                 ///< computed the parity blocks
    off_t mSizeGroup;           ///< size of a gourp of blocks
                                 ///< eg. RAIDDP: group = noDataStr^2 blocks

    std::string mAlgorithmType;                     ///< layout type used
    std::string mBookingOpaque;                     ///< opaque information
    std::vector<char*> mDataBlocks;                 ///< vector containing the data in a group
    std::vector<FileIo*> mStripeFiles;
    std::vector<HeaderCRC*> mHdUrls;                ///< headers of the stripe files
    std::vector<AsyncReadHandler*> mReadHandlers;   ///< async read handlers for each stripe
    std::vector<AsyncWriteHandler*> mWriteHandlers; ///< async write handlers for each stripe
    std::map<unsigned int, unsigned int> mapLP;     ///< map of url to stripes
    std::map<unsigned int, unsigned int> mapPL;     ///< map of stripes to url
    std::map<off_t, size_t> mMapPieces;             ///< map of pieces written for which parity
                                                    ///< computation has not been done yet

    //--------------------------------------------------------------------------
    //! Test and recover any corrupted headers in the stripe files
    //--------------------------------------------------------------------------
    virtual bool ValidateHeader( const char* opaque);

    //--------------------------------------------------------------------------
    //! Recover corrupted pieces
    //!
    //! @param offsetInit file offset corresponding to byte 0 from the buffer
    //! @param buffer container where we read the data
    //! @param mapPieces map of corrupted pieces
    //!
    //! @return true if recovery successful, false otherwise
    //!
    //--------------------------------------------------------------------------
    virtual bool RecoverPieces( off_t                    offsetInit,
                                char*                    buffer,
                                std::map<off_t, size_t>& mapPieces ) = 0;

    //--------------------------------------------------------------------------
    //! Add new data block to the current group for parity computation, used
    //! when writing a file in streaming mode
    //!
    //! @param offset offset of the block added
    //! @param buffer data contained in the block
    //! @param length length of the data
    //!
    //--------------------------------------------------------------------------
    virtual void AddDataBlock( off_t offset, char* buffer, size_t length ) = 0;

    // -------------------------------------------------------------------------
    //! Compute and write parity blocks corresponding to a group of blocks
    //!
    //! @param offsetGroup offset of group
    //!
    // -------------------------------------------------------------------------
    void DoBlockParity( off_t offsetGroup );

    // -------------------------------------------------------------------------
    //! Compute parity information for a group of blocks
    // -------------------------------------------------------------------------
    virtual void ComputeParity() = 0;

    // -------------------------------------------------------------------------
    //! Write parity information corresponding to a group to files
    //!
    //! @param offsetGroup offset of the group of blocks
    //!
    //! @return 0 if successful, otherwise error
    //!
    // -------------------------------------------------------------------------
    virtual int WriteParityToFiles( off_t offsetGroup ) = 0;

    // -------------------------------------------------------------------------
    //! Map index from mNbDataBlocks representation to mNbTotalBlocks
    //!
    //! @param idSmall with values between 0 and 15, for exmaple in RAID-DP
    //!
    //! @return index with values between 0 and 23, -1 if error
    //!
    // -------------------------------------------------------------------------
    virtual unsigned int MapSmallToBig( unsigned int idSmall ) = 0;

  private:

    //--------------------------------------------------------------------------
    //! Non-streaming operation
    //! Add a new piece to the map of pieces written to the file
    //!
    //! @param offset offset of the new piece added
    //! @param length length of the new piece added
    //!
    //--------------------------------------------------------------------------
    void AddPiece( off_t offset, size_t length );

    //--------------------------------------------------------------------------
    //! Non-streaming operation
    //! Merge in place the pieces from the map
    //--------------------------------------------------------------------------
    void MergePieces();

    //--------------------------------------------------------------------------
    //! Non-streaming operation
    //! Get a list of the group offsets for which we can compute the parity info
    //!
    //! @param offsetGroups set of group offsets
    //! @param forceAll if true return also offsets of incomplete groups
    //!
    //--------------------------------------------------------------------------
    void GetOffsetGroups( std::set<off_t>& offsetGroups, bool forceAll );

    //--------------------------------------------------------------------------
    //! Non-streaming operation
    //! Read data from the current group for parity computation
    //!
    //! @param offsetGroup offset of the group about to be read
    //!
    //! @return true if operation successful, otherwise error
    //!
    //--------------------------------------------------------------------------
    bool ReadGroup( off_t offsetGroup );

    //--------------------------------------------------------------------------
    //! Non-streaming operation
    //! Compute parity for the non-streaming case and write it to files
    //!
    //! @param force if true force parity computation of incomplete groups
    //!
    //! @return true if successful, otherwise error
    //!
    //--------------------------------------------------------------------------
    bool SparseParityComputation( bool force );

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_IO_RAIDIO_HH__
