//------------------------------------------------------------------------------
//! @file RaidMetaLayout.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Generic class to read/write RAID-like layout files using a gateway
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

#ifndef __EOSFST_RAIDMETALAYOUT_HH__
#define __EOSFST_RAIDMETALAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include <vector>
#include <string>
#include <list>
/*----------------------------------------------------------------------------*/
#include "fst/layout/Layout.hh"
#include "fst/io/HeaderCRC.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/io/AsyncMetaHandler.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Generic class to read/write different RAID-like layout files
//------------------------------------------------------------------------------
class RaidMetaLayout : public Layout
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
    //! @param storeRecovery force writing back the recovered blocks to the files
    //! @param isStreaming file is written in streaming mode
    //! @param targetSize initial file size
    //! @param bookingOpaque opaque information
    //!
    //--------------------------------------------------------------------------
    RaidMetaLayout( XrdFstOfsFile*                 file,
                    int                            lid,
                    const XrdSecEntity*            client,
                    XrdOucErrInfo*                 outError,
                    eos::common::LayoutId::eIoType io,
                    bool                           storeRecovery,
                    bool                           isStreaming,
                    off_t                          targetSize,
                    std::string                    bookingOpaque );


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~RaidMetaLayout();


    //--------------------------------------------------------------------------
    //! Open file using a gateway
    //!
    //! @param path path to the file
    //! @param flags flags O_RDWR/O_RDONLY/O_WRONLY
    //! @param mode creation permissions
    //! @param opaque opaque information
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Open( const std::string& path,
                      XrdSfsFileOpenMode flags,
                      mode_t             mode,
                      const char*        opaque );


    //--------------------------------------------------------------------------
    //! Open file using parallel IO
    //!
    //! @param flags flags O_RDWR/O_RDONLY/O_WRONLY
    //! @param mode creation permissions
    //! @param opaque opaque information
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int OpenPio( std::vector<std::string>&& stripeUrls,
                         XrdSfsFileOpenMode         flags,  
                         mode_t                     mode = 0,
                         const char*                opaque = "fst.pio" );

  

    //--------------------------------------------------------------------------
    //! Read from file
    //!
    //! @param offset offset
    //! @param buffer place to hold the read data
    //! @param length length
    //!
    //! @return number of bytes read or -1 if error
    //!
    //--------------------------------------------------------------------------
    virtual int64_t Read( XrdSfsFileOffset offset,
                          char*            buffer,
                          XrdSfsXferSize   length );


    //--------------------------------------------------------------------------
    //! Write to file
    //!
    //! @param offset offset
    //! @param buffer data to be written
    //! @param length length
    //!
    //! @return number of bytes written or -1 if error
    //!
    //--------------------------------------------------------------------------
    virtual int64_t Write( XrdSfsFileOffset offset,
                           const char*      buffer,
                           XrdSfsXferSize   length );


    //--------------------------------------------------------------------------
    //! Truncate
    //!
    //! @param offset truncate file to this value
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Truncate( XrdSfsFileOffset offset ) = 0;


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


  protected:

    bool mIsRw;                        ///< mark for writing
    bool mIsOpen;                      ///< mark if open
    bool mIsPio;                       ///< mark if opened for parallel IO access
    bool mDoTruncate;                  ///< mark if there is a need to truncate
    bool mUpdateHeader;                ///< mark if header updated
    bool mDoneRecovery;                ///< mark if recovery done
    bool mFullDataBlocks;              ///< mark if we have all data blocks to compute parity
    bool mIsStreaming;                 ///< file is written in streaming mode
    bool mStoreRecovery;               ///< set if recovery also triggers writing back to the
                                       ///< files, this also means that all files must be available

    unsigned int mStripeHead;          ///< head stripe value
    unsigned int mPhysicalStripeIndex; ///< physical index of the current stripe
    unsigned int mLogicalStripeIndex;  ///< logical index of the current stripe
    unsigned int mNbParityFiles;       ///< number of parity files
    unsigned int mNbDataFiles;         ///< number of data files
    unsigned int mNbTotalFiles;        ///< total number of files ( data + parity )
    unsigned int mNbDataBlocks;        ///< no. data blocks in a group
    unsigned int mNbTotalBlocks;       ///< no. data and parity blocks in a group

    off_t mStripeWidth;                ///< stripe width
    off_t mSizeHeader;                 ///< size of header = 4KB
    off_t mFileSize;                   ///< total size of current file
    off_t mTargetSize;                 ///< expected final size (?!)
    off_t mSizeLine;                   ///< size of a line in a group
    off_t mOffGroupParity;             ///< offset of the last group for which we
                                       ///< computed the parity blocks
    off_t mSizeGroup;                  ///< size of a group of blocks
                                       ///< eg. RAIDDP: group = noDataStr^2 blocks

    std::string mBookingOpaque;                     ///< opaque information
    std::vector<char*> mDataBlocks;                 ///< vector containing the data in a group
    std::vector<FileIo*> mStripeFiles;              ///< vector containing the file IO layout
    std::vector<HeaderCRC*> mHdrInfo;               ///< headers of the stripe files
    std::vector<AsyncMetaHandler*> mMetaHandlers;   ///< rd/wr handlers for each stripe
    std::map<unsigned int, unsigned int> mapLP;     ///< map of url to stripes
    std::map<unsigned int, unsigned int> mapPL;     ///< map of stripes to url
    std::map<off_t, size_t> mMapPieces;             ///< map of pieces written for which parity
                                                    ///< computation has not been done yet

    char* mFirstBlock;                 ///< first extra block for reading aligned
    char* mLastBlock;                  ///< last extra block for reading aligned
    std::vector<char*> mPtrBlocks;     ///< vector containing pointers to where
                                       ///< new blocks are to be read
  
    //--------------------------------------------------------------------------
    //! Test and recover any corrupted headers in the stripe files
    //--------------------------------------------------------------------------
    virtual bool ValidateHeader();


    //--------------------------------------------------------------------------
    //! Recover corrupted pieces for the whole file
    //!
    //! @param offsetInit file offset corresponding to byte 0 from the buffer
    //! @param buffer container where we read the data
    //! @param mapPieces map of corrupted pieces from the whole file
    //!
    //! @return true if recovery successful, false otherwise
    //!
    //--------------------------------------------------------------------------
    virtual bool RecoverPieces( off_t                    offsetInit,
                                char*                    buffer,
                                std::map<off_t, size_t>& mapPieces );


    //--------------------------------------------------------------------------
    //! Compute and write parity blocks corresponding to a group of blocks
    //!
    //! @param offsetGroup offset of group of blocks
    //!
    //--------------------------------------------------------------------------
    virtual void DoBlockParity( off_t offsetGroup );

  
    //--------------------------------------------------------------------------
    //! Recover corrupted pieces from the current group
    //!
    //! @param offsetInit file offset corresponding to byte 0 from the buffer
    //! @param buffer container where we read the data
    //! @param mapPieces map of corrupted pieces in the current group
    //!
    //! @return true if recovery successful, false otherwise
    //!
    //--------------------------------------------------------------------------
    virtual bool RecoverPiecesInGroup( off_t                    offsetInit,
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
    virtual void AddDataBlock( off_t offset, const char* buffer, size_t length ) = 0;

  
    //--------------------------------------------------------------------------
    //! Compute parity information for a group of blocks
    //--------------------------------------------------------------------------
    virtual void ComputeParity() = 0;


    //--------------------------------------------------------------------------
    //! Write parity information corresponding to a group to files
    //!
    //! @param offsetGroup offset of the group of blocks
    //!
    //! @return 0 if successful, otherwise error
    //!
    //--------------------------------------------------------------------------
    virtual int WriteParityToFiles( off_t offsetGroup ) = 0;


    //--------------------------------------------------------------------------
    //! Map index from mNbDataBlocks representation to mNbTotalBlocks
    //!
    //! @param idSmall with values between 0 and 15, for exmaple in RAID-DP
    //!
    //! @return index with values between 0 and 23, -1 if error
    //!
    //--------------------------------------------------------------------------
    virtual unsigned int MapSmallToBig( unsigned int idSmall ) = 0;

  
    //--------------------------------------------------------------------------
    //! Non-streaming operation
    //! Compute parity for the non-streaming case and write it to files
    //!
    //! @param force if true force parity computation of incomplete groups,
    //!              this means that parity will be computed even if there are
    //!              still some pieces missing - this is useful at the end of
    //!              a write operation when closing the file
    //!
    //! @return true if successful, otherwise error
    //!
    //--------------------------------------------------------------------------
    bool SparseParityComputation( bool force );



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
    //! Expand the current range so that it is aligned with respect to the
    //! block xs size 
    //!
    //! @param offset offset
    //! @param length length
    //! @param sizeBlockXs block xs size
    //! @param alignedOffset aligned offset value
    //! @param alignedLength aligned length value
    //!
    //--------------------------------------------------------------------------
    void AlignExpandBlocks( char*             ptrBuffer,
                            XrdSfsFileOffset  offset,
                            XrdSfsXferSize    sizeBlockXs,
                            XrdSfsFileOffset& alignedOffset,
                            XrdSfsXferSize&   alignedLength );

  
    //--------------------------------------------------------------------------
    //!Copy any data from the extra block back to the original buffer
    //!
    //! @param buffer pointer to original buffer
    //! @param offset original offset
    //! @param length original length
    //! @param alignedOffset aligned offset value
    //! @param alignedLength aligned length value
    //!
    //--------------------------------------------------------------------------
    void CopyExtraBlocks( char*            buffer,
                          XrdSfsFileOffset offset,
                          XrdSfsXferSize   length,
                          XrdSfsFileOffset alignedOffset,
                          XrdSfsXferSize   alignedLength );


    //--------------------------------------------------------------------------
    //! Return matching part between original buffer and the extra block which 
    //! was read, so that we can copy back only the required data
    //!
    //! @param offset original offset
    //! @param length original length
    //! @param blockOffset offset of extra block
    //!
    //! @return the overlapping part in terms of offset and length in the
    //!         original file
    //--------------------------------------------------------------------------
    std::pair<off_t, size_t> GetMatchingPart( XrdSfsFileOffset offset,
                                              XrdSfsXferSize   length,
                                              XrdSfsFileOffset blockOffset );
  
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_RAIDMETALAYOUT_HH__

