// -----------------------------------------------------------------------------
// File: RaidIO.hh
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

/*----------------------------------------------------------------------------*/
#include <vector>
#include <string>
#include <list>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "fst/io/HeaderCRC.hh"
#include "fst/io/AsyncReadHandler.hh"
#include "fst/io/AsyncWriteHandler.hh"
#include "fst/XrdFstOfsFile.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
/*----------------------------------------------------------------------------*/

#ifndef __EOSFST_RAIDIO_HH__
#define __EOSFST_RAIDIO_HH__

EOSFSTNAMESPACE_BEGIN

using namespace XrdCl;

//------------------------------------------------------------------------------
//! Generic class to read/write different layout files
//------------------------------------------------------------------------------
class RaidIO : public eos::common::LogId
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param algorithm type of layout used
    //! @param stripeurl vector containing the location of the stripe files
    //! @param nparitystripes number of stripes used for parity
    //! @param stroerecovery force writing back the recovered blocks to the files
    //! @param isstreaming file is written in streaming mode
    //! @param targetsize exepected size (?!)
    //! @param bookingopaque opaque information
    //!
    //--------------------------------------------------------------------------
    RaidIO( std::string              algorithm,
            std::vector<std::string> stripeurl,
            unsigned int             nparitystripes,
            bool                     storerecovery,
            bool                     isstreaming, 
            off_t                    targetsize = 0,
            std::string              bookingopaque = "oss.size" );

    //--------------------------------------------------------------------------
    //! Open file
    //!
    //! @param flags flags O_RDWR/O_RDONLY/O_WRONLY
    //!
    //! @return o if successful, otherwise error
    //--------------------------------------------------------------------------
    virtual int open( int flags );

    //--------------------------------------------------------------------------
    //! Read from file
    //!
    //! @param offset file offset
    //! @param buffer data to be read
    //! @param length length of the data
    //!
    //! @return length of data read
    //--------------------------------------------------------------------------
    virtual int read( off_t offset, char* buffer, size_t length );

    //--------------------------------------------------------------------------
    //! Write to file
    //!
    //! @param offset file offset
    //! @param buffer data to be written
    //! @param length length of the data
    //!
    //! @return length of data written
    //--------------------------------------------------------------------------
    virtual int write( off_t offset, char* buffer, size_t length );

    //--------------------------------------------------------------------------
    //! Truncate file
    //!
    //! @param offset size to truncate
    //!
    //! @return 0 if successful, otherwise error
    //--------------------------------------------------------------------------
    virtual int truncate( off_t offset ) = 0;

    //--------------------------------------------------------------------------
    //! Unlink all connected pieces
    //!
    //! @return 0 if successful, otherwise error
    //--------------------------------------------------------------------------
    virtual int remove();

    //--------------------------------------------------------------------------
    //! Sync all connected pieces to disk
    //!
    //! @return 0 if successful, otherwise error
    //--------------------------------------------------------------------------
    virtual int sync();

    //--------------------------------------------------------------------------
    //! Close file
    //!
    //! @return 0 if successful, otherwise error
    //--------------------------------------------------------------------------
    virtual int close();

    //--------------------------------------------------------------------------
    //! Get stats about the file
    //!
    //! @param buf stat structure for the file
    //!
    //! @return 0 if successful, otherwise error
    //--------------------------------------------------------------------------
    virtual int stat( struct stat* buf );

    //--------------------------------------------------------------------------
    //! Get size of file
    //--------------------------------------------------------------------------
    virtual off_t size(); // returns the total size of the file

    //--------------------------------------------------------------------------
    //! Get size of the stripe
    //--------------------------------------------------------------------------
    static const int getSizeStripe() {
      return 1024 * 1024;     // 1MB
    };

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~RaidIO();

  protected:

    File** xrdFile;              //< xrd clients corresponding to the stripes
    HeaderCRC* hdUrl;            //< array of header objects

    bool isRW;                   //< mark for writing
    bool isOpen;                 //< mark if open
    bool doTruncate;             //< mark if there is a need to truncate
    bool updateHeader;           //< mark if header updated
    bool doneRecovery;           //< mark if recovery done
    bool fullDataBlocks;         //< mark if we have all data blocks to compute parity
    bool storeRecovery;          //< set if recovery also triggers writing back to the
                                 //< files this also means that all files must be available
    bool isStreaming;            //< file is written in streaming mode

    unsigned int nParityFiles;   //< number of parity files
    unsigned int nDataFiles;     //< number of data files
    unsigned int nTotalFiles;    //< total number of files ( data + parity )

    off_t targetSize;            //< expected final size (?!)
    off_t offGroupParity;        //< offset of the last group for which we
                                 //< computed the parity blocks

    size_t sizeHeader;           //< size of header = 4KB
    size_t stripeWidth;          //< stripe width
    size_t fileSize;             //< total size of current file
    size_t sizeGroup;            //< size of a gourp of blocks
                                 //< eg. RAIDDP: group = noDataStr^2 blocks

    std::string algorithmType;   //< layout type used
    std::string bookingOpaque;   //< opaque information
    std::vector<char*> dataBlocks;                 //< vector containg the data in a group
    std::vector<std::string> stripeUrls;           //< urls of the stripe files
    std::vector<AsyncReadHandler*> vReadHandler;   //< async read handlers for each stripe
    std::vector<AsyncWriteHandler*> vWriteHandler; //< async write handlers for each stripe
    std::map<unsigned int, unsigned int> mapUS;    //< map of url to stripes
    std::map<unsigned int, unsigned int> mapSU;    //< map os stripes to url
    std::map<off_t, size_t> mapPieces;             //< map of pieces written without doing
                                                   //< parity computation for them 

    //--------------------------------------------------------------------------
    //! Test and recover any corrupted headers in the stripe files
    //--------------------------------------------------------------------------
    virtual bool validateHeader();

    //--------------------------------------------------------------------------
    //! Recover corrupted pieces
    //--------------------------------------------------------------------------
    virtual bool recoverPieces( off_t                    offsetInit,
                                char*                    buffer,
                                 std::map<off_t, size_t>& mapPieces ) = 0;

    //--------------------------------------------------------------------------
    //! Add new data block to the current group for parity computation
    //--------------------------------------------------------------------------
    virtual void addDataBlock( off_t offset, char* buffer, size_t length ) = 0;

    //--------------------------------------------------------------------------
    //! Compute and write parity blocks corresponding to a group
    //--------------------------------------------------------------------------
    virtual void doBlockParity( off_t offsetGroup ) = 0;

    //--------------------------------------------------------------------------
    //! Non-streaming operation 
    //! Get a list of the group offsets for which we can compute the parity info
    //--------------------------------------------------------------------------
  virtual void GetOffsetGroups(std::set<off_t>& offGroups, bool forceAll) = 0;

    //--------------------------------------------------------------------------
    //! Non-streaming operation 
    //! Read data from the current group ofr parity computation
    //!
    //! @param offsetGroup offset of the grou about to be read
    //!
    //! @return true if operation successful, otherwise error
    //--------------------------------------------------------------------------
    virtual bool ReadGroup(off_t offsetGroup) = 0;

 private:

    //--------------------------------------------------------------------------
    //! Non-streaming operation
    //! Add a new piece to the map of pieces written to the file
    //--------------------------------------------------------------------------
    void AddPiece(off_t offset, size_t length);

    //--------------------------------------------------------------------------
    //! Non-streaming operation
    //! Merge the pieces from the map
    //--------------------------------------------------------------------------
    void MergePieces();
  
    //--------------------------------------------------------------------------
    //! Non-streaming operation
    //! Compute parity for the non-streaming case and write it to files
    //!
    //! @return true if successful, otherwise erro
    //--------------------------------------------------------------------------
    bool SparseParityComputation( bool force );
 
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_RAIDIO_HH__
