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

//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog like store
//------------------------------------------------------------------------------

#ifndef EOS_NS_CHANGE_LOG_FILE_HH
#define EOS_NS_CHANGE_LOG_FILE_HH

#include <string>
#include <stdint.h>
#include <ctime>
#include "namespace/persistency/Buffer.hh"
#include "namespace/utils/Descriptor.hh"
#include "namespace/MDException.hh"

namespace eos
{
  //----------------------------------------------------------------------------
  //! Interface for a class scanning the logfile
  //----------------------------------------------------------------------------
  class ILogRecordScanner
  {
    public:
      //------------------------------------------------------------------------
      //! Process record
      //------------------------------------------------------------------------
      virtual void processRecord( uint64_t offset, char type,
                                  const Buffer &buffer ) = 0;
  };

  //----------------------------------------------------------------------------
  //! Statistics of the repair process
  //----------------------------------------------------------------------------
  struct LogRepairStats
  {
    LogRepairStats(): fixedWrongMagic(0), fixedWrongSize(0),
                      fixedWrongChecksum(0), notFixed(0), scanned(0),
                      healthy(0), bytesDiscarded(0), bytesAccepted(0),
                      bytesTotal(0), timeElapsed(0) {}

    uint64_t fixedWrongMagic;
    uint64_t fixedWrongSize;
    uint64_t fixedWrongChecksum;
    uint64_t notFixed;
    uint64_t scanned;
    uint64_t healthy;
    uint64_t bytesDiscarded;
    uint64_t bytesAccepted;
    uint64_t bytesTotal;
    time_t   timeElapsed;
  };

  //----------------------------------------------------------------------------
  //! Feedback from the changelog reparation process
  //----------------------------------------------------------------------------
  class ILogRepairFeedback
  {
    public:
      //------------------------------------------------------------------------
      //! Called to report progress to the outside world
      //------------------------------------------------------------------------
      virtual void reportProgress( LogRepairStats &stats ) = 0;

      //------------------------------------------------------------------------
      //! Report the log header status
      //------------------------------------------------------------------------
      virtual void reportHeaderStatus( bool               isOk,
                                       const std::string &message,
                                       uint8_t            version,
                                       uint16_t           contentFlag ) = 0;
  };

  //----------------------------------------------------------------------------
  //! Changelog like store
  //----------------------------------------------------------------------------
  class ChangeLogFile
  {
    public:
      //------------------------------------------------------------------------
      //! Open flags
      //------------------------------------------------------------------------
      enum OpenFlags
      {
        ReadOnly = 0x01, //!< Read only
        Truncate = 0x02, //!< Truncate if possible
        Create   = 0x04, //!< Create if does not exist
        Append   = 0x08  //!< Append  to the existing file
      };

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      ChangeLogFile(): pFd(-1), pIsOpen( false ), pVersion( 0 ), pSeqNumber( 0 ),
                       pContentFlag( 0 ) {};

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~ChangeLogFile() {};

      //------------------------------------------------------------------------
      //! Open the log file, create if needed
      //!
      //! @param name        name of the file
      //! @param flags       flags the mode for opening the file (OpenFlags)
      //! @param contentFlag user-defined valriable identifying the content
      //!                    of the file 0x0000 is reserved for undefined
      //!                    content
      //------------------------------------------------------------------------
      void open( const std::string &name, int flags = Create | Append,
                 uint16_t contentFile = 0x0000 ) throw( MDException );

      //------------------------------------------------------------------------
      //! Check if the changelog file is opened already
      //------------------------------------------------------------------------
      bool isOpen() const
      {
        return pIsOpen;
      }

      //------------------------------------------------------------------------
      //! Close the log
      //------------------------------------------------------------------------
      void close();

      //------------------------------------------------------------------------
      //! Get version
      //------------------------------------------------------------------------
      uint8_t getVersion() const
      {
        return pVersion;
      }

      //------------------------------------------------------------------------
      //! Get content flag
      //------------------------------------------------------------------------
      uint16_t getContentFlag() const
      {
        return pContentFlag;
      }

      //------------------------------------------------------------------------
      //! Sync the buffers to disk
      //------------------------------------------------------------------------
      void sync() throw( MDException );

      //------------------------------------------------------------------------
      //! Store the record in the log
      //!
      //! @param type   user defined type of record
      //! @param record a record buffer, it is not const because zeros may be
      //!               appended to the end to make it aligned to 4 bytes
      //!
      //! @return the offset in the log
      //------------------------------------------------------------------------
      uint64_t storeRecord( char type, Buffer &record ) throw( MDException );

      //------------------------------------------------------------------------
      //! Read the record at given offset
      //------------------------------------------------------------------------
      uint8_t readRecord( uint64_t offset, Buffer &record ) throw( MDException);

      //------------------------------------------------------------------------
      //! Scan all the records in the changelog file
      //------------------------------------------------------------------------
      void scanAllRecords( ILogRecordScanner *scanner ) throw( MDException );

      //------------------------------------------------------------------------
      //! Follow a file
      //!
      //! @param scanner a listener to be notified about a new record
      //! @param poll    look for new data every poll microseconds
      //------------------------------------------------------------------------
      void follow( ILogRecordScanner* scanner, unsigned poll = 100000 )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Repair a changelog file
      //!
      //! @param filename    name of the file to be repaired (read only)
      //! @param newFilename placeholder for the fixed records
      //! @param feedback    instance of a feedback class to determine reactions
      //!                    to problems
      //! @param stats       placeholder for the statistics
      //------------------------------------------------------------------------
      static void repair( const std::string  &filename,
                          const std::string  &newFilename,
                          LogRepairStats     &stats,
                          ILogRepairFeedback *feedback ) throw( MDException );
    private:

      //------------------------------------------------------------------------
      // Decode the header flags of the log file
      //------------------------------------------------------------------------
      static void decodeHeaderFlags( uint32_t flags, uint8_t &version,
                                     uint16_t &contentFlag )
      {
        version     = flags & 0x000000ff;
        contentFlag = (flags >> 8) & 0x0000ffff;
      }

      //------------------------------------------------------------------------
      // Data members
      //------------------------------------------------------------------------
      int      pFd;
      bool     pIsOpen;
      uint8_t  pVersion;
      uint64_t pSeqNumber;
      uint16_t pContentFlag;
  };
}

#endif // EOS_NS_CHANGE_LOG_FILE_HH
