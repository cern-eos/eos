//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog like store
//------------------------------------------------------------------------------

#ifndef EOS_CHANGE_LOG_FILE_HH
#define EOS_CHANGE_LOG_FILE_HH

#include <string>
#include <stdint.h>
#include "Namespace/persistency/Buffer.hh"
#include "Namespace/utils/Descriptor.hh"
#include "Namespace/MDException.hh"

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
  //! Changelog like store
  //----------------------------------------------------------------------------
  class ChangeLogFile
  {
    public:
      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      ChangeLogFile(): pIsOpen( false ), pVersion( 0 ) {};

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~ChangeLogFile() {};

      //------------------------------------------------------------------------
      //! Open the log file
      //------------------------------------------------------------------------
      void open( const std::string &name ) throw( MDException );

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

    private:

      //------------------------------------------------------------------------
      // Data members
      //------------------------------------------------------------------------
      int      pFd;
      bool     pIsOpen;
      uint8_t  pVersion;
      uint64_t pSeqNumber;
  };
}

#endif // EOS_CHANGE_LOG_FILE_HH
