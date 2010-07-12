//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog like store
//------------------------------------------------------------------------------

#include "Namespace/persistency/ChangeLogFile.hh"
#include "Namespace/utils/SmartPtrs.hh"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <stdint.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <iomanip>

#define CHANGELOG_MAGIC 0x45434847
#define RECORD_MAGIC    0x4552

namespace eos
{
  //---------------------------------------------------------------------------
  // Open the log file
  //----------------------------------------------------------------------------
  void ChangeLogFile::open( const std::string &name ) throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Open the try to open the file for reading and writing
    //--------------------------------------------------------------------------
    int fd = ::open( name.c_str(), O_RDWR );
    FileSmartPtr fdPtr( fd );

    //--------------------------------------------------------------------------
    // Check the format
    //--------------------------------------------------------------------------
    if( fd >= 0 )
    {
      uint32_t magic;
      uint8_t  version;

      if( read( fd, &magic, 4 ) != 4 )
      {
        MDException ex( EFAULT );
        ex.getMessage() << "Unable to read the magic number from: " << name;
        throw ex;
      }

      if( magic != CHANGELOG_MAGIC )
      {
        MDException ex( EFAULT );
        ex.getMessage() << "Unrecognized file type: " << name;
        throw ex;
      }

      if( read( fd, &version, 2 ) != 2 )
      {
        MDException ex( EFAULT );
        ex.getMessage() << "Unable to read the version number from: " << name;
        throw ex;
      }

      if( version > 1 )
      {
        MDException ex( EFAULT );
        ex.getMessage() << "Unsupported version: " << name;
        throw ex;
      }

      //------------------------------------------------------------------------
      // Move to the end
      //------------------------------------------------------------------------
      lseek( fd, 0, SEEK_END );
      fdPtr.release();
      pFd = fd;
      pIsOpen  = true;
      pVersion = version;
      return;
    }

    //--------------------------------------------------------------------------
    // Create the file
    //--------------------------------------------------------------------------
    fd = ::open( name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0644 );
    fdPtr.grab( fd );

    //--------------------------------------------------------------------------
    // Check if the file was successfuly created
    //--------------------------------------------------------------------------
    if( fd == -1 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Unable to create changelog file " << name;
      ex.getMessage() << ": " << strerror( errno );
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Write the magic number and version
    //--------------------------------------------------------------------------
    uint32_t magic = CHANGELOG_MAGIC;
    if( write( fd, &magic, 4 ) != 4 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Unable to write magic number: " << name;
      throw ex;
    }

    uint8_t version = 1;
    if( write( fd, &version, 2 ) != 2 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Unable to write version  number: " << name;
      throw ex;
    }
    fdPtr.release();
    pFd = fd;
    pIsOpen  = true;
    pVersion = 1;
  }

  //----------------------------------------------------------------------------
  // Close the log
  //----------------------------------------------------------------------------
  void ChangeLogFile::close()
  {
    ::close( pFd );
  }

  //----------------------------------------------------------------------------
  // Sync the buffers to disk
  //----------------------------------------------------------------------------
  void ChangeLogFile::sync() throw( MDException )
  {
    if( !pIsOpen )
      return;

    if( fsync( pFd ) != 0 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Unable to sync the changelog file: ";
      ex.getMessage() << strerror( errno );
      throw ex;
    }
  }

  //----------------------------------------------------------------------------
  // Store the record in the log
  //----------------------------------------------------------------------------
  uint64_t ChangeLogFile::storeRecord( char type, const Buffer &record )
    throw( MDException )
  {
    if( !pIsOpen )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Changelog file is not open";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Sum up the size of the record and calculate the checksum
    //--------------------------------------------------------------------------
    uint16_t size = record.size();
    uint32_t chkSum = 12;

    //--------------------------------------------------------------------------
    // Store the data
    //--------------------------------------------------------------------------
    uint64_t offset = ::lseek( pFd, 0, SEEK_END );
    uint16_t magic  = RECORD_MAGIC;

    iovec vec[6];
    vec[0].iov_base = &magic;  vec[0].iov_len = 2;
    vec[1].iov_base = &size;   vec[1].iov_len = 2;
    vec[2].iov_base = &chkSum; vec[2].iov_len = 4;
    vec[3].iov_base = &type;   vec[3].iov_len = 1;
    vec[5].iov_base = &chkSum; vec[5].iov_len = 4;
    vec[4].iov_base = (void*)record.getDataPtr();
    vec[4].iov_len = record.size();

    if( writev( pFd, vec, 6 ) != (unsigned)(13+record.size()) )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Unable to write the record data at offset 0x";
      ex.getMessage() << std::setbase(16) << offset << "; ";
      ex.getMessage() << strerror( errno );
      throw ex;
    }

    return offset;
  }

  //----------------------------------------------------------------------------
  // Read the record at given offset
  //----------------------------------------------------------------------------
  uint8_t ChangeLogFile::readRecord( uint64_t offset, Buffer &record )
    throw( MDException)
  {
    if( !pIsOpen )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Changelog file is not open";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Read first part of the record
    //--------------------------------------------------------------------------
    uint16_t *magic;
    uint16_t *size;
    uint32_t *chkSum1;
    uint32_t  chkSum2;
    uint8_t  *type;
    char      buffer[9];


    if( pread( pFd, buffer, 9, offset ) != 9 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Error reading at offset: " << offset;
      throw ex;
    }

    magic   = (uint16_t*)(buffer);
    size    = (uint16_t*)(buffer+2);
    chkSum1 = (uint32_t*)(buffer+4);
    type    = (uint8_t*) (buffer+8);

    //--------------------------------------------------------------------------
    // Check the consistency
    //--------------------------------------------------------------------------
    if( *magic != RECORD_MAGIC )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "The record is inconsistent. Perhaps the offset ";
      ex.getMessage() << "is incorrect.";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Read the second part of the buffer
    //--------------------------------------------------------------------------
    record.resize( *size+4, 0 );
    if( pread( pFd, record.getDataPtr(), *size+4, offset+9 ) != *size+4 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Error reading at offset: " << offset + 9 << std::endl;
      throw ex;
    }
    record.grabData( record.size()-4, &chkSum2, 4 );
    record.resize( *size );

    //--------------------------------------------------------------------------
    // Check the checksum
    //--------------------------------------------------------------------------
    if( *chkSum1 != chkSum2 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "The record is inconsistent. Perhaps the offset ";
      ex.getMessage() << "is incorrect.";
      throw ex;
    }

    return *type;
  }

  //----------------------------------------------------------------------------
  // Scan all the records in the changelog file
  //----------------------------------------------------------------------------
  void ChangeLogFile::scanAllRecords( ILogRecordScanner *scanner )
                                                            throw( MDException )
  {
    if( !pIsOpen )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Changelog file is not open";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Get the offset information
    //--------------------------------------------------------------------------
    uint64_t end = ::lseek( pFd, 0, SEEK_END );
    uint64_t offset = ::lseek( pFd, 6, SEEK_SET );
    if( offset != 6 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Unable to find the record data at offset 0x";
      ex.getMessage() << std::setbase(16) << 6 << "; ";
      ex.getMessage() << strerror( errno );
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Read all the records
    //--------------------------------------------------------------------------
    uint8_t          type;
    Buffer           data;

    while( offset < end )
    {
      type = readRecord( offset, data );
      scanner->processRecord( offset, type, data );
      offset += data.size();
      offset += 13;
    }
  }

  //----------------------------------------------------------------------------
  // Follow a file
  //----------------------------------------------------------------------------
  void ChangeLogFile::follow( ILogRecordScanner* scanner, unsigned poll )
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Check if the file is open
    //--------------------------------------------------------------------------
    if( !pIsOpen )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Changelog file is not open";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Off we go - we only exit if an error occurs
    //--------------------------------------------------------------------------
    Descriptor   fd( pFd );
    off_t        offset = 6;
    uint16_t    *magic;
    uint16_t    *size;
    uint32_t    *chkSum1;
    uint32_t     chkSum2;
    uint8_t     *type;
    char         buffer[9];
    Buffer       record;

    while( 1 )
    {
      //------------------------------------------------------------------------
      // Read the header
      //------------------------------------------------------------------------
      try
      {
         fd.offsetReadNonBlocking( buffer, 9, offset, poll );
      }
      catch( DescriptorException &e )
      {
        MDException ex( EFAULT );
        ex.getMessage() << "Error reading at offset: " << offset << ": ";
        ex.getMessage() << e.getMessage().str();
        throw ex;
      }

      magic   = (uint16_t*)(buffer);
      size    = (uint16_t*)(buffer+2);
      chkSum1 = (uint32_t*)(buffer+4);
      type    = (uint8_t*) (buffer+8);

      //------------------------------------------------------------------------
      // Check the consistency
      //------------------------------------------------------------------------
      if( *magic != RECORD_MAGIC )
      {
        MDException ex( EFAULT );
        ex.getMessage() << "The record is inconsistent. Perhaps the offset ";
        ex.getMessage() << "is incorrect.";
        throw ex;
      }

      //--------------------------------------------------------------------------
      // Read the second part of the buffer
      //--------------------------------------------------------------------------
      record.resize( *size+4, 0 );
      try
      {
        fd.offsetReadNonBlocking( record.getDataPtr(), *size+4, offset+9, poll );
      }
      catch( DescriptorException &e )
      {
        MDException ex( EFAULT );
        ex.getMessage() << "Error reading at offset: " << offset + 9 << ": ";
        ex.getMessage() << e.getMessage().str();
        throw ex;
      }
      record.grabData( record.size()-4, &chkSum2, 4 );
      record.resize( *size );

      //------------------------------------------------------------------------
      // Check the checksum
      //------------------------------------------------------------------------
      if( *chkSum1 != chkSum2 )
      {
        MDException ex( EFAULT );
        ex.getMessage() << "The record is inconsistent. Perhaps the offset ";
        ex.getMessage() << "is incorrect.";
        throw ex;
      }

      //------------------------------------------------------------------------
      // Call the listener and clean up
      //------------------------------------------------------------------------
      scanner->processRecord( offset, *type, record );
      offset += record.size();
      offset += 13;
      record.clear();
    }
  }
}
