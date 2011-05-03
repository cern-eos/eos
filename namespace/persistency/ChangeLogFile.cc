//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog like store
//------------------------------------------------------------------------------

#include "namespace/persistency/ChangeLogFile.hh"
#include "namespace/utils/SmartPtrs.hh"
#include "namespace/utils/DataHelper.hh"

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
  //----------------------------------------------------------------------------
  // Check the header - returns flags number
  //----------------------------------------------------------------------------
  static uint32_t checkHeader( int fd, const std::string &name )
  {
    uint32_t magic;
    uint32_t flags;

    if( read( fd, &magic, 4 ) != 4 )
    {
      MDException ex( errno );
      ex.getMessage() << "Unable to read the magic number from: " << name;
      throw ex;
    }

    if( magic != CHANGELOG_MAGIC )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Unrecognized file type: " << name;
      throw ex;
    }

    if( read( fd, &flags, 4 ) != 4 )
    {
      MDException ex( errno );
      ex.getMessage() << "Unable to read the version number from: " << name;
      throw ex;
    }

    return flags;
  }

  //---------------------------------------------------------------------------
  // Open the log file
  //----------------------------------------------------------------------------
  void ChangeLogFile::open( const std::string &name, bool readOnly,
                            uint16_t contentFlag ) throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Checki fh the file is open already
    //--------------------------------------------------------------------------
    if( pIsOpen )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Changelog file is already open";
      throw ex;
    }

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
      uint32_t flags   = checkHeader( fd, name );
      uint8_t  version = 0;
      decodeHeaderFlags( flags, version, pContentFlag );

      if( version == 0 || version > 1 )
      {
        MDException ex( EFAULT );
        ex.getMessage() << "Unsupported version: " << name;
        throw ex;
      }

      if( contentFlag && contentFlag != pContentFlag )
      {
        MDException ex( EFAULT );
        ex.getMessage() << "Log file exists: " << name << " ";
        ex.getMessage() << "and the requested content flag (0x";
        ex.getMessage() << std::setbase(16) << contentFlag << ") does not ";
        ex.getMessage() << "match the one read from file (0x";
        ex.getMessage() << std::setbase(16) << pContentFlag << ")";
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
    if( readOnly )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Cannot create a new file in read-only mode: " << name;
      throw ex;
    }
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
      MDException ex( errno );
      ex.getMessage() << "Unable to write magic number: " << name;
      throw ex;
    }

    uint8_t  version = 1;
    uint32_t tmp;
    uint32_t flags   = 0;

    pContentFlag = contentFlag;
    flags |= version;
    tmp = contentFlag;
    flags |= (tmp << 8);

    if( write( fd, &flags, 4 ) != 4 )
    {
      MDException ex( errno );
      ex.getMessage() << "Unable to write the flags: " << name;
      throw ex;
    }

    fdPtr.release();
    pFd        = fd;
    pIsOpen    = true;
    pVersion   = 1;
    pSeqNumber = 0;
  }

  //----------------------------------------------------------------------------
  // Close the log
  //----------------------------------------------------------------------------
  void ChangeLogFile::close()
  {
    ::close( pFd );
    pIsOpen = false;
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
      MDException ex( errno );
      ex.getMessage() << "Unable to sync the changelog file: ";
      ex.getMessage() << strerror( errno );
      throw ex;
    }
  }

  //----------------------------------------------------------------------------
  // Store the record in the log
  //----------------------------------------------------------------------------
  uint64_t ChangeLogFile::storeRecord( char type, Buffer &record )
    throw( MDException )
  {
    if( !pIsOpen )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Changelog file is not open";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Allign the buffer to 4 bytes and calculate the checksum
    //--------------------------------------------------------------------------
    uint32_t nsize = record.size();
    nsize = (nsize+3) >> 2 << 2;

    if( nsize > 65535 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Record too big";
      throw ex;
    }
    record.resize( nsize );

    //--------------------------------------------------------------------------
    // Initialize the data and calculate the checksum
    //--------------------------------------------------------------------------
    uint16_t size   = record.size();
    uint64_t offset = ::lseek( pFd, 0, SEEK_END );
    uint64_t seq    = 0;
    uint16_t magic  = RECORD_MAGIC;
    uint32_t opts   = type; // occupy the first byte (little endian)
                            // the rest is unused for the moment

    uint32_t chkSum = DataHelper::computeCRC32( &seq, 8 );
    chkSum = DataHelper::updateCRC32( chkSum, &opts, 4 );
    chkSum = DataHelper::updateCRC32( chkSum,
                                      record.getDataPtr(),
                                      record.getSize() );

    //--------------------------------------------------------------------------
    // Store the data
    //--------------------------------------------------------------------------

    iovec vec[7];
    vec[0].iov_base = &magic;  vec[0].iov_len = 2;
    vec[1].iov_base = &size;   vec[1].iov_len = 2;
    vec[2].iov_base = &chkSum; vec[2].iov_len = 4;
    vec[3].iov_base = &seq;    vec[3].iov_len = 8;
    vec[4].iov_base = &opts;   vec[4].iov_len = 4;
    vec[5].iov_base = (void*)record.getDataPtr();
    vec[5].iov_len = record.size();
    vec[6].iov_base = &chkSum; vec[6].iov_len = 4;

    if( writev( pFd, vec, 7 ) != (unsigned)(24+record.size()) )
    {
      MDException ex( errno );
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
      ex.getMessage() << "Read: Changelog file is not open";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Read first part of the record
    //--------------------------------------------------------------------------
    uint16_t *magic;
    uint16_t *size;
    uint32_t *chkSum1;
    uint64_t *seq;
    uint32_t  chkSum2;
    uint8_t  *type;
    char      buffer[20];

    if( pread( pFd, buffer, 20, offset ) != 20 )
    {
      MDException ex( errno );
      ex.getMessage() << "Read: Error reading at offset: " << offset;
      throw ex;
    }

    magic   = (uint16_t*)(buffer);
    size    = (uint16_t*)(buffer+2);
    chkSum1 = (uint32_t*)(buffer+4);
    seq     = (uint64_t*)(buffer+8);
    type    = (uint8_t*) (buffer+16);

    //--------------------------------------------------------------------------
    // Check the consistency
    //--------------------------------------------------------------------------
    if( *magic != RECORD_MAGIC )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Read: Record's magic number is wrong.";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Read the second part of the buffer
    //--------------------------------------------------------------------------
    record.resize( *size+4, 0 );
    if( pread( pFd, record.getDataPtr(), *size+4, offset+20 ) != *size+4 )
    {
      MDException ex( errno );
      ex.getMessage() << "Read: Error reading at offset: " << offset + 9;
      throw ex;
    }
    record.grabData( record.size()-4, &chkSum2, 4 );
    record.resize( *size );

    //--------------------------------------------------------------------------
    // Check the checksum
    //--------------------------------------------------------------------------
    uint32_t crc = DataHelper::computeCRC32( seq, 8 );
    crc = DataHelper::updateCRC32( crc, (buffer+16), 4 ); // opts
    crc = DataHelper::updateCRC32( crc, record.getDataPtr(), record.getSize() );

    if( *chkSum1 != crc || *chkSum1 != chkSum2 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Read: Record's checksums do not match.";
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
      ex.getMessage() << "Scan: Changelog file is not open";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Get the offset information
    //--------------------------------------------------------------------------
    off_t end = ::lseek( pFd, 0, SEEK_END );
    if( end == -1 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Scan: Unable to find the end of the log file: ";
      ex.getMessage() << strerror( errno );
      throw ex;
    }

    off_t offset = ::lseek( pFd, 8, SEEK_SET );
    if( offset != 8 )
    {
      MDException ex( EFAULT );
      ex.getMessage() << "Scan: Unable to find the record data at offset 0x";
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
      offset += 24;
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
      ex.getMessage() << "Follow: Changelog file is not open";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Off we go - we only exit if an error occurs
    //--------------------------------------------------------------------------
    Descriptor   fd( pFd );
    off_t        offset = 8;
    uint16_t    *magic;
    uint16_t    *size;
    uint32_t    *chkSum1;
    uint64_t    *seq;
    uint32_t     chkSum2;
    uint8_t     *type;
    char         buffer[20];
    Buffer       record;

    while( 1 )
    {
      //------------------------------------------------------------------------
      // Read the header
      //------------------------------------------------------------------------
      try
      {
         fd.offsetReadNonBlocking( buffer, 20, offset, poll );
      }
      catch( DescriptorException &e )
      {
        MDException ex( errno );
        ex.getMessage() << "Follow: Error reading at offset: " << offset << ": ";
        ex.getMessage() << e.getMessage().str();
        throw ex;
      }

      magic   = (uint16_t*)(buffer);
      size    = (uint16_t*)(buffer+2);
      chkSum1 = (uint32_t*)(buffer+4);
      seq     = (uint64_t*)(buffer+8);
      type    = (uint8_t*) (buffer+16);

      //------------------------------------------------------------------------
      // Check the consistency
      //------------------------------------------------------------------------
      if( *magic != RECORD_MAGIC )
      {
        MDException ex( EFAULT );
        ex.getMessage() << "Follow: Record's magic number is wrong.";
        throw ex;
      }

      //------------------------------------------------------------------------
      // Read the second part of the buffer
      //------------------------------------------------------------------------
      record.resize( *size+4, 0 );
      try
      {
        fd.offsetReadNonBlocking( record.getDataPtr(), *size+4, offset+20,
                                  poll );
      }
      catch( DescriptorException &e )
      {
        MDException ex( errno );
        ex.getMessage() << "Follow: Error reading at offset: " << offset + 9;
        ex.getMessage() << ": " << e.getMessage().str();
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
        ex.getMessage() << "Follow: Record's checksums do not match.";
        throw ex;
      }

      //------------------------------------------------------------------------
      // Call the listener and clean up
      //------------------------------------------------------------------------
      scanner->processRecord( offset, *type, record );
      offset += record.size();
      offset += 24;
      record.clear();
    }
  }

  //----------------------------------------------------------------------------
  // Find the record header starting at offset - the log files are aligned
  // to 4 bytes so the magic should be at [(offset mod 4) == 0]
  //----------------------------------------------------------------------------
  static off_t findRecordMagic( int fd, off_t offset, off_t offsetLimit = 0 )
  {
    uint32_t magic = 0;
    while( 1 )
    {
      if( pread( fd, &magic, 4, offset ) != 4 )
        return -1;

      if( (magic & 0x0000ffff) == RECORD_MAGIC )
        return offset;

      offset += 4;
      if( offsetLimit && offset >= offsetLimit )
        return -1;
    }
    return -1;
  }

  //----------------------------------------------------------------------------
  // Adjust size
  //----------------------------------------------------------------------------
  off_t guessSize( int fd, off_t offset, Buffer &buffer, off_t startHint = 0 )
  {
    //--------------------------------------------------------------------------
    // Find a magic number of the next record
    //--------------------------------------------------------------------------
    if( startHint && startHint - offset >= 70000 )
      return -1;

    if( !startHint )
      startHint = offset+24;

    off_t newOffset = findRecordMagic( fd, startHint, offset+70000 );
    if( newOffset == (off_t)-1 )
      return -1;

    //--------------------------------------------------------------------------
    // Is the new size correct?
    //--------------------------------------------------------------------------
    off_t newSize = newOffset-offset-24;
    if( newSize > 65535 || newSize < 0 )
      return -1;

    buffer.resize( newSize );
    if( pread( fd, buffer.getDataPtr(), newSize, offset+20 ) != newSize )
      return -1;

    return newSize;
  }

  //----------------------------------------------------------------------------
  // Reconstruct record at offset
  //----------------------------------------------------------------------------
  static off_t reconstructRecord( int fd, off_t offset,
                                  off_t fsize, Buffer &buffer, uint8_t &type,
                                  LogRepairStats &stats )
  {
    uint16_t *magic;
    uint16_t  size;
    uint32_t *chkSum1;
    uint64_t *seq;
    uint32_t  chkSum2;
    char      buff[20];

    //--------------------------------------------------------------------------
    // Read the record header data and second checksum
    //--------------------------------------------------------------------------
    if( pread( fd, buff, 20, offset ) != 20 )
      return -1;

    magic   =  (uint16_t*)(buff);
    size    = *(uint16_t*)(buff+2);
    chkSum1 =  (uint32_t*)(buff+4);
    seq     =  (uint64_t*)(buff+8);
    type    = *(uint8_t*) (buff+16);

    uint32_t crcHead = DataHelper::computeCRC32( seq, 8 );
    crcHead = DataHelper::updateCRC32( crcHead, (buff+16), 4 ); // opts

    //--------------------------------------------------------------------------
    // Try to reading the record data - if the read fails then the size
    // may be incorrect, so try to compensate
    //--------------------------------------------------------------------------
    buffer.resize( size );
    if( pread( fd, buffer.getDataPtr(), size, offset+20 ) != size )
    {
      ++stats.fixedWrongSize;
      off_t offSize;
      offSize = guessSize( fd, offset, buffer );
      if( offSize == (off_t)-1 )
        return -1;
      size = offSize;
    }

    if( pread( fd, &chkSum2, 4, offset+20+size ) != 4 )
      return -1;

    //--------------------------------------------------------------------------
    // The magic wrong
    //--------------------------------------------------------------------------
    bool wrongMagic = false;
    if( *magic != RECORD_MAGIC )
      wrongMagic = true;

    //--------------------------------------------------------------------------
    // Check the sums
    //--------------------------------------------------------------------------
    bool okChecksum1 = true;
    bool okChecksum2 = true;

    uint32_t crc = DataHelper::updateCRC32( crcHead,
                                            buffer.getDataPtr(),
                                            buffer.getSize() );

    if( *chkSum1 != crc )
      okChecksum1 = false;

    if( chkSum2 != crc )
      okChecksum2 = false;

    if( okChecksum1 || okChecksum2 )
    {
      if( !okChecksum1 || !okChecksum2 )
        ++stats.fixedWrongChecksum;
      if( wrongMagic )
        ++stats.fixedWrongMagic;
      return offset+size+24;
    }

    //--------------------------------------------------------------------------
    // Checksums incorrect - parhaps size is wrong - try to find another
    // record magic
    // The first magic we find may not be the right one, so we need to do
    // it many times
    //--------------------------------------------------------------------------
    off_t startHint = offset+24;
    while( 1 )
    {
      //------------------------------------------------------------------------
      // Estimate new size
      //------------------------------------------------------------------------
      off_t offSize;
      offSize = guessSize( fd, offset, buffer, startHint );

      if( offSize == (off_t)-1 )
        return -1;

      size = offSize;

      if( pread( fd, &chkSum2, 4, offset+20+size ) != 4 )
        return -1;

      startHint += size + 4;

      //------------------------------------------------------------------------
      // Check the checksums
      //------------------------------------------------------------------------
      okChecksum1 = true;
      okChecksum2 = true;


      crc = DataHelper::updateCRC32( crcHead,
                                     buffer.getDataPtr(),
                                     buffer.getSize() );

      if( *chkSum1 != crc )
        okChecksum1 = false;

      if( chkSum2 != crc )
        okChecksum2 = false;

      if( okChecksum1 || okChecksum2 )
      {
        if( !okChecksum1 || !okChecksum2 )
          ++stats.fixedWrongChecksum;
        if( wrongMagic )
          ++stats.fixedWrongMagic;

        ++stats.fixedWrongSize;
        return offset+size+24;
      }
    }
    return -1;
  }

  //----------------------------------------------------------------------------
  //! Repair a changelog
  //----------------------------------------------------------------------------
  void ChangeLogFile::repair( const std::string  &filename,
                              const std::string  &newFilename,
                              LogRepairStats     &stats,
                              ILogRepairFeedback *feedback ) throw( MDException )
  {
    time_t startTime = time(0);

    //--------------------------------------------------------------------------
    // Open the input and output files and check out the header
    //--------------------------------------------------------------------------
    int fd = ::open( filename.c_str(), O_RDONLY );
    if( fd == -1 )
    {
      MDException ex( errno );
      ex.getMessage() << "Unrecognized file type: " << filename;
      throw ex;
    }
    FileSmartPtr fdPtr( fd );

    uint16_t contentFlag = 0;
    try
    {
      uint32_t headerFlags = checkHeader( fd, filename );
      uint8_t  version     = 0;
      decodeHeaderFlags( headerFlags, version, contentFlag );
      if( feedback )
        feedback->reportHeaderStatus( true, "", version, contentFlag );
    }
    catch( MDException &e )
    {
      if( feedback )
        feedback->reportHeaderStatus( false, e.getMessage().str(), 0, 0 );
    }

    ChangeLogFile output;
    output.open( newFilename, false, contentFlag );

    //--------------------------------------------------------------------------
    // Reconstructing...
    //--------------------------------------------------------------------------
    Buffer  buff;
    uint8_t type;
    off_t   fsize  = ::lseek( fd, 0, SEEK_END );
    off_t   offset = 8;

    stats.bytesTotal    = fsize;
    stats.bytesAccepted = 8;  // offset

    while( offset < fsize )
    {
      //------------------------------------------------------------------------
      // Reconstruct the header at the offset
      //------------------------------------------------------------------------
      off_t newOffset = reconstructRecord( fd, offset, fsize, buff, type,
                                           stats );

      ++stats.scanned;

      //------------------------------------------------------------------------
      // We were successful
      //------------------------------------------------------------------------
      if( newOffset != (off_t)-1 )
      {
        ++stats.healthy;
        stats.bytesAccepted += (newOffset-offset);
        output.storeRecord( type, buff );
      }

      //------------------------------------------------------------------------
      // Unsuccessful for whatever reason - offsets cannot be trusted anymore
      // so try to find a magic number of a new record
      //------------------------------------------------------------------------
      else
      {
        ++stats.notFixed;
        newOffset = findRecordMagic( fd, offset+4 );

        if( newOffset == (off_t)-1 )
        {
          stats.bytesDiscarded += (fsize-offset);
          break;
        }
        stats.bytesDiscarded += (newOffset-offset);
      }

      //------------------------------------------------------------------------
      // We either successfully reconstructed the previous record or found
      // an offset of another one
      //------------------------------------------------------------------------
      offset = newOffset;

      stats.timeElapsed = time(0) - startTime;
      if( feedback )
        feedback->reportProgress( stats );
    }
  }
}
