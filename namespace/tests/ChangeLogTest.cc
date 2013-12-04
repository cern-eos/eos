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
// desc:   ChangeLog test
//------------------------------------------------------------------------------

#include <cppunit/extensions/HelperMacros.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <ctime>
#include <cstdlib>
#include <utility>
#include <set>
#include <vector>
#include <algorithm>
#include <ext/algorithm>
#include <cstdio>

#define protected public
#include "namespace/FileMD.hh"
#undef protected
#include "namespace/persistency/ChangeLogConstants.hh"
#include "namespace/persistency/ChangeLogFile.hh"
#include "namespace/IFileMDSvc.hh"

#include "namespace/tests/TestHelpers.hh"

#define NUMTESTFILES 1000

//------------------------------------------------------------------------------
// Declaration
//------------------------------------------------------------------------------
class ChangeLogTest: public CppUnit::TestCase
{
  public:
    CPPUNIT_TEST_SUITE( ChangeLogTest );
    CPPUNIT_TEST( readWriteCorrectness );
    CPPUNIT_TEST( followingTest );
    CPPUNIT_TEST( fsckTest );
    CPPUNIT_TEST_SUITE_END();
    void readWriteCorrectness();
    void followingTest();
    void fsckTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION( ChangeLogTest );

//------------------------------------------------------------------------------
// Dummy file MD service
//------------------------------------------------------------------------------
class DummyFileMDSvc: public eos::IFileMDSvc
{
  public:
    virtual void initialize() throw( eos::MDException ) {}
    virtual void configure( std::map<std::string, std::string> &config )
      throw( eos::MDException ) {}
    virtual void finalize() throw( eos::MDException ) {}
    virtual eos::FileMD *getFileMD( eos::FileMD::id_t id )
      throw( eos::MDException )
    { return 0; }
    virtual eos::FileMD *createFile() throw( eos::MDException ) { return 0; }
    virtual void updateStore( eos::FileMD *obj ) throw( eos::MDException ) {}
    virtual void removeFile( eos::FileMD *obj ) throw( eos::MDException ) {}
    virtual void removeFile( eos::FileMD::id_t fileId )
      throw( eos::MDException ) {}
    virtual uint64_t getNumFiles() const { return 0; }
    virtual void visit( eos::IFileVisitor *visitor ) {}
    virtual void addChangeListener( eos::IFileMDChangeListener *listener ) {}
    virtual void notifyListeners( eos::IFileMDChangeListener::Event *event ) {}
};

//------------------------------------------------------------------------------
// Fill a file metadata
//------------------------------------------------------------------------------
void fillFileMD( eos::FileMD &fileMetadata, int i )
{
  uint32_t chkSum = i*423;
  eos::FileMD::ctime_t time;
  time.tv_sec = i*1234;
  time.tv_nsec = i*456;
  std::ostringstream o;
  o << "filename_" << i;
  fileMetadata.pId    = i;
  fileMetadata.setCTime( time );
  fileMetadata.setSize( i*987 );
  fileMetadata.setFlag( i%16, true );
  fileMetadata.setContainerId( i*765 );
  fileMetadata.setChecksum( &chkSum, sizeof( chkSum ) );
  fileMetadata.setName( o.str() );
  fileMetadata.setCUid( i*2 );
  fileMetadata.setCGid( i*3 );
  fileMetadata.setLayoutId( i*4 );
  for( int j = 0; j < 5; ++j )
    fileMetadata.addLocation( i*j*2 );
}

//------------------------------------------------------------------------------
// Check file metadata
//------------------------------------------------------------------------------
void checkFileMD( eos::FileMD &fileMetadata, unsigned i )
{
  std::ostringstream o;
  o << "filename_" << i;
  uint32_t checkSum = i*423;
  eos::FileMD::ctime_t time;

  fileMetadata.getCTime( time );
  CPPUNIT_ASSERT( fileMetadata.pId == i );
  CPPUNIT_ASSERT( time.tv_sec == i*1234 );
  CPPUNIT_ASSERT( time.tv_nsec == i*456 );
  CPPUNIT_ASSERT( fileMetadata.getSize() == i*987 );
  for( unsigned k = 0; k < 16; ++k )
    CPPUNIT_ASSERT( fileMetadata.getFlag( k ) == (k==i%16 ? true : false) );
  CPPUNIT_ASSERT( fileMetadata.getContainerId() == i*765 );
  CPPUNIT_ASSERT( fileMetadata.checksumMatch( &checkSum ) );
  CPPUNIT_ASSERT( fileMetadata.getCUid() == i*2 );
  CPPUNIT_ASSERT( fileMetadata.getCGid() == i*3 );
  CPPUNIT_ASSERT( fileMetadata.getLayoutId() == i*4 );

  CPPUNIT_ASSERT( o.str() == fileMetadata.getName() );

  for( int j = 0; j < 5; ++j )
    CPPUNIT_ASSERT( fileMetadata.hasLocation( i*j*2 ) );
}

//------------------------------------------------------------------------------
// File scanner
//------------------------------------------------------------------------------
class FileScanner: public eos::ILogRecordScanner
{
  public:
    virtual bool processRecord( uint64_t offset, char type,
                                const eos::Buffer &buffer )
    {
      pRecords.push_back( std::make_pair( offset, buffer.size() ) );
      return true;
    }

    std::vector<std::pair<uint64_t, uint16_t> > &getRecords()
    {
      return pRecords;
    }

  private:
    std::vector<std::pair<uint64_t, uint16_t> > pRecords;
};

//------------------------------------------------------------------------------
// File follower
//------------------------------------------------------------------------------
class FileFollower: public eos::ILogRecordScanner
{
  public:
    FileFollower(): pIndex( 0 ) {}
    virtual bool processRecord( uint64_t offset, char type,
                                const eos::Buffer &buffer )
    {
      DummyFileMDSvc fmd;
      eos::FileMD fileMetadata( 0, &fmd );
      fileMetadata.deserialize( buffer );
      checkFileMD( fileMetadata, pIndex++ );
      if( pIndex == NUMTESTFILES )
        pthread_exit( 0 );
      return true;
    }

  private:
    int pIndex;
};

//------------------------------------------------------------------------------
// Concrete implementation tests
//------------------------------------------------------------------------------
void ChangeLogTest::readWriteCorrectness()
{
  //----------------------------------------------------------------------------
  // Test the file creation
  //----------------------------------------------------------------------------
  eos::ChangeLogFile file;
  std::string        fileName = getTempName( "/tmp", "eosns" );
  CPPUNIT_ASSERT_NO_THROW( file.open( fileName, eos::ChangeLogFile::Create,
                                      0x1212 ) );

  //----------------------------------------------------------------------------
  // Store 1000 files
  //----------------------------------------------------------------------------
  DummyFileMDSvc fmd;
  eos::FileMD fileMetadata( 0, &fmd );
  eos::Buffer buffer;

  std::vector<uint64_t> offsets;
  for( int i = 0; i < NUMTESTFILES; ++i )
  {
    buffer.clear();
    fillFileMD( fileMetadata, i );
    CPPUNIT_ASSERT_NO_THROW( fileMetadata.serialize( buffer ) );
    CPPUNIT_ASSERT_NO_THROW( offsets.push_back(
                               file.storeRecord(
                                 eos::UPDATE_RECORD_MAGIC, buffer ) ) );
    fileMetadata.clearLocations();
    fileMetadata.setFlags( 0 );
  }
  file.close();

  //----------------------------------------------------------------------------
  // Scan the file and compare the offsets
  //----------------------------------------------------------------------------
  CPPUNIT_ASSERT_NO_THROW( file.open( fileName, eos::ChangeLogFile::ReadOnly,
                                      0x0000 ) );
  CPPUNIT_ASSERT( file.getContentFlag() == 0x1212 );
  FileScanner scanner;
  CPPUNIT_ASSERT_NO_THROW( file.scanAllRecords( &scanner ) );
  std::vector<std::pair<uint64_t, uint16_t> > &readRecords = scanner.getRecords();
  CPPUNIT_ASSERT( readRecords.size() == offsets.size() );
  for( unsigned i = 0; i < readRecords.size(); ++i )
    CPPUNIT_ASSERT( readRecords[i].first == offsets[i] );

  //----------------------------------------------------------------------------
  // Check the records
  //----------------------------------------------------------------------------
  for( unsigned i = 0; i < readRecords.size(); ++i )
  {
    CPPUNIT_ASSERT_NO_THROW( file.readRecord( readRecords[i].first, buffer ) );
    CPPUNIT_ASSERT_NO_THROW( fileMetadata.deserialize( buffer ) );
    checkFileMD( fileMetadata, i );
    fileMetadata.clearLocations();
  }
  file.close();
  unlink( fileName.c_str() );
}

//------------------------------------------------------------------------------
// Follow the changelog
//------------------------------------------------------------------------------
void *followerThread( void *data )
{
  eos::ChangeLogFile &file = *reinterpret_cast<eos::ChangeLogFile*>(data);
  FileFollower f;
  uint64_t offset = file.getFirstOffset();
  while( 1 )
  {
    offset = file.follow( &f, offset );
    file.wait( 1 );
  }
  return 0;
}

//------------------------------------------------------------------------------
// Concrete implementation tests
//------------------------------------------------------------------------------
void ChangeLogTest::followingTest()
{
  //----------------------------------------------------------------------------
  // Test the file creation
  //----------------------------------------------------------------------------
  eos::ChangeLogFile file;
  eos::ChangeLogFile fileFollow;
  std::string        fileName = getTempName( "/tmp", "eosns" );
  CPPUNIT_ASSERT_NO_THROW( file.open( fileName ) );
  CPPUNIT_ASSERT_NO_THROW( fileFollow.open( fileName,
                                            eos::ChangeLogFile::ReadOnly ) );

  //----------------------------------------------------------------------------
  // Spawn a follower thread
  //----------------------------------------------------------------------------
  pthread_t thread;
  CPPUNIT_ASSERT_MESSAGE( "Unable to spawn the follower thread",
               pthread_create( &thread, 0, followerThread, &fileFollow ) == 0 );

  //----------------------------------------------------------------------------
  // Store 1000 files
  //----------------------------------------------------------------------------
  DummyFileMDSvc fmd;
  eos::FileMD fileMetadata( 0, &fmd );
  eos::Buffer buffer;

  std::vector<uint64_t> offsets;
  for( int i = 0; i < 1000; ++i )
  {
    buffer.clear();
    fillFileMD( fileMetadata, i );
    CPPUNIT_ASSERT_NO_THROW( fileMetadata.serialize( buffer ) );
    CPPUNIT_ASSERT_NO_THROW(
            offsets.push_back( file.storeRecord(
                                   eos::UPDATE_RECORD_MAGIC, buffer ) ) );
    fileMetadata.clearLocations();
    fileMetadata.setFlags( 0 );
    usleep( 60000 );
  }
  pthread_join( thread, 0 );

  file.close();
  unlink( fileName.c_str() );
}

//------------------------------------------------------------------------------
// Create a changelog file with random records
//------------------------------------------------------------------------------
static void createRandomLog( const std::string &path, uint64_t numRecords )
{
  srandom( time( 0 ) );
  eos::ChangeLogFile file;
  eos::Buffer        buffer;
  file.open( path );
  for( uint64_t i = 0; i < numRecords; ++i )
  {
    uint8_t numBlocks = (random()%254) + 1;
    buffer.clear();
    buffer.reserve( numBlocks*4 );
    for( uint16_t j = 0; j < numBlocks; ++j )
    {
      uint32_t block = random();
      buffer.putData( &block, 4 );
    }
    file.storeRecord( 1, buffer );
  }
  file.close();
}

//------------------------------------------------------------------------------
// Break the record data
//------------------------------------------------------------------------------
static void breakRecordData( char *buffer, uint16_t size )
{
  uint16_t breakDWord = ((random() % (size-24))+20)/4;
  uint32_t newDWord   = random();
  uint32_t *dword = ((uint32_t*)buffer)+breakDWord;

  while( newDWord == *dword )
    newDWord = random();

  *dword = newDWord;
}

//------------------------------------------------------------------------------
// Break one of the checksums
//------------------------------------------------------------------------------
static void breakRecordChecksum( char *buffer, uint16_t size )
{
  int breakSecond  = random() % 2;
  uint16_t offset = 4;
  if( breakSecond )
    offset = size - 4;

  uint32_t newChkSum = random();
  uint32_t *dword = (uint32_t*)(buffer+offset);

  while( newChkSum == *dword )
    newChkSum = random();

  *dword = newChkSum;
}

//------------------------------------------------------------------------------
// Break the size
//------------------------------------------------------------------------------
static void breakRecordSize( char *buffer, uint16_t size )
{
  uint16_t *sz = (uint16_t*)(buffer+2);
  uint16_t newSize = random();

  while( newSize == *sz )
    newSize = random();

  *sz = newSize;
}

//------------------------------------------------------------------------------
// Break the Magic
//------------------------------------------------------------------------------
static void breakRecordMagic( char *buffer, uint16_t size )
{
  uint16_t *magic = (uint16_t*)buffer;
  uint16_t newMagic = random();

  while( newMagic == *magic )
    newMagic = random();

  *magic = newMagic;
}

//------------------------------------------------------------------------------
// Apply breaking function to many records
//------------------------------------------------------------------------------
void breakRecordsFunc( int fd,
                       std::vector<std::pair<uint64_t, uint16_t> > &recRead,
                       std::vector<size_t>                         &toBreak,
                       void(*f)( char *, uint16_t ) )
{
  std::vector<size_t>::iterator it;
  for( it = toBreak.begin(); it != toBreak.end(); ++it )
  {
    uint32_t size = recRead[*it].second+24;
    char buffer[size];
    if( pread( fd, buffer, size, recRead[*it].first ) != size )
      throw std::string( "Unable to read enough data" );

    f( buffer, size );

    if( pwrite( fd, buffer, size, recRead[*it].first ) != size )
      throw std::string( "Unable to write the broken data" );
  }
}

//------------------------------------------------------------------------------
// Got throught the log and break things
//------------------------------------------------------------------------------
static void breakRecords( const std::string &path, uint64_t numBreak,
                          eos::LogRepairStats &stats )
{
  //----------------------------------------------------------------------------
  // Read the record offsets and sizes
  //----------------------------------------------------------------------------
  eos::ChangeLogFile file;
  FileScanner        scanner;

  CPPUNIT_ASSERT_NO_THROW( file.open( path ) );
  CPPUNIT_ASSERT_NO_THROW( file.scanAllRecords( &scanner ) );
  std::vector<std::pair<uint64_t, uint16_t> > &readRecords = scanner.getRecords();
  CPPUNIT_ASSERT( readRecords.size() == 10000 );
  file.close();

  //----------------------------------------------------------------------------
  // Select some records to break - it's complicated if we want to have an
  // automatically testable/semi-deterministic result, because the
  // size correction algorithm depend on the correctness of the magic number
  // of the following record thus we cannot break the magic of a record
  // that emmediately follows the on with broken size
  //----------------------------------------------------------------------------
  uint64_t partSize  = numBreak*0.25;
  uint64_t reminder  = numBreak - 3*partSize;

  std::set<size_t> randomBreakRecs;
  std::set<size_t> randomMagicBreak;
  std::vector<size_t> dataBreak(partSize);
  std::vector<size_t> chksumBreak(partSize);
  std::vector<size_t> sizeBreak(partSize);
  std::vector<size_t> magicBreak(reminder);

  while( randomBreakRecs.size() != 3*partSize )
    randomBreakRecs.insert( random() % readRecords.size() );

  std::set<size_t>::iterator itB = randomBreakRecs.begin();
  std::set<size_t>::iterator itE = randomBreakRecs.begin();

  std::advance( itE, partSize );
  std::copy( itB, itE, dataBreak.begin() );
  itB = itE; std::advance( itE, partSize );
  std::copy( itB, itE, chksumBreak.begin() );
  itB = itE; std::advance( itE, partSize );
  std::copy( itB, itE, sizeBreak.begin() );

  while( randomMagicBreak.size() != reminder )
  {
    size_t toBreak = (1+random()) % readRecords.size();
    if( randomBreakRecs.find( toBreak-1 ) == randomBreakRecs.end() &&
        randomBreakRecs.find( toBreak ) == randomBreakRecs.end() )
      randomMagicBreak.insert( toBreak );
  }

  std::copy( randomMagicBreak.begin(), randomMagicBreak.end(),
             magicBreak.begin() );

  stats.notFixed           = dataBreak.size();
  stats.fixedWrongSize     = sizeBreak.size();
  stats.fixedWrongChecksum = chksumBreak.size();
  stats.fixedWrongMagic    = magicBreak.size();

  //----------------------------------------------------------------------------
  // Open the changelog file
  //----------------------------------------------------------------------------
  int fd = ::open( path.c_str(), O_RDWR );
  if( fd == -1 )
    throw std::string( "Unable to open changelog for breaking" );

  //----------------------------------------------------------------------------
  // Break the records
  //----------------------------------------------------------------------------
  breakRecordsFunc( fd, readRecords, dataBreak,   breakRecordData );
  breakRecordsFunc( fd, readRecords, chksumBreak, breakRecordChecksum );
  breakRecordsFunc( fd, readRecords, sizeBreak,   breakRecordSize );
  breakRecordsFunc( fd, readRecords, magicBreak,  breakRecordMagic );

  //----------------------------------------------------------------------------
  // Cleanup
  //----------------------------------------------------------------------------
  ::close( fd );
}
//------------------------------------------------------------------------------
// FSCK test
//------------------------------------------------------------------------------
void ChangeLogTest::fsckTest()
{
  eos::LogRepairStats stats;
  eos::LogRepairStats brokenStats;
  std::string         fileNameBroken   = getTempName( "/tmp", "eosns" );
  std::string         fileNameRepaired = getTempName( "/tmp", "eosns" );
  CPPUNIT_ASSERT_NO_THROW( createRandomLog( fileNameBroken, 10000 ) );
  CPPUNIT_ASSERT_NO_THROW( breakRecords( fileNameBroken, 100, brokenStats ) );
  CPPUNIT_ASSERT_NO_THROW( eos::ChangeLogFile::repair( fileNameBroken, fileNameRepaired,
                                                       stats, 0 ) );

  // stats.scanned may be more than 10000 and it's fine
  CPPUNIT_ASSERT( stats.scanned == stats.healthy + stats.notFixed );
  uint64_t totalFixed = brokenStats.fixedWrongMagic +
    brokenStats.fixedWrongChecksum +
    brokenStats.fixedWrongSize;
  CPPUNIT_ASSERT( stats.healthy == 9900+totalFixed );
  CPPUNIT_ASSERT( stats.fixedWrongMagic    == brokenStats.fixedWrongMagic );
  CPPUNIT_ASSERT( stats.fixedWrongChecksum == brokenStats.fixedWrongChecksum );
  CPPUNIT_ASSERT( stats.fixedWrongSize     == brokenStats.fixedWrongSize );

  unlink( fileNameBroken.c_str() );
  unlink( fileNameRepaired.c_str() );
}
