//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog test
//------------------------------------------------------------------------------

#include <cppunit/CompilerOutputter.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include "ChangeLogTest.hh"

#include <stdint.h>
#include <unistd.h>
#include <pthread.h>

#define protected public
#include "Namespace/FileMD.hh"
#undef protected
#include "Namespace/persistency/ChangeLogConstants.hh"
#include "Namespace/persistency/ChangeLogFile.hh"
#include "Namespace/IFileMDSvc.hh"

#define NUMTESTFILES 1000

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
// Generic file md test declaration
//------------------------------------------------------------------------------
CppUnit::Test *ChangeLogTest::suite()
{
  CppUnit::TestSuite *suiteOfTests
              = new CppUnit::TestSuite( "ChangeLogTest" );

  suiteOfTests->addTest( new CppUnit::TestCaller<ChangeLogTest>(
                               "readWriteCorrectness",
                               &ChangeLogTest::readWriteCorrectness ) );
  suiteOfTests->addTest( new CppUnit::TestCaller<ChangeLogTest>(
                               "followingTest",
                               &ChangeLogTest::followingTest ) );

  return suiteOfTests;
}

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
    virtual void processRecord( uint64_t offset, char type,
                                const eos::Buffer &buffer )
    {
      pOffsets.push_back( offset );
    }

    std::vector<uint64_t> &getOffsets()
    {
      return pOffsets;
    }

  private:
    std::vector<uint64_t> pOffsets;
};

//------------------------------------------------------------------------------
// File follower
//------------------------------------------------------------------------------
class FileFollower: public eos::ILogRecordScanner
{
  public:
    FileFollower(): pIndex( 0 ) {}
    virtual void processRecord( uint64_t offset, char type,
                                const eos::Buffer &buffer )
    {
      DummyFileMDSvc fmd;
      eos::FileMD fileMetadata( 0, &fmd );
      fileMetadata.deserialize( buffer );
      checkFileMD( fileMetadata, pIndex++ );
      if( pIndex == NUMTESTFILES )
        pthread_exit( 0 );
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
  try
  {
    file.open( "/tmp/test_changelog.dat" );

    //--------------------------------------------------------------------------
    // Store 1000 files
    //--------------------------------------------------------------------------
    DummyFileMDSvc fmd;
    eos::FileMD fileMetadata( 0, &fmd );
    eos::Buffer buffer;

    std::vector<uint64_t> offsets;
    for( int i = 0; i < NUMTESTFILES; ++i )
    {
      buffer.clear();
      fillFileMD( fileMetadata, i );
      fileMetadata.serialize( buffer );
      offsets.push_back( file.storeRecord( eos::UPDATE_RECORD, buffer ) );
      fileMetadata.clearLocations();
    }

    //--------------------------------------------------------------------------
    // Scan the file and compare the offsets
    //--------------------------------------------------------------------------
    FileScanner scanner;
    file.scanAllRecords( &scanner );
    std::vector<uint64_t> &readOffsets = scanner.getOffsets();
    CPPUNIT_ASSERT( readOffsets.size() == offsets.size() );
    for( unsigned i = 0; i < readOffsets.size(); ++i )
      CPPUNIT_ASSERT( readOffsets[i] == offsets[i] );

    //--------------------------------------------------------------------------
    // Check the records
    //--------------------------------------------------------------------------
    for( unsigned i = 0; i < readOffsets.size(); ++i )
    {
      file.readRecord( readOffsets[i], buffer );
      fileMetadata.deserialize( buffer );
      checkFileMD( fileMetadata, i );
      fileMetadata.clearLocations();
    }
  }
  catch( eos::MDException e )
  {
    file.close();
    unlink( "/tmp/test_changelog.dat" );
    CPPUNIT_ASSERT_MESSAGE( e.getMessage().str(), false );
  }
  file.close();
  unlink( "/tmp/test_changelog.dat" );
}

//------------------------------------------------------------------------------
// Follow the changelog
//------------------------------------------------------------------------------
void *followerThread( void *data )
{
  try
  {
    eos::ChangeLogFile &file = *reinterpret_cast<eos::ChangeLogFile*>(data);
    FileFollower f;
    file.follow( &f, 100 );
  }
  catch( eos::MDException e )
  {
    unlink( "/tmp/test_changelog.dat" );
    CPPUNIT_ASSERT_MESSAGE( e.getMessage().str(), false );
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
  try
  {
    file.open( "/tmp/test_changelog.dat" );

    //--------------------------------------------------------------------------
    // Spawn a follower thread
    //--------------------------------------------------------------------------
    pthread_t thread;
    if( pthread_create( &thread, 0, followerThread, &file ) )
    {
      CPPUNIT_ASSERT_MESSAGE( "Unable to spawn the follower thread", false );
    }

    //--------------------------------------------------------------------------
    // Store 1000 files
    //--------------------------------------------------------------------------
    DummyFileMDSvc fmd;
    eos::FileMD fileMetadata( 0, &fmd );
    eos::Buffer buffer;

    std::vector<uint64_t> offsets;
    for( int i = 0; i < 1000; ++i )
    {
      buffer.clear();
      fillFileMD( fileMetadata, i );
      fileMetadata.serialize( buffer );
      offsets.push_back( file.storeRecord( eos::UPDATE_RECORD, buffer ) );
      fileMetadata.clearLocations();
      usleep( 60000 );
    }
    pthread_join( thread, 0 );
  }
  catch( eos::MDException e )
  {
    file.close();
    unlink( "/tmp/test_changelog.dat" );
    CPPUNIT_ASSERT_MESSAGE( e.getMessage().str(), false );
  }
  file.close();
  unlink( "/tmp/test_changelog.dat" );
}

//------------------------------------------------------------------------------
// Start the show
//------------------------------------------------------------------------------
int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  runner.addTest( ChangeLogTest::suite() );
  runner.setOutputter( new CppUnit::CompilerOutputter( &runner.result(),
                                                       std::cerr ) );
  return !runner.run();
}
