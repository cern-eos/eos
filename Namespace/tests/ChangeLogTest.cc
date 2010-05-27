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

#define protected public
#include "FileMD.hh"
#undef protected
#include "persistency/ChangeLogConstants.hh"
#include "persistency/ChangeLogFile.hh"

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
  return suiteOfTests;
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
    eos::FileMD fileMetadata( 0 );
    eos::Buffer buffer;

    std::vector<uint64_t> offsets;
    for( int i = 0; i < 100; ++i )
    {
      buffer.clear();
      std::ostringstream o;
      o << "filename_" << i;
      fileMetadata.pId    = i;
      fileMetadata.pCTime = i*12345;
      fileMetadata.setSize( i*987 );
      fileMetadata.setContainerId( i*765 );
      fileMetadata.setChecksum( i*432 );
      fileMetadata.setName( o.str() );
      fileMetadata.allocateLocations( 5 );
      for( int j = 0; j < 5; ++j )
        fileMetadata.setLocation( j, i*j*123 );

      fileMetadata.serialize( buffer );
      offsets.push_back( file.storeRecord( eos::UPDATE_RECORD, buffer ) );
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
      std::ostringstream o;
      o << "filename_" << i;

      file.readRecord( readOffsets[i], buffer );
      fileMetadata.deserialize( buffer );
      CPPUNIT_ASSERT( fileMetadata.pId == i );
      CPPUNIT_ASSERT( fileMetadata.pCTime == i*12345 );
      CPPUNIT_ASSERT( fileMetadata.getSize() == i*987 );
      CPPUNIT_ASSERT( fileMetadata.getContainerId() == i*765 );
      CPPUNIT_ASSERT( fileMetadata.getChecksum() == i*432 );

      CPPUNIT_ASSERT( o.str() == fileMetadata.getName() );

      for( int j = 0; j < 5; ++j )
        CPPUNIT_ASSERT( fileMetadata.getLocation(j) == i*j*123 );
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
