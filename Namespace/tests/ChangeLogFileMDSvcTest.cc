//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog test
//------------------------------------------------------------------------------

#include <cppunit/CompilerOutputter.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include "ChangeLogFileMDSvcTest.hh"

#include <stdint.h>
#include <unistd.h>

#include "Namespace/persistency/ChangeLogFileMDSvc.hh"

//------------------------------------------------------------------------------
// Generic file md test declaration
//------------------------------------------------------------------------------
CppUnit::Test *ChangeLogFileMDSvcTest::suite()
{
  CppUnit::TestSuite *suiteOfTests
              = new CppUnit::TestSuite( "ChangeLogFileMDSvcTest" );

  suiteOfTests->addTest( new CppUnit::TestCaller<ChangeLogFileMDSvcTest>( 
                               "reloadTest", 
                               &ChangeLogFileMDSvcTest::reloadTest ) );
  return suiteOfTests;
}

//------------------------------------------------------------------------------
// Concrete implementation tests
//------------------------------------------------------------------------------
void ChangeLogFileMDSvcTest::reloadTest()
{
  try
  {
    eos::IFileMDSvc *fileSvc = new eos::ChangeLogFileMDSvc;
    std::map<std::string, std::string> config;
    config["changelog_path"] = "/tmp/file_log.log";
    fileSvc->configure( config );
    fileSvc->initialize();

    eos::FileMD *file1 = fileSvc->createFile();
    eos::FileMD *file2 = fileSvc->createFile();
    eos::FileMD *file3 = fileSvc->createFile(); 
    eos::FileMD *file4 = fileSvc->createFile();
    eos::FileMD *file5 = fileSvc->createFile();

    CPPUNIT_ASSERT( file1 != 0 );
    CPPUNIT_ASSERT( file2 != 0 );
    CPPUNIT_ASSERT( file3 != 0 );
    CPPUNIT_ASSERT( file4 != 0 );
    CPPUNIT_ASSERT( file5 != 0 );

    file1->setName( "file1" );
    file2->setName( "file2" );
    file3->setName( "file3" );
    file4->setName( "file4" );
    file5->setName( "file5" );

    eos::FileMD::id_t id1 = file1->getId();
    eos::FileMD::id_t id2 = file2->getId();
    eos::FileMD::id_t id3 = file3->getId();
    eos::FileMD::id_t id4 = file4->getId();
    eos::FileMD::id_t id5 = file5->getId();

    fileSvc->updateStore( file1 );
    fileSvc->updateStore( file2 );
    fileSvc->updateStore( file3 );
    fileSvc->updateStore( file4 );
    fileSvc->updateStore( file5 );

    fileSvc->removeFile( file2 );
    fileSvc->removeFile( file4 );

    fileSvc->finalize();

    fileSvc->initialize();
    eos::FileMD *fileRec1 = fileSvc->getFileMD( id1 );
    eos::FileMD *fileRec3 = fileSvc->getFileMD( id3 );
    eos::FileMD *fileRec5 = fileSvc->getFileMD( id5 );

    CPPUNIT_ASSERT( fileRec1 != 0 );
    CPPUNIT_ASSERT( fileRec3 != 0 );
    CPPUNIT_ASSERT( fileRec5 != 0 );
    CPPUNIT_ASSERT( fileRec1->getName() == "file1" );
    CPPUNIT_ASSERT( fileRec3->getName() == "file3" );
    CPPUNIT_ASSERT( fileRec5->getName() == "file5" );

    try
    {
      fileSvc->getFileMD( id2 );
      CPPUNIT_ASSERT_MESSAGE( "An exception should have been thrown", false );
    }
    catch( eos::MDException &e )
    {
    }

    try
    {
      fileSvc->getFileMD( id4 );
      CPPUNIT_ASSERT_MESSAGE( "An exception should have been thrown", false );
    }
    catch( eos::MDException &e )
    {
    }

    fileSvc->finalize();

    delete fileSvc;
    unlink( "/tmp/file_log.log" );
  }
  catch( eos::MDException &e )
  {
    CPPUNIT_ASSERT_MESSAGE( e.getMessage().str(), false );
  }
}

//------------------------------------------------------------------------------
// Start the show
//------------------------------------------------------------------------------
int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  runner.addTest( ChangeLogFileMDSvcTest::suite() );
  runner.setOutputter( new CppUnit::CompilerOutputter( &runner.result(),
                                                       std::cerr ) );
  return !runner.run();
}
