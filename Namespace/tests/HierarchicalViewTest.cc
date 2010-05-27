//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   HierarchicalView test
//------------------------------------------------------------------------------

#include <cppunit/CompilerOutputter.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include "HierarchicalViewTest.hh"

#include <stdint.h>
#include <unistd.h>

#include "views/HierarchicalView.hh"
#include "persistency/ChangeLogContainerMDSvc.hh"
#include "persistency/ChangeLogFileMDSvc.hh"

//------------------------------------------------------------------------------
// Generic file md test declaration
//------------------------------------------------------------------------------
CppUnit::Test *HierarchicalViewTest::suite()
{
  CppUnit::TestSuite *suiteOfTests
              = new CppUnit::TestSuite( "HierarchicalViewTest" );

  suiteOfTests->addTest( new CppUnit::TestCaller<HierarchicalViewTest>( 
                               "reloadTest",
                               &HierarchicalViewTest::reloadTest ) );
  return suiteOfTests;
}

//------------------------------------------------------------------------------
// Concrete implementation tests
//------------------------------------------------------------------------------
void HierarchicalViewTest::reloadTest()
{
  try
  {
    eos::IContainerMDSvc *contSvc = new eos::ChangeLogContainerMDSvc;
    eos::IFileMDSvc      *fileSvc = new eos::ChangeLogFileMDSvc;
    eos::IView           *view    = new eos::HierarchicalView;

    std::map<std::string, std::string> fileSettings;
    std::map<std::string, std::string> contSettings;
    std::map<std::string, std::string> settings;
    contSettings["changelog_path"] = "/tmp/container_log.log";
    fileSettings["changelog_path"] = "/tmp/file_log.log";

    fileSvc->configure( contSettings );
    contSvc->configure( fileSettings );

    view->setContainerMDSvc( contSvc );
    view->setFileMDSvc( fileSvc );

    view->configure( settings );
    view->initialize();

    eos::ContainerMD *cont1 = view->createContainer( "/test/embed/embed1", true );
    eos::ContainerMD *cont2 = view->createContainer( "/test/embed/embed2", true );
    eos::ContainerMD *cont3 = view->createContainer( "/test/embed/embed3", true );

    eos::ContainerMD *root  = view->getContainer( "/" );
    eos::ContainerMD *test  = view->getContainer( "/test" );
    eos::ContainerMD *embed = view->getContainer( "/test/embed" );

    CPPUNIT_ASSERT( root != 0 );
    CPPUNIT_ASSERT( root->getId() == root->getParentId() );
    CPPUNIT_ASSERT( test != 0 );
    CPPUNIT_ASSERT( test->findContainer( "embed" ) != 0 );
    CPPUNIT_ASSERT( embed != 0 );
    CPPUNIT_ASSERT( embed->findContainer( "embed1" ) != 0 );
    CPPUNIT_ASSERT( embed->findContainer( "embed2" ) != 0 );
    CPPUNIT_ASSERT( embed->findContainer( "embed3" ) != 0 );
    CPPUNIT_ASSERT( cont1->getName() == embed->findContainer( "embed1" )->getName() );
    CPPUNIT_ASSERT( cont2->getName() == embed->findContainer( "embed2" )->getName() );
    CPPUNIT_ASSERT( cont3->getName() == embed->findContainer( "embed3" )->getName() );

    view->removeContainer( "/test/embed/embed2" );
    CPPUNIT_ASSERT( embed->findContainer( "embed2" ) == 0 );

    view->createFile( "/test/embed/file1" );
    view->createFile( "/test/embed/file2" );
    view->createFile( "/test/embed/embed1/file1" );
    view->createFile( "/test/embed/embed1/file2" );
    view->createFile( "/test/embed/embed1/file3" );

    CPPUNIT_ASSERT( view->getFile( "/test/embed/file1" ) );
    CPPUNIT_ASSERT( view->getFile( "/test/embed/file2" ) );
    CPPUNIT_ASSERT( view->getFile( "/test/embed/embed1/file1" ) );
    CPPUNIT_ASSERT( view->getFile( "/test/embed/embed1/file2" ) );
    CPPUNIT_ASSERT( view->getFile( "/test/embed/embed1/file3" ) );

    view->removeFile( "/test/embed/embed1/file2" );

    CPPUNIT_ASSERT_THROW( view->getFile( "/test/embed/embed1/file2" ), eos::MDException );
    CPPUNIT_ASSERT( cont1->findFile( "file2") == 0 );

    view->finalize();

    view->initialize();
    CPPUNIT_ASSERT( view->getContainer( "/" ) );
    CPPUNIT_ASSERT( view->getContainer( "/test" ) );
    CPPUNIT_ASSERT( view->getContainer( "/test/embed" ) );
    CPPUNIT_ASSERT( view->getContainer( "/test/embed/embed1" ) );
    CPPUNIT_ASSERT( view->getFile( "/test/embed/file1" ) );
    CPPUNIT_ASSERT( view->getFile( "/test/embed/file2" ) );
    CPPUNIT_ASSERT( view->getFile( "/test/embed/embed1/file1" ) );
    CPPUNIT_ASSERT( view->getFile( "/test/embed/embed1/file3" ) );
    view->finalize();

    unlink( "/tmp/container_log.log" );
    unlink( "/tmp/file_log.log" );

    delete view;
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
  runner.addTest( HierarchicalViewTest::suite() );
  runner.setOutputter( new CppUnit::CompilerOutputter( &runner.result(),
                                                       std::cerr ) );
  return !runner.run();
}
