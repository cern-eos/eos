//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   HierarchicalView test
//------------------------------------------------------------------------------

#include <cppunit/CompilerOutputter.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>

#include <stdint.h>
#include <unistd.h>

#include "Namespace/views/HierarchicalView.hh"
#include "Namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "Namespace/persistency/ChangeLogFileMDSvc.hh"

//------------------------------------------------------------------------------
// Declaration
//------------------------------------------------------------------------------
class HierarchicalViewTest: public CppUnit::TestCase
{
  public:
    CPPUNIT_TEST_SUITE( HierarchicalViewTest );
      CPPUNIT_TEST( reloadTest );
    CPPUNIT_TEST_SUITE_END();

    void reloadTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION( HierarchicalViewTest );

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
    std::string fileNameFileMD = tempnam( "/tmp", "eosns" );
    std::string fileNameContMD = tempnam( "/tmp", "eosns" );
    contSettings["changelog_path"] = fileNameContMD;
    fileSettings["changelog_path"] = fileNameFileMD;

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

    unlink( fileNameFileMD.c_str() );
    unlink( fileNameContMD.c_str() );

    delete view;
    delete contSvc;
    delete fileSvc;

  }
  catch( eos::MDException &e )
  {
    CPPUNIT_ASSERT_MESSAGE( e.getMessage().str(), false );
  }
}
