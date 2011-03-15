//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   FileSystemView test
//------------------------------------------------------------------------------

#include <cppunit/CompilerOutputter.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>

#include <stdint.h>
#include <unistd.h>
#include <sstream>
#include <cstdlib>
#include <ctime>

#include "Namespace/views/HierarchicalView.hh"
#include "Namespace/accounting/FileSystemView.hh"
#include "Namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "Namespace/persistency/ChangeLogFileMDSvc.hh"

//------------------------------------------------------------------------------
// Declaration
//------------------------------------------------------------------------------
class FileSystemViewTest: public CppUnit::TestCase
{
  public:
    CPPUNIT_TEST_SUITE( FileSystemViewTest );
      CPPUNIT_TEST( fileSystemViewTest );
    CPPUNIT_TEST_SUITE_END();

    void fileSystemViewTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION( FileSystemViewTest );

//------------------------------------------------------------------------------
// Randomize a location
//------------------------------------------------------------------------------
eos::FileMD::location_t getRandomLocation()
{
  return 1+random()%50;
}

//------------------------------------------------------------------------------
// Count replicas
//------------------------------------------------------------------------------
size_t countReplicas( eos::FileSystemView *fs )
{
  size_t replicas = 0;
  for( size_t i = 0; i < fs->getNumFileSystems(); ++i )
    replicas += fs->getFileList( i ).size();
  return replicas;
}

//------------------------------------------------------------------------------
// Count unlinked
//------------------------------------------------------------------------------
size_t countUnlinked( eos::FileSystemView *fs )
{
  size_t unlinked = 0;
  for( size_t i = 0; i < fs->getNumFileSystems(); ++i )
    unlinked += fs->getUnlinkedFileList( i ).size();
  return unlinked;
}

//------------------------------------------------------------------------------
// Concrete implementation tests
//------------------------------------------------------------------------------
void FileSystemViewTest::fileSystemViewTest()
{
  srandom(time(0));
  try
  {
    eos::IContainerMDSvc *contSvc = new eos::ChangeLogContainerMDSvc;
    eos::IFileMDSvc      *fileSvc = new eos::ChangeLogFileMDSvc;
    eos::IView           *view    = new eos::HierarchicalView;
    eos::FileSystemView  *fsView  = new eos::FileSystemView;

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
    fsView->initialize();
    fileSvc->addChangeListener( fsView );

    view->createContainer( "/test/embed/embed1", true );
    eos::ContainerMD *c = view->createContainer( "/test/embed/embed2", true );
    view->createContainer( "/test/embed/embed3", true );

    for( int i = 0; i < 1000; ++i )
    {
      std::ostringstream o;
      o << "file" << i;
      eos::FileMD *files[4];
      files[0] = view->createFile( std::string("/test/embed/") + o.str() );
      files[1] = view->createFile( std::string("/test/embed/embed1/") + o.str() );
      files[2] = view->createFile( std::string("/test/embed/embed2/") + o.str() );
      files[3] = view->createFile( std::string("/test/embed/embed3/") + o.str() );

      for( int j = 0; j < 4; ++j )
      {
        while( files[j]->getNumLocation() != 5 )
          files[j]->addLocation( getRandomLocation() );
        view->updateFileStore( files[j] );
      }
    }

    //--------------------------------------------------------------------------
    // Sum up all the locations
    //--------------------------------------------------------------------------
    size_t numReplicas = countReplicas( fsView );
    CPPUNIT_ASSERT( numReplicas == 20000 );

    size_t numUnlinked = countUnlinked( fsView );
    CPPUNIT_ASSERT( numUnlinked == 0 );

    for( int i = 100; i < 500; ++i )
    {
      std::ostringstream o;
      o << "file" << i;
      // unlink some replicas
      eos::FileMD *f = c->findFile( o.str() );
      f->unlinkLocation( f->getLocation( 0 ) );
      f->unlinkLocation( f->getLocation( 0 ) );
      view->updateFileStore( f );
    }

    numReplicas = countReplicas( fsView );
    CPPUNIT_ASSERT( numReplicas == 19200 );
    numUnlinked = countUnlinked( fsView );
    CPPUNIT_ASSERT( numUnlinked == 800 );

    for( int i = 500; i < 900; ++i )
    {
      std::ostringstream o;
      o << "file" << i;
      // unlink some replicas
      eos::FileMD *f = c->findFile( o.str() );
      f->unlinkAllLocations();
      c->removeFile( o.str() );
      f->setContainerId( 0 );
      view->updateFileStore( f );
    }

    numReplicas = countReplicas( fsView );
    CPPUNIT_ASSERT( numReplicas == 17200 );

    numUnlinked = countUnlinked( fsView );
    CPPUNIT_ASSERT( numUnlinked == 2800 );

    view->finalize();
    fsView->finalize();
    view->initialize();
    fsView->initialize();

    numReplicas = countReplicas( fsView );
    CPPUNIT_ASSERT( numReplicas == 17200 );

    numUnlinked = countUnlinked( fsView );
    CPPUNIT_ASSERT( numUnlinked == 2800 );

    view->finalize();
    fsView->finalize();

    unlink( fileNameFileMD.c_str() );
    unlink( fileNameContMD.c_str() );

    delete contSvc;
    delete fileSvc;
    delete fsView;
    delete view;
  }
  catch( eos::MDException &e )
  {
    CPPUNIT_ASSERT_MESSAGE( e.getMessage().str(), false );
  }
}
