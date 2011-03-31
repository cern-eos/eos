//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   HierarchicalView test
//------------------------------------------------------------------------------

#include <cppunit/CompilerOutputter.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>

#include <stdint.h>
#include <unistd.h>
#include <sstream>

#include "namespace/views/HierarchicalView.hh"
#include "namespace/accounting/QuotaStats.hh"
#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogFileMDSvc.hh"

//------------------------------------------------------------------------------
// Declaration
//------------------------------------------------------------------------------
class HierarchicalViewTest: public CppUnit::TestCase
{
  public:
    CPPUNIT_TEST_SUITE( HierarchicalViewTest );
      CPPUNIT_TEST( reloadTest );
      CPPUNIT_TEST( quotaTest );
    CPPUNIT_TEST_SUITE_END();

    void reloadTest();
    void quotaTest();
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

    //--------------------------------------------------------------------------
    // Test the "reverse" lookup
    //--------------------------------------------------------------------------
    eos::FileMD      *file = view->getFile( "/test/embed/embed1/file3" );
    eos::ContainerMD *container = view->getContainer( "/test/embed/embed1" );

    CPPUNIT_ASSERT( view->getUri( container ) == "/test/embed/embed1/" );
    CPPUNIT_ASSERT( view->getUri( file ) == "/test/embed/embed1/file3" );
    CPPUNIT_ASSERT_THROW( view->getUri( (eos::FileMD*)0 ), eos::MDException );

    eos::FileMD *toBeDeleted = view->getFile( "/test/embed/embed1/file2" );
    toBeDeleted->addLocation( 12 );

    //--------------------------------------------------------------------------
    // This should not succeed since the file should have a replica
    //--------------------------------------------------------------------------
    CPPUNIT_ASSERT_THROW( view->removeFile( toBeDeleted ), eos::MDException );

    //--------------------------------------------------------------------------
    // We unlink the file - at this point the file should not be attached to the
    // hierarchy bu should still be accessible by id and thus the md pointer
    // should stay valid
    //--------------------------------------------------------------------------
    view->unlinkFile( "/test/embed/embed1/file2" );
    CPPUNIT_ASSERT_THROW( view->getFile( "/test/embed/embed1/file2" ),
                          eos::MDException );
    CPPUNIT_ASSERT( cont1->findFile( "file2") == 0 );

    //--------------------------------------------------------------------------
    // We remove the replicas and the file
    //--------------------------------------------------------------------------
    eos::FileMD::id_t id = toBeDeleted->getId();
    toBeDeleted->removeUnlinkedLocations();
    CPPUNIT_ASSERT_NO_THROW( view->removeFile( toBeDeleted ) );
    CPPUNIT_ASSERT_THROW( view->getFileMDSvc()->getFileMD( id ),
                          eos::MDException );

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

//------------------------------------------------------------------------------
// File size mapping function
//------------------------------------------------------------------------------
static uint64_t mapSize( const eos::FileMD *file )
{
  eos::FileMD::layoutId_t lid = file->getLayoutId();
  if( lid > 3 )
  {
    eos::MDException e( ENOENT );
    e.getMessage() << "Location does not exist" << std::endl;
    throw( e );
  }
  return lid*file->getSize();
}

//------------------------------------------------------------------------------
// Create files at given path
//------------------------------------------------------------------------------
static void createFiles( const std::string         &path,
                         eos::IView                *view,
                         std::map<uid_t, uint64_t> &users,
                         std::map<gid_t, uint64_t> &groups )
{
  eos::QuotaNode *node = view->getQuotaNode( view->getContainer( path ) );
  for( int i = 0; i < 1000; ++i )
  {
    std::ostringstream p;
    p << path << "file" << i;
    eos::FileMD *file = view->createFile( p.str() );
    file->setCUid( random()%10+1 );
    file->setCGid( random()%3+1 );
    file->setSize( random()%1000000+1 );
    file->setLayoutId( random()%3+1 );
    view->updateFileStore( file );
    node->addFile( file );
    users[file->getCUid()]  += mapSize( file );
    groups[file->getCGid()] += mapSize( file );
  }
}

//------------------------------------------------------------------------------
// Quota test
//------------------------------------------------------------------------------
void HierarchicalViewTest::quotaTest()
{
  srandom( time( 0 ) );

  //----------------------------------------------------------------------------
  // Initialize the system
  //----------------------------------------------------------------------------
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

  view->getQuotaStats()->registerSizeMapper( mapSize );

  CPPUNIT_ASSERT_NO_THROW( view->initialize() );

  //----------------------------------------------------------------------------
  // Create some structures, insert quota nodes and test their correctness
  //----------------------------------------------------------------------------
  eos::ContainerMD *cont1 = view->createContainer( "/test/embed/embed1", true );
  eos::ContainerMD *cont2 = view->createContainer( "/test/embed/embed2", true );
  eos::ContainerMD *cont3 = view->createContainer( "/test/embed/embed3", true );
  eos::ContainerMD *cont4 = view->getContainer( "/test/embed" );
  eos::ContainerMD *cont5 = view->getContainer( "/test" );

  eos::QuotaNode *qnCreated1 = view->registerQuotaNode( cont1 );
  eos::QuotaNode *qnCreated2 = view->registerQuotaNode( cont3 );
  eos::QuotaNode *qnCreated3 = view->registerQuotaNode( cont5 );

  CPPUNIT_ASSERT_THROW( view->registerQuotaNode( cont1 ), eos::MDException );

  CPPUNIT_ASSERT( qnCreated1 );
  CPPUNIT_ASSERT( qnCreated2 );
  CPPUNIT_ASSERT( qnCreated3 );

  eos::QuotaNode *qn1 = view->getQuotaNode( cont1 );
  eos::QuotaNode *qn2 = view->getQuotaNode( cont2 );
  eos::QuotaNode *qn3 = view->getQuotaNode( cont3 );
  eos::QuotaNode *qn4 = view->getQuotaNode( cont4 );
  eos::QuotaNode *qn5 = view->getQuotaNode( cont5 );

  CPPUNIT_ASSERT( qn1 );
  CPPUNIT_ASSERT( qn2 );
  CPPUNIT_ASSERT( qn3 );
  CPPUNIT_ASSERT( qn4 );
  CPPUNIT_ASSERT( qn5 );

  CPPUNIT_ASSERT( qn2 == qn5 );
  CPPUNIT_ASSERT( qn4 == qn5 );
  CPPUNIT_ASSERT( qn1 != qn5 );
  CPPUNIT_ASSERT( qn3 != qn5 );
  CPPUNIT_ASSERT( qn3 != qn2 );

  //----------------------------------------------------------------------------
  // Create some files
  //----------------------------------------------------------------------------
  std::map<uid_t, uint64_t> users1;
  std::map<gid_t, uint64_t> groups1;
  std::string path1 = "/test/embed/embed1/";
  createFiles( path1, view, users1, groups1 );

  std::map<uid_t, uint64_t> users2;
  std::map<gid_t, uint64_t> groups2;
  std::string path2 = "/test/embed/embed2/";
  createFiles( path2, view, users2, groups2 );

  //----------------------------------------------------------------------------
  // Verify correctness
  //----------------------------------------------------------------------------
  eos::QuotaNode *node1 = view->getQuotaNode( view->getContainer( path1 ) );
  eos::QuotaNode *node2 = view->getQuotaNode( view->getContainer( path2 ) );

  for( int i = 1; i<= 10; ++i )
  {
    CPPUNIT_ASSERT( node1->getOccupancyByUser( i ) == users1[i] );
    CPPUNIT_ASSERT( node2->getOccupancyByUser( i ) == users2[i] );
  }

  for( int i = 1; i<= 3; ++i )
  {
    CPPUNIT_ASSERT( node1->getOccupancyByGroup( i ) == groups1[i] );
    CPPUNIT_ASSERT( node2->getOccupancyByGroup( i ) == groups2[i] );
  }

  //----------------------------------------------------------------------------
  // Restart and check if the quota stats are reloaded correctly
  //----------------------------------------------------------------------------
  CPPUNIT_ASSERT_NO_THROW( view->finalize() );
  delete view->getQuotaStats();
  view->setQuotaStats( new eos::QuotaStats );
  view->getQuotaStats()->registerSizeMapper( mapSize );

  CPPUNIT_ASSERT_NO_THROW( view->initialize() );

  node1 = view->getQuotaNode( view->getContainer( path1 ) );
  node2 = view->getQuotaNode( view->getContainer( path2 ) );

  CPPUNIT_ASSERT( node1 );
  CPPUNIT_ASSERT( node2 );

  for( int i = 1; i<= 10; ++i )
  {
    CPPUNIT_ASSERT( node1->getOccupancyByUser( i ) == users1[i] );
    CPPUNIT_ASSERT( node2->getOccupancyByUser( i ) == users2[i] );
  }

  for( int i = 1; i<= 3; ++i )
  {
    CPPUNIT_ASSERT( node1->getOccupancyByGroup( i ) == groups1[i] );
    CPPUNIT_ASSERT( node2->getOccupancyByGroup( i ) == groups2[i] );
  }

  CPPUNIT_ASSERT_NO_THROW( view->finalize() );

  unlink( fileNameFileMD.c_str() );
  unlink( fileNameContMD.c_str() );

  delete view;
  delete contSvc;
  delete fileSvc;
}
