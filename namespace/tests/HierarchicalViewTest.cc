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
// desc:   HierarchicalView test
//------------------------------------------------------------------------------

#include <cppunit/extensions/HelperMacros.h>

#include <stdint.h>
#include <unistd.h>
#include <sstream>

#include "namespace/views/HierarchicalView.hh"
#include "namespace/accounting/QuotaStats.hh"
#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogFileMDSvc.hh"
#include "namespace/tests/TestHelpers.hh"

//------------------------------------------------------------------------------
// Declaration
//------------------------------------------------------------------------------
class HierarchicalViewTest: public CppUnit::TestCase
{
public:
  CPPUNIT_TEST_SUITE( HierarchicalViewTest );
  CPPUNIT_TEST( reloadTest );
  CPPUNIT_TEST( quotaTest );
  CPPUNIT_TEST( lostContainerTest );
  CPPUNIT_TEST_SUITE_END();

  void reloadTest();
  void quotaTest();
  void lostContainerTest();
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
      std::string fileNameFileMD = getTempName( "/tmp", "eosns" );
      std::string fileNameContMD = getTempName( "/tmp", "eosns" );
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
static void createFiles( const std::string                          &path,
                         eos::IView                                 *view,
                         std::map<uid_t, eos::QuotaNode::UsageInfo> &users,
                         std::map<gid_t, eos::QuotaNode::UsageInfo> &groups )
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
      uint64_t size = mapSize( file );
      eos::QuotaNode::UsageInfo &user  = users[file->getCUid()];
      eos::QuotaNode::UsageInfo &group = groups[file->getCGid()];
      user.space += file->getSize();
      user.physicalSpace += size;
      user.files++;
      group.space += file->getSize();
      group.physicalSpace += size;
      group.files++;
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
  std::string fileNameFileMD = getTempName( "/tmp", "eosns" );
  std::string fileNameContMD = getTempName( "/tmp", "eosns" );
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
  // Test quota node melding
  //----------------------------------------------------------------------------
  std::map<uid_t, eos::QuotaNode::UsageInfo> users;
  std::map<gid_t, eos::QuotaNode::UsageInfo> groups;
  std::map<uid_t, eos::QuotaNode::UsageInfo>::iterator userIt;
  std::map<gid_t, eos::QuotaNode::UsageInfo>::iterator groupIt;
  eos::QuotaNode *meldNode1 = new eos::QuotaNode(0);
  eos::QuotaNode *meldNode2 = new eos::QuotaNode(0);
  for( int i = 0; i < 10000; ++i )
    {
      uid_t uid = random();
      gid_t gid = random();
      eos::QuotaNode::UsageInfo &user  = users[uid];
      eos::QuotaNode::UsageInfo &group = groups[gid];

      uint64_t userSpace          = random()%100000;
      uint64_t userPhysicalSpace  = random()%100000;
      uint64_t userFiles          = random()%1000;
      uint64_t groupSpace         = random()%100000;
      uint64_t groupPhysicalSpace = random()%100000;
      uint64_t groupFiles         = random()%1000;

      if( random() % 3 )
        {
          meldNode1->changeSpaceUser( uid, userSpace );
          meldNode1->changePhysicalSpaceUser( uid, userPhysicalSpace );
          meldNode1->changeNumFilesUser( uid, userFiles );
          user.space          += userSpace;
          user.physicalSpace  += userPhysicalSpace;
          user.files          += userFiles;
          meldNode1->changeSpaceGroup( gid, groupSpace );
          meldNode1->changePhysicalSpaceGroup( gid, groupPhysicalSpace );
          meldNode1->changeNumFilesGroup( gid, groupFiles );
          group.space         += groupSpace;
          group.physicalSpace += groupPhysicalSpace;
          group.files         += groupFiles;
        }

      if( random() % 3 )
        {
          meldNode2->changeSpaceUser( uid, userSpace );
          meldNode2->changePhysicalSpaceUser( uid, userPhysicalSpace );
          meldNode2->changeNumFilesUser( uid, userFiles );
          user.space          += userSpace;
          user.physicalSpace  += userPhysicalSpace;
          user.files          += userFiles;
          meldNode2->changeSpaceGroup( gid, groupSpace );
          meldNode2->changePhysicalSpaceGroup( gid, groupPhysicalSpace );
          meldNode2->changeNumFilesGroup( gid, groupFiles );
          group.space         += groupSpace;
          group.physicalSpace += groupPhysicalSpace;
          group.files         += groupFiles;
        }
    }

  meldNode1->meld( meldNode2 );

  for( userIt = users.begin(); userIt != users.end(); ++userIt )
    {
      CPPUNIT_ASSERT( userIt->second.space == meldNode1->getUsedSpaceByUser( userIt->first ) );
      CPPUNIT_ASSERT( userIt->second.physicalSpace == meldNode1->getPhysicalSpaceByUser( userIt->first ) );
      CPPUNIT_ASSERT( userIt->second.files == meldNode1->getNumFilesByUser( userIt->first ) );
    }

  for( groupIt = groups.begin(); groupIt != groups.end(); ++groupIt )
    {
      CPPUNIT_ASSERT( groupIt->second.space == meldNode1->getUsedSpaceByGroup( groupIt->first ) );
      CPPUNIT_ASSERT( groupIt->second.physicalSpace == meldNode1->getPhysicalSpaceByGroup( groupIt->first ) );
      CPPUNIT_ASSERT( groupIt->second.files == meldNode1->getNumFilesByGroup( groupIt->first ) );
    }

  delete meldNode1;
  delete meldNode2;

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
  std::map<uid_t, eos::QuotaNode::UsageInfo> users1;
  std::map<gid_t, eos::QuotaNode::UsageInfo> groups1;
  std::string path1 = "/test/embed/embed1/";
  createFiles( path1, view, users1, groups1 );

  std::map<uid_t, eos::QuotaNode::UsageInfo> users2;
  std::map<gid_t, eos::QuotaNode::UsageInfo> groups2;
  std::string path2 = "/test/embed/embed2/";
  createFiles( path2, view, users2, groups2 );

  std::map<uid_t, eos::QuotaNode::UsageInfo> users3;
  std::map<gid_t, eos::QuotaNode::UsageInfo> groups3;
  std::string path3 = "/test/embed/embed3/";
  createFiles( path3, view, users3, groups3 );

  //----------------------------------------------------------------------------
  // Verify correctness
  //----------------------------------------------------------------------------
  eos::QuotaNode *node1 = view->getQuotaNode( view->getContainer( path1 ) );
  eos::QuotaNode *node2 = view->getQuotaNode( view->getContainer( path2 ) );

  for( int i = 1; i<= 10; ++i )
    {
      CPPUNIT_ASSERT( node1->getPhysicalSpaceByUser( i ) == users1[i].physicalSpace );
      CPPUNIT_ASSERT( node2->getPhysicalSpaceByUser( i ) == users2[i].physicalSpace );
      CPPUNIT_ASSERT( node1->getUsedSpaceByUser( i ) == users1[i].space );
      CPPUNIT_ASSERT( node2->getUsedSpaceByUser( i ) == users2[i].space );
      CPPUNIT_ASSERT( node1->getNumFilesByUser( i )  == users1[i].files );
      CPPUNIT_ASSERT( node2->getNumFilesByUser( i )  == users2[i].files );
    }

  for( int i = 1; i<= 3; ++i )
    {
      CPPUNIT_ASSERT( node1->getPhysicalSpaceByGroup( i ) == groups1[i].physicalSpace );
      CPPUNIT_ASSERT( node2->getPhysicalSpaceByGroup( i ) == groups2[i].physicalSpace );
      CPPUNIT_ASSERT( node1->getUsedSpaceByGroup( i ) == groups1[i].space );
      CPPUNIT_ASSERT( node2->getUsedSpaceByGroup( i ) == groups2[i].space );
      CPPUNIT_ASSERT( node1->getNumFilesByGroup( i )  == groups1[i].files );
      CPPUNIT_ASSERT( node2->getNumFilesByGroup( i )  == groups2[i].files );
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
      CPPUNIT_ASSERT( node1->getPhysicalSpaceByUser( i ) == users1[i].physicalSpace );
      CPPUNIT_ASSERT( node2->getPhysicalSpaceByUser( i ) == users2[i].physicalSpace );
      CPPUNIT_ASSERT( node1->getUsedSpaceByUser( i ) == users1[i].space );
      CPPUNIT_ASSERT( node2->getUsedSpaceByUser( i ) == users2[i].space );
      CPPUNIT_ASSERT( node1->getNumFilesByUser( i )  == users1[i].files );
      CPPUNIT_ASSERT( node2->getNumFilesByUser( i )  == users2[i].files );
    }

  for( int i = 1; i<= 3; ++i )
    {
      CPPUNIT_ASSERT( node1->getPhysicalSpaceByGroup( i ) == groups1[i].physicalSpace );
      CPPUNIT_ASSERT( node2->getPhysicalSpaceByGroup( i ) == groups2[i].physicalSpace );
      CPPUNIT_ASSERT( node1->getUsedSpaceByGroup( i ) == groups1[i].space );
      CPPUNIT_ASSERT( node2->getUsedSpaceByGroup( i ) == groups2[i].space );
      CPPUNIT_ASSERT( node1->getNumFilesByGroup( i )  == groups1[i].files );
      CPPUNIT_ASSERT( node2->getNumFilesByGroup( i )  == groups2[i].files );
    }


  //----------------------------------------------------------------------------
  // Remove the quota nodes on /test/embed/embed1 and /dest/embed/embed2
  // and check if the on /test has been updated
  //----------------------------------------------------------------------------
  eos::QuotaNode *parentNode = 0;
  CPPUNIT_ASSERT_NO_THROW(
                          parentNode = view->getQuotaNode( view->getContainer( "/test" ) ) );

  CPPUNIT_ASSERT_NO_THROW( view->removeQuotaNode( view->getContainer( path1 ) ) );

  for( int i = 1; i <= 10; ++i )
    {
      CPPUNIT_ASSERT( parentNode->getPhysicalSpaceByUser( i )  == users1[i].physicalSpace + users2[i].physicalSpace );
      CPPUNIT_ASSERT( parentNode->getUsedSpaceByUser( i )      == users1[i].space         + users2[i].space );
      CPPUNIT_ASSERT( parentNode->getNumFilesByUser( i )       == users1[i].files         + users2[i].files );
    }

  for( int i = 1; i <= 3; ++i )
    {
      CPPUNIT_ASSERT( parentNode->getPhysicalSpaceByGroup( i ) == groups1[i].physicalSpace + groups2[i].physicalSpace );
      CPPUNIT_ASSERT( parentNode->getUsedSpaceByGroup( i )     == groups1[i].space + groups2[i].space );
      CPPUNIT_ASSERT( parentNode->getNumFilesByGroup( i )      == groups1[i].files + groups2[i].files );
    }

  CPPUNIT_ASSERT_NO_THROW( view->removeQuotaNode( view->getContainer( path3 ) ) );
  CPPUNIT_ASSERT_THROW( view->removeQuotaNode( view->getContainer( path3 ) ), eos::MDException );

  for( int i = 1; i <= 10; ++i )
    {
      CPPUNIT_ASSERT( parentNode->getPhysicalSpaceByUser( i )  ==
                      users1[i].physicalSpace + users2[i].physicalSpace + users3[i].physicalSpace );
      CPPUNIT_ASSERT( parentNode->getUsedSpaceByUser( i )      ==
                      users1[i].space + users2[i].space + users3[i].space );
      CPPUNIT_ASSERT( parentNode->getNumFilesByUser( i )       ==
                      users1[i].files + users2[i].files + users3[i].files );
    }

  for( int i = 1; i <= 3; ++i )
    {
      CPPUNIT_ASSERT( parentNode->getPhysicalSpaceByGroup( i )  ==
                      groups1[i].physicalSpace + groups2[i].physicalSpace + groups3[i].physicalSpace );
      CPPUNIT_ASSERT( parentNode->getUsedSpaceByGroup( i )      ==
                      groups1[i].space + groups2[i].space + groups3[i].space );
      CPPUNIT_ASSERT( parentNode->getNumFilesByGroup( i )       ==
                      groups1[i].files + groups2[i].files + groups3[i].files );
    }

  CPPUNIT_ASSERT_NO_THROW( view->finalize() );

  unlink( fileNameFileMD.c_str() );
  unlink( fileNameContMD.c_str() );

  delete view;
  delete contSvc;
  delete fileSvc;
}

//------------------------------------------------------------------------------
// Lost container test
//------------------------------------------------------------------------------
void HierarchicalViewTest::lostContainerTest()
{
  //----------------------------------------------------------------------------
  // Initializer
  //----------------------------------------------------------------------------
  eos::IContainerMDSvc *contSvc = new eos::ChangeLogContainerMDSvc;
  eos::IFileMDSvc      *fileSvc = new eos::ChangeLogFileMDSvc;
  eos::IView           *view    = new eos::HierarchicalView;

  std::map<std::string, std::string> fileSettings;
  std::map<std::string, std::string> contSettings;
  std::map<std::string, std::string> settings;
  std::string fileNameFileMD = getTempName( "/tmp", "eosns" );
  std::string fileNameContMD = getTempName( "/tmp", "eosns" );
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

  //----------------------------------------------------------------------------
  // Create some files
  //----------------------------------------------------------------------------
  for( int i = 0; i < 1000; ++i )
    {
      std::ostringstream s1; s1 << "/test/embed/embed1/file" << i;
      std::ostringstream s2; s2 << "/test/embed/embed2/file" << i;
      std::ostringstream s3; s3 << "/test/embed/embed3/file" << i;
      view->createFile( s1.str() );
      view->createFile( s2.str() );
      view->createFile( s3.str() );
    }

  //----------------------------------------------------------------------------
  // Remove one of the container keeping the files register with it to
  // simulate directory metadata loss
  //----------------------------------------------------------------------------
  eos::ContainerMD::id_t removedId = cont1->getId();
  view->getContainerMDSvc()->removeContainer( cont1 );

  //----------------------------------------------------------------------------
  // Reboot
  //----------------------------------------------------------------------------
  view->finalize();
  view->initialize();

  //----------------------------------------------------------------------------
  // Check the containers
  //----------------------------------------------------------------------------
  std::ostringstream s; s << "/lost+found/" << removedId;
  CPPUNIT_ASSERT_NO_THROW( cont1 = view->getContainer( s.str() ) );
  CPPUNIT_ASSERT_NO_THROW( cont2 = view->getContainer( "/test/embed/embed2" ) );
  CPPUNIT_ASSERT_NO_THROW( cont3 = view->getContainer( "/test/embed/embed3" ) );
  CPPUNIT_ASSERT( cont1->getNumFiles() == 1000 );
  CPPUNIT_ASSERT( cont2->getNumFiles() == 1000 );
  CPPUNIT_ASSERT( cont3->getNumFiles() == 1000 );

  //----------------------------------------------------------------------------
  // Cleanup
  //----------------------------------------------------------------------------
  view->finalize();
  unlink( fileNameFileMD.c_str() );
  unlink( fileNameContMD.c_str() );
  delete view;
  delete contSvc;
  delete fileSvc;
}
