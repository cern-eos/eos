//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   HierarchicalView test
//------------------------------------------------------------------------------
// EOS - the CERN Disk Storage System
// Copyright (C) 2011 CERN/Switzerland
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//------------------------------------------------------------------------------

#include <cppunit/extensions/HelperMacros.h>

#include <stdint.h>
#include <unistd.h>
#include <sstream>
#include <cstdlib>
#include <ctime>

#include "namespace/views/HierarchicalView.hh"
#include "namespace/accounting/QuotaStats.hh"
#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogFileMDSvc.hh"
#include "namespace/persistency/LogManager.hh"
#include "namespace/utils/Locking.hh"
#include "namespace/tests/TestHelpers.hh"

#include <XrdSys/XrdSysPthread.hh>

//------------------------------------------------------------------------------
// Declaration
//------------------------------------------------------------------------------
class HierarchicalSlaveTest: public CppUnit::TestCase
{
  public:
    CPPUNIT_TEST_SUITE( HierarchicalSlaveTest );
    CPPUNIT_TEST( functionalTest );
    CPPUNIT_TEST_SUITE_END();

    void functionalTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION( HierarchicalSlaveTest );

//------------------------------------------------------------------------------
// Lock handler
//------------------------------------------------------------------------------
class RWLock: public eos::LockHandler
{
  public:
    //--------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------
    RWLock()
    {
    }

    //------------------------------------------------------------------------
    // Destructor
    //------------------------------------------------------------------------
    virtual ~RWLock()
    {
    }

    //------------------------------------------------------------------------
    // Take a read lock
    //------------------------------------------------------------------------
    virtual void readLock()
    {
      pLock.ReadLock();
    }

    //------------------------------------------------------------------------
    // Take a write lock
    //------------------------------------------------------------------------
    virtual void writeLock()
    {
      pLock.WriteLock();
    }

    //------------------------------------------------------------------------
    // Unlock
    //------------------------------------------------------------------------
    virtual void unLock()
    {
      pLock.UnLock();
    }
  private:
    XrdSysRWLock pLock;
};

//------------------------------------------------------------------------------
// Create the directory subtree
//------------------------------------------------------------------------------
void createSubTree( eos::IView        *view,
                    const std::string &prefix,
                    int                depth,
                    int                numDirs,
                    int                numFiles )
{
  if( !depth ) return;

  for( int i = 0; i < numDirs; ++i )
  {
    std::ostringstream o;
    o << prefix << "/dir" << i;
    view->createContainer( o.str(), true );
    createSubTree( view, o.str(), depth-1, numDirs, numFiles );
  }

  eos::ContainerMD *container = view->getContainer( prefix );
  eos::QuotaNode   *qn        = view->getQuotaNode( container );
  for( int i = 0; i < numFiles; ++i )
  {
    std::ostringstream o;
    o << prefix << "/file" << i;
    eos::FileMD *file = view->createFile( o.str() );
    if( qn ) qn->addFile( file );
  }
}

//------------------------------------------------------------------------------
// Modify subtree
//------------------------------------------------------------------------------
void modifySubTree( eos::IView *view, const std::string &root )
{
  for( int i = 0; i < 5; ++i )
  {
    std::ostringstream o;
    o << root << "/dir" << i;
    int i;
    eos::ContainerMD *container = view->getContainer( o.str() );
    eos::QuotaNode   *qn = view->getQuotaNode( container );
    eos::ContainerMD::FileMap::iterator it;
    std::vector<eos::FileMD*> toDel;
    std::vector<eos::FileMD*>::iterator itD;
    for( i = 1, it = container->filesBegin();
         it != container->filesEnd();
         ++it, ++i )
    {
      if( qn ) qn->removeFile( it->second );
      it->second->setSize( random() % 1000000 );
      if( qn ) qn->addFile( it->second );
      view->updateFileStore( it->second );
      if(  i % 4 == 0 )
        toDel.push_back( it->second );
    }
    for( itD = toDel.begin(); itD != toDel.end(); ++itD )
    {
      if( qn ) qn->removeFile( *itD );
      view->removeFile( *itD );
    }
  }
}

//------------------------------------------------------------------------------
// Calculate total size
//------------------------------------------------------------------------------
uint64_t calcSize( eos::ContainerMD *cont )
{
  uint64_t size = 0;
  eos::ContainerMD::FileMap::iterator itF;
  for( itF = cont->filesBegin(); itF != cont->filesEnd(); ++itF )
    size += itF->second->getSize();

  eos::ContainerMD::ContainerMap::iterator itC;
  for( itC = cont->containersBegin(); itC != cont->containersEnd(); ++itC )
    size += calcSize( itC->second );

  return size;
}

//------------------------------------------------------------------------------
// Calculate number of files
//------------------------------------------------------------------------------
uint64_t calcFiles( eos::ContainerMD *cont )
{
  uint64_t files = cont->getNumFiles();

  eos::ContainerMD::ContainerMap::iterator itC;
  for( itC = cont->containersBegin(); itC != cont->containersEnd(); ++itC )
    files += calcFiles( itC->second );

  return files;
}

//------------------------------------------------------------------------------
// Compare trees
//------------------------------------------------------------------------------
bool compareTrees( eos::IView       *view1, eos::IView       *view2,
                   eos::ContainerMD *tree1, eos::ContainerMD *tree2 )
{
  std::string treeMsg = view1->getUri( tree1 ) + " " + view2->getUri( tree2 );

  CPPUNIT_ASSERT_MESSAGE( treeMsg, tree1->getId() == tree2->getId() );
  CPPUNIT_ASSERT_MESSAGE( treeMsg, tree1->getName() == tree2->getName() );
  CPPUNIT_ASSERT_MESSAGE( treeMsg, tree1->getNumFiles() == tree2->getNumFiles() );
  CPPUNIT_ASSERT_MESSAGE( treeMsg, tree1->getNumContainers() == tree2->getNumContainers() );

  eos::ContainerMD::FileMap::iterator itF;
  for( itF = tree1->filesBegin(); itF != tree1->filesEnd(); ++itF )
  {
    eos::FileMD *file = tree2->findFile( itF->second->getName() );
    std::string fileMsg = treeMsg + " file: " + itF->second->getName();
    CPPUNIT_ASSERT_MESSAGE( fileMsg, file );
    CPPUNIT_ASSERT_MESSAGE( fileMsg, file->getSize() == itF->second->getSize() );
    CPPUNIT_ASSERT_MESSAGE( fileMsg, file->getId() == itF->second->getId() );
  }

  eos::ContainerMD::ContainerMap::iterator itC;
  for( itC = tree1->containersBegin(); itC != tree1->containersEnd(); ++itC )
  {
    eos::ContainerMD *container = tree2->findContainer( itC->second->getName() );
    std::string contMsg = treeMsg + " container: " + itC->second->getName();
    CPPUNIT_ASSERT_MESSAGE( contMsg, container );
    compareTrees( view1, view2, itC->second, container );
  }

  return true;
}

//------------------------------------------------------------------------------
// File size mapping function
//------------------------------------------------------------------------------
static uint64_t mapSize( const eos::FileMD *file )
{
  return file->getSize();
}

//------------------------------------------------------------------------------
// Slave test
//------------------------------------------------------------------------------
void HierarchicalSlaveTest::functionalTest()
{
  srandom(time(0));

  //----------------------------------------------------------------------------
  // Set up the master namespace
  //----------------------------------------------------------------------------
  eos::ChangeLogContainerMDSvc *contSvcMaster = new eos::ChangeLogContainerMDSvc;
  eos::ChangeLogFileMDSvc      *fileSvcMaster = new eos::ChangeLogFileMDSvc;
  eos::IView                   *viewMaster    = new eos::HierarchicalView;

  fileSvcMaster->setContainerService( contSvcMaster );

  std::map<std::string, std::string> fileSettings1;
  std::map<std::string, std::string> contSettings1;
  std::map<std::string, std::string> settings1;
  std::string fileNameFileMD = getTempName( "/tmp", "eosns" );
  std::string fileNameContMD = getTempName( "/tmp", "eosns" );
  contSettings1["changelog_path"] = fileNameContMD;
  fileSettings1["changelog_path"] = fileNameFileMD;

  fileSvcMaster->configure( fileSettings1 );
  contSvcMaster->configure( contSettings1 );

  viewMaster->setContainerMDSvc( contSvcMaster );
  viewMaster->setFileMDSvc( fileSvcMaster );

  viewMaster->configure( settings1 );

  viewMaster->getQuotaStats()->registerSizeMapper( mapSize );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->initialize() );

  createSubTree( viewMaster, "/", 4, 10, 100 );

  //----------------------------------------------------------------------------
  // Modify some stuff
  //----------------------------------------------------------------------------
  modifySubTree( viewMaster, "/dir1" );
  viewMaster->removeContainer( "/dir1/dir1/dir1", true );

  //----------------------------------------------------------------------------
  // Run compaction
  //----------------------------------------------------------------------------
  viewMaster->finalize();

  eos::LogCompactingStats stats;
  eos::LogManager::compactLog( fileNameFileMD, fileNameFileMD+"c", stats, 0 );
  eos::LogManager::compactLog( fileNameContMD, fileNameContMD+"c", stats, 0 );
  unlink( fileNameFileMD.c_str() );
  unlink( fileNameContMD.c_str() );

  contSettings1["changelog_path"] = fileNameContMD+"c";
  fileSettings1["changelog_path"] = fileNameFileMD+"c";
  fileSvcMaster->configure( fileSettings1 );
  contSvcMaster->configure( contSettings1 );

  viewMaster->getQuotaStats()->registerSizeMapper( mapSize );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->initialize() );

  viewMaster->createContainer( "/newdir1", true );
  createSubTree( viewMaster, "/newdir1", 2, 10, 100 );
  modifySubTree( viewMaster, "/newdir1" );
  viewMaster->removeContainer( "/newdir1/dir1", true );
  eos::ContainerMD *contMaster2 = 0;
  eos::ContainerMD *contMaster3 = 0;
  CPPUNIT_ASSERT_NO_THROW( contMaster2 = viewMaster->createContainer( "/newdir2", true ) );
  CPPUNIT_ASSERT_NO_THROW( contMaster3 = viewMaster->createContainer( "/newdir3", true ) );

  eos::QuotaNode *qnMaster2 = 0;
  eos::QuotaNode *qnMaster3 = 0;
  CPPUNIT_ASSERT_NO_THROW( qnMaster2 = viewMaster->registerQuotaNode( contMaster2 ) );
  CPPUNIT_ASSERT_NO_THROW( qnMaster3 = viewMaster->registerQuotaNode( contMaster3 ) );
  CPPUNIT_ASSERT( qnMaster2 );
  CPPUNIT_ASSERT( qnMaster3 );

  //----------------------------------------------------------------------------
  // Set up the slave
  //----------------------------------------------------------------------------
  eos::ChangeLogContainerMDSvc *contSvcSlave = new eos::ChangeLogContainerMDSvc;
  eos::ChangeLogFileMDSvc      *fileSvcSlave = new eos::ChangeLogFileMDSvc;
  eos::IView                   *viewSlave    = new eos::HierarchicalView;

  fileSvcSlave->setContainerService( contSvcSlave );
  RWLock lock;
  contSvcSlave->setSlaveLock( &lock );
  fileSvcSlave->setSlaveLock( &lock );

  std::map<std::string, std::string> fileSettings2;
  std::map<std::string, std::string> contSettings2;
  std::map<std::string, std::string> settings2;
  contSettings2["changelog_path"]   = fileNameContMD+"c";
  contSettings2["slave_mode"]       = "true";
  contSettings2["poll_interval_us"] = "1000";
  fileSettings2["changelog_path"]   = fileNameFileMD+"c";
  fileSettings2["slave_mode"]       = "true";
  fileSettings2["poll_interval_us"] = "1000";

  contSvcSlave->configure( contSettings2 );
  fileSvcSlave->configure( fileSettings2 );

  viewSlave->setContainerMDSvc( contSvcSlave );
  viewSlave->setFileMDSvc( fileSvcSlave );
  viewSlave->configure( settings2 );

  viewSlave->getQuotaStats()->registerSizeMapper( mapSize );
  fileSvcSlave->setQuotaStats( viewSlave->getQuotaStats() );
  CPPUNIT_ASSERT_NO_THROW( viewSlave->initialize() );
  CPPUNIT_ASSERT_NO_THROW( contSvcSlave->startSlave() );
  CPPUNIT_ASSERT_NO_THROW( fileSvcSlave->startSlave() );

  contMaster2 = 0;
  contMaster3 = 0;
  CPPUNIT_ASSERT_NO_THROW( contMaster2 = viewMaster->getContainer( "/newdir2" ) );
  CPPUNIT_ASSERT_NO_THROW( contMaster3 = viewMaster->getContainer( "/newdir3" ) );

  qnMaster2 = 0;
  qnMaster3 = 0;
  CPPUNIT_ASSERT_NO_THROW( qnMaster2 = viewMaster->getQuotaNode( contMaster2 ) );
  CPPUNIT_ASSERT_NO_THROW( qnMaster3 = viewMaster->getQuotaNode( contMaster3 ) );
  CPPUNIT_ASSERT( qnMaster2 );
  CPPUNIT_ASSERT( qnMaster3 );

  CPPUNIT_ASSERT_NO_THROW( viewMaster->createContainer( "/newdir4", true ) );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->createContainer( "/newdir5", true ) );
  CPPUNIT_ASSERT_NO_THROW( createSubTree( viewMaster, "/newdir2", 2, 10, 100 ) );
  CPPUNIT_ASSERT_NO_THROW( modifySubTree( viewMaster, "/newdir2" ) );
  CPPUNIT_ASSERT_NO_THROW( createSubTree( viewMaster, "/newdir3", 2, 10, 100 ) );
  uint64_t corrSMaster2 = calcSize( viewMaster->getContainer( "/newdir2/dir3" ) );
  uint64_t corrNMaster2 = calcFiles( viewMaster->getContainer( "/newdir2/dir3" ) );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->removeContainer( "/newdir2/dir3", true ) );
  CPPUNIT_ASSERT_NO_THROW( modifySubTree( viewMaster, "/newdir3" ) );
  CPPUNIT_ASSERT_NO_THROW( createSubTree( viewMaster, "/newdir4", 2, 10, 100 ) );
  CPPUNIT_ASSERT_NO_THROW( createSubTree( viewMaster, "/newdir5", 2, 10, 100 ) );
  CPPUNIT_ASSERT_NO_THROW( modifySubTree( viewMaster, "/newdir4" ) );
  uint64_t corrSMaster3 = calcSize( viewMaster->getContainer( "/newdir3/dir1" ) );
  uint64_t corrNMaster3 = calcFiles( viewMaster->getContainer( "/newdir3/dir1" ) );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->removeContainer( "/newdir3/dir1", true ) );

  //----------------------------------------------------------------------------
  // Check
  //----------------------------------------------------------------------------
  sleep( 5 );
  lock.readLock();
  compareTrees( viewMaster, viewSlave,
                viewMaster->getContainer( "/" ),
                viewSlave->getContainer( "/" ) );


  eos::QuotaNode *qnSlave2 = 0;
  eos::QuotaNode *qnSlave3 = 0;
  eos::ContainerMD *contSlave2 = viewSlave->getContainer( "/newdir2" );
  eos::ContainerMD *contSlave3 = viewSlave->getContainer( "/newdir3" );
  CPPUNIT_ASSERT( contSlave2 );
  CPPUNIT_ASSERT( contSlave3 );
  CPPUNIT_ASSERT_NO_THROW( qnSlave2 = viewSlave->getQuotaNode( contSlave2 ) );
  CPPUNIT_ASSERT_NO_THROW( qnSlave3 = viewSlave->getQuotaNode( contSlave3 ) );
  CPPUNIT_ASSERT( qnSlave2 );
  CPPUNIT_ASSERT( qnSlave3 );

  CPPUNIT_ASSERT( qnSlave2 != qnMaster2 );
  CPPUNIT_ASSERT( qnSlave3 != qnMaster3 );

  eos::QuotaNode *qnS[2]; qnS[0] = qnSlave2;  qnS[1] = qnSlave3;
  eos::QuotaNode *qnM[2]; qnM[0] = qnMaster2; qnM[1] = qnMaster3;
  uint64_t qnCS[2]; qnCS[0] = corrSMaster2; qnCS[1] = corrSMaster3;
  uint64_t qnCN[2]; qnCN[0] = corrNMaster2; qnCN[1] = corrNMaster3;
  for( int i = 0; i < 2; ++i )
  {
    eos::QuotaNode *qnSlave  = qnS[i];
    eos::QuotaNode *qnMaster = qnM[i];
    CPPUNIT_ASSERT( qnSlave->getPhysicalSpaceByUser(0)  == qnMaster->getPhysicalSpaceByUser(0)-qnCS[i] );
    CPPUNIT_ASSERT( qnSlave->getUsedSpaceByUser(0)      == qnMaster->getUsedSpaceByUser(0)-qnCS[i] );
    CPPUNIT_ASSERT( qnSlave->getPhysicalSpaceByGroup(0) == qnMaster->getPhysicalSpaceByGroup(0)-qnCS[i] );
    CPPUNIT_ASSERT( qnSlave->getUsedSpaceByGroup(0)     == qnMaster->getUsedSpaceByGroup(0)-qnCS[i] );
    CPPUNIT_ASSERT( qnSlave->getNumFilesByUser(0)       == qnMaster->getNumFilesByUser(0)-qnCN[i] );
    CPPUNIT_ASSERT( qnSlave->getNumFilesByGroup(0)      == qnMaster->getNumFilesByGroup(0)-qnCN[i] );
  }

  lock.unLock();

  //----------------------------------------------------------------------------
  // Clean up
  //----------------------------------------------------------------------------
  CPPUNIT_ASSERT_NO_THROW( contSvcSlave->stopSlave() );
  CPPUNIT_ASSERT_NO_THROW( fileSvcSlave->stopSlave() );
  viewSlave->finalize();
  viewMaster->finalize();
  delete viewSlave;
  delete viewMaster;
  delete contSvcSlave;
  delete contSvcMaster;
  delete fileSvcSlave;
  delete fileSvcMaster;
  unlink( fileNameFileMD.c_str() );
  unlink( fileNameContMD.c_str() );
  unlink( (fileNameFileMD+"c").c_str() );
  unlink( (fileNameContMD+"c").c_str() );
}
