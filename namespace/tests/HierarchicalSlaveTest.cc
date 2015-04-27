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
#include "namespace/accounting/FileSystemView.hh"

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
// Add some replicas to the files inside of a container
//------------------------------------------------------------------------------
void addReplicas( eos::IView *view, eos::IContainerMD *cont )
{
  eos::ContainerMD* container = dynamic_cast<eos::ContainerMD*>(cont);
  eos::ContainerMD::FileMap::iterator it;
  for( it = container->filesBegin(); it != container->filesEnd(); ++it )
  {
    for( int i = 0; i < random()%10; ++i )
      it->second->addLocation( random()%10 );
    view->updateFileStore( it->second );
  }
}

//------------------------------------------------------------------------------
// Unlink some replicas from the files in the container
//------------------------------------------------------------------------------
void unlinkReplicas( eos::IView *view, eos::IContainerMD *cont )
{
  eos::ContainerMD* container = dynamic_cast<eos::ContainerMD*>(cont);
  eos::ContainerMD::FileMap::iterator it;
  for( it = container->filesBegin(); it != container->filesEnd(); ++it )
  {
    eos::FileMD *file = it->second;
    eos::FileMD::LocationVector::const_iterator itL;
    eos::FileMD::LocationVector toUnlink;
    int n = random()%3;
    int i = 0;
    for( itL = file->locationsBegin();
         itL != file->locationsEnd() && i < n; ++itL, ++i )
    {
      toUnlink.push_back( *itL );
    }

    for( itL = toUnlink.begin(); itL != toUnlink.end(); ++itL )
      file->unlinkLocation( *itL );
    view->updateFileStore( file );
  }
}

//------------------------------------------------------------------------------
// Recursively remove the files from accounting
//------------------------------------------------------------------------------
void cleanUpQuotaRec( eos::IView *view, eos::IContainerMD *cont )
{
  eos::ContainerMD* container = dynamic_cast<eos::ContainerMD*>(cont);
  eos::QuotaNode *qn = view->getQuotaNode( container );
  eos::ContainerMD::FileMap::iterator it;
  for( it = container->filesBegin(); it != container->filesEnd(); ++it )
    qn->removeFile( it->second );

  eos::ContainerMD::ContainerMap::iterator itC;
  for( itC = container->containersBegin();
       itC != container->containersEnd();
       ++itC )
    unlinkReplicas( view, itC->second );
}

//------------------------------------------------------------------------------
// Delete some replicas from the files in the container
//------------------------------------------------------------------------------
void deleteReplicas( eos::IView *view, eos::ContainerMD *container )
{
  eos::ContainerMD::FileMap::iterator it;
  for( it = container->filesBegin(); it != container->filesEnd(); ++it )
  {
    eos::FileMD *file = it->second;
    eos::FileMD::LocationVector::const_iterator itL;
    eos::FileMD::LocationVector toDelete;
    int n = random()%3;
    int i = 0;
    for( itL = file->unlinkedLocationsBegin();
         itL != file->unlinkedLocationsEnd() && i < n; ++itL, ++i )
    {
      toDelete.push_back( *itL );
    }

    for( itL = toDelete.begin(); itL != toDelete.end(); ++itL )
      file->removeLocation( *itL );
    view->updateFileStore( file );
  }
}

//------------------------------------------------------------------------------
// Delete all replicas from all the files in the container recursively
//------------------------------------------------------------------------------
void deleteAllReplicas( eos::IView *view, eos::IContainerMD *cont )
{
  eos::ContainerMD* container = dynamic_cast<eos::ContainerMD*>(cont);
  eos::ContainerMD::FileMap::iterator it;
  for( it = container->filesBegin(); it != container->filesEnd(); ++it )
  {
    eos::FileMD *file = it->second;
    eos::FileMD::LocationVector::const_iterator itL;
    eos::FileMD::LocationVector toDelete;
    for( itL = file->locationsBegin();
         itL != file->locationsEnd(); ++itL )
    {
      toDelete.push_back( *itL );
    }

    for( itL = toDelete.begin(); itL != toDelete.end(); ++itL )
      file->unlinkLocation( *itL );
    view->updateFileStore( file );

    for( itL = toDelete.begin(); itL != toDelete.end(); ++itL )
      file->removeLocation( *itL );
    view->updateFileStore( file );
  }
}

//------------------------------------------------------------------------------
// Delete all replicas recursively
//------------------------------------------------------------------------------
void deleteAllReplicasRec( eos::IView *view, eos::IContainerMD *cont )
{
  deleteAllReplicas( view, cont );
  eos::ContainerMD* container = dynamic_cast<eos::ContainerMD*>(cont);
  eos::ContainerMD::ContainerMap::iterator itC;

  for( itC = container->containersBegin(); itC != container->containersEnd();
       ++itC )
    deleteAllReplicasRec( view, itC->second );
}

//------------------------------------------------------------------------------
// Delete all replicas recursively
//------------------------------------------------------------------------------
void deleteAllReplicasRec( eos::IView *view, const std::string &path )
{
  eos::IContainerMD *container = view->getContainer( path );
  deleteAllReplicasRec( view, container );
}

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

  eos::IContainerMD *container = view->getContainer( prefix );
  eos::QuotaNode   *qn         = view->getQuotaNode( container );
  for( int i = 0; i < numFiles; ++i )
  {
    std::ostringstream o;
    o << prefix << "/file" << i;
    eos::FileMD *file = view->createFile( o.str() );
    if( qn ) qn->addFile( file );
  }

  addReplicas( view, container );
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
    eos::IContainerMD *cont = view->getContainer( o.str() );
    eos::QuotaNode   *qn = view->getQuotaNode( cont );
    eos::ContainerMD::FileMap::iterator it;
    std::vector<eos::FileMD*> toDel;
    std::vector<eos::FileMD*>::iterator itD;
    eos::ContainerMD* container = dynamic_cast<eos::ContainerMD*>(cont);

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
      view->unlinkFile( view->getUri( *itD ) );

    while( 1 )
    {
      bool locationsLeft = false;
      for( itD = toDel.begin(); itD != toDel.end(); ++itD )
      {
        eos::FileMD *file = *itD;
        if( file->getNumUnlinkedLocation() == 0 )
          continue;
        file->removeLocation( *(file->unlinkedLocationsBegin()) );
        view->updateFileStore( file );
        if( file->getNumUnlinkedLocation() )
          locationsLeft = true;
      }

      if( !locationsLeft )
        break;
      sleep(1);
    }

    for( itD = toDel.begin(); itD != toDel.end(); ++itD )
    {
      eos::FileMD *file = *itD;
      if( qn ) qn->removeFile( file );
      view->removeFile( file );
    }
  }
}

//------------------------------------------------------------------------------
// Calculate total size
//------------------------------------------------------------------------------
uint64_t calcSize( eos::IContainerMD *cont )
{
  uint64_t size = 0;
  eos::ContainerMD::FileMap::iterator itF;
  eos::ContainerMD* container = dynamic_cast<eos::ContainerMD*>(cont);

  for( itF = container->filesBegin(); itF != container->filesEnd(); ++itF )
    size += itF->second->getSize();

  eos::ContainerMD::ContainerMap::iterator itC;
  for( itC = container->containersBegin(); itC != container->containersEnd(); ++itC )
    size += calcSize( itC->second );

  return size;
}

//------------------------------------------------------------------------------
// Calculate number of files
//------------------------------------------------------------------------------
uint64_t calcFiles( eos::IContainerMD *cont )
{
  uint64_t files = cont->getNumFiles();
  eos::ContainerMD::ContainerMap::iterator itC;
  eos::ContainerMD* container = dynamic_cast<eos::ContainerMD*>(cont);

  for( itC = container->containersBegin(); itC != container->containersEnd(); ++itC )
    files += calcFiles( itC->second );

  return files;
}

//------------------------------------------------------------------------------
// Compare trees
//------------------------------------------------------------------------------
bool compareTrees( eos::IView        *view1, eos::IView       *view2,
                   eos::IContainerMD *tree1, eos::IContainerMD *tree2 )
{
  std::string treeMsg = view1->getUri( tree1 ) + " " + view2->getUri( tree2 );
  std::ostringstream o1, o2, o3, o4;

  o1 << " -- " << tree1->getId() << " " << tree2->getId();
  CPPUNIT_ASSERT_MESSAGE( treeMsg + o1.str(),
                          tree1->getId() == tree2->getId() );
  o2 << " -- " << tree1->getName() << " " << tree2->getName();
  CPPUNIT_ASSERT_MESSAGE( treeMsg + o2.str(),
                          tree1->getName() == tree2->getName() );
  o3 << " -- " << tree1->getNumFiles() << " " << tree2->getNumFiles();
  CPPUNIT_ASSERT_MESSAGE( treeMsg + o3.str(),
                          tree1->getNumFiles() == tree2->getNumFiles() );
  o4 << " -- " << tree1->getNumContainers() << " " << tree2->getNumContainers();
  CPPUNIT_ASSERT_MESSAGE( treeMsg + o4.str(),
                          tree1->getNumContainers() == tree2->getNumContainers() );

  eos::ContainerMD::FileMap::iterator itF;
  eos::ContainerMD* tree1md = dynamic_cast<eos::ContainerMD*>(tree1);

  for( itF = tree1md->filesBegin(); itF != tree1md->filesEnd(); ++itF )
  {
    eos::FileMD *file = tree2->findFile( itF->second->getName() );
    std::string fileMsg = treeMsg + " file " + itF->second->getName();
    CPPUNIT_ASSERT_MESSAGE( fileMsg + " missing", file );
    CPPUNIT_ASSERT_MESSAGE( fileMsg + " wrong size", file->getSize() == itF->second->getSize() );
    CPPUNIT_ASSERT_MESSAGE( fileMsg + " wrong id", file->getId() == itF->second->getId() );
    CPPUNIT_ASSERT_MESSAGE( fileMsg, file->getFileMDSvc() );
    CPPUNIT_ASSERT_MESSAGE( fileMsg, itF->second->getFileMDSvc() );
  }

  eos::ContainerMD::ContainerMap::iterator itC;

  for( itC = tree1md->containersBegin(); itC != tree1md->containersEnd(); ++itC )
  {
    eos::IContainerMD *container = tree2->findContainer( itC->second->getName() );
    std::string contMsg = treeMsg + " container: " + itC->second->getName();
    CPPUNIT_ASSERT_MESSAGE( contMsg, container );
    compareTrees( view1, view2, itC->second, container );
  }

  return true;
}

//------------------------------------------------------------------------------
// Compare filesystems
//------------------------------------------------------------------------------
void compareFileSystems( eos::FileSystemView *viewMaster,
                         eos::FileSystemView *viewSlave )
{
  CPPUNIT_ASSERT( viewMaster->getNumFileSystems() == viewSlave->getNumFileSystems() );
  for( size_t i = 0; i != viewMaster->getNumFileSystems(); ++i )
  {
    CPPUNIT_ASSERT( viewMaster->getFileList( i ).size() ==
                    viewSlave->getFileList( i ).size() );
    CPPUNIT_ASSERT( viewMaster->getUnlinkedFileList( i ).size() ==
                    viewSlave->getUnlinkedFileList( i ).size() );

  }

  CPPUNIT_ASSERT( viewMaster->getNoReplicasFileList().size() ==
                  viewSlave->getNoReplicasFileList().size() );
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
  deleteAllReplicasRec( viewMaster, "/dir1/dir1/dir1" );
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

  //----------------------------------------------------------------------------
  // Reboot the master
  //----------------------------------------------------------------------------
  eos::FileSystemView *fsViewMaster  = new eos::FileSystemView;
  eos::FileSystemView *fsViewSlave   = new eos::FileSystemView;

  contSettings1["changelog_path"] = fileNameContMD+"c";
  fileSettings1["changelog_path"] = fileNameFileMD+"c";
  fileSvcMaster->configure( fileSettings1 );
  contSvcMaster->configure( contSettings1 );
  fileSvcMaster->addChangeListener( fsViewMaster );
  viewMaster->getQuotaStats()->registerSizeMapper( mapSize );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->initialize() );

  viewMaster->createContainer( "/newdir1", true );
  createSubTree( viewMaster, "/newdir1", 2, 10, 100 );
  modifySubTree( viewMaster, "/newdir1" );
  deleteAllReplicasRec( viewMaster, "/newdir1/dir1" );
  viewMaster->removeContainer( "/newdir1/dir1", true );
  eos::IContainerMD *contMaster2 = 0;
  eos::IContainerMD *contMaster3 = 0;
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

  fileSvcSlave->addChangeListener( fsViewSlave );
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
  contSvcSlave->setQuotaStats( viewSlave->getQuotaStats() );

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
  CPPUNIT_ASSERT_NO_THROW( cleanUpQuotaRec( viewMaster,
                                            viewMaster->getContainer( "/newdir2/dir3" ) ) );
  deleteAllReplicasRec( viewMaster, "/newdir2/dir3" );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->removeContainer( "/newdir2/dir3", true ) );
  CPPUNIT_ASSERT_NO_THROW( modifySubTree( viewMaster, "/newdir3" ) );
  CPPUNIT_ASSERT_NO_THROW( createSubTree( viewMaster, "/newdir4", 2, 10, 100 ) );
  CPPUNIT_ASSERT_NO_THROW( createSubTree( viewMaster, "/newdir5", 2, 10, 100 ) );
  CPPUNIT_ASSERT_NO_THROW( modifySubTree( viewMaster, "/newdir4" ) );
  CPPUNIT_ASSERT_NO_THROW( cleanUpQuotaRec( viewMaster,
                                            viewMaster->getContainer( "/newdir3/dir1" ) ) );
  deleteAllReplicasRec( viewMaster, "/newdir3/dir1" );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->removeContainer( "/newdir3/dir1", true ) );

  deleteAllReplicasRec( viewMaster, "/newdir3/dir2" );
  unlinkReplicas( viewMaster, viewMaster->getContainer( "/newdir1/dir2" ) );
  unlinkReplicas( viewMaster, viewMaster->getContainer( "/newdir4/dir2" ) );
  unlinkReplicas( viewMaster, viewMaster->getContainer( "/newdir5/dir1" ) );
  unlinkReplicas( viewMaster, viewMaster->getContainer( "/newdir5/dir2" ) );

  //----------------------------------------------------------------------------
  // Move some files around and rename them
  //----------------------------------------------------------------------------
  eos::IContainerMD *parent1     = 0;
  eos::IContainerMD *parent2     = 0;
  eos::FileMD      *toBeMoved   = 0;
  eos::FileMD      *toBeRenamed = 0;
  CPPUNIT_ASSERT_NO_THROW( parent1 = viewMaster->createContainer( "/dest", true ) );
  CPPUNIT_ASSERT_NO_THROW( parent2 = viewMaster->getContainer( "/dir0/dir0" ) );
  CPPUNIT_ASSERT_NO_THROW( toBeMoved = viewMaster->getFile( "/dir0/dir0/file0" ) );
  CPPUNIT_ASSERT_NO_THROW( toBeRenamed = viewMaster->getFile( "/dir0/dir0/file1" ) );
  CPPUNIT_ASSERT( parent1 );
  CPPUNIT_ASSERT( parent2 );
  CPPUNIT_ASSERT( toBeMoved );
  CPPUNIT_ASSERT( toBeRenamed );

  parent2->removeFile( toBeMoved->getName() );
  parent1->addFile( toBeMoved );
  viewMaster->updateFileStore( toBeMoved );

  viewMaster->renameFile( toBeRenamed, "file0" );

  //----------------------------------------------------------------------------
  // Check
  //----------------------------------------------------------------------------
  sleep( 5 );
  lock.readLock();
  compareTrees( viewMaster, viewSlave,
                viewMaster->getContainer( "/" ),
                viewSlave->getContainer( "/" ) );

  compareFileSystems( fsViewMaster, fsViewSlave );


  eos::QuotaNode *qnSlave2 = 0;
  eos::QuotaNode *qnSlave3 = 0;
  eos::IContainerMD *contSlave2 = viewSlave->getContainer( "/newdir2" );
  eos::IContainerMD *contSlave3 = viewSlave->getContainer( "/newdir3" );
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
  for( int i = 0; i < 2; ++i )
  {
    eos::QuotaNode *qnSlave  = qnS[i];
    eos::QuotaNode *qnMaster = qnM[i];
    CPPUNIT_ASSERT( qnSlave->getPhysicalSpaceByUser(0)  == qnMaster->getPhysicalSpaceByUser(0) );
    CPPUNIT_ASSERT( qnSlave->getUsedSpaceByUser(0)      == qnMaster->getUsedSpaceByUser(0) );
    CPPUNIT_ASSERT( qnSlave->getPhysicalSpaceByGroup(0) == qnMaster->getPhysicalSpaceByGroup(0) );
    CPPUNIT_ASSERT( qnSlave->getUsedSpaceByGroup(0)     == qnMaster->getUsedSpaceByGroup(0) );
    CPPUNIT_ASSERT( qnSlave->getNumFilesByUser(0)       == qnMaster->getNumFilesByUser(0) );
    CPPUNIT_ASSERT( qnSlave->getNumFilesByGroup(0)      == qnMaster->getNumFilesByGroup(0) );
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
  delete fsViewMaster;
  delete fsViewSlave;
  unlink( fileNameFileMD.c_str() );
  unlink( fileNameContMD.c_str() );
  unlink( (fileNameFileMD+"c").c_str() );
  unlink( (fileNameContMD+"c").c_str() );
}
