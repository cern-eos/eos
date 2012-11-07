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

  for( int i = 0; i < numFiles; ++i )
  {
    std::ostringstream o;
    o << prefix << "/file" << i;
    view->createFile( o.str() );
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
    eos::ContainerMD::FileMap::iterator it;
    std::vector<eos::FileMD*> toDel;
    std::vector<eos::FileMD*>::iterator itD;
    for( i = 1, it = container->filesBegin();
         it != container->filesEnd();
         ++it, ++i )
    {
      it->second->setSize( random() % 1000000 );
      view->updateFileStore( it->second );
      if(  i % 4 == 0 )
        toDel.push_back( it->second );
    }
    for( itD = toDel.begin(); itD != toDel.end(); ++itD )
      view->removeFile( *itD );
  }
}

//------------------------------------------------------------------------------
// Compare trees
//------------------------------------------------------------------------------
bool compareTrees( eos::ContainerMD *tree1, eos::ContainerMD *tree2 )
{
  if( tree1->getId() != tree2->getId() )
    return false;

  if( tree1->getName() != tree2->getName() )
    return false;

  if( tree1->getNumFiles() != tree2->getNumFiles() )
    return false;

  if( tree1->getNumContainers() != tree2->getNumContainers() )
    return false;

  eos::ContainerMD::FileMap::iterator itF;
  for( itF = tree1->filesBegin(); itF != tree1->filesEnd(); ++itF )
  {
    eos::FileMD *file = tree2->findFile( itF->second->getName() );
    if( !file )
      return false;
    if( file->getSize() != itF->second->getSize() )
      return false;
    if( file->getId() != itF->second->getId() )
      return false;
  }

  eos::ContainerMD::ContainerMap::iterator itC;
  for( itC = tree1->containersBegin(); itC != tree1->containersEnd(); ++itC )
  {
    eos::ContainerMD *container = tree2->findContainer( itC->second->getName() );
    if( !container )
      return false;
    bool status = compareTrees( itC->second, container );
    if( !status )
      return false;
  }

  return true;
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

  CPPUNIT_ASSERT_NO_THROW( viewMaster->initialize() );

  viewMaster->createContainer( "/newdir1", true );
  createSubTree( viewMaster, "/newdir1", 2, 10, 100 );
  modifySubTree( viewMaster, "/newdir1" );
  viewMaster->removeContainer( "/newdir1/dir1", true );

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

  CPPUNIT_ASSERT_NO_THROW( viewSlave->initialize() );
  CPPUNIT_ASSERT_NO_THROW( contSvcSlave->startSlave() );
  CPPUNIT_ASSERT_NO_THROW( fileSvcSlave->startSlave() );

  CPPUNIT_ASSERT_NO_THROW( viewMaster->createContainer( "/newdir2", true ) );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->createContainer( "/newdir3", true ) );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->createContainer( "/newdir4", true ) );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->createContainer( "/newdir5", true ) );
  CPPUNIT_ASSERT_NO_THROW( createSubTree( viewMaster, "/newdir2", 2, 10, 100 ) );
  CPPUNIT_ASSERT_NO_THROW( modifySubTree( viewMaster, "/newdir2" ) );
  CPPUNIT_ASSERT_NO_THROW( createSubTree( viewMaster, "/newdir3", 2, 10, 100 ) );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->removeContainer( "/newdir2/dir3", true ) );
  CPPUNIT_ASSERT_NO_THROW( modifySubTree( viewMaster, "/newdir3" ) );
  CPPUNIT_ASSERT_NO_THROW( createSubTree( viewMaster, "/newdir4", 2, 10, 100 ) );
  CPPUNIT_ASSERT_NO_THROW( createSubTree( viewMaster, "/newdir5", 2, 10, 100 ) );
  CPPUNIT_ASSERT_NO_THROW( modifySubTree( viewMaster, "/newdir4" ) );
  CPPUNIT_ASSERT_NO_THROW( viewMaster->removeContainer( "/newdir3/dir1", true ) );

  //----------------------------------------------------------------------------
  // Check
  //----------------------------------------------------------------------------
  sleep( 5 );
  lock.readLock();
  CPPUNIT_ASSERT( compareTrees( viewMaster->getContainer( "/" ),
                                viewSlave->getContainer( "/" ) ) );
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
