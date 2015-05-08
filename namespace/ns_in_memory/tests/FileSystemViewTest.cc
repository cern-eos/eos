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
// desc:   FileSystemView test
//------------------------------------------------------------------------------

#include <cppunit/extensions/HelperMacros.h>

#include <stdint.h>
#include <unistd.h>
#include <sstream>
#include <cstdlib>
#include <ctime>

#include "namespace/utils/TestHelpers.hh"
#include "namespace/ns_in_memory/views/HierarchicalView.hh"
#include "namespace/ns_in_memory/accounting/FileSystemView.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogFileMDSvc.hh"

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
eos::IFileMD::location_t getRandomLocation()
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
    eos::ChangeLogContainerMDSvc *contSvc = new eos::ChangeLogContainerMDSvc;
    eos::ChangeLogFileMDSvc      *fileSvc = new eos::ChangeLogFileMDSvc;
    eos::IView                   *view    = new eos::HierarchicalView;
    eos::FileSystemView          *fsView  = new eos::FileSystemView;
    fileSvc->setContainerService( contSvc );
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
    fsView->initialize();
    fileSvc->addChangeListener( fsView );

    view->createContainer( "/test/embed/embed1", true );
    eos::IContainerMD *c = view->createContainer( "/test/embed/embed2", true );
    view->createContainer( "/test/embed/embed3", true );

    //--------------------------------------------------------------------------
    // Create some files
    //--------------------------------------------------------------------------
    for( int i = 0; i < 1000; ++i )
    {
      std::ostringstream o;
      o << "file" << i;
      eos::IFileMD *files[4];
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
    // Create some file without replicas assigned
    //--------------------------------------------------------------------------
    for( int i = 0; i < 500; ++i )
    {
      std::ostringstream o;
      o << "noreplicasfile" << i;
      view->createFile( std::string("/test/embed/embed1/") + o.str() );
    }

    //--------------------------------------------------------------------------
    // Sum up all the locations
    //--------------------------------------------------------------------------
    size_t numReplicas = countReplicas( fsView );
    CPPUNIT_ASSERT( numReplicas == 20000 );

    size_t numUnlinked = countUnlinked( fsView );
    CPPUNIT_ASSERT( numUnlinked == 0 );

    CPPUNIT_ASSERT( fsView->getNoReplicasFileList().size() == 500 );

    //--------------------------------------------------------------------------
    // Unlinke replicas
    //--------------------------------------------------------------------------
    for( int i = 100; i < 500; ++i )
    {
      std::ostringstream o;
      o << "file" << i;
      // unlink some replicas
      eos::IFileMD *f = c->findFile( o.str() );
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
      eos::IFileMD *f = c->findFile( o.str() );
      f->unlinkAllLocations();
      c->removeFile( o.str() );
      f->setContainerId( 0 );
      view->updateFileStore( f );
    }

    numReplicas = countReplicas( fsView );
    CPPUNIT_ASSERT( numReplicas == 17200 );

    numUnlinked = countUnlinked( fsView );
    CPPUNIT_ASSERT( numUnlinked == 2800 );

    //--------------------------------------------------------------------------
    // Restart
    //--------------------------------------------------------------------------
    view->finalize();
    fsView->finalize();
    view->initialize();
    fsView->initialize();

    numReplicas = countReplicas( fsView );
    CPPUNIT_ASSERT( numReplicas == 17200 );

    numUnlinked = countUnlinked( fsView );
    CPPUNIT_ASSERT( numUnlinked == 2800 );

    CPPUNIT_ASSERT( fsView->getNoReplicasFileList().size() == 500 );
    eos::IFileMD *f = view->getFile( std::string("/test/embed/embed1/file1") );
    f->unlinkAllLocations();
    numReplicas = countReplicas( fsView );
    CPPUNIT_ASSERT( numReplicas == 17195 );
    numUnlinked = countUnlinked( fsView );
    CPPUNIT_ASSERT( numUnlinked == 2805 );
    f->removeAllLocations();
    numUnlinked = countUnlinked( fsView );
    CPPUNIT_ASSERT( numUnlinked == 2800 );
    view->updateFileStore( f );
    CPPUNIT_ASSERT( fsView->getNoReplicasFileList().size() == 501 );
    view->removeFile( f );
    CPPUNIT_ASSERT( fsView->getNoReplicasFileList().size() == 500 );
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
