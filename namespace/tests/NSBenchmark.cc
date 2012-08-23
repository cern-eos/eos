//------------------------------------------------------------------------------
// Copyright (c) 2012 by European Organization for Nuclear Research (CERN)
// Author: Lukasz Janyst <ljanyst@cern.ch>
//------------------------------------------------------------------------------
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

#include <iostream>
#include "namespace/views/HierarchicalView.hh"
#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogFileMDSvc.hh"

//------------------------------------------------------------------------------
// File size mapping function
//------------------------------------------------------------------------------
static uint64_t mapSize( const eos::FileMD *file )
{
  return 0;
}

//------------------------------------------------------------------------------
// Zero the CPUTIME
//------------------------------------------------------------------------------
void zeroTimer( clockid_t type = CLOCK_PROCESS_CPUTIME_ID )
{
  timespec tS;
  tS.tv_sec = 0;
  tS.tv_nsec = 0;
  clock_settime( type, &tS );
}

//------------------------------------------------------------------------------
// Get time in microsecs
//------------------------------------------------------------------------------
uint64_t clockGetTime( clockid_t type = CLOCK_REALTIME )
{
  timespec ts;
  clock_gettime( type, &ts );
  return (uint64_t)ts.tv_sec * 1000000LL + (uint64_t)ts.tv_nsec / 1000LL;
}

//------------------------------------------------------------------------------
// Boot the namespace
//------------------------------------------------------------------------------
eos::IView *bootNamespace( const std::string &dirLog,
                           const std::string &fileLog )
  throw( eos::MDException )
{
  eos::IContainerMDSvc *contSvc = new eos::ChangeLogContainerMDSvc();
  eos::IFileMDSvc      *fileSvc = new eos::ChangeLogFileMDSvc();
  eos::IView           *view    = new eos::HierarchicalView();

  std::map<std::string, std::string> fileSettings;
  std::map<std::string, std::string> contSettings;
  std::map<std::string, std::string> settings;
  contSettings["changelog_path"] = dirLog;
  fileSettings["changelog_path"] = fileLog;

  fileSvc->configure( fileSettings );
  contSvc->configure( contSettings );

  view->setContainerMDSvc( contSvc );
  view->setFileMDSvc( fileSvc );

  view->configure( settings );
  view->getQuotaStats()->registerSizeMapper( mapSize );
  view->initialize();
  return view;
}

//------------------------------------------------------------------------------
// Close the namespace
//------------------------------------------------------------------------------
void closeNamespace( eos::IView *view ) throw( eos::MDException )
{
  eos::IContainerMDSvc *contSvc = view->getContainerMDSvc();
  eos::IFileMDSvc      *fileSvc = view->getFileMDSvc();
  view->finalize();
  delete view;
  delete contSvc;
  delete fileSvc;
}

int main( int argc, char **argv )
{
  //----------------------------------------------------------------------------
  // Check up the commandline params
  //----------------------------------------------------------------------------
  if( argc != 3 )
  {
    std::cerr << "Usage:"                                << std::endl;
    std::cerr << "  ns-benchmark directory.log file.log" << std::endl;
    return 1;
  };

  //----------------------------------------------------------------------------
  // Do things
  //----------------------------------------------------------------------------
  try
  {
    std::cerr << "[i] Booting up..." << std::endl;
    zeroTimer( CLOCK_PROCESS_CPUTIME_ID );
    uint64_t realTimeStart = clockGetTime( CLOCK_REALTIME );
    eos::IView *view = bootNamespace( argv[1], argv[2] );
    uint64_t realTimeStop = clockGetTime( CLOCK_REALTIME );
    uint64_t cpuTimeStop = clockGetTime( CLOCK_PROCESS_CPUTIME_ID );
    double realTime = (double)(realTimeStop-realTimeStart)/1000000.0;
    double cpuTime  = (double)cpuTimeStop/1000000.0;
    std::cerr << "[i] Booted." << std::endl;
    std::cerr << "[i] Real time: " << realTime << std::endl;
    std::cerr << "[i] CPU time: "  << cpuTime  << std::endl;
    closeNamespace( view );
  }
  catch( eos::MDException &e )
  {
    std::cerr << "[!] Error: " << e.getMessage().str() << std::endl;
    return 2;
  }

  return 0;
}
