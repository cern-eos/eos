//------------------------------------------------------------------------------
// Copyright (c) 2011 by European Organization for Nuclear Research (CERN)
// Author: Lukasz Janyst <ljanyst@cern.ch>
// See the LICENCE file for details.
//------------------------------------------------------------------------------

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

#include <iomanip>
#include <stdexcept>
#include <cppunit/TestPath.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/BriefTestProgressListener.h>
#include <cppunit/CompilerOutputter.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include <dlfcn.h>
#include "namespace/utils/PathProcessor.hh"

//------------------------------------------------------------------------------
// Print all the tests present in the test suite
//------------------------------------------------------------------------------
void printTests( const CppUnit::Test *t, std::string prefix = "" )
{
  if( t == 0 )
    return;

  const CppUnit::TestSuite* suite = dynamic_cast<const CppUnit::TestSuite*>( t );
  std::cerr << prefix << t->getName();

  if( suite )
  {
    std::cerr << "/" << std::endl;
    std::string prefix1 = "  "; prefix1 += prefix;
    prefix1 += t->getName(); prefix1 += "/";
    const std::vector<CppUnit::Test*> &tests = suite->getTests();
    std::vector<CppUnit::Test*>::const_iterator it;

    for( it = tests.begin(); it != tests.end(); ++it )
      printTests( *it, prefix1 );
  }
  else
    std::cerr << std::endl;
}


//------------------------------------------------------------------------------
// Start the show
//------------------------------------------------------------------------------
int main( int argc, char **argv)
{
  // Load the test library
  if( argc < 2 )
  {
    std::cerr << "Usage: " << argv[0] << " libname.so testname" << std::endl;
    return 1;
  }
  void *libHandle = dlopen( argv[1], RTLD_LAZY );
  if( libHandle == 0 )
  {
    std::cerr << "Unable to load the test library: " << dlerror() << std::endl;
    return 1;
  }

  // Print help
  CppUnit::Test *all = CppUnit::TestFactoryRegistry::getRegistry().makeTest();

  if( argc == 2 )
  {
    std::cerr << "Select your tests:" << std::endl << std::endl;
    printTests( all );
    std::cerr << std::endl;

    if (dlclose( libHandle ))
      std::cerr << "Error during dynamic library unloading" << std::endl;

    delete all;
    return 1;
  }

  // Build the test suite for the requested path
  std::string test_path = argv[2];

  // Create event manager and test controller
  CppUnit::TestResult controller;

  // Add listener that collects test results
  CppUnit::TestResultCollector result;
  controller.addListener(&result);

  // Add listener that prints the name of the test and status
  CppUnit::BriefTestProgressListener brief_progress;
  controller.addListener(&brief_progress);

  // Add the top suite to the test runner
  CppUnit::TestRunner runner;
  runner.addTest(all);

  try
  {
    std::cout << std::endl << "Running:" <<  std::endl;
    runner.run(controller, test_path);
    std::cerr << std::endl;

    // Print test in a compiler compatible format
    CppUnit::CompilerOutputter outputter(&result, std::cerr);
    outputter.write();
  }
  catch (std::invalid_argument &e)
  {
    std::cerr << std::endl
	      << "ERROR: " << e.what()
	      << std::endl;

    if (dlclose( libHandle ))
      std::cerr << "Error during dynamic library unloading" << std::endl;

    return 0;
  }

  if (dlclose( libHandle ))
    std::cerr << "Error during dynamic library unloading" << std::endl;

  return result.wasSuccessful() ? 0 : 1;
}
