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
// desc:   Text runner
//------------------------------------------------------------------------------

#include <cppunit/CompilerOutputter.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include "namespace/utils/PathProcessor.hh"

//------------------------------------------------------------------------------
// Print all the tests present in the test suite
//------------------------------------------------------------------------------
void printTests( const CppUnit::Test *t, std::string prefix = "" )
{
  if( t == 0 )
    return;

  const CppUnit::TestSuite *suite = dynamic_cast<const CppUnit::TestSuite*>( t );
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
// Find a test
//------------------------------------------------------------------------------
CppUnit::Test *findTest( CppUnit::Test *t, const std::string &test )
{
  //----------------------------------------------------------------------------
  // Check the suit and the path
  //----------------------------------------------------------------------------
  std::vector<std::string> elements;
  eos::PathProcessor::splitPath( elements, test );

  if( t == 0 )
    return 0;

  if( elements.empty() )
    return 0;

  if( t->getName() != elements[0] )
    return 0;

  //----------------------------------------------------------------------------
  // Look for the requested test
  //----------------------------------------------------------------------------
  CppUnit::Test *ret = t;
  for( size_t i = 1; i < elements.size(); ++i )
    {
      CppUnit::TestSuite *suite = dynamic_cast<CppUnit::TestSuite*>( ret );
      CppUnit::Test      *next  = 0;
      const std::vector<CppUnit::Test*> &tests = suite->getTests();
      std::vector<CppUnit::Test*>::const_iterator it;
      for( it = tests.begin(); it != tests.end(); ++it )
        if( (*it)->getName() == elements[i] )
          next = *it;
      if( !next )
        return 0;
      ret = next;
    }

  return ret;
}


//------------------------------------------------------------------------------
// Start the show
//------------------------------------------------------------------------------
int main( int argc, char **argv)
{
  //----------------------------------------------------------------------------
  // Print help
  //----------------------------------------------------------------------------
  CppUnit::Test *all = CppUnit::TestFactoryRegistry::getRegistry().makeTest();
  if( argc == 1 )
    {
      std::cerr << "Select your tests:" << std::endl << std::endl;
      printTests( all );
      std::cerr << std::endl;
      return 1;
    }

  //----------------------------------------------------------------------------
  // Build the test suite
  //----------------------------------------------------------------------------
  CppUnit::TestSuite *selected = new CppUnit::TestSuite( "Selected tests" );
  for( int i = 1; i < argc; ++i )
    {
      CppUnit::Test *t = findTest( all, std::string( argv[i]) );
      if( !t )
        {
          std::cerr << "Unable to find: " << argv[i] << std::endl;
          return 2;
        }
      selected->addTest( t );
    }

  std::cerr << "You have selected: " << std::endl << std::endl;
  printTests( selected );
  std::cerr << std::endl;

  //----------------------------------------------------------------------------
  // Run the tests
  //----------------------------------------------------------------------------
  std::cerr << "Running:" << std::endl << std::endl;
  CppUnit::TextUi::TestRunner runner;
  runner.addTest( selected );

  runner.setOutputter(
                      new CppUnit::CompilerOutputter( &runner.result(), std::cerr ) );

  bool wasSuccessful = runner.run();
  return wasSuccessful ? 0 : 1;
}
