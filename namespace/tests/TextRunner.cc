//------------------------------------------------------------------------------
// Copyright (c) 2011 by European Organization for Nuclear Research (CERN)
// Author: Lukasz Janyst <ljanyst@cern.ch>
// See the LICENCE file for details.
//------------------------------------------------------------------------------

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
  // Load the test library
  //----------------------------------------------------------------------------
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

  //----------------------------------------------------------------------------
  // Print help
  //----------------------------------------------------------------------------
  CppUnit::Test *all = CppUnit::TestFactoryRegistry::getRegistry().makeTest();
  if( argc == 2 )
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
  for( int i = 2; i < argc; ++i )
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
  dlclose( libHandle );
  return wasSuccessful ? 0 : 1;
}
