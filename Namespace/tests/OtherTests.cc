//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Other tests
//------------------------------------------------------------------------------

#include <cppunit/CompilerOutputter.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>

#include <sstream>

#include "OtherTests.hh"
#include "utils/PathProcessor.hh"

//------------------------------------------------------------------------------
// Generic file md test declaration
//------------------------------------------------------------------------------
CppUnit::Test *OtherTests::suite()
{
  CppUnit::TestSuite *suiteOfTests
              = new CppUnit::TestSuite( "OtherTests" );

  suiteOfTests->addTest( new CppUnit::TestCaller<OtherTests>( 
                               "pathSplitterTest", 
                               &OtherTests::pathSplitterTest ) );
  return suiteOfTests;
}

//------------------------------------------------------------------------------
// Check the path
//------------------------------------------------------------------------------
bool checkPath( const std::vector<std::string> &elements, size_t depth )
{
  if( elements.size() < depth )
    return false;

  for( size_t i = 1; i <= depth; ++i )
  {
    std::ostringstream o;
    o << "test" << i;
    if( elements[i-1] != o.str() )
      return false;
  }
  return true;
}

//------------------------------------------------------------------------------
// Test the path splitter
//------------------------------------------------------------------------------
void OtherTests::pathSplitterTest()
{
  //----------------------------------------------------------------------------
  // Test the string path splitter
  //----------------------------------------------------------------------------
  std::string path1 = "/test1/test2/test3/test4/";
  std::string path2 = "/test1/test2/test3/test4";
  std::string path3 = "test1/test2/test3/test4/";
  std::string path4 = "test1/test2/test3/test4";

  std::vector<std::string> elements;
  eos::PathProcessor::splitPath( elements, path1 );
  CPPUNIT_ASSERT( checkPath( elements, 4 ) );

  elements.clear();
  eos::PathProcessor::splitPath( elements, path2 );
  CPPUNIT_ASSERT( checkPath( elements, 4 ) );

  elements.clear();
  eos::PathProcessor::splitPath( elements, path3 );
  CPPUNIT_ASSERT( checkPath( elements, 4 ) );

  elements.clear();
  eos::PathProcessor::splitPath( elements, path4 );
  CPPUNIT_ASSERT( checkPath( elements, 4 ) );

  elements.clear();
  eos::PathProcessor::splitPath( elements, "/" );
  CPPUNIT_ASSERT( elements.size() == 0 );

  elements.clear();
  eos::PathProcessor::splitPath( elements, "" );
  CPPUNIT_ASSERT( elements.size() == 0 );

}

//------------------------------------------------------------------------------
// Start the show
//------------------------------------------------------------------------------
int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  runner.addTest( OtherTests::suite() );
  runner.setOutputter( new CppUnit::CompilerOutputter( &runner.result(),
                                                       std::cerr ) );
  return !runner.run();
}
