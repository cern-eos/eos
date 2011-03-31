//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Other tests
//------------------------------------------------------------------------------

#include <cppunit/extensions/HelperMacros.h>
#include <sstream>

#include "namespace/utils/PathProcessor.hh"
#include "namespace/tests/TestHelpers.hh"

//------------------------------------------------------------------------------
// Declaration
//------------------------------------------------------------------------------
class OtherTests: public CppUnit::TestCase
{
  public:
    CPPUNIT_TEST_SUITE( OtherTests );
      CPPUNIT_TEST( pathSplitterTest );
    CPPUNIT_TEST_SUITE_END();

    void pathSplitterTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION( OtherTests );

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
