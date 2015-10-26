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
// desc:   Other tests
//------------------------------------------------------------------------------

#include <cppunit/extensions/HelperMacros.h>
#include <sstream>

#include "namespace/utils/TestHelpers.hh"
#include "namespace/utils/PathProcessor.hh"

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
