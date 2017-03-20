//------------------------------------------------------------------------------
//! @file ConfigEngineTest.hh
//! @author Andrea Manzi <amanzi@cern.ch>
//! @brief Class containing unit test for Config Engine
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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
#ifndef __EOSMGMTEST_CONFIGENGINETEST_HH__
#define __EOSMGMTEST_CONFIGENGINETEST_HH__

#include <iostream>
#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include "mgm/IConfigEngine.hh"
#include "mgm/FileConfigEngine.hh"
#ifdef HAVE_HIREDIS
#include "mgm/RedisConfigEngine.hh"
#endif

class ConfigEngineTest: public CppUnit::TestCase
{

private:
  eos::mgm::IConfigEngine* engine;

public:
  void setUp();
  void tearDown();

  CPPUNIT_TEST_SUITE(ConfigEngineTest);
  CPPUNIT_TEST(ListConfigsTest);
  //CPPUNIT_TEST( LoadConfigTest );
  //CPPUNIT_TEST( SaveConfigsTest );
  //CPPUNIT_TEST( DumpConfigsTest );
  CPPUNIT_TEST_SUITE_END();

  void ListConfigsTest();
  void LoadConfigTest();
  //void SaveConfigTest();
  //void DumpConfigTest();

};

CPPUNIT_TEST_SUITE_REGISTRATION(ConfigEngineTest);

#endif // __EOSMGMTEST_CONFIGENGINETEST_HH__
