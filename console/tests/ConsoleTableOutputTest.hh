//------------------------------------------------------------------------------
//! @file ConsoleTableOutputTest.hh
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
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

#ifndef __CONSOLETABLEOUTPUTTEST__HH__
#define __CONSOLETABLEOUTPUTTEST__HH__

#include <string>
#include <cppunit/extensions/HelperMacros.h>
#include "../ConsoleTableOutput.hh"

class ConsoleTableOutputTest : public CppUnit::TestCase
{
  CPPUNIT_TEST_SUITE(ConsoleTableOutputTest);
  CPPUNIT_TEST(TestUtility);
  CPPUNIT_TEST_SUITE_END();

public:
  //  CPPUNIT required methods
  void setUp() {};
  void tearDown() {};
  void TestUtility();
};

#endif //__CONSOLETABLEOUTPUTTEST__HH__
