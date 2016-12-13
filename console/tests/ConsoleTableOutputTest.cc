//------------------------------------------------------------------------------
//! @file ConsoleTableOutputTest.cc
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

#include "ConsoleTableOutputTest.hh"
#include <fstream>

CPPUNIT_TEST_SUITE_REGISTRATION(ConsoleTableOutputTest);

void ConsoleTableOutputTest::TestUtility()
{
  ConsoleTableOutput test;
  test.SetHeader({{"title1",  8}, {"title2", 8} });
  std::string test_out = "------------------\n  title1  title2\n";
  test_out += "------------------\n";
  CPPUNIT_ASSERT(test.Str() ==  test_out);
  test.AddRow("Value1",  3);
  test_out +=  "  Value1\33[0m       3\33[0m\n";
  CPPUNIT_ASSERT(test.Str() ==  test_out);
  test.AddRow(0xAB,  "Value2");
  test_out +=  "     171\33[0m  Value2\33[0m\n";
  CPPUNIT_ASSERT(test.Str() ==  test_out);
  test.CustomRow(std::make_pair("Test test 1, 2, 3",  20));
  test_out +=  "   Test test 1, 2, 3\33[0m\n";
  CPPUNIT_ASSERT(test.Str() ==  test_out);
  test.AddRow(
    test.Colorify(ConsoleTableOutput::RED,   "test_red"),
    45
  );
  test_out +=  "\33[31mtest_red\33[0m      45\33[0m\n";
  CPPUNIT_ASSERT(test.Str() ==  test_out);
  CPPUNIT_ASSERT_THROW(test.AddRow(1, 2, 3),  std::string);
}
