//------------------------------------------------------------------------------
//! @file AclCommandTest.hh
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

#ifndef __ACLCOMMANDTEST_HH__
#define __ACLCOMMANDTEST_HH__

#include <cppunit/extensions/HelperMacros.h>
#include "console/commands/AclCommand.hh"
#include <iostream>
#include <string>
#include <functional>

class AclCommandTest : public CppUnit::TestCase
{
  CPPUNIT_TEST_SUITE(AclCommandTest);
  CPPUNIT_TEST(TestSyntax);
  CPPUNIT_TEST(TestCheckId);
  CPPUNIT_TEST(TestGetRuleInt);
  CPPUNIT_TEST(TestAclRuleFromString);
  CPPUNIT_TEST(TestFunctionality);
  CPPUNIT_TEST_SUITE_END();

public:
  // CPPUNIT required methods
  void setUp(void) {}
  void tearDown(void) {}

  // Test helper method
  void TestSyntaxCommand(std::string command, bool outcome = true);
  void TestSyntax();
  void TestCheckId();
  void TestGetRuleInt();
  void TestAclRuleFromString();
  void TestFunctionality();
};

#endif //__ACLCOMMANDTEST_HH__
