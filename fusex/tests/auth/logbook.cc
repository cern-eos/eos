//------------------------------------------------------------------------------
// File: logbook.cc
// Author: Georgios Bitzes - CERN
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

#include "auth/Logbook.hh"
#include "auth/Utils.hh"
#include "gtest/gtest.h"

TEST(Logbook, BasicSanity) {
  Logbook logbook(true);
  logbook.insert("Test Test");
  logbook.insert("123");

  ASSERT_EQ(logbook.toString(), "Test Test\n123\n");
}

TEST(Logbook, NotActive) {
  Logbook logbook(false);
  logbook.insert("123");
  ASSERT_EQ(logbook.toString(), "");
}

TEST(Logbook, Scoping) {
  Logbook logbook(true);

  LogbookScope scope1(logbook.makeScope("Scope 1"));
  scope1.insert("Message 1");
  scope1.insert("Message 2");

  std::cerr << logbook.toString() << std::endl;
  ASSERT_EQ(logbook.toString(), "-- Scope 1\n  Message 1\n  Message 2\n");

  LogbookScope scope2(scope1.makeScope("Sub-scope"));
  scope2.insert("Some other message 1");
  scope2.insert("Some other message 2");

  std::cerr << logbook.toString() << std::endl;
  ASSERT_EQ(logbook.toString(),
    "-- Scope 1\n"
    "  Message 1\n"
    "  Message 2\n"
    "  -- Sub-scope\n"
    "    Some other message 1\n"
    "    Some other message 2\n"
  );

}