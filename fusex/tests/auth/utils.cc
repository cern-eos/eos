//------------------------------------------------------------------------------
// File: utils.cc
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

#include "auth/Utils.hh"
#include "common/SymKeys.hh"
#include <gtest/gtest.h>

TEST(ChopTrailingSlashes, BasicSanity) {
  ASSERT_EQ(chopTrailingSlashes("/test/b"), "/test/b");
  ASSERT_EQ(chopTrailingSlashes("/test/b/"), "/test/b");
  ASSERT_EQ(chopTrailingSlashes("/test/b///"), "/test/b");
  ASSERT_EQ(chopTrailingSlashes("/b///"), "/b");
  ASSERT_EQ(chopTrailingSlashes("//"), "/");
  ASSERT_EQ(chopTrailingSlashes("/"), "/");
  ASSERT_EQ(chopTrailingSlashes(""), "");
}

TEST(sha256, BasicSanity) {
  ASSERT_EQ(eos::common::SymKey::Sha256("12345"), "5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5");
}

TEST(FileReadWrite, BasicSanity) {
  ASSERT_EQ(system("rm -rf /tmp/eos-fusex-unit-tests/"), 0);
  ASSERT_EQ(system("mkdir /tmp/eos-fusex-unit-tests/"), 0);

  ASSERT_TRUE(writeFile("/tmp/eos-fusex-unit-tests/pickles", "chicken chicken chicken chicken"));

  std::string contents;
  ASSERT_TRUE(readFile("/tmp/eos-fusex-unit-tests/pickles", contents));
  ASSERT_EQ(contents, "chicken chicken chicken chicken");
}
