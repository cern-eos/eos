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
#include "auth/JailIdentifier.hh"
#include "auth/UuidStore.hh"
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

TEST(JailIdentifier, IdentifyMyself) {
  JailResolver jr;
  JailIdentifier id = jr.resolveIdentifier(getpid());
  std::cout << id.describe() << std::endl;
  ASSERT_TRUE(id.ok());

  JailIdentifier id2 = jr.resolveIdentifier(getppid());
  std::cout << id2.describe() << std::endl;
  ASSERT_TRUE(id2.ok());

  JailIdentifier id3 = jr.resolveIdentifier(getsid(getpid()));
  std::cout << id3.describe() << std::endl;
  ASSERT_TRUE(id3.ok());

  ASSERT_EQ(id, id2);
  ASSERT_EQ(id, id3);

  JailInformation ji = jr.resolve(getpid());
  ASSERT_EQ(ji.id, id);
  ASSERT_EQ(ji.pid, getpid());
  ASSERT_TRUE(ji.sameJailAsThisPid);
}

TEST(UuidStore, BasicSanity) {
  ASSERT_EQ(system("rm -rf /tmp/eos-fusex-unit-tests/"), 0);
  ASSERT_EQ(system("mkdir /tmp/eos-fusex-unit-tests/"), 0);

  ASSERT_EQ(system("touch /tmp/eos-fusex-unit-tests/random-file"), 0);
  ASSERT_EQ(system("touch /tmp/eos-fusex-unit-tests/eos-fusex-uuid-store-asdf"), 0);

  UuidStore store("/tmp/eos-fusex-unit-tests/");

  // ensure files starting with "eos-fusex-uuid-store-" were cleared out, but not
  // any others
  struct stat repostat;
  ASSERT_EQ(::stat("/tmp/eos-fusex-unit-tests/random-file", &repostat), 0);
  ASSERT_EQ(::stat("/tmp/eos-fusex-unit-tests/eos-fusex-uuid-store-asdf", &repostat), -1);

  std::string path = store.put("pickles");
  std::cout << path << std::endl;
  std::string contents;
  ASSERT_TRUE(readFile(path, contents));
  ASSERT_EQ(contents, "pickles");
}
