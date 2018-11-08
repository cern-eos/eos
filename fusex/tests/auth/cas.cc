//------------------------------------------------------------------------------
// File: cas.cc
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

#include "auth/ContentAddressableStore.hh"
#include "auth/Utils.hh"
#include "common/SymKeys.hh"
#include "gtest/gtest.h"

TEST(ContentAddressableStore, BasicSanity) {
  ASSERT_EQ(system("rm -rf /tmp/eos-fusex-unit-tests/"), 0);
  ASSERT_EQ(system("mkdir /tmp/eos-fusex-unit-tests/"), 0);

  ContentAddressableStore store("/tmp/eos-fusex-unit-tests/",
	std::chrono::milliseconds(100),
	false);

  ASSERT_EQ(store.put("pickles"), "/tmp/eos-fusex-unit-tests/eos-fusex-store-3614e3639c0a98b1006a50ffe5744f054cf4499592fe8ef1b339601208e80066");
  std::string contents;
  ASSERT_TRUE(readFile("/tmp/eos-fusex-unit-tests/eos-fusex-store-3614e3639c0a98b1006a50ffe5744f054cf4499592fe8ef1b339601208e80066",
	contents));
  ASSERT_EQ(contents, "pickles");

  // TODO: test expiration
}

TEST(ContentAddressableStore, FakedResponses) {
  ContentAddressableStore store("/dev/null",
	std::chrono::milliseconds(100), /* doesn't matter */
	true);

  ASSERT_EQ(store.put("pickles"), "/dev/null/eos-fusex-store-3614e3639c0a98b1006a50ffe5744f054cf4499592fe8ef1b339601208e80066");
  ASSERT_EQ(eos::common::SymKey::Sha256("pickles"), "3614e3639c0a98b1006a50ffe5744f054cf4499592fe8ef1b339601208e80066");
}
