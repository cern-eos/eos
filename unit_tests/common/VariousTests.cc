//------------------------------------------------------------------------------
// File: VariousTests.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "common/Statfs.hh"
#include "Namespace.hh"
#include "gtest/gtest.h"
#include <list>

EOSCOMMONTESTING_BEGIN

TEST(StatFs, BasicSanity)
{
  std::unique_ptr<eos::common::Statfs> statfs =
    eos::common::Statfs::DoStatfs("/");
  ASSERT_NE(statfs, nullptr);

  statfs = eos::common::Statfs::DoStatfs("aaaaaaaa");
  ASSERT_EQ(statfs, nullptr);
}

EOSCOMMONTESTING_END
