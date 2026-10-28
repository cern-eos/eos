//------------------------------------------------------------------------------
// File: FmdTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "common/Fmd.hh"
#include "gtest/gtest.h"

using eos::common::FmdHelper;

//------------------------------------------------------------------------------
// The stripe checksum is carried over the env encoding returned by the FST
// getfmd query, which the drain uses to decide on a RAIN stripe copy
//------------------------------------------------------------------------------
TEST(Fmd, EnvRoundTripStripeChecksum)
{
  FmdHelper fmd(0x1234, 7);
  fmd.mProtoFmd.set_lid(0x40100642);
  fmd.mProtoFmd.set_size(5 * 1024 * 1024);
  fmd.mProtoFmd.set_checksum("1a2b3c4d");
  fmd.mProtoFmd.set_stripechecksum("deadbeef");
  auto env = fmd.FmdToEnv();
  ASSERT_STREQ("deadbeef", env->Get("stripechecksum"));
  FmdHelper decoded;
  ASSERT_TRUE(eos::common::EnvToFstFmd(*env, decoded));
  ASSERT_EQ(0x1234u, decoded.mProtoFmd.fid());
  ASSERT_EQ(7u, decoded.mProtoFmd.fsid());
  ASSERT_EQ(0x40100642u, decoded.mProtoFmd.lid());
  ASSERT_EQ("1a2b3c4d", decoded.mProtoFmd.checksum());
  ASSERT_EQ("deadbeef", decoded.mProtoFmd.stripechecksum());
}

TEST(Fmd, EnvRoundTripNoStripeChecksum)
{
  FmdHelper fmd(0x1234, 7);
  auto env = fmd.FmdToEnv();
  ASSERT_EQ(nullptr, env->Get("stripechecksum"));
  FmdHelper decoded;
  ASSERT_TRUE(eos::common::EnvToFstFmd(*env, decoded));
  ASSERT_FALSE(decoded.mProtoFmd.has_stripechecksum());
  ASSERT_TRUE(decoded.mProtoFmd.stripechecksum().empty());
}
