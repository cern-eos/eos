//------------------------------------------------------------------------------
// File: VidTests.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "common/Mapping.hh"
#include "common/VirtualIdentity.hh"
#include "mgm/vid/Vid.hh"
#include <gtest/gtest.h>

//------------------------------------------------------------------------------
// Fixture cleaning up the global sudoer map around each test
//------------------------------------------------------------------------------
class VidSudoerTest : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    ClearSudoerMap();
  }

  void
  TearDown() override
  {
    ClearSudoerMap();
  }

  void
  ClearSudoerMap()
  {
    eos::common::RWMutexWriteLock lock(eos::common::Mapping::gMapMutex);
    eos::common::Mapping::gSudoerMap.clear();
  }

  //----------------------------------------------------------------------------
  //! Check if given uid is in the sudoer map
  //----------------------------------------------------------------------------
  bool
  IsSudoer(uid_t uid)
  {
    eos::common::RWMutexReadLock lock(eos::common::Mapping::gMapMutex);
    return (eos::common::Mapping::gSudoerMap.count(uid) != 0);
  }
};

//------------------------------------------------------------------------------
// A user name which can not be resolved must never end up as sudoer under
// the "nobody" uid
//------------------------------------------------------------------------------
TEST_F(VidSudoerTest, UnknownUserNameNotSudoer)
{
  const std::string input = "eos.rgid=0&eos.ruid=0&mgm.cmd=vid&mgm.subcmd=set"
                            "&mgm.vid.cmd=membership&mgm.vid.key=xyxabcde:root"
                            "&mgm.vid.source.uid=xyzabcde&mgm.vid.target.sudo=true";
  ASSERT_FALSE(eos::mgm::Vid::Set(input.c_str(), false));
  ASSERT_FALSE(IsSudoer(eos::common::VirtualIdentity::kNobodyUid));
}

//------------------------------------------------------------------------------
// Explicitly asking for the "nobody" user to become sudoer is also refused
//------------------------------------------------------------------------------
TEST_F(VidSudoerTest, ExplicitNobodyNotSudoer)
{
  const std::string input = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=membership"
                            "&mgm.vid.key=nobody:root&mgm.vid.source.uid=nobody"
                            "&mgm.vid.target.sudo=true";
  ASSERT_FALSE(eos::mgm::Vid::Set(input.c_str(), false));
  ASSERT_FALSE(IsSudoer(eos::common::VirtualIdentity::kNobodyUid));
}

//------------------------------------------------------------------------------
// The numeric uid of "nobody" is refused just the same
//------------------------------------------------------------------------------
TEST_F(VidSudoerTest, NumericNobodyUidNotSudoer)
{
  const std::string input =
      "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=membership&mgm.vid.key=" +
      std::to_string(eos::common::VirtualIdentity::kNobodyUid) + ":root" +
      "&mgm.vid.source.uid=" + std::to_string(eos::common::VirtualIdentity::kNobodyUid) +
      "&mgm.vid.target.sudo=true";
  ASSERT_FALSE(eos::mgm::Vid::Set(input.c_str(), false));
  ASSERT_FALSE(IsSudoer(eos::common::VirtualIdentity::kNobodyUid));
}

//------------------------------------------------------------------------------
// A missing source uid must not silently grant sudo rights to nobody
//------------------------------------------------------------------------------
TEST_F(VidSudoerTest, MissingSourceUidNotSudoer)
{
  const std::string input = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=membership"
                            "&mgm.vid.key=xyzabcde:root&mgm.vid.target.sudo=true";
  ASSERT_FALSE(eos::mgm::Vid::Set(input.c_str(), false));
  ASSERT_FALSE(IsSudoer(eos::common::VirtualIdentity::kNobodyUid));
}

//------------------------------------------------------------------------------
// A resolvable uid is still added and can be removed again
//------------------------------------------------------------------------------
TEST_F(VidSudoerTest, ValidUidIsSudoer)
{
  const uid_t uid = 12345;
  ASSERT_NE(uid, eos::common::VirtualIdentity::kNobodyUid);
  const std::string add = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=membership"
                          "&mgm.vid.key=12345:root&mgm.vid.source.uid=12345"
                          "&mgm.vid.target.sudo=true";
  ASSERT_TRUE(eos::mgm::Vid::Set(add.c_str(), false));
  ASSERT_TRUE(IsSudoer(uid));
  const std::string rm = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=membership"
                         "&mgm.vid.key=12345:root&mgm.vid.source.uid=12345"
                         "&mgm.vid.target.sudo=false";
  ASSERT_TRUE(eos::mgm::Vid::Set(rm.c_str(), false));
  ASSERT_FALSE(IsSudoer(uid));
}

//------------------------------------------------------------------------------
// Removing sudo rights for an unresolvable user name is still allowed so that
// stale entries can be cleaned up
//------------------------------------------------------------------------------
TEST_F(VidSudoerTest, UnknownUserNameRemovalAllowed)
{
  {
    eos::common::RWMutexWriteLock lock(eos::common::Mapping::gMapMutex);
    eos::common::Mapping::gSudoerMap[eos::common::VirtualIdentity::kNobodyUid] = 1;
  }
  const std::string input = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=membership"
                            "&mgm.vid.key=xyzabce:root&mgm.vid.source.uid=xyzabcde"
                            "&mgm.vid.target.sudo=false";
  ASSERT_TRUE(eos::mgm::Vid::Set(input.c_str(), false));
  ASSERT_FALSE(IsSudoer(eos::common::VirtualIdentity::kNobodyUid));
}
