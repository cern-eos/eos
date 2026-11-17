//------------------------------------------------------------------------------
// File: AdminCmdRoleTests.cc
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

#include "common/Constants.hh"
#include "common/VirtualIdentity.hh"
#include "mgm/proc/admin/ConfigCmd.hh"
#include "mgm/proc/admin/FsckCmd.hh"
#include <gtest/gtest.h>

namespace {

eos::common::VirtualIdentity
Identity(uid_t uid, gid_t gid)
{
  eos::common::VirtualIdentity vid;
  vid.uid = uid;
  vid.gid = gid;
  vid.allowed_uids.insert(uid);
  vid.allowed_gids.insert(gid);
  return vid;
}

} // namespace

//------------------------------------------------------------------------------
// config ls and dump are read-only and need an admin, not root
//------------------------------------------------------------------------------
TEST(AdminCmdRole, configReadsNeedAdminOnly)
{
  using eos::console::ConfigProto;
  using eos::mgm::ConfigCmd;
  const auto root = eos::common::VirtualIdentity::Root();
  const auto admin = Identity(12345, eos::common::ADM_GID);
  const auto user = Identity(12346, 12346);

  for (const auto subcmd : {
         ConfigProto::kLs, ConfigProto::kDump
       }) {
    EXPECT_TRUE(ConfigCmd::IsAllowed(subcmd, root));
    EXPECT_TRUE(ConfigCmd::IsAllowed(subcmd, admin));
    EXPECT_FALSE(ConfigCmd::IsAllowed(subcmd, user));
  }

  for (const auto subcmd : {
         ConfigProto::kReset, ConfigProto::kExp, ConfigProto::kSave,
         ConfigProto::kLoad, ConfigProto::kChangelog
       }) {
    EXPECT_TRUE(ConfigCmd::IsAllowed(subcmd, root));
    EXPECT_FALSE(ConfigCmd::IsAllowed(subcmd, admin));
    EXPECT_FALSE(ConfigCmd::IsAllowed(subcmd, user));
  }
}

//------------------------------------------------------------------------------
// fsck stat needs an admin, report stays open, the rest needs root
//------------------------------------------------------------------------------
TEST(AdminCmdRole, fsckStatNeedsAdminOnly)
{
  using eos::console::FsckProto;
  using eos::mgm::FsckCmd;
  const auto root = eos::common::VirtualIdentity::Root();
  const auto admin = Identity(12345, eos::common::ADM_GID);
  const auto user = Identity(12346, 12346);
  EXPECT_TRUE(FsckCmd::IsAllowed(FsckProto::kStat, root));
  EXPECT_TRUE(FsckCmd::IsAllowed(FsckProto::kStat, admin));
  EXPECT_FALSE(FsckCmd::IsAllowed(FsckProto::kStat, user));
  EXPECT_TRUE(FsckCmd::IsAllowed(FsckProto::kReport, user));

  for (const auto subcmd : {
         FsckProto::kConfig, FsckProto::kRepair, FsckProto::kCleanOrphans
       }) {
    EXPECT_TRUE(FsckCmd::IsAllowed(subcmd, root));
    EXPECT_FALSE(FsckCmd::IsAllowed(subcmd, admin));
    EXPECT_FALSE(FsckCmd::IsAllowed(subcmd, user));
  }
}
