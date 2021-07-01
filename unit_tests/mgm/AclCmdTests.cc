//------------------------------------------------------------------------------
// File: AclTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "gtest/gtest.h"
#include "mgm/proc/user/AclCmd.hh"

EOSMGMNAMESPACE_BEGIN

TEST(AclCmd, RuleMap)
{
  using namespace eos::mgm;
  RuleMap expect_map = {
    { "u:99", 0b011111111111}, { "u:0", 0b01010101010}
  };
  RuleMap result_map;
  const std::string acl = "u:99:rwxm!m!d+d!u+uqc,u:0:wm!d!uq";
  AclCmd::GenerateRuleMap(acl, result_map);
  ASSERT_EQ(result_map.size(), expect_map.size());
  ASSERT_EQ(result_map, expect_map);
}

EOSMGMNAMESPACE_END
