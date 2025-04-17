//------------------------------------------------------------------------------
// File: QuarkDBConfigTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
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

#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "mgm/config/QuarkDBConfigEngine.hh"
#undef IN_TEST_HARNESS

using namespace eos::mgm;

TEST(QuarkDBConfig, BasicTests)
{
  QuarkDBConfigEngine cfg;
  setenv("EOS_MGM_CONFIG_CLEANUP", "1", 1);
  cfg.sConfigDefinitions = {};
  ASSERT_FALSE(cfg.RemoveUnusedNodes());
  cfg.sConfigDefinitions = {
    {"global:/config/eos/space/default#atime", "604800"},
    {"global:/config/eos/space/default#autorepair", "off"},
    {"global:/config/eos/space/default#balancer", "on"}
  };
  ASSERT_FALSE(cfg.RemoveUnusedNodes());
  // A node with status of is not removed!
  cfg.sConfigDefinitions =  {
    {"global:/config/eos/space/default#atime", "604800"},
    {"global:/config/eos/space/default#autorepair", "off"},
    {"global:/config/eos/space/default#balancer", "on"},
    {"global:/config/eos/node/st-096-100gb-ip315-0f706.cern.ch:1095#stat.hostport", "st-096-100gb-ip315-0f706.cern.ch:1095"},
    {"global:/config/eos/node/st-096-100gb-ip315-0f706.cern.ch:1095#status", "on"}
  };
  ASSERT_FALSE(cfg.RemoveUnusedNodes());
  // A node with status of and no file systems should be removed
  cfg.sConfigDefinitions["global:/config/eos/node/st-096-100gb-ip315-0f706.cern.ch:1095#status"]
    = "off";
  ASSERT_TRUE(cfg.RemoveUnusedNodes());
  // A node with file systems and status off or on should not be removed
  cfg.sConfigDefinitions =  {
    {"global:/config/eos/space/default#atime", "604800"},
    {"global:/config/eos/space/default#autorepair", "off"},
    {"global:/config/eos/space/default#balancer", "on"},
    {"global:/config/eos/node/st-096-100gb-ip315-0f706.cern.ch:1095#stat.hostport", "st-096-100gb-ip315-0f706.cern.ch:1095"},
    {"global:/config/eos/node/st-096-100gb-ip315-0f706.cern.ch:1095#status", "on"},
    {"fs:/eos/st-096-100gb-ip315-0f706.cern.ch:1095/fst/data95", "bootcheck=0 configstatus=rw"},
    {"fs:/eos/st-096-100gb-ip315-0f706.cern.ch:1095/fst/data96", "bootcheck=0 configstatus=rw"},
  };
  ASSERT_FALSE(cfg.RemoveUnusedNodes());
  cfg.sConfigDefinitions["global:/config/eos/node/st-096-100gb-ip315-0f706.cern.ch:1095#status"]
    = "off";
  ASSERT_FALSE(cfg.RemoveUnusedNodes());
  // Add some new entries in the configuration map
  cfg.sConfigDefinitions["global:/config/eos/space/default#lru"] = "off";
  cfg.sConfigDefinitions["global:/config/eos/space/default#lru.interval"] =
    "14400";
  ASSERT_FALSE(cfg.RemoveUnusedNodes());
  unsetenv("EOS_MGM_CONFIG_CLEANUP");
}
