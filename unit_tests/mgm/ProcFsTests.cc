//------------------------------------------------------------------------------
// File: ProcFsTest.cc
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
#include "mgm/proc/proc_fs.hh"

//------------------------------------------------------------------------------
// Test entity classification
//------------------------------------------------------------------------------
TEST(ProcFs, EntityClassification)
{
  using namespace eos::mgm;
  XrdOucString out, err;
  ASSERT_TRUE(EntityType::FS == get_entity_type("1234", out, err));
  ASSERT_TRUE(EntityType::GROUP == get_entity_type("default.3", out, err));
  ASSERT_TRUE(EntityType::SPACE == get_entity_type("default", out, err));
  ASSERT_TRUE(EntityType::UNKNOWN == get_entity_type("2.default", out, err));
  ASSERT_TRUE(EntityType::UNKNOWN == get_entity_type("spare.default", out, err));
  ASSERT_TRUE(EntityType::UNKNOWN == get_entity_type("spare.4default", out, err));
}

//------------------------------------------------------------------------------
// Test fs mv operation classification
//------------------------------------------------------------------------------
TEST(ProcFs, MvOpClassification)
{
  using namespace eos::mgm;
  XrdOucString out, err;
  ASSERT_TRUE(MvOpType::FS_2_GROUP == get_operation_type("1234", "default.23",
              out, err));
  ASSERT_TRUE(MvOpType::FS_2_SPACE == get_operation_type("3214", "default", out,
              err));
  ASSERT_TRUE(MvOpType::GRP_2_SPACE == get_operation_type("meyrin.65", "default",
              out, err));
  ASSERT_TRUE(MvOpType::SPC_2_SPACE == get_operation_type("meyrin", "default",
              out, err));
  ASSERT_TRUE(MvOpType::UNKNOWN == get_operation_type("meyrin.65", "default.12",
              out, err));
  ASSERT_TRUE(MvOpType::UNKNOWN == get_operation_type("meyrin", "default.78", out,
              err));
  ASSERT_TRUE(MvOpType::UNKNOWN == get_operation_type("meyrin.53", "78", out,
              err));
  ASSERT_TRUE(MvOpType::UNKNOWN == get_operation_type("meyrin", "8176", out,
              err));
}
