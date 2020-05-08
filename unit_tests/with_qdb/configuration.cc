/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes - CERN
//! @brief Tests related to the configuration
//------------------------------------------------------------------------------

#include "unit_tests/with_qdb/TestUtils.hh"
#include "mgm/config/QuarkConfigHandler.hh"

#include <qclient/QClient.hh>

using namespace eos;
class VariousTests : public eos::UnitTestsWithQDBFixture {};
class ConfigurationTests : public eos::UnitTestsWithQDBFixture {};

TEST_F(VariousTests, Ping) {
  std::unique_ptr<qclient::QClient> qcl = makeQClient();

  qclient::redisReplyPtr reply = qcl->exec("PING").get();
  ASSERT_EQ(qclient::describeRedisReply(reply),
    "PONG");
}

TEST_F(ConfigurationTests, BasicFetch) {
  std::unique_ptr<qclient::QClient> qcl = makeQClient();

  qclient::redisReplyPtr reply = qcl->exec("HSET", "eos-config:default", "a", "b").get();
  ASSERT_EQ(qclient::describeRedisReply(reply), "(integer) 1");

  eos::mgm::QuarkConfigHandler ch(getContactDetails());

  std::map<std::string, std::string> cfmap;
  ASSERT_TRUE(ch.fetchConfiguration("default", cfmap));

  ASSERT_EQ(cfmap.size(), 1u);
  ASSERT_EQ(cfmap["a"], "b");
}

TEST_F(ConfigurationTests, HashKeys) {
  ASSERT_EQ(eos::mgm::QuarkConfigHandler::formHashKey("default"), "eos-config:default");
  ASSERT_EQ(eos::mgm::QuarkConfigHandler::formBackupHashKey("default", 1588936606), "eos-config-backup:default-20200508111646");
}
