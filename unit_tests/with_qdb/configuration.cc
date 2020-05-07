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
#include <qclient/QClient.hh>

using namespace eos;
class VariousTests : public eos::UnitTestsWithQDBFixture {};

TEST_F(VariousTests, Ping) {
  std::unique_ptr<qclient::QClient> qcl = makeQClient();

  qclient::redisReplyPtr reply = qcl->exec("PING").get();
  ASSERT_EQ(qclient::describeRedisReply(reply),
    "PONG");
}
