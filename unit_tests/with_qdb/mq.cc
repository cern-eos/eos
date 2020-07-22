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
//! @brief Tests related to the MQ
//------------------------------------------------------------------------------

#include "unit_tests/with_qdb/TestUtils.hh"
#include "common/Assert.hh"
#include "mq/SharedQueueWrapper.hh"

using namespace eos;
class SharedDeque : public eos::UnitTestsWithQDBFixture {};

TEST_F(SharedDeque, BasicSanity) {
  mq::MessagingRealm *realm1 = getMessagingRealm(1);
  mq::MessagingRealm *realm2 = getMessagingRealm(2);
  mq::MessagingRealm *realm3 = getMessagingRealm(2);

  common::TransferQueueLocator locator("/eos/example-host.cern.ch:1095/fst", "some-tag");

  mq::SharedQueueWrapper queue1(realm1, locator, true);
  mq::SharedQueueWrapper queue2(realm2, locator, true);
  mq::SharedQueueWrapper queue3(realm3, locator, true);

  ASSERT_EQ(queue2.size(), 0u);
  ASSERT_TRUE(queue1.push_back("chickens"));
  ASSERT_EQ(queue1.size(), 1u);

  RETRY_ASSERT_TRUE(queue2.size() != 0u);
  RETRY_ASSERT_TRUE(queue3.size() != 0u);

  std::string out = queue2.getItem();
  ASSERT_EQ(out, "chickens");
  ASSERT_EQ(queue2.size(), 0u);

  RETRY_ASSERT_TRUE(queue1.size() == 0u);
  RETRY_ASSERT_TRUE(queue3.size() == 0u);
}

