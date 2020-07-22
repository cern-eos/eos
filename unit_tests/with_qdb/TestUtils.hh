//------------------------------------------------------------------------------
// File: TestUtils.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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
#pragma once

#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include <gtest/gtest.h>
#include <memory>
#include <chrono>
#include <thread>

#define RETRY_ASSERT_TRUE_3(cond, retry, waitInterval) { \
  bool ok = false; \
  size_t nretries = 0; \
  while(nretries++ < retry) { \
    std::this_thread::sleep_for(std::chrono::milliseconds(waitInterval)); \
    if((cond)) { \
      std::cerr << "Condition '" << #cond << "' is true after " << nretries << " attempts" << std::endl; \
      ok = true; \
      break; \
    } \
  } \
  if(!ok) { ASSERT_TRUE(cond) << " - failure after " << nretries << " retries "; } \
}

// retry every 1 ms, 5000 max retries
#define RETRY_ASSERT_TRUE(cond) RETRY_ASSERT_TRUE_3(cond, 5000, 1)

namespace qclient {
  class QClient;
  class SharedManager;
}

namespace eos
{

namespace mq {
  class MessagingRealm;
}

//------------------------------------------------------------------------------
//! Class FlushAllOnConstruction
//------------------------------------------------------------------------------
class FlushAllOnConstruction
{
public:
  FlushAllOnConstruction(const QdbContactDetails& cd);

private:
  QdbContactDetails contactDetails;
};

//------------------------------------------------------------------------------
//! Test fixture providing generic utilities and initialization / destruction
//! boilerplate code
//------------------------------------------------------------------------------
class UnitTestsWithQDBFixture : public ::testing::Test
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  UnitTestsWithQDBFixture();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~UnitTestsWithQDBFixture();

  //----------------------------------------------------------------------------
  //! Make QClient object
  //----------------------------------------------------------------------------
  std::unique_ptr<qclient::QClient> makeQClient() const;

  //----------------------------------------------------------------------------
  //! Get MessagingRealm object, lazy init
  //----------------------------------------------------------------------------
  mq::MessagingRealm* getMessagingRealm(int tag = 0);

  //----------------------------------------------------------------------------
  //! Get SharedManager object, lazy init
  //----------------------------------------------------------------------------
  qclient::SharedManager* getSharedManager(int tag = 0);

  //----------------------------------------------------------------------------
  //! Retrieve contact details
  //----------------------------------------------------------------------------
  QdbContactDetails getContactDetails() const;


private:
  QdbContactDetails mContactDetails;
  std::unique_ptr<FlushAllOnConstruction> mFlushGuard;

  std::map<int, std::unique_ptr<mq::MessagingRealm>> mMessagingRealms;
  std::map<int, std::unique_ptr<qclient::SharedManager>> mSharedManagers;

};

}

