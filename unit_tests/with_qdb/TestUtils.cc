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
// author: Georgios Bitzes <georgios.bitzes@cern.ch>
// desc:   Test utilities
//------------------------------------------------------------------------------

#include "TestUtils.hh"
#include "mq/MessagingRealm.hh"
#include <qclient/QClient.hh>
#include <qclient/shared/SharedManager.hh>
#include <fstream>

namespace eos{

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FlushAllOnConstruction::FlushAllOnConstruction(const QdbContactDetails& cd)
  : contactDetails(cd)
{
  qclient::QClient qcl(cd.members, cd.constructOptions());
  qcl.exec("FLUSHALL").get();
  qcl.exec("SET", "QDB-INSTANCE-FOR-EOS-NS-TESTS", "YES");
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
UnitTestsWithQDBFixture::UnitTestsWithQDBFixture() {

  // Connection parameters
  std::string qdb_hostport = getenv("EOS_QUARKDB_HOSTPORT") ?
                             getenv("EOS_QUARKDB_HOSTPORT") : "localhost:9999";
  std::string qdb_passwd = getenv("EOS_QUARKDB_PASSWD") ?
                           getenv("EOS_QUARKDB_PASSWD") : "";
  std::string qdb_passwd_file = getenv("EOS_QUARKDB_PASSWD_FILE") ?
                                getenv("EOS_QUARKDB_PASSWD_FILE") : "/etc/eos.keytab";

  if (qdb_passwd.empty() && !qdb_passwd_file.empty()) {
    // Read the password from the file
    std::ifstream f(qdb_passwd_file);
    std::stringstream buff;
    buff << f.rdbuf();
    qdb_passwd = buff.str();
  }

  mContactDetails = QdbContactDetails(qclient::Members::fromString(qdb_hostport),
    qdb_passwd);
  mFlushGuard.reset(new FlushAllOnConstruction(mContactDetails));
}

//------------------------------------------------------------------------------
//! Destructor
//------------------------------------------------------------------------------
UnitTestsWithQDBFixture::~UnitTestsWithQDBFixture() {}

//------------------------------------------------------------------------------
//! Make QClient object
//------------------------------------------------------------------------------
std::unique_ptr<qclient::QClient> UnitTestsWithQDBFixture::makeQClient() const {
  return std::unique_ptr<qclient::QClient>(
           new qclient::QClient(mContactDetails.members, mContactDetails.constructOptions())
  );
}

//------------------------------------------------------------------------------
// Get MessagingRealm object, lazy init
//------------------------------------------------------------------------------
mq::MessagingRealm* UnitTestsWithQDBFixture::getMessagingRealm() {
  if(!mMessagingRealm) {
    mMessagingRealm.reset(new mq::MessagingRealm(nullptr, nullptr,
      nullptr, getSharedManager()));
  }

  return mMessagingRealm.get();
}

//------------------------------------------------------------------------------
// Get SharedManager object, lazy init
//------------------------------------------------------------------------------
qclient::SharedManager* UnitTestsWithQDBFixture::getSharedManager() {
  if(!mSharedManager) {
    mSharedManager.reset(new qclient::SharedManager(mContactDetails.members,
      mContactDetails.constructSubscriptionOptions()));
  }

  return mSharedManager.get();
}

//------------------------------------------------------------------------------
// Retrieve contact details
//------------------------------------------------------------------------------
QdbContactDetails UnitTestsWithQDBFixture::getContactDetails() const {
  return mContactDetails;
}

}
