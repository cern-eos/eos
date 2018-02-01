/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include <qclient/QClient.hh>
#include <gtest/gtest.h>
#include <sstream>
#include "TestUtils.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"

EOSNSTESTING_BEGIN

FlushAllOnDestruction::FlushAllOnDestruction(const qclient::Members &mbr)
: members(mbr) {
  qclient::RetryStrategy strategy {true, std::chrono::seconds(10)};
  qclient::QClient qcl(members, true, strategy);
  qcl.exec("FLUSHALL").get();
}

FlushAllOnDestruction::~FlushAllOnDestruction() {
    qclient::RetryStrategy strategy {true, std::chrono::seconds(10)};
    qclient::QClient qcl(members, true, strategy);
    qcl.exec("FLUSHALL").get();
  }



NsTestsFixture::NsTestsFixture() {
  FileMDSvc::OverrideNumberOfBuckets(128);
  ContainerMDSvc::OverrideNumberOfBuckets(128);

  srandom(time(nullptr));
  testconfig = {
    {"qdb_cluster", "localhost:7778"},
    {"qdb_flusher_md", "tests_md"},
    {"qdb_flusher_quota", "tests_quota"}
  };

  guard.reset(new eos::ns::testing::FlushAllOnDestruction(qclient::Members::fromString(testconfig["qdb_cluster"])));
}

NsTestsFixture::~NsTestsFixture() {
  shut_down_everything();

  // Restore default values
  FileMDSvc::OverrideNumberOfBuckets();
  ContainerMDSvc::OverrideNumberOfBuckets();
}

qclient::Members NsTestsFixture::getMembers() {
  return qclient::Members::fromString(testconfig["qdb_cluster"]);
}

void NsTestsFixture::initServices() {
  if(containerSvcPtr) {
    // Already initialized.
    return;
  }

  containerSvcPtr.reset(new eos::ContainerMDSvc());
  fileSvcPtr.reset(new eos::FileMDSvc());
  viewPtr.reset(new eos::HierarchicalView());

  fileSvcPtr->setContMDService(containerSvcPtr.get());
  containerSvcPtr->setFileMDService(fileSvcPtr.get());

  fileSvcPtr->configure(testconfig);
  containerSvcPtr->configure(testconfig);

  viewPtr->setContainerMDSvc(containerSvcPtr.get());
  viewPtr->setFileMDSvc(fileSvcPtr.get());
  viewPtr->configure(testconfig);
  viewPtr->initialize();
}

eos::IContainerMDSvc* NsTestsFixture::containerSvc() {
  initServices();
  return containerSvcPtr.get();
}

eos::IFileMDSvc* NsTestsFixture::fileSvc() {
  initServices();
  return fileSvcPtr.get();
}

eos::IView* NsTestsFixture::view() {
  initServices();
  return viewPtr.get();
}

void NsTestsFixture::shut_down_everything() {
  if(viewPtr) {
    viewPtr->finalize();
  }

  viewPtr.reset();
  fileSvcPtr.reset();
  containerSvcPtr.reset();
}

std::unique_ptr<qclient::QClient> NsTestsFixture::createQClient() {
  qclient::RetryStrategy retryStrategy {true, std::chrono::seconds(60) };
  return std::unique_ptr<qclient::QClient>(
    new qclient::QClient(getMembers(), true, retryStrategy)
  );
}

EOSNSTESTING_END
