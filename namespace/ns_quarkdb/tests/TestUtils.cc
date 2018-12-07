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
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"

EOSNSTESTING_BEGIN

FlushAllOnConstruction::FlushAllOnConstruction(const QdbContactDetails& cd)
  : contactDetails(cd)
{
  qclient::QClient qcl(cd.members, cd.constructOptions());
  qcl.exec("FLUSHALL").get();
  qcl.exec("SET", "QDB-INSTANCE-FOR-EOS-NS-TESTS", "YES");
}

FlushAllOnConstruction::~FlushAllOnConstruction() { }

NsTestsFixture::NsTestsFixture()
{
  RequestBuilder::OverrideNumberOfFileBuckets(128);
  RequestBuilder::OverrideNumberOfContainerBuckets(128);
  srandom(time(nullptr));
  testconfig = {
    {"qdb_cluster", "localhost:9999"},
    {"qdb_flusher_md", "tests_md"},
    {"qdb_flusher_quota", "tests_quota"},
    {"qdb_password", "turtles_turtles_turtles_turtles_turtles"}
  };
  guard.reset(new eos::ns::testing::FlushAllOnConstruction(getContactDetails()));
}

NsTestsFixture::~NsTestsFixture()
{
  shut_down_everything();
}

QdbContactDetails NsTestsFixture::getContactDetails()
{
  return QdbContactDetails(getMembers(), testconfig["qdb_password"]);
}

qclient::Members NsTestsFixture::getMembers()
{
  return qclient::Members::fromString(testconfig["qdb_cluster"]);
}

void NsTestsFixture::setSizeMapper(IQuotaStats::SizeMapper mapper)
{
  sizeMapper = mapper;
}

void NsTestsFixture::initServices()
{
  if (containerSvcPtr) {
    // Already initialized.
    return;
  }

  containerSvcPtr.reset(new eos::QuarkContainerMDSvc());
  fileSvcPtr.reset(new eos::QuarkFileMDSvc());
  viewPtr.reset(new eos::QuarkHierarchicalView());
  fsViewPtr.reset(new eos::QuarkFileSystemView());
  fileSvcPtr->setContMDService(containerSvcPtr.get());
  containerSvcPtr->setFileMDService(fileSvcPtr.get());
  fileSvcPtr->configure(testconfig);
  containerSvcPtr->configure(testconfig);
  fsViewPtr->configure(testconfig);
  viewPtr->setContainerMDSvc(containerSvcPtr.get());
  viewPtr->setFileMDSvc(fileSvcPtr.get());
  viewPtr->configure(testconfig);

  if (sizeMapper) {
    view()->getQuotaStats()->registerSizeMapper(sizeMapper);
  }

  viewPtr->initialize();
  fileSvcPtr->addChangeListener(fsViewPtr.get());
}

eos::IContainerMDSvc* NsTestsFixture::containerSvc()
{
  initServices();
  return containerSvcPtr.get();
}

eos::IFileMDSvc* NsTestsFixture::fileSvc()
{
  initServices();
  return fileSvcPtr.get();
}

eos::IView* NsTestsFixture::view()
{
  initServices();
  return viewPtr.get();
}

eos::IFsView* NsTestsFixture::fsview()
{
  initServices();
  return fsViewPtr.get();
}

qclient::QClient& NsTestsFixture::qcl()
{
  if (!qclPtr) {
    qclPtr = createQClient();
  }

  return *qclPtr.get();
}

std::shared_ptr<eos::MetadataFlusher> NsTestsFixture::mdFlusher()
{
  if (!mdFlusherPtr) {
    mdFlusherPtr = eos::MetadataFlusherFactory::getInstance(
                     testconfig["qdb_flusher_md"], getContactDetails());
  }

  return mdFlusherPtr;
}

std::shared_ptr<eos::MetadataFlusher> NsTestsFixture::quotaFlusher()
{
  if (!quotaFlusherPtr) {
    quotaFlusherPtr = eos::MetadataFlusherFactory::getInstance(
                        testconfig["qdb_flusher_quota"], getContactDetails());
  }

  return quotaFlusherPtr;
}

void NsTestsFixture::shut_down_everything()
{
  if (viewPtr) {
    viewPtr->finalize();
  }

  if (fsViewPtr) {
    fsViewPtr->finalize();
  }

  viewPtr.reset();
  fileSvcPtr.reset();
  containerSvcPtr.reset();
  qclPtr.reset();

  if (mdFlusherPtr) {
    mdFlusherPtr->synchronize();
    mdFlusherPtr = nullptr;
  }

  if (quotaFlusherPtr) {
    quotaFlusherPtr->synchronize();
    quotaFlusherPtr = nullptr;
  }
}

std::unique_ptr<qclient::QClient> NsTestsFixture::createQClient()
{
  QdbContactDetails cd = getContactDetails();
  return std::unique_ptr<qclient::QClient>(
           new qclient::QClient(cd.members, cd.constructOptions())
         );
}

void NsTestsFixture::populateDummyData1()
{
  // Be careful when making changes! Lots of tests depend on this structure,
  // you should probably create a new dummy dataset.
  view()->createContainer("/eos/d1/d2/d3/d4/d5/d6/d7/d8/", true);
  view()->createContainer("/eos/d1/d2-1/", true);
  view()->createContainer("/eos/d1/d2-2/", true);
  view()->createContainer("/eos/d1/d2-3/", true);
  view()->createContainer("/eos/d1/d2/d3-1/", true);
  view()->createContainer("/eos/d1/d2/d3-2/", true);
  view()->createContainer("/eos/d2/d3-1", true);
  view()->createContainer("/eos/d2/d3-2", true);
  view()->createContainer("/eos/d3/", true);
  view()->createFile("/eos/d1/f1", true);
  view()->createFile("/eos/d1/f2", true);
  view()->createFile("/eos/d1/f3", true);
  view()->createFile("/eos/d1/f4", true);
  view()->createFile("/eos/d1/f5", true);
  view()->createFile("/eos/d2/d3-2/my-file", true);
  view()->createContainer("/eos/d2/d4/1/2/3/4/5/6/7/", true);
  view()->createFile("/eos/d2/asdf1", true);
  view()->createFile("/eos/d2/asdf2", true);
  view()->createFile("/eos/d2/asdf3", true);
  view()->createFile("/eos/d2/b", true);
  view()->createFile("/eos/d2/zzzzz1", true);
  view()->createFile("/eos/d2/zzzzz2", true);
  view()->createFile("/eos/d2/zzzzz3", true);
  view()->createFile("/eos/d2/zzzzz4", true);
  view()->createFile("/eos/d2/zzzzz5", true);
  view()->createFile("/eos/d2/zzzzz6", true);
  mdFlusher()->synchronize();
}



EOSNSTESTING_END
