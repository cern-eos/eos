//------------------------------------------------------------------------------
// File: NsTests.cc
// Author: Cedric Caffy <cedric.caffy@cern.ch>
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


#include "NsTests.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include <sstream>
#include <fstream>
#include <qclient/QClient.hh>

EOSNSTESTING_BEGIN

FlushAllOnConstruction::FlushAllOnConstruction(const QdbContactDetails& cd)
    : contactDetails(cd)
{
  qclient::QClient qcl(cd.members, cd.constructOptions());
  qcl.exec("FLUSHALL").get();
  qcl.exec("SET", "QDB-INSTANCE-FOR-EOS-NS-TESTS", "YES");
}

FlushAllOnConstruction::~FlushAllOnConstruction() { }

NsTests::NsTests()
{
  srandom(time(nullptr));
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
    // Right trim password, remove whitespace
    qdb_passwd.erase(qdb_passwd.find_last_not_of(" \t\n\r\f\v") + 1);
  }

  testconfig = {
    {"queue_path", "/tmp/eos-ns-tests/"},
    {"qdb_cluster", qdb_hostport},
    {"qdb_flusher_md", "tests_md"},
    {"qdb_flusher_quota", "tests_quota"},
    {"qdb_password", qdb_passwd }
  };
  guard.reset(new eos::ns::testing::FlushAllOnConstruction(getContactDetails()));
}

NsTests::~NsTests()
{
  shut_down_everything();
}

QdbContactDetails NsTests::getContactDetails()
{
  return QdbContactDetails(getMembers(), testconfig["qdb_password"]);
}

qclient::Members NsTests::getMembers()
{
  return qclient::Members::fromString(testconfig["qdb_cluster"]);
}

void NsTests::setSizeMapper(IQuotaStats::SizeMapper mapper)
{
  sizeMapper = mapper;
}

void NsTests::initServices()
{
  if (namespaceGroupPtr) {
    // Already initialized.
    return;
  }

  namespaceGroupPtr.reset(new eos::QuarkNamespaceGroup());
  std::string err;

  if (!namespaceGroupPtr->initialize(&nsMutex, testconfig, err,nullptr)) {
    std::cerr << "Test error: could not initialize namespace group! Terminating." <<
              std::endl;
    std::terminate();
  }

  namespaceGroupPtr->getFileService()->configure(testconfig);
  namespaceGroupPtr->getContainerService()->configure(testconfig);
  namespaceGroupPtr->getContainerAccountingView();
  namespaceGroupPtr->getSyncTimeAccountingView();
  namespaceGroupPtr->getFilesystemView()->configure(testconfig);
  namespaceGroupPtr->getHierarchicalView()->configure(testconfig);

  if (sizeMapper) {
    namespaceGroupPtr->getQuotaStats()->registerSizeMapper(sizeMapper);
  }

  namespaceGroupPtr->getHierarchicalView()->initialize();
}

eos::IContainerMDSvc* NsTests::containerSvc()
{
  initServices();
  return namespaceGroupPtr->getContainerService();
}

eos::IFileMDSvc* NsTests::fileSvc()
{
  initServices();
  return namespaceGroupPtr->getFileService();
}

eos::IView* NsTests::view()
{
  initServices();
  return namespaceGroupPtr->getHierarchicalView();
}

eos::IFsView* NsTests::fsview()
{
  initServices();
  return namespaceGroupPtr->getFilesystemView();
}

qclient::QClient& NsTests::qcl()
{
  initServices();
  return *(namespaceGroupPtr->getQClient());
}

folly::Executor* NsTests::executor()
{
  return namespaceGroupPtr->getExecutor();
}

eos::MetadataFlusher* NsTests::mdFlusher()
{
  initServices();
  return namespaceGroupPtr->getMetadataFlusher();
}

eos::MetadataFlusher* NsTests::quotaFlusher()
{
  initServices();
  return namespaceGroupPtr->getQuotaFlusher();
}

void NsTests::shut_down_everything()
{
  if (namespaceGroupPtr) {
    namespaceGroupPtr->getHierarchicalView()->finalize();
    namespaceGroupPtr->getFilesystemView()->finalize();
  }

  namespaceGroupPtr.reset();
}

std::unique_ptr<qclient::QClient> NsTests::createQClient()
{
  QdbContactDetails cd = getContactDetails();
  return std::unique_ptr<qclient::QClient>(
           new qclient::QClient(cd.members, cd.constructOptions())
         );
}

void NsTests::populateDummyData1()
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
  view()->createFile("/eos/d2/d4/adsf", true);
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

void NsTests::cleanNSCache()
{
  using namespace eos::constants;
  std::map<std::string, std::string> map_cfg;
  map_cfg[sMaxNumCacheFiles] = std::to_string(UINT64_MAX);
  map_cfg[sMaxSizeCacheFiles] = std::to_string(UINT64_MAX);
  map_cfg[sMaxNumCacheDirs] = std::to_string(UINT64_MAX);
  map_cfg[sMaxSizeCacheDirs] = std::to_string(UINT64_MAX);
  view()->getFileMDSvc()->configure(map_cfg);
  view()->getContainerMDSvc()->configure(map_cfg);
}

EOSNSTESTING_END