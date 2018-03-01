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
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Various namespace tests
//------------------------------------------------------------------------------

#include <memory>
#include <gtest/gtest.h>

#include "namespace/ns_quarkdb/explorer/NamespaceExplorer.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "TestUtils.hh"

using namespace eos;

class VariousTests : public eos::ns::testing::NsTestsFixture {};
class NamespaceExplorerF : public eos::ns::testing::NsTestsFixture {};
class FileMDFetching : public eos::ns::testing::NsTestsFixture {};

TEST_F(VariousTests, BasicSanity) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  std::shared_ptr<eos::IContainerMD> cont1 = view()->createContainer("/eos/", true);
  ASSERT_EQ(cont1->getId(), 2);
  ASSERT_THROW(view()->createFile("/eos/", true), eos::MDException);

  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/eos/my-file.txt", true);
  ASSERT_EQ(file1->getId(), 1);
  ASSERT_EQ(file1->getNumLocation(), 0u);
  file1->addLocation(1);
  file1->addLocation(7);
  ASSERT_EQ(file1->getNumLocation(), 2u);

  containerSvc()->updateStore(root.get());
  containerSvc()->updateStore(cont1.get());
  fileSvc()->updateStore(file1.get());

  shut_down_everything();

  file1 = view()->getFile("/eos/my-file.txt");
  ASSERT_EQ(view()->getUri(file1.get()), "/eos/my-file.txt");
  ASSERT_EQ(file1->getId(), 1);
  ASSERT_EQ(file1->getNumLocation(), 2u);
  ASSERT_EQ(file1->getLocation(0), 1);
  ASSERT_EQ(file1->getLocation(1), 7);

  root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  // Ensure fsview for location 1 contains file1
  std::shared_ptr<eos::ICollectionIterator<eos::IFileMD::id_t>> it = fsview()->getFileList(1);
  ASSERT_TRUE(it->valid());
  ASSERT_EQ(it->getElement(), file1->getId());
  it->next();
  ASSERT_FALSE(it->valid());

  // Create some subdirectories
  std::shared_ptr<eos::IContainerMD> subdir1 = view()->createContainer("/eos/subdir1", true);
  std::shared_ptr<eos::IContainerMD> subdir2 = view()->createContainer("/eos/subdir2", true);
  std::shared_ptr<eos::IContainerMD> subdir3 = view()->createContainer("/eos/subdir3", true);

  ASSERT_LT(subdir1->getId(), subdir2->getId());
  ASSERT_LT(subdir2->getId(), subdir3->getId());
  mdFlusher()->synchronize();

  ASSERT_EQ(subdir1->getId(), eos::MetadataFetcher::getContainerIDFromName(qcl(), 2, "subdir1").get());
  ASSERT_EQ(subdir2->getId(), eos::MetadataFetcher::getContainerIDFromName(qcl(), 2, "subdir2").get());
  ASSERT_EQ(subdir3->getId(), eos::MetadataFetcher::getContainerIDFromName(qcl(), 2, "subdir3").get());

  IContainerMD::ContainerMap containerMap = eos::MetadataFetcher::getSubContainers(qcl(), subdir1->getId()).get();
  IContainerMD::FileMap fileMap = eos::MetadataFetcher::getSubContainers(qcl(), subdir1->getId()).get();

  ASSERT_TRUE(containerMap.empty());
  ASSERT_TRUE(fileMap.empty());
}

TEST_F(FileMDFetching, CorruptionTest) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/my-file.txt", true);
  ASSERT_EQ(file1->getId(), 1);

  shut_down_everything();

  qcl().exec("HSET", FileMDSvc::getBucketKey(1), "1", "chicken_chicken_chicken_chicken").get();

  try {
    MetadataFetcher::getFileFromId(qcl(), 1).get();
    FAIL();
  }
  catch(const MDException &exc) {
    ASSERT_STREQ(exc.what(), "Error while fetching FileMD #1 protobuf from QDB: FileMD object checksum mismatch");
  }

  shut_down_everything();

  qcl().exec("DEL", FileMDSvc::getBucketKey(1)).get();
  qcl().exec("SADD", FileMDSvc::getBucketKey(1), "zzzz").get();

  try {
    MetadataFetcher::getFileFromId(qcl(), 1).get();
    FAIL();
  }
  catch(const MDException &exc) {
    ASSERT_STREQ(exc.what(), "Error while fetching FileMD #1 protobuf from QDB: Received unexpected response: (error) ERR Invalid argument: WRONGTYPE Operation against a key holding the wrong kind of value");
  }
}

TEST_F(NamespaceExplorerF, BasicSanity) {
  populateDummyData1();

  ExplorationOptions options;
  options.depthLimit = 999;

  // Invalid path
  ASSERT_THROW(eos::NamespaceExplorer("/eos/invalid/path", options, qcl()), eos::MDException);

  // Find on single file - weird, but possible
  NamespaceExplorer explorer("/eos/d2/d3-2/my-file", options, qcl());

  NamespaceItem item;
  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-2/my-file");
  ASSERT_FALSE(explorer.fetch(item));

  // Find on directory
  NamespaceExplorer explorer2("/eos/d2", options, qcl());
  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/");

  for(size_t i = 1; i <= 3; i++) {
    ASSERT_TRUE(explorer2.fetch(item));
    ASSERT_TRUE(item.isFile);
    ASSERT_EQ(item.fullPath, SSTR("/eos/d2/asdf" << i));
  }

  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_TRUE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/b");

  for(size_t i = 1; i <= 6; i++) {
    ASSERT_TRUE(explorer2.fetch(item));
    ASSERT_TRUE(item.isFile);
    ASSERT_EQ(item.fullPath, SSTR("/eos/d2/zzzzz" << i));
  }

  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-1/");

  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-2/");

  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_TRUE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-2/my-file");

  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d4/");

  std::stringstream path;
  path << "/eos/d2/d4/";
  for(size_t i = 1; i <= 7; i++) {
    path << i << "/";
    ASSERT_TRUE(explorer2.fetch(item));
    ASSERT_FALSE(item.isFile);
    ASSERT_EQ(item.fullPath, path.str());
  }

  ASSERT_FALSE(explorer2.fetch(item));
  ASSERT_FALSE(explorer2.fetch(item));
  ASSERT_FALSE(explorer2.fetch(item));
}
