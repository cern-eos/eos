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
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "namespace/common/QuotaNodeCore.hh"
#include "namespace/PermissionHandler.hh"
#include "TestUtils.hh"
#include <folly/futures/Future.h>

using namespace eos;

class VariousTests : public eos::ns::testing::NsTestsFixture {};
class NamespaceExplorerF : public eos::ns::testing::NsTestsFixture {};
class FileMDFetching : public eos::ns::testing::NsTestsFixture {};

bool validateReply(qclient::redisReplyPtr reply) {
  if(reply->type != REDIS_REPLY_STRING) return false;
  if(std::string(reply->str, reply->len) != "ayy-lmao") return false;
  return true;
}

TEST_F(VariousTests, FollyWithGloriousContinuations) {
  folly::Future<bool> ok = qcl().follyExec("PING", "ayy-lmao").then(validateReply);
  ASSERT_TRUE(ok.get());
}

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

  ASSERT_EQ(ContainerIdentifier(subdir1->getId()), eos::MetadataFetcher::getContainerIDFromName(qcl(), ContainerIdentifier(2), "subdir1").get());
  ASSERT_EQ(ContainerIdentifier(subdir2->getId()), eos::MetadataFetcher::getContainerIDFromName(qcl(), ContainerIdentifier(2), "subdir2").get());
  ASSERT_EQ(ContainerIdentifier(subdir3->getId()), eos::MetadataFetcher::getContainerIDFromName(qcl(), ContainerIdentifier(2), "subdir3").get());

  IContainerMD::ContainerMap containerMap = eos::MetadataFetcher::getSubContainers(qcl(), ContainerIdentifier(subdir1->getId())).get();
  IContainerMD::FileMap fileMap = eos::MetadataFetcher::getSubContainers(qcl(), ContainerIdentifier(subdir1->getId())).get();

  ASSERT_TRUE(containerMap.empty());
  ASSERT_TRUE(fileMap.empty());

  ASSERT_THROW(view()->getFile("/"), eos::MDException);
}

TEST_F(VariousTests, FileMDGetEnv) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  IFileMDPtr file1 = view()->createFile("/file1", true);

  struct timespec mtime;
  mtime.tv_sec = 123;
  mtime.tv_nsec = 345;

  file1->setMTime(mtime);
  file1->setCUid(999);
  file1->setSize(1337);

  std::string output;
  file1->getEnv(output);
  DBG(output);
}

TEST_F(VariousTests, SymlinkExtravaganza) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  // Basic symlink sanity checks.
  IFileMDPtr file1 = view()->createFile("/file1", true);
  file1->setLink("/cont1");

  IContainerMDPtr cont1 = view()->createContainer("/cont1", true);
  IFileMDPtr awesomeFile = view()->createFile("/cont1/awesome-file", true);

  fileSvc()->updateStore(file1.get());
  fileSvc()->updateStore(awesomeFile.get());
  containerSvc()->updateStore(cont1.get());

  IContainerMDPtr cont2 = view()->getContainer("/file1", true);
  ASSERT_TRUE(cont2.get() != nullptr);
  ASSERT_EQ(cont1.get(), cont2.get());
  ASSERT_THROW(view()->getContainer("/file1", false), MDException);

  IFileMDPtr file2 = view()->createFile("/file2", true);
  file2->setLink("/file1");
  fileSvc()->updateStore(file2.get());

  // NOTE: The following does currently not work on citrine + old NS.
  IContainerMDPtr cont3 = view()->getContainer("/file2", true);
  ASSERT_TRUE(cont3.get() != nullptr);
  ASSERT_EQ(cont1.get(), cont3.get());
  ASSERT_THROW(view()->getFile("/file2", true), MDException); // it actually points to a container

  // Retrieve awesome-file through the symlink.
  IFileMDPtr awesomeFile1 = view()->getFile("/file1/awesome-file", true);
  ASSERT_TRUE(awesomeFile1.get() != nullptr);
  ASSERT_EQ(awesomeFile.get(), awesomeFile1.get());

  // Retrieve awesome-file through two levels of symlinks.
  // NOTE: The following does currently not work on citrine + old NS.
  IFileMDPtr awesomeFile2 = view()->getFile("/file2/awesome-file", true);
  ASSERT_TRUE(awesomeFile2.get() != nullptr);
  ASSERT_EQ(awesomeFile.get(), awesomeFile2.get());
  ASSERT_THROW(view()->getContainer("/file2/awesome-file", true), MDException);

  // Let's create a symlink loop, composed of four files.
  IFileMDPtr symlinkLoop1 = view()->createFile("/loop1", true);
  IFileMDPtr symlinkLoop2 = view()->createFile("/loop2", true);
  IFileMDPtr symlinkLoop3 = view()->createFile("/loop3", true);
  IFileMDPtr symlinkLoop4 = view()->createFile("/loop4", true);

  symlinkLoop1->setLink("/loop2");
  symlinkLoop2->setLink("/loop3");
  symlinkLoop3->setLink("/loop4");
  symlinkLoop4->setLink("/loop1");

  fileSvc()->updateStore(symlinkLoop1.get());
  fileSvc()->updateStore(symlinkLoop2.get());
  fileSvc()->updateStore(symlinkLoop3.get());
  fileSvc()->updateStore(symlinkLoop4.get());

  ASSERT_THROW(view()->getContainer("/loop1", true), MDException);
  ASSERT_THROW(view()->getContainer("/loop2", true), MDException);
  ASSERT_THROW(view()->getContainer("/loop3", true), MDException);
  ASSERT_THROW(view()->getContainer("/loop4", true), MDException);

  ASSERT_THROW(view()->getFile("/loop1", true), MDException);
  ASSERT_THROW(view()->getFile("/loop2", true), MDException);
  ASSERT_THROW(view()->getFile("/loop3", true), MDException);
  ASSERT_THROW(view()->getFile("/loop4", true), MDException);

  ASSERT_THROW(view()->getFile("/", true), MDException);

  // But: We should be able to retrieve the loop-files with follow = false.
  ASSERT_EQ(view()->getFile("/loop1", false), symlinkLoop1);
  ASSERT_EQ(view()->getFile("/loop2", false), symlinkLoop2);
  ASSERT_EQ(view()->getFile("/loop3", false), symlinkLoop3);
  ASSERT_EQ(view()->getFile("/loop4", false), symlinkLoop4);

  // Try out the following ridiculous situation:
  //   /folder1/f2   -> /folder2
  //   /folder2/f3   -> /folder3
  //   /folder3/f4   -> /folder4
  //   /folder4/f1   -> /folder1
  //   /folder1/target-file
  //
  // We should be able to access target-file through
  // /folder1/f2/f3/f4/f1/target-file

  IContainerMDPtr folder1 = view()->createContainer("/folder1", true);
  IContainerMDPtr folder2 = view()->createContainer("/folder2", true);
  IContainerMDPtr folder3 = view()->createContainer("/folder3", true);
  IContainerMDPtr folder4 = view()->createContainer("/folder4", true);

  IFileMDPtr f2 = view()->createFile("/folder1/f2", true);
  f2->setLink("/folder2");

  IFileMDPtr f3 = view()->createFile("/folder2/f3", true);
  f3->setLink("/folder3");

  IFileMDPtr f4 = view()->createFile("/folder3/f4", true);
  f4->setLink("/folder4");

  IFileMDPtr f1 = view()->createFile("/folder4/f1", true);
  f1->setLink("/folder1");

  IFileMDPtr targetFile1 = view()->createFile("/folder1/target-file", true);

  fileSvc()->updateStore(f1.get());
  fileSvc()->updateStore(f2.get());
  fileSvc()->updateStore(f3.get());
  fileSvc()->updateStore(f4.get());
  fileSvc()->updateStore(targetFile1.get());

  IFileMDPtr targetFile2 = view()->getFile("/folder1/f2/f3/f4/f1/target-file", true);
  ASSERT_TRUE(targetFile2.get() != nullptr);
  ASSERT_EQ(targetFile1.get(), targetFile2.get());

  IFileMDPtr symlinkFile = view()->getFile("/folder1/f2/f3/f4/f1", false);
  ASSERT_EQ(view()->getUri(symlinkFile.get()), "/folder4/f1");
  ASSERT_TRUE(symlinkFile->isLink());
  ASSERT_EQ(symlinkFile->getLink(), "/folder1");

  // Use relative symlinks
  IFileMDPtr ff1 = view()->createFile("/ff1", true);
  IFileMDPtr ff2 = view()->createFile("/ff2", true);
  ff2->setLink("./ff1");

  fileSvc()->updateStore(ff1.get());
  fileSvc()->updateStore(ff2.get());

  ASSERT_EQ(view()->getFile("/ff2", true), ff1);
  ASSERT_EQ(view()->getFile("/ff2", false), ff2);

  IFileMDPtr ff3 = view()->createFile("/folder1/ff3", true);
  ff3->setLink("../ff1");
  fileSvc()->updateStore(ff3.get());

  ASSERT_EQ(view()->getFile("/folder1/ff3", true), ff1);
  ASSERT_EQ(view()->getFile("/folder1/ff3", false), ff3);

  // More relative symlinks
  containerSvc()->updateStore(view()->createContainer("/eos", true).get());
  containerSvc()->updateStore(view()->createContainer("/eos/dev", true).get());
  containerSvc()->updateStore(view()->createContainer("/eos/dev/test", true).get());
  containerSvc()->updateStore(view()->createContainer("/eos/dev/test/instancetest", true).get());
  containerSvc()->updateStore(view()->createContainer("/eos/dev/test/instancetest/ref", true).get());

  IFileMDPtr touch = view()->createFile("/eos/dev/test/instancetest/ref/touch", true);
  IFileMDPtr symdir = view()->createFile("/eos/dev/test/instancetest/symrel2", true);
  symdir->setLink("../../test/instancetest/ref");

  fileSvc()->updateStore(touch.get());
  fileSvc()->updateStore(symdir.get());

  ASSERT_EQ(view()->getFile("/eos/dev/test/instancetest/symrel2/touch", true), touch);

  ASSERT_EQ(view()->getRealPath("/eos/dev/test/instancetest/symrel2/touch"), "/eos/dev/test/instancetest/ref/touch");
  ASSERT_EQ(view()->getRealPath("/eos/dev/test/instancetest/symrel2"), "/eos/dev/test/instancetest/symrel2");
}

TEST_F(VariousTests, MoreSymlinks) {
  containerSvc()->updateStore(view()->createContainer("/eos/dev/user", true).get());

  IFileMDPtr myFile = view()->createFile("/eos/dev/user/my-file", true);
  fileSvc()->updateStore(myFile.get());

  IFileMDPtr link = view()->createFile("/eos/dev/user/link", true);
  link->setLink("my-file");
  fileSvc()->updateStore(link.get());

  ASSERT_EQ(view()->getFile("/eos/dev/user/link", true), myFile);
  ASSERT_EQ(view()->getFile("/eos/dev/user/link", false), link);


  containerSvc()->updateStore(view()->createContainer("/eos/dev/user/dir1", true).get());
  containerSvc()->updateStore(view()->createContainer("/eos/dev/user/dir1/dir2", true).get());

  IFileMDPtr myFile2 = view()->createFile("/eos/dev/user/dir1/dir2/my-file-2", true);
  fileSvc()->updateStore(myFile2.get());

  link->setLink("dir1/dir2/my-file-2");
  fileSvc()->updateStore(link.get());

  ASSERT_EQ(view()->getFile("/eos/dev/user/link", true), myFile2);
  ASSERT_EQ(view()->getFile("/eos/dev/user/link", false), link);
}

TEST_F(FileMDFetching, CorruptionTest) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/my-file.txt", true);
  ASSERT_EQ(file1->getId(), 1);

  shut_down_everything();

  qcl().exec(RequestBuilder::writeFileProto(FileIdentifier(1), "hint", "chicken_chicken_chicken_chicken")).get();

  try {
    MetadataFetcher::getFileFromId(qcl(), FileIdentifier(1)).get();
    FAIL();
  }
  catch(const MDException &exc) {
    ASSERT_STREQ(exc.what(), "Error while deserializing FileMD #1 protobuf: FileMD object checksum mismatch");
  }

  shut_down_everything();

  qcl().exec("DEL", constants::sFileKey).get();
  qcl().exec("SADD", constants::sFileKey, "zzz").get();

  try {
    MetadataFetcher::getFileFromId(qcl(), FileIdentifier(1)).get();
    FAIL();
  }
  catch(const MDException &exc) {
    ASSERT_STREQ(exc.what(), "Error while fetching FileMD #1 protobuf from QDB: Received unexpected response, was expecting string: (error) ERR Invalid argument: WRONGTYPE Operation against a key holding the wrong kind of value");
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

class ContainerFilter : public eos::ExpansionDecider {
public:

  virtual bool shouldExpandContainer(const eos::ns::ContainerMdProto &proto) override {
    if(proto.name() == "d4") {
      std::cerr << "INFO: Filtering out encountered container with name d4." << std::endl;
      return false;
    }

    return true;
  }
};

TEST_F(NamespaceExplorerF, ExpansionDecider) {
  populateDummyData1();

  ExplorationOptions options;
  options.depthLimit = 999;
  options.expansionDecider.reset(new ContainerFilter());

  NamespaceExplorer explorer("/eos/d2", options, qcl());
  NamespaceItem item;

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/");

  for(size_t i = 1; i <= 3; i++) {
    ASSERT_TRUE(explorer.fetch(item));
    ASSERT_TRUE(item.isFile);
    ASSERT_EQ(item.fullPath, SSTR("/eos/d2/asdf" << i));
  }

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_TRUE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/b");

  for(size_t i = 1; i <= 6; i++) {
    ASSERT_TRUE(explorer.fetch(item));
    ASSERT_TRUE(item.isFile);
    ASSERT_EQ(item.fullPath, SSTR("/eos/d2/zzzzz" << i));
  }

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-1/");

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-2/");

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_TRUE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-2/my-file");

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d4/");

  ASSERT_FALSE(explorer.fetch(item));
  ASSERT_FALSE(explorer.fetch(item));
  ASSERT_FALSE(explorer.fetch(item));
}

TEST(OctalParsing, BasicSanity) {
  mode_t mode;
  ASSERT_TRUE(PermissionHandler::parseOctalMask("0700", mode));
  ASSERT_EQ(mode, 0700);

  ASSERT_TRUE(PermissionHandler::parseOctalMask("700", mode));
  ASSERT_EQ(mode, 0700);

  ASSERT_TRUE(PermissionHandler::parseOctalMask("744", mode));
  ASSERT_EQ(mode, 0744);

  ASSERT_TRUE(PermissionHandler::parseOctalMask("777", mode));
  ASSERT_EQ(mode, 0777);

  ASSERT_TRUE(PermissionHandler::parseOctalMask("000", mode));
  ASSERT_EQ(mode, 0000);

  ASSERT_FALSE(PermissionHandler::parseOctalMask("chicken", mode));
  ASSERT_FALSE(PermissionHandler::parseOctalMask("700turtles", mode));
  ASSERT_FALSE(PermissionHandler::parseOctalMask("chicken777", mode));
  ASSERT_FALSE(PermissionHandler::parseOctalMask("999", mode));
  ASSERT_FALSE(PermissionHandler::parseOctalMask("0789", mode));
  ASSERT_FALSE(PermissionHandler::parseOctalMask("0709", mode));
  ASSERT_FALSE(PermissionHandler::parseOctalMask("0x123", mode));
}

TEST(SysMask, BasicSanity) {
  std::map<std::string, std::string> xattr;
  xattr.emplace("chicken.chicken", "chicken chicken chicken chicken");
  ASSERT_EQ(0700, PermissionHandler::filterWithSysMask(xattr, 0700));
  ASSERT_EQ(0770, PermissionHandler::filterWithSysMask(xattr, 0770));
  ASSERT_EQ(0774, PermissionHandler::filterWithSysMask(xattr, 0774));

  xattr.emplace("sys.mask", "700");
  ASSERT_EQ(0700, PermissionHandler::filterWithSysMask(xattr, 0777));
  ASSERT_EQ(0700, PermissionHandler::filterWithSysMask(xattr, 0744));
  ASSERT_EQ(0700, PermissionHandler::filterWithSysMask(xattr, 0755));
  ASSERT_EQ(0400, PermissionHandler::filterWithSysMask(xattr, 0444));

  xattr["sys.mask"] = "0700";
  ASSERT_EQ(0700, PermissionHandler::filterWithSysMask(xattr, 0777));
  ASSERT_EQ(0700, PermissionHandler::filterWithSysMask(xattr, 0744));
  ASSERT_EQ(0700, PermissionHandler::filterWithSysMask(xattr, 0755));
  ASSERT_EQ(0400, PermissionHandler::filterWithSysMask(xattr, 0444));

  xattr["sys.mask"] = "0400";
  ASSERT_EQ(0400, PermissionHandler::filterWithSysMask(xattr, 0777));
  ASSERT_EQ(0400, PermissionHandler::filterWithSysMask(xattr, 0744));
  ASSERT_EQ(0400, PermissionHandler::filterWithSysMask(xattr, 0755));
  ASSERT_EQ(0400, PermissionHandler::filterWithSysMask(xattr, 0444));

  xattr["sys.mask"] = "744";
  ASSERT_EQ(0744, PermissionHandler::filterWithSysMask(xattr, 0744));
  ASSERT_EQ(0744, PermissionHandler::filterWithSysMask(xattr, 0757));
  ASSERT_EQ(0404, PermissionHandler::filterWithSysMask(xattr, 0407));

  xattr["sys.mask"] = "chicken";
  ASSERT_EQ(0700, PermissionHandler::filterWithSysMask(xattr, 0700));
  ASSERT_EQ(0770, PermissionHandler::filterWithSysMask(xattr, 0770));
  ASSERT_EQ(0774, PermissionHandler::filterWithSysMask(xattr, 0774));
}

TEST(QuotaNodeCore, BasicSanity) {
  QuotaNodeCore qn;

  std::unordered_set<uint64_t> uids;
  std::unordered_set<uint64_t> gids;

  ASSERT_EQ(qn.getNumFilesByUser(12), 0u);
  ASSERT_EQ(qn.getNumFilesByGroup(12), 0u);

  qn.addFile(12, 13, 1024, 2048);

  ASSERT_EQ(qn.getNumFilesByUser(12), 1u);
  ASSERT_EQ(qn.getNumFilesByUser(13), 0u);

  ASSERT_EQ(qn.getNumFilesByGroup(12), 0u);
  ASSERT_EQ(qn.getNumFilesByGroup(13), 1u);

  ASSERT_EQ(qn.getPhysicalSpaceByUser(12), 2048);
  ASSERT_EQ(qn.getPhysicalSpaceByGroup(12), 0);
  ASSERT_EQ(qn.getPhysicalSpaceByGroup(13), 2048);

  uids.emplace(12);
  gids.emplace(13);
  ASSERT_EQ(qn.getUids(), uids);
  ASSERT_EQ(qn.getGids(), gids);

  qn.addFile(12, 12, 1, 2);

  ASSERT_EQ(qn.getPhysicalSpaceByUser(12), 2050);
  ASSERT_EQ(qn.getPhysicalSpaceByGroup(12), 2);
  ASSERT_EQ(qn.getPhysicalSpaceByGroup(13), 2048);

  ASSERT_EQ(qn.getNumFilesByUser(12), 2u);
  ASSERT_EQ(qn.getNumFilesByUser(13), 0u);

  ASSERT_EQ(qn.getNumFilesByGroup(12), 1u);
  ASSERT_EQ(qn.getNumFilesByGroup(13), 1u);

  gids.emplace(12);
  ASSERT_EQ(qn.getUids(), uids);
  ASSERT_EQ(qn.getGids(), gids);

  qn.removeFile(12, 13, 1024, 2048);

  ASSERT_EQ(qn.getPhysicalSpaceByUser(12), 2);
  ASSERT_EQ(qn.getPhysicalSpaceByGroup(12), 2);
  ASSERT_EQ(qn.getPhysicalSpaceByGroup(13), 0);

  gids.erase(13);
  ASSERT_EQ(qn.getUids(), uids);
  ASSERT_EQ(qn.getGids(), gids);

  qn.removeFile(12, 12, 1, 2);

  ASSERT_EQ(qn.getPhysicalSpaceByUser(12), 0);
  ASSERT_EQ(qn.getPhysicalSpaceByGroup(12), 0);
  ASSERT_EQ(qn.getPhysicalSpaceByGroup(13), 0);

  ASSERT_EQ(qn.getNumFilesByUser(12), 0u);
  ASSERT_EQ(qn.getNumFilesByUser(13), 0u);

  ASSERT_EQ(qn.getNumFilesByGroup(12), 0u);
  ASSERT_EQ(qn.getNumFilesByGroup(13), 0u);

  uids.clear();
  gids.clear();

  ASSERT_EQ(qn.getUids(), uids);
  ASSERT_EQ(qn.getGids(), gids);
}
