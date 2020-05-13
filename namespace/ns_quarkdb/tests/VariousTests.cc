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
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/ns_quarkdb/utils/FutureVectorIterator.hh"
#include "namespace/ns_quarkdb/inspector/Printing.hh"
#include "namespace/ns_quarkdb/persistency/FileSystemIterator.hh"
#include "namespace/ns_quarkdb/inspector/AttributeExtraction.hh"
#include "namespace/common/QuotaNodeCore.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/utils/Etag.hh"
#include "namespace/utils/Attributes.hh"
#include "namespace/PermissionHandler.hh"
#include "namespace/Resolver.hh"
#include "TestUtils.hh"
#include <folly/futures/Future.h>
#include "google/protobuf/util/message_differencer.h"
#include <folly/executors/IOThreadPoolExecutor.h>


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
  folly::Future<bool> ok = qcl().follyExec("PING", "ayy-lmao").thenValue(validateReply);
  ASSERT_TRUE(std::move(ok).get());
}

TEST_F(VariousTests, FileCacheInvalidation) {
  ASSERT_THROW(view()->getFile("/dir/my-file.txt", true), eos::MDException);

  view()->createContainer("/dir", true);
  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/dir/my-file.txt");
  ASSERT_EQ(file1->getId(), 1);
  mdFlusher()->synchronize();

  std::cout << qclient::describeRedisReply(qcl().exec("hdel", "2:map_files", "my-file.txt").get()) << std::endl;

  eos::IFileMDPtr file2 = view()->getFile("/dir/my-file.txt");

  // Cache not updated, view still thinks path is valid
  ASSERT_EQ(file1.get(), file2.get());

  file1.reset();
  file2.reset();

  fileSvc()->dropCachedFileMD(FileIdentifier(1));
  containerSvc()->dropCachedContainerMD(ContainerIdentifier(2));

  // cache dropped, should no longer be able to lookup file
  ASSERT_THROW(view()->getFile("/dir/my-file.txt", true), eos::MDException);
}

TEST_F(VariousTests, CheckLocationInFsView) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  std::shared_ptr<eos::IFileMD> file = view()->createFile("/my-file.txt", true);
  ASSERT_EQ(file->getId(), 1);
  ASSERT_EQ(file->getNumLocation(), 0u);

  file->addLocation(99);
  file->addLocation(77);

  file->addLocation(11);
  file->addLocation(22);

  file->unlinkLocation(11);
  file->unlinkLocation(22);

  std::shared_ptr<eos::IFileMD> file2 = view()->createFile("/my-file-2.txt", true);
  file2->addLocation(22);

  mdFlusher()->synchronize();

  ASSERT_TRUE(eos::MetadataFetcher::locationExistsInFsView(qcl(), FileIdentifier(1), 99, false).get());
  ASSERT_TRUE(eos::MetadataFetcher::locationExistsInFsView(qcl(), FileIdentifier(1), 77, false).get());

  ASSERT_FALSE(eos::MetadataFetcher::locationExistsInFsView(qcl(), FileIdentifier(1), 11, false).get());
  ASSERT_FALSE(eos::MetadataFetcher::locationExistsInFsView(qcl(), FileIdentifier(1), 22, false).get());
  ASSERT_FALSE(eos::MetadataFetcher::locationExistsInFsView(qcl(), FileIdentifier(1), 33, false).get());

  ASSERT_TRUE(eos::MetadataFetcher::locationExistsInFsView(qcl(), FileIdentifier(1), 11, true).get());
  ASSERT_TRUE(eos::MetadataFetcher::locationExistsInFsView(qcl(), FileIdentifier(1), 22, true).get());

  ASSERT_FALSE(eos::MetadataFetcher::locationExistsInFsView(qcl(), FileIdentifier(1), 99, true).get());
  ASSERT_FALSE(eos::MetadataFetcher::locationExistsInFsView(qcl(), FileIdentifier(1), 77, true).get());
  ASSERT_FALSE(eos::MetadataFetcher::locationExistsInFsView(qcl(), FileIdentifier(1), 33, true).get());

  // Try to confuse the iterator object
  qcl().exec("SET", "fsview:22:pickles", "123").get();

  FileSystemIterator fsIter(qcl());
  ASSERT_TRUE(fsIter.valid());
  ASSERT_EQ(fsIter.getFileSystemID(), 11);
  ASSERT_TRUE(fsIter.isUnlinked());
  ASSERT_EQ(fsIter.getRedisKey(), "fsview:11:unlinked");

  fsIter.next();

  ASSERT_TRUE(fsIter.valid());
  ASSERT_EQ(fsIter.getFileSystemID(), 22);
  ASSERT_FALSE(fsIter.isUnlinked());
  ASSERT_EQ(fsIter.getRedisKey(), "fsview:22:files");

  fsIter.next();

  ASSERT_TRUE(fsIter.valid());
  ASSERT_EQ(fsIter.getFileSystemID(), 22);
  ASSERT_TRUE(fsIter.isUnlinked());
  ASSERT_EQ(fsIter.getRedisKey(), "fsview:22:unlinked");

  fsIter.next();

  ASSERT_TRUE(fsIter.valid());
  ASSERT_EQ(fsIter.getFileSystemID(), 77);
  ASSERT_FALSE(fsIter.isUnlinked());
  ASSERT_EQ(fsIter.getRedisKey(), "fsview:77:files");

  fsIter.next();

  ASSERT_TRUE(fsIter.valid());
  ASSERT_EQ(fsIter.getFileSystemID(), 99);
  ASSERT_FALSE(fsIter.isUnlinked());
  ASSERT_EQ(fsIter.getRedisKey(), "fsview:99:files");

  fsIter.next();
  ASSERT_FALSE(fsIter.valid());
}

TEST_F(VariousTests, ReconstructContainerPath) {
  std::shared_ptr<eos::IContainerMD> cont = view()->createContainer("/eos/a/b/c/d/e", true);
  std::shared_ptr<eos::IFileMD> file = view()->createFile("/eos/a/b/c/d/e/my-file");

  ASSERT_EQ(cont->getId(), 7);
  ASSERT_EQ(file->getId(), 1);

  mdFlusher()->synchronize();

  ASSERT_EQ("/",  eos::MetadataFetcher::resolveFullPath(qcl(), ContainerIdentifier(1)).get());
  ASSERT_EQ("/eos/",  eos::MetadataFetcher::resolveFullPath(qcl(), ContainerIdentifier(2)).get());
  ASSERT_EQ("/eos/a/",  eos::MetadataFetcher::resolveFullPath(qcl(), ContainerIdentifier(3)).get());
  ASSERT_EQ("/eos/a/b/",  eos::MetadataFetcher::resolveFullPath(qcl(), ContainerIdentifier(4)).get());
  ASSERT_EQ("/eos/a/b/c/",  eos::MetadataFetcher::resolveFullPath(qcl(), ContainerIdentifier(5)).get());
  ASSERT_EQ("/eos/a/b/c/d/",  eos::MetadataFetcher::resolveFullPath(qcl(), ContainerIdentifier(6)).get());
  ASSERT_EQ("/eos/a/b/c/d/e/",  eos::MetadataFetcher::resolveFullPath(qcl(), ContainerIdentifier(7)).get());
  ASSERT_THROW(eos::MetadataFetcher::resolveFullPath(qcl(), ContainerIdentifier(8)).get(), eos::MDException) ;

  ASSERT_EQ(eos::MetadataFetcher::resolvePathToID(qcl(), "/").get(), ContainerIdentifier(1));
  ASSERT_EQ(eos::MetadataFetcher::resolvePathToID(qcl(), "/eos").get(), ContainerIdentifier(2));
  ASSERT_EQ(eos::MetadataFetcher::resolvePathToID(qcl(), "/eos/a").get(), ContainerIdentifier(3));
  ASSERT_EQ(eos::MetadataFetcher::resolvePathToID(qcl(), "/eos/a/b").get(), ContainerIdentifier(4));
  ASSERT_EQ(eos::MetadataFetcher::resolvePathToID(qcl(), "/eos/a/b/c").get(), ContainerIdentifier(5));
  ASSERT_EQ(eos::MetadataFetcher::resolvePathToID(qcl(), "/eos/a/b/c/d").get(), ContainerIdentifier(6));
  ASSERT_EQ(eos::MetadataFetcher::resolvePathToID(qcl(), "/eos/a/b/c/d/e").get(), ContainerIdentifier(7));

  ASSERT_EQ(eos::MetadataFetcher::resolvePathToID(qcl(), "/eos/a/b/c/d/e/my-file").get(), FileIdentifier(1));
  ASSERT_THROW(eos::MetadataFetcher::resolvePathToID(qcl(), "/aaaaaaa").get(), eos::MDException);
  ASSERT_THROW(eos::MetadataFetcher::resolvePathToID(qcl(), "/eos/aaaaaaa").get(), eos::MDException);
}

TEST_F(VariousTests, BasicSanity) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);
  ASSERT_EQ(view()->getUri(root.get()), "/");
  ASSERT_EQ(view()->getUri(1), "/");

  std::shared_ptr<eos::IContainerMD> cont1 = view()->createContainer("/eos/", true);
  ASSERT_EQ(cont1->getId(), 2);
  ASSERT_THROW(view()->createFile("/eos/", true), eos::MDException);
  ASSERT_EQ(view()->getUri(cont1.get()), "/eos/");
  ASSERT_EQ(view()->getUri(cont1->getId()), "/eos/");
  ASSERT_EQ(view()->getUri(cont1->getParentId()), "/");
  ASSERT_EQ(view()->getUriFut(cont1->getIdentifier()).get(), "/eos/");

  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/eos/my-file.txt", true);
  ASSERT_EQ(file1->getId(), 1);
  ASSERT_EQ(file1->getNumLocation(), 0u);
  file1->addLocation(1);
  file1->addLocation(7);
  file1->setCUid(333);
  file1->setCGid(999);
  file1->setSize(555);
  file1->setFlags( (S_IRWXU | S_IRWXG | S_IRWXO));

  char buff[32];
  buff[0] = 0x12; buff[1] = 0x23; buff[2] = 0x55; buff[3] = 0x99;
  buff[4] = 0xAA; buff[5] = 0xDD; buff[6] = 0x00; buff[7] = 0x55;

  file1->setChecksum(buff, 8);

  std::string out;
  ASSERT_FALSE(eos::appendChecksumOnStringAsHex(file1.get(), out));

  unsigned long layout = eos::common::LayoutId::GetId(
    eos::common::LayoutId::kReplica,
    eos::common::LayoutId::kMD5,
    2,
    eos::common::LayoutId::k4k);

  file1->setLayoutId(layout);

  ASSERT_EQ(file1->getNumLocation(), 2u);
  ASSERT_EQ(view()->getUri(file1.get()), "/eos/my-file.txt");
  ASSERT_EQ(view()->getUriFut(file1->getIdentifier()).get(), "/eos/my-file.txt");

  struct timespec ctime;
  ctime.tv_sec = 1999;
  ctime.tv_nsec = 8888;
  file1->setCTime(ctime);

  struct timespec mtime;
  mtime.tv_sec = 2000;
  mtime.tv_nsec = 999;
  file1->setMTime(mtime);

  ASSERT_EQ(eos::Printing::printMultiline(static_cast<eos::QuarkFileMD*>(file1.get())->getProto()),
    SSTR("ID: 1\n"
         "Name: my-file.txt\n"
         "Link name: \n"
         "Container ID: 2\n"
         "uid: 333, gid: 999\n"
         "Size: 555\n"
         "Modify: " << Printing::timespecToFileinfo(mtime) << "\n"
         "Change: " << Printing::timespecToFileinfo(ctime) << "\n"
         "Flags: 0777\n"
         "Checksum type: md5, checksum bytes: 12235599aadd00550000000000000000\n"
         "Expected number of replicas / stripes: 2\n"
         "Etag: \"12235599aadd00550000000000000000\"\n"
         "Locations: [1, 7]\n"
         "Unlinked locations: []\n")
            );

  containerSvc()->updateStore(root.get());
  containerSvc()->updateStore(cont1.get());
  fileSvc()->updateStore(file1.get());

  shut_down_everything();

  file1 = view()->getFile("/eos/my-file.txt");
  ASSERT_EQ(view()->getUri(file1.get()), "/eos/my-file.txt");
  ASSERT_EQ(view()->getUriFut(file1->getIdentifier()).get(), "/eos/my-file.txt");

  ASSERT_EQ(file1->getId(), 1);
  ASSERT_EQ(file1->getNumLocation(), 2u);
  ASSERT_EQ(file1->getLocation(0), 1);
  ASSERT_EQ(file1->getLocation(1), 7);

  root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  FileOrContainerMD item = view()->getItem("/").get();
  ASSERT_TRUE(item.container);
  ASSERT_FALSE(item.file);
  ASSERT_EQ(item.container->getId(), 1);

  item = view()->getItem("/eos/my-file.txt").get();
  ASSERT_TRUE(item.file);
  ASSERT_FALSE(item.container);
  ASSERT_EQ(item.file->getId(), 1);

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

  ASSERT_EQ(subdir1->getId(), eos::MetadataFetcher::getContainerFromName(qcl(), ContainerIdentifier(2), "subdir1").get().id());
  ASSERT_EQ(subdir2->getId(), eos::MetadataFetcher::getContainerFromName(qcl(), ContainerIdentifier(2), "subdir2").get().id());
  ASSERT_EQ(subdir3->getId(), eos::MetadataFetcher::getContainerFromName(qcl(), ContainerIdentifier(2), "subdir3").get().id());

  IContainerMD::ContainerMap containerMap = eos::MetadataFetcher::getContainerMap(qcl(), ContainerIdentifier(subdir1->getId())).get();
  IContainerMD::FileMap fileMap = eos::MetadataFetcher::getContainerMap(qcl(), ContainerIdentifier(subdir1->getId())).get();

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
}

TEST_F(VariousTests, MkdirOnBrokenSymlink) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  IFileMDPtr file1 = view()->createFile("/file1", true);
  file1->setLink("/not-existing");

  fileSvc()->updateStore(file1.get());
  containerSvc()->updateStore(root.get());

  ASSERT_THROW(view()->createContainer("/file1", true), eos::MDException);
}

TEST_F(VariousTests, SymlinkExtravaganza) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  // Basic symlink sanity checks.
  IFileMDPtr file1 = view()->createFile("/file1", true);
  file1->setLink("/cont1");

  IContainerMDPtr cont1 = view()->createContainer("/cont1", true);
  IFileMDPtr awesomeFile = view()->createFile("/cont1/awesome-file", true);
  ASSERT_EQ(view()->getUri(cont1.get()), "/cont1/");
  ASSERT_EQ(view()->getUri(cont1->getId()), "/cont1/");
  ASSERT_EQ(view()->getUriFut(cont1->getIdentifier()).get(), "/cont1/");

  fileSvc()->updateStore(file1.get());
  fileSvc()->updateStore(awesomeFile.get());
  containerSvc()->updateStore(cont1.get());

  IContainerMDPtr cont2 = view()->getContainer("/file1", true);
  ASSERT_TRUE(cont2.get() != nullptr);
  ASSERT_EQ(cont1.get(), cont2.get());
  ASSERT_THROW(view()->getContainer("/file1", false), MDException);
  ASSERT_EQ(view()->getUri(cont2.get()), "/cont1/");
  ASSERT_EQ(view()->getUri(cont2->getId()), "/cont1/");
  ASSERT_EQ(view()->getUriFut(cont2->getIdentifier()).get(), "/cont1/");

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
  ASSERT_EQ(view()->getUri(awesomeFile.get()), "/cont1/awesome-file");
  ASSERT_EQ(view()->getUriFut(awesomeFile->getIdentifier()).get(), "/cont1/awesome-file");
  ASSERT_EQ(view()->getUri(awesomeFile->getContainerId()), "/cont1/");

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
  ASSERT_EQ(view()->getUri(targetFile2.get()), "/folder1/target-file");
  ASSERT_EQ(view()->getUriFut(targetFile2->getIdentifier()).get(), "/folder1/target-file");

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

TEST_F(VariousTests, createFile) {
  containerSvc()->updateStore(view()->createContainer("/eos/dev/user", true).get());

  IFileMDPtr myFile = view()->createFile("/eos/dev/user/my-file");
  fileSvc()->updateStore(myFile.get());

  ASSERT_THROW(view()->createFile("/eos/dev/user/my-file"), eos::MDException);
  ASSERT_THROW(view()->createFile("/eos/dev/user"), eos::MDException);
  ASSERT_THROW(view()->createFile("/eos/dev/user/my-file/aaaa"), eos::MDException);
}

TEST_F(VariousTests, createContainerMadness) {
  containerSvc()->updateStore(view()->createContainer("/eos/dev/../dev/", true).get());
  containerSvc()->updateStore(view()->createContainer(
    "/eos/dev/./my-dir-1/./../my-dir-2/../my-dir-3/./my-dir-4/../my-dir-5", true).get()); // MUAHAHAHAH

  // This is how "mkdir -p" on Linux behaves, as well. We want to be compatible.

  view()->getContainer("/eos");
  view()->getContainer("/eos/dev");
  view()->getContainer("/eos/dev/my-dir-1");
  view()->getContainer("/eos/dev/my-dir-2");
  view()->getContainer("/eos/dev/my-dir-3");
  view()->getContainer("/eos/dev/my-dir-3/my-dir-4");
  view()->getContainer("/eos/dev/my-dir-3/my-dir-5");

  shut_down_everything();

  view()->getContainer("/eos");
  view()->getContainer("/eos/dev");
  view()->getContainer("/eos/dev/my-dir-1");
  view()->getContainer("/eos/dev/my-dir-2");
  view()->getContainer("/eos/dev/my-dir-3");
  view()->getContainer("/eos/dev/my-dir-3/my-dir-4");
  view()->getContainer("/eos/dev/my-dir-3/my-dir-5");

  ASSERT_THROW(view()->createContainer("/eos/dev/my-dir-1/aaa/bbb", false), eos::MDException);

  IFileMDPtr file1 = view()->createFile("/eos/dev/my-dir-1/link", true);
  file1->setLink("/eos/dev/my-dir-3/my-dir-4");
  fileSvc()->updateStore(file1.get());

  shut_down_everything();

  ASSERT_THROW(view()->createContainer(
    "/eos/dev/../dev/my-dir-1/./link/../my-dir-4/what-am-i-doing/aaaaaa/../bbbbbbb/../bbbbbbb/chicken", false), eos::MDException);

  containerSvc()->updateStore(view()->createContainer(
    "/eos/dev/../dev/my-dir-1/./link/../my-dir-4/what-am-i-doing/aaaaaa/../bbbbbbb/../bbbbbbb/chicken", true).get());

  view()->getContainer("/eos/dev/my-dir-3/my-dir-4/what-am-i-doing");
  view()->getContainer("/eos/dev/my-dir-3/my-dir-4/what-am-i-doing/aaaaaa");
  view()->getContainer("/eos/dev/my-dir-3/my-dir-4/what-am-i-doing/bbbbbbb");

  auto chicken = view()->getContainer("/eos/dev/my-dir-3/my-dir-4/what-am-i-doing/bbbbbbb/chicken");
  ASSERT_EQ(view()->getUri(chicken.get()), "/eos/dev/my-dir-3/my-dir-4/what-am-i-doing/bbbbbbb/chicken/");
  ASSERT_EQ(view()->getUri(chicken->getId()), "/eos/dev/my-dir-3/my-dir-4/what-am-i-doing/bbbbbbb/chicken/");
  ASSERT_EQ(view()->getUri(chicken->getParentId()), "/eos/dev/my-dir-3/my-dir-4/what-am-i-doing/bbbbbbb/");
}

TEST_F(VariousTests, ChecksumFormatting) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/my-file.txt", true);
  ASSERT_EQ(file1->getId(), 1);

  char buff[32];
  buff[0] = 0x12; buff[1] = 0x23; buff[2] = 0x55; buff[3] = 0x99;
  buff[4] = 0xAA; buff[5] = 0xDD; buff[6] = 0x00; buff[7] = 0x55;

  file1->setChecksum(buff, 8);

  std::string out;
  ASSERT_FALSE(eos::appendChecksumOnStringAsHex(file1.get(), out));

  unsigned long layout = eos::common::LayoutId::GetId(
    eos::common::LayoutId::kReplica,
    eos::common::LayoutId::kMD5,
    2,
    eos::common::LayoutId::k4k);

  file1->setLayoutId(layout);

  ASSERT_TRUE(eos::appendChecksumOnStringAsHex(file1.get(), out));
  ASSERT_EQ(out, "12235599aadd00550000000000000000");

  layout = eos::common::LayoutId::GetId(
    eos::common::LayoutId::kReplica,
    eos::common::LayoutId::kCRC32,
    2,
    eos::common::LayoutId::k4k);

  file1->setLayoutId(layout);

  out.clear();
  ASSERT_TRUE(eos::appendChecksumOnStringAsHex(file1.get(), out));
  ASSERT_EQ(out, "12235599");

  out.clear();
  ASSERT_TRUE(eos::appendChecksumOnStringAsHex(file1.get(), out, ' '));
  ASSERT_EQ(out, "12 23 55 99");

  out.clear();
  ASSERT_TRUE(eos::appendChecksumOnStringAsHex(file1.get(), out, '_'));
  ASSERT_EQ(out, "12_23_55_99");

  out.clear();
  ASSERT_TRUE(eos::appendChecksumOnStringAsHex(file1.get(), out, '_', 20));
  ASSERT_EQ(out, "12_23_55_99_00_00_00_00_00_00_00_00_00_00_00_00_00_00_00_00");

  ASSERT_FALSE(eos::appendChecksumOnStringAsHex(nullptr, out));
}

TEST(HexToByteString, EdgeCases) {
  std::string byteArray;

  ASSERT_FALSE(eos::hexArrayToByteArray("chickens", byteArray));

  ASSERT_TRUE(eos::hexArrayToByteArray("", byteArray));
  ASSERT_EQ(byteArray, "");

  ASSERT_FALSE(eos::hexArrayToByteArray("deadbeeg", byteArray));
}

TEST(HexToByteString, BasicSanity) {
  std::string byteArray;
  ASSERT_TRUE(eos::hexArrayToByteArray("deadbeef", byteArray));
  ASSERT_EQ(byteArray.size(), 4);
  ASSERT_EQ(byteArray[0], '\xde');
  ASSERT_EQ(byteArray[1], '\xad');
  ASSERT_EQ(byteArray[2], '\xbe');
  ASSERT_EQ(byteArray[3], '\xef');

  std::string tmp;
  ASSERT_TRUE(eos::hexArrayToByteArray("DEADBEEF", tmp));
  ASSERT_EQ(tmp, byteArray);

  ASSERT_TRUE(eos::hexArrayToByteArray("DeAdbEEf", tmp));
  ASSERT_EQ(tmp, byteArray);
}

namespace eos {

TEST_F(VariousTests, EtagFormatting) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  // Create a test file.
  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/my-file.txt", true);
  ASSERT_EQ(file1->getId(), 1);

  eos::IFileMD::ctime_t mtime;
  mtime.tv_sec = 1537360812;
  mtime.tv_nsec = 0;
  file1->setCTime(mtime);

  eos::QuarkFileMD *file1f = reinterpret_cast<QuarkFileMD*>(file1.get());
  file1f->mFile.set_id(4697755903ull);

  // File has no checksum, using inode + modification time.
  std::string outcome;
  eos::calculateEtag(file1.get(), outcome);
  ASSERT_EQ(outcome, "\"1261044247998496768:1537360812\"");

  // Force temporary etag
  file1->setAttribute("sys.tmp.etag", "lmao");
  eos::calculateEtag(file1.get(), outcome);
  ASSERT_EQ(outcome, "lmao");

  // Remove temporary etag
  file1->removeAttribute("sys.tmp.etag");

  // etag based on inode + mtime
  char buff[4];
  buff[0] = 0xa7; buff[1] = 0x25; buff[2] = 0x99; buff[3] = 0x97;
  file1->setChecksum(buff, 4);
  file1f->mFile.set_id(4697755939ull);

  unsigned long layout = eos::common::LayoutId::GetId(
    eos::common::LayoutId::kReplica,
    eos::common::LayoutId::kAdler,
    2,
    eos::common::LayoutId::k4k);

  file1->setLayoutId(layout);

  eos::calculateEtag(file1.get(), outcome);
  ASSERT_EQ(outcome, "\"1261044257662173184:a7259997\"");


  char buff2[32];
  buff2[0] = 0x65; buff2[1] = 0x01; buff2[2] = 0xe9; buff2[3] = 0xc7;
  buff2[4] = 0xbf; buff2[5] = 0x20; buff2[6] = 0xb1; buff2[7] = 0xdc;
  buff2[8] = 0x56; buff2[9] = 0xf0; buff2[10] = 0x15; buff2[11] = 0xe3;
  buff2[12] = 0x41; buff2[13] = 0xf7; buff2[14] = 0x98; buff2[15] = 0x33;

  file1->setChecksum(buff2, 16);

  layout = eos::common::LayoutId::GetId(
    eos::common::LayoutId::kReplica,
    eos::common::LayoutId::kMD5,
    2,
    eos::common::LayoutId::k4k);

  file1->setLayoutId(layout);

  eos::calculateEtag(file1.get(), outcome);
  ASSERT_EQ(outcome, "\"6501e9c7bf20b1dc56f015e341f79833\"");
}

TEST_F(VariousTests, EtagFormattingContainer) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  // Create a test directory.
  std::shared_ptr<eos::IContainerMD> cont1 = view()->createContainer("/my-file.txt", true);
  ASSERT_EQ(cont1->getId(), 2);

  eos::IFileMD::ctime_t mtime;
  mtime.tv_sec = 1534776794;
  mtime.tv_nsec = 97343404;
  cont1->setTMTime(mtime);

  eos::QuarkContainerMD *cont1c = reinterpret_cast<QuarkContainerMD*>(cont1.get());
  cont1c->mCont.set_id(5734137);

  std::string outcome;
  cont1->setAttribute("sys.tmp.etag", "lmao");
  eos::calculateEtag(cont1.get(), outcome);
  ASSERT_EQ(outcome, "lmao");

  cont1->removeAttribute("sys.tmp.etag");

  eos::calculateEtag(cont1.get(), outcome);
  ASSERT_EQ(outcome, "577ef9:1534776794.097");
}

}

TEST_F(FileMDFetching, ExistenceTest) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/my-file.txt", true);
  ASSERT_EQ(file1->getId(), 1);

  mdFlusher()->synchronize();

  ASSERT_TRUE(MetadataFetcher::doesFileMdExist(qcl(), FileIdentifier(1)).get());
  ASSERT_FALSE(MetadataFetcher::doesFileMdExist(qcl(), FileIdentifier(2)).get());

  ASSERT_TRUE(fileSvc()->hasFileMD(FileIdentifier(1)).get());
  ASSERT_FALSE(fileSvc()->hasFileMD(FileIdentifier(2)).get());

  ASSERT_TRUE(MetadataFetcher::doesContainerMdExist(qcl(), ContainerIdentifier(1)).get());
  ASSERT_FALSE(MetadataFetcher::doesContainerMdExist(qcl(), ContainerIdentifier(2)).get());
}

TEST_F(FileMDFetching, FilemapToFutureVector) {
  populateDummyData1();

  eos::IContainerMDPtr cont = view()->getContainer("/eos/d1");
  ASSERT_EQ(cont->getId(), 3);

  IContainerMD::FileMap filemap = MetadataFetcher::getFileMap(qcl(), ContainerIdentifier(3)).get();
  std::map<std::string, IFileMD::id_t> sorted;
  std::map<std::string, IFileMD::id_t> expected = {
    {"f1", 1}, {"f2", 2}, {"f3", 3}, {"f4", 4}, {"f5", 5}
  };

  for(auto it = filemap.cbegin(); it != filemap.cend(); ++it) {
    sorted[it->first] = it->second;
  }

  ASSERT_EQ(sorted, expected);

  std::vector<folly::Future<eos::ns::FileMdProto>> mdvector = MetadataFetcher::getFilesFromFilemap(qcl(), filemap);
  ASSERT_EQ(mdvector.size(), 5u);

  std::unique_ptr<folly::Executor> executor(new folly::IOThreadPoolExecutor(4));

  std::vector<folly::Future<eos::ns::FileMdProto>> mdvector3 =
    MetadataFetcher::getFileMDsInContainer(qcl(), ContainerIdentifier(3),
      executor.get()).get();

  ASSERT_EQ(mdvector3.size(), 5u);

  eos::ns::FileMdProto f1 = std::move(mdvector[0]).get();
  eos::ns::FileMdProto f2 = std::move(mdvector[1]).get();
  eos::ns::FileMdProto f3 = std::move(mdvector[2]).get();
  eos::ns::FileMdProto f4 = std::move(mdvector[3]).get();
  eos::ns::FileMdProto f5 = std::move(mdvector[4]).get();

  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(f1, std::move(mdvector3[0]).get()));
  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(f2, std::move(mdvector3[1]).get()));
  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(f3, std::move(mdvector3[2]).get()));
  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(f4, std::move(mdvector3[3]).get()));
  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(f5, std::move(mdvector3[4]).get()));

  ASSERT_EQ(f1.name(), "f1");
  ASSERT_EQ(f1.id(), 1);

  ASSERT_EQ(f2.name(), "f2");
  ASSERT_EQ(f2.id(), 2);

  ASSERT_EQ(f3.name(), "f3");
  ASSERT_EQ(f3.id(), 3);

  ASSERT_EQ(f4.name(), "f4");
  ASSERT_EQ(f4.id(), 4);

  ASSERT_EQ(f5.name(), "f5");
  ASSERT_EQ(f5.id(), 5);


  IContainerMD::FileMap containermap = MetadataFetcher::getContainerMap(qcl(), ContainerIdentifier(3)).get();
  std::map<std::string, IFileMD::id_t> sorted2;
  std::map<std::string, IFileMD::id_t> expected2 = {
    {"d2", 4}, {"d2-1", 11}, {"d2-2", 12}, {"d2-3", 13}
  };

  for(auto it = containermap.cbegin(); it != containermap.cend(); ++it) {
    sorted2[it->first] = it->second;
  }

  ASSERT_EQ(sorted2, expected2);

  std::vector<folly::Future<eos::ns::ContainerMdProto>> mdvector2 = MetadataFetcher::getContainersFromContainerMap(qcl(), containermap);
  ASSERT_EQ(mdvector2.size(), 4u);

  std::vector<folly::Future<eos::ns::ContainerMdProto>> mdvector5 =
    MetadataFetcher::getContainerMDsInContainer(qcl(), ContainerIdentifier(3),
    executor.get()).get();

  ASSERT_EQ(mdvector5.size(), 4u);

  eos::ns::ContainerMdProto d0 = std::move(mdvector2[0]).get();
  eos::ns::ContainerMdProto d1 = std::move(mdvector2[1]).get();
  eos::ns::ContainerMdProto d2 = std::move(mdvector2[2]).get();
  eos::ns::ContainerMdProto d3 = std::move(mdvector2[3]).get();

  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(d0, std::move(mdvector5[0]).get()));
  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(d1, std::move(mdvector5[1]).get()));
  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(d2, std::move(mdvector5[2]).get()));
  ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(d3, std::move(mdvector5[3]).get()));

  ASSERT_EQ(d0.name(), "d2");
  ASSERT_EQ(d0.id(), 4);

  ASSERT_EQ(d1.name(), "d2-1");
  ASSERT_EQ(d1.id(), 11);

  ASSERT_EQ(d2.name(), "d2-2");
  ASSERT_EQ(d2.id(), 12);

  ASSERT_EQ(d3.name(), "d2-3");
  ASSERT_EQ(d3.id(), 13);
}

TEST_F(FileMDFetching, CorruptionTest) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);

  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/my-file.txt", true);
  ASSERT_EQ(file1->getId(), 1);

  shut_down_everything();

  qcl().execute(RequestBuilder::writeFileProto(FileIdentifier(1), "hint", "chicken_chicken_chicken_chicken")).get();

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
  ASSERT_THROW(eos::NamespaceExplorer("/eos/invalid/path", options, qcl(), executor()), eos::MDException);

  // Find on single file - weird, but possible
  NamespaceExplorer explorer("/eos/d2/d3-2/my-file", options, qcl(), executor());

  NamespaceItem item;
  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-2/my-file");
  ASSERT_FALSE(explorer.fetch(item));

  // Find on directory
  NamespaceExplorer explorer2("/eos/d2", options, qcl(), executor());
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

  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_TRUE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d4/adsf");

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

TEST_F(NamespaceExplorerF, NoFiles) {
  populateDummyData1();

  ExplorationOptions options;
  options.depthLimit = 999;
  options.ignoreFiles = true;

  // Find on directory
  NamespaceExplorer explorer2("/eos/d2", options, qcl(), executor());
  NamespaceItem item;

  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/");

  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-1/");

  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-2/");

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

TEST_F(NamespaceExplorerF, LinkedAttributes) {
  std::shared_ptr<eos::IContainerMD> root = view()->getContainer("/");
  ASSERT_EQ(root->getId(), 1);
  root->setAttribute("sys.chickens", "no");
  root->setAttribute("sys.qwerty", "asdf");
  containerSvc()->updateStore(root.get());

  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/my-file.txt", true);
  ASSERT_EQ(file1->getId(), 1);
  file1->setAttribute("sys.chickens", "yes");
  file1->setAttribute("sys.attr.link", "/some-file");
  fileSvc()->updateStore(file1.get());

  mdFlusher()->synchronize();

  // Find on single file - weird, but possible
  ExplorationOptions options;
  options.depthLimit = 999;
  options.populateLinkedAttributes = true;

  // attrs asked, but view not provided
  ASSERT_THROW(eos::NamespaceExplorer("/", options, qcl(), executor()), eos::MDException);
  options.view = view();

  eos::NamespaceExplorer explorer("/", options, qcl(), executor());

  NamespaceItem item;
  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/");

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_TRUE(item.isFile);
  ASSERT_EQ(item.fullPath, "/my-file.txt");
  std::map<std::string, std::string> predictedAttrs {
    {"sys.chickens", "yes" },
    {"sys.attr.link", "/some-file"}
  };
  ASSERT_EQ(item.attrs, predictedAttrs);


  file1->setAttribute("sys.attr.link", "/");
  fileSvc()->updateStore(file1.get());
  mdFlusher()->synchronize();

  eos::NamespaceExplorer explorer2("/", options, qcl(), executor());
  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/");

  ASSERT_TRUE(explorer2.fetch(item));
  ASSERT_TRUE(item.isFile);
  ASSERT_EQ(item.fullPath, "/my-file.txt");
  predictedAttrs = {
    {"sys.chickens", "yes" },
    {"sys.attr.link", "/"},
    {"sys.qwerty", "asdf"},
  };
  ASSERT_EQ(item.attrs, predictedAttrs);
}

class ContainerFilter : public eos::ExpansionDecider {
public:

  virtual bool shouldExpandContainer(const eos::ns::ContainerMdProto &proto,
    const eos::IContainerMD::XAttrMap &attrs) override {

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

  NamespaceExplorer explorer("/eos/d2", options, qcl(), executor());
  NamespaceItem item;

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/");
  ASSERT_FALSE(item.expansionFilteredOut);

  for(size_t i = 1; i <= 3; i++) {
    ASSERT_TRUE(explorer.fetch(item));
    ASSERT_TRUE(item.isFile);
    ASSERT_EQ(item.fullPath, SSTR("/eos/d2/asdf" << i));
    ASSERT_FALSE(item.expansionFilteredOut);
  }

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_TRUE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/b");
  ASSERT_FALSE(item.expansionFilteredOut);

  for(size_t i = 1; i <= 6; i++) {
    ASSERT_TRUE(explorer.fetch(item));
    ASSERT_TRUE(item.isFile);
    ASSERT_EQ(item.fullPath, SSTR("/eos/d2/zzzzz" << i));
    ASSERT_FALSE(item.expansionFilteredOut);
  }

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-1/");
  ASSERT_FALSE(item.expansionFilteredOut);

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-2/");
  ASSERT_FALSE(item.expansionFilteredOut);

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_TRUE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d3-2/my-file");
  ASSERT_FALSE(item.expansionFilteredOut);

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_FALSE(item.isFile);
  ASSERT_EQ(item.fullPath, "/eos/d2/d4/");
  ASSERT_TRUE(item.expansionFilteredOut);

  ASSERT_FALSE(explorer.fetch(item));
  ASSERT_FALSE(explorer.fetch(item));
  ASSERT_FALSE(explorer.fetch(item));
}

TEST_F(VariousTests, LinkedExtendedAttributes) {
  IContainerMDPtr cont1 = view()->createContainer("/eos/dir1", true);
  IContainerMDPtr cont2 = view()->createContainer("/eos/dir1/dir2", true);

  cont1->setAttribute("sys.chickens", "yes");
  cont1->setAttribute("user.qwerty", "asdf");

  cont2->setAttribute("sys.chickens", "no");
  cont2->setAttribute("sys.attr.link", "/eos/dir4");

  eos::IContainerMD::XAttrMap out;
  eos::listAttributes(view(), cont1.get(), out, false);
  ASSERT_EQ(out.size(), 2u);
  ASSERT_EQ(out["sys.chickens"], "yes");
  ASSERT_EQ(out["user.qwerty"], "asdf");

  eos::listAttributes(view(), cont2.get(), out, false);
  ASSERT_EQ(out.size(), 2u);
  ASSERT_EQ(out["sys.chickens"], "no");
  ASSERT_EQ(out["sys.attr.link"], "/eos/dir4 - not found");

  cont2->setAttribute("sys.attr.link", "/eos/dir1");

  eos::listAttributes(view(), cont2.get(), out, false);
  ASSERT_EQ(out.size(), 3u);
  ASSERT_EQ(out["sys.chickens"], "no");
  ASSERT_EQ(out["sys.attr.link"], "/eos/dir1");
  ASSERT_EQ(out["user.qwerty"], "asdf");

  eos::listAttributes(view(), cont2.get(), out, true);
  ASSERT_EQ(out.size(), 3u);
  ASSERT_EQ(out["sys.chickens"], "no");
  ASSERT_EQ(out["sys.attr.link"], "/eos/dir1");
  ASSERT_EQ(out["user.qwerty"], "asdf");

  cont2->removeAttribute("sys.chickens");
  eos::listAttributes(view(), cont2.get(), out, false);
  ASSERT_EQ(out.size(), 3u);
  ASSERT_EQ(out["sys.chickens"], "yes");
  ASSERT_EQ(out["sys.attr.link"], "/eos/dir1");
  ASSERT_EQ(out["user.qwerty"], "asdf");

  eos::listAttributes(view(), cont2.get(), out, true);
  ASSERT_EQ(out.size(), 3u);

  ASSERT_EQ(out["sys.link.chickens"], "yes");
  ASSERT_EQ(out["sys.attr.link"], "/eos/dir1");
  ASSERT_EQ(out["user.qwerty"], "asdf");
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

  // gids.erase(13);
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

  // uids.clear();
  // gids.clear();

  ASSERT_EQ(qn.getUids(), uids);
  ASSERT_EQ(qn.getGids(), gids);
}

TEST(Resolver, FidParsing) {
  XrdOucString str = "fid:123";
  ASSERT_EQ(FileIdentifier(123), Resolver::retrieveFileIdentifier(str));

  str = "asdef234";
  ASSERT_EQ(FileIdentifier(0), Resolver::retrieveFileIdentifier(str));

  str = "fxid:0x12f";
  ASSERT_EQ(FileIdentifier(303), Resolver::retrieveFileIdentifier(str));

  str = "fxid:12f";
  ASSERT_EQ(FileIdentifier(303), Resolver::retrieveFileIdentifier(str));

  str = "ino:0x3e70000000"; // fid: 999, old encoding
  ASSERT_EQ(FileIdentifier(999), Resolver::retrieveFileIdentifier(str));

  str = "ino:zzzz";
  ASSERT_EQ(FileIdentifier(0), Resolver::retrieveFileIdentifier(str));

  str = "ino:123"; // cid: 123
  ASSERT_EQ(FileIdentifier(0), Resolver::retrieveFileIdentifier(str));

  str = "ino:0x80000000000003e7"; // fid: 999, new encoding
  ASSERT_EQ(FileIdentifier(999), Resolver::retrieveFileIdentifier(str));

  str = "ino:80000000000003e7"; // fid: 999, new encoding
  ASSERT_EQ(FileIdentifier(999), Resolver::retrieveFileIdentifier(str));
}

TEST(FileOrContainerIdentifier, BasicSanity) {
  FileOrContainerIdentifier empty;
  ASSERT_TRUE(empty.empty());
  ASSERT_FALSE(empty.isFile());
  ASSERT_FALSE(empty.isContainer());

  ASSERT_EQ(empty.toFileIdentifier(), FileIdentifier(0));
  ASSERT_EQ(empty.toContainerIdentifier(), ContainerIdentifier(0));

  FileOrContainerIdentifier file(FileIdentifier(111));
  ASSERT_FALSE(file.empty());
  ASSERT_TRUE(file.isFile());
  ASSERT_FALSE(file.isContainer());

  ASSERT_EQ(file.toFileIdentifier(), FileIdentifier(111));
  ASSERT_EQ(file.toContainerIdentifier(), ContainerIdentifier(0));

  FileOrContainerIdentifier container(ContainerIdentifier(222));
  ASSERT_FALSE(container.empty());
  ASSERT_FALSE(container.isFile());
  ASSERT_TRUE(container.isContainer());

  ASSERT_EQ(container.toFileIdentifier(), FileIdentifier(0));
  ASSERT_EQ(container.toContainerIdentifier(), ContainerIdentifier(222));
}

TEST(FutureVectorIterator, EmptyConstructor) {
  FutureVectorIterator<int> fvi;
  ASSERT_TRUE(fvi.isReady());
  ASSERT_TRUE(fvi.isMainFutureReady());

  int out;
  ASSERT_FALSE(fvi.fetchNext(out));
}

TEST(FutureVectorIterator, BasicSanity) {
  folly::Promise<std::vector<folly::Future<int>>> mainPromise;

  FutureVectorIterator<int> fvi(mainPromise.getFuture());
  ASSERT_FALSE(fvi.isReady());
  ASSERT_FALSE(fvi.isMainFutureReady());

  // Build our future vector
  std::vector<folly::Future<int>> mainVector;

  folly::Promise<int> p1;
  folly::Promise<int> p2;
  folly::Promise<int> p3;

  mainVector.emplace_back(p1.getFuture());
  mainVector.emplace_back(p2.getFuture());
  mainVector.emplace_back(p3.getFuture());

  mainPromise.setValue(std::move(mainVector));

  ASSERT_FALSE(fvi.isReady());
  ASSERT_TRUE(fvi.isMainFutureReady());
  ASSERT_EQ(fvi.size(), 3u);

  p1.setValue(9);

  ASSERT_TRUE(fvi.isReady());
  ASSERT_TRUE(fvi.isMainFutureReady());

  int val;
  ASSERT_TRUE(fvi.fetchNext(val));
  ASSERT_EQ(val, 9);

  ASSERT_FALSE(fvi.isReady());

  p3.setValue(999);
  ASSERT_FALSE(fvi.isReady());

  p2.setValue(8);
  ASSERT_TRUE(fvi.isReady());

  ASSERT_TRUE(fvi.fetchNext(val));
  ASSERT_EQ(val, 8);

  ASSERT_TRUE(fvi.isReady());
  ASSERT_TRUE(fvi.fetchNext(val));
  ASSERT_EQ(val, 999);

  ASSERT_TRUE(fvi.isReady());

  ASSERT_FALSE(fvi.fetchNext(val));
  ASSERT_TRUE(fvi.isReady());

  ASSERT_FALSE(fvi.fetchNext(val));
  ASSERT_TRUE(fvi.isReady());
}

TEST_F(VariousTests, QuotanodeCorruption) {
  IContainerMDPtr cont = view()->createContainer("/a/b/c/d/e/f/g", true);
  ASSERT_EQ(cont->getId(), 8);
  ASSERT_EQ(cont->getParentId(), 7);

  ASSERT_EQ(view()->getQuotaNode(cont.get()), nullptr);
  cont->setParentId(999);
  containerSvc()->updateStore(cont.get());
  ASSERT_EQ(view()->getQuotaNode(cont.get()), nullptr);

  shut_down_everything();

  cont = containerSvc()->getContainerMD(8);
  ASSERT_EQ(cont->getParentId(), 999);
  ASSERT_EQ(view()->getQuotaNode(cont.get()), nullptr);
}

TEST_F(VariousTests, UnlinkAllLocations) {
  std::shared_ptr<eos::IFileMD> file1 = view()->createFile("/my-file.txt");
  ASSERT_EQ(file1->getId(), 1);

  file1->addLocation(13);
  file1->unlinkLocation(13);
  file1->addLocation(13);

  file1->unlinkAllLocations();

  ASSERT_EQ(file1->getLocations().size(), 0u);
  ASSERT_EQ(file1->getUnlinkedLocations().size(), 1u);
}

TEST_F(VariousTests, CountContents) {
  eos::IContainerMDPtr cont1 = view()->createContainer("/dir-1/");
  eos::IContainerMDPtr cont2 = view()->createContainer("/dir-2/");
  ASSERT_EQ(cont1->getId(), 2);
  ASSERT_EQ(cont2->getId(), 3);

  eos::IFileMDPtr file1 = view()->createFile("/file-1");
  eos::IFileMDPtr file2 = view()->createFile("/file-2");
  eos::IFileMDPtr file3 = view()->createFile("/file-3");
  eos::IFileMDPtr file4 = view()->createFile("/file-4");

  ASSERT_EQ(file1->getId(), 1);
  ASSERT_EQ(file2->getId(), 2);

  mdFlusher()->synchronize();

  std::pair<folly::Future<uint64_t>, folly::Future<uint64_t>> counts =
    eos::MetadataFetcher::countContents(qcl(), ContainerIdentifier(1));
  ASSERT_EQ(std::move(counts.first).get(), 4u);
  ASSERT_EQ(std::move(counts.second).get(), 2u);

  counts = eos::MetadataFetcher::countContents(qcl(), ContainerIdentifier(2));
  ASSERT_EQ(std::move(counts.first).get(), 0u);
  ASSERT_EQ(std::move(counts.second).get(), 0u);
}

TEST_F(NamespaceExplorerF, MissingFile) {
  view()->createContainer("/dir-1/");
  view()->createContainer("/dir-2/");
  view()->createContainer("/dir-3/");
  view()->createContainer("/dir-4/");

  view()->createFile("/dir-1/file-1");
  view()->createFile("/dir-1/file-2");

  IFileMDPtr f = view()->createFile("/dir-1/file-3");
  ASSERT_EQ(f->getId(), 3);

  view()->createFile("/dir-1/file-4");
  view()->createFile("/dir-1/file-5");

  ASSERT_EQ(
    qclient::describeRedisReply(qcl().exec("lhdel", "eos-file-md", "3").get()),
    "(integer) 1"
  );

  mdFlusher()->synchronize();

  ExplorationOptions options;
  options.depthLimit = 999;

  NamespaceExplorer explorer("/", options, qcl(), executor());

  NamespaceItem item;

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_EQ(item.fullPath, "/");

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_EQ(item.fullPath, "/dir-1/");

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_EQ(item.fullPath, "/dir-1/file-1");

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_EQ(item.fullPath, "/dir-1/file-2");

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_EQ(item.fullPath, "/dir-1/file-4");

  ASSERT_TRUE(explorer.fetch(item));
  ASSERT_EQ(item.fullPath, "/dir-1/file-5");
}

TEST(AttributeExtraction, BasicSanity) {
  eos::ns::FileMdProto proto;
  std::string out;

  ASSERT_FALSE(AttributeExtraction::asString(proto, "aaa", out));

  ASSERT_TRUE(AttributeExtraction::asString(proto, "xattr.aaa", out));
  ASSERT_TRUE(out.empty());

  (*proto.mutable_xattrs())["user.test"] = "123";
  ASSERT_TRUE(AttributeExtraction::asString(proto, "xattr.user.test", out));
  ASSERT_EQ(out, "123");

  proto.set_id(1111);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "fid", out));
  ASSERT_EQ(out, "1111");

  proto.set_cont_id(22222);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "pid", out));
  ASSERT_EQ(out, "22222");

  proto.set_gid(333);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "gid", out));
  ASSERT_EQ(out, "333");

  proto.set_uid(444);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "uid", out));
  ASSERT_EQ(out, "444");

  proto.set_size(555);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "size", out));
  ASSERT_EQ(out, "555");

  unsigned long layout = eos::common::LayoutId::GetId(
    eos::common::LayoutId::kReplica,
    eos::common::LayoutId::kAdler,
    2,
    eos::common::LayoutId::k4k);

  proto.set_layout_id(layout);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "layout_id", out));
  ASSERT_EQ(out, "1048850");

  proto.set_flags(0777);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "flags", out));
  ASSERT_EQ(out, "777");

  proto.set_name("aaaaa");
  ASSERT_TRUE(AttributeExtraction::asString(proto, "name", out));
  ASSERT_EQ(out, "aaaaa");

  proto.set_link_name("bbbbbb");
  ASSERT_TRUE(AttributeExtraction::asString(proto, "link_name", out));
  ASSERT_EQ(out, "bbbbbb");

  struct timespec ctime;
  ctime.tv_sec = 1999;
  ctime.tv_nsec = 8888;
  proto.set_ctime(&ctime, sizeof(ctime));
  ASSERT_TRUE(AttributeExtraction::asString(proto, "ctime", out));
  ASSERT_EQ(out, "1999.8888");

  struct timespec mtime;
  mtime.tv_sec = 1998;
  mtime.tv_nsec = 7777;
  proto.set_mtime(&mtime, sizeof(mtime));
  ASSERT_TRUE(AttributeExtraction::asString(proto, "mtime", out));
  ASSERT_EQ(out, "1998.7777");

  char buff[32];
  buff[0] = 0x12; buff[1] = 0x23; buff[2] = 0x55; buff[3] = 0x99;
  buff[4] = 0xAA; buff[5] = 0xDD; buff[6] = 0x00; buff[7] = 0x55;
  proto.set_checksum(buff, 8);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "xs", out));
  ASSERT_EQ(out, "12235599");

  proto.add_locations(3);
  proto.add_locations(2);
  proto.add_locations(1);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "locations", out));
  ASSERT_EQ(out, "3,2,1");

  proto.add_unlink_locations(4);
  proto.add_unlink_locations(5);
  proto.add_unlink_locations(6);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "unlink_locations", out));
  ASSERT_EQ(out, "4,5,6");

  struct timespec stime;
  stime.tv_sec = 1997;
  stime.tv_nsec = 5555;
  proto.set_stime(&stime, sizeof(stime));
  ASSERT_TRUE(AttributeExtraction::asString(proto, "stime", out));
  ASSERT_EQ(out, "1997.5555");
}

