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
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief FileSystemView test
//------------------------------------------------------------------------------
#include "namespace/ns_quarkdb/accounting/SetChangeList.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemHandler.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/ns_quarkdb/tests/TestUtils.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/utils/BulkNsObjectLocker.hh"
#include "namespace/utils/RmrfHelper.hh"
#include <gtest/gtest.h>
#include <cstdlib>
#include <cstdint>
#include <ctime>
#include <sstream>
#include <unistd.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <chrono>

//------------------------------------------------------------------------------
// Randomize a location
//------------------------------------------------------------------------------
eos::IFileMD::location_t
getRandomLocation()
{
  return 1 + random() % 50;
}

//------------------------------------------------------------------------------
// Count replicas
//------------------------------------------------------------------------------
size_t
countReplicas(eos::IFsView* fs)
{
  size_t replicas = 0;

  for (auto it = fs->getFileSystemIterator(); it->valid(); it->next()) {
    replicas += fs->getNumFilesOnFs(it->getElement());
  }

  return replicas;
}

//------------------------------------------------------------------------------
// Count unlinked
//------------------------------------------------------------------------------
size_t
countUnlinked(eos::IFsView* fs)
{
  size_t unlinked = 0;

  for (auto it = fs->getFileSystemIterator(); it->valid(); it->next()) {
    unlinked += fs->getNumUnlinkedFilesOnFs(it->getElement());
  }

  return unlinked;
}

//------------------------------------------------------------------------------
// Test utility classes
//------------------------------------------------------------------------------
TEST(FileSystemView, FileSetKey)
{
  ASSERT_EQ(eos::RequestBuilder::keyFilesystemFiles(50), "fsview:50:files");
  ASSERT_EQ(eos::RequestBuilder::keyFilesystemFiles(123), "fsview:123:files");
  ASSERT_EQ(eos::RequestBuilder::keyFilesystemUnlinked(10), "fsview:10:unlinked");
  ASSERT_EQ(eos::RequestBuilder::keyFilesystemUnlinked(999),
            "fsview:999:unlinked");
}

TEST(FileSystemView, ParseFsId)
{
  eos::IFileMD::location_t fsid;
  bool unlinked;
  ASSERT_TRUE(eos::parseFsId("fsview:1:files", fsid, unlinked));
  ASSERT_EQ(fsid, 1);
  ASSERT_FALSE(unlinked);
  ASSERT_TRUE(eos::parseFsId("fsview:999:unlinked", fsid, unlinked));
  ASSERT_EQ(fsid, 999);
  ASSERT_TRUE(unlinked);
  ASSERT_FALSE(eos::parseFsId("fsview:9:99:unlinked", fsid, unlinked));
  ASSERT_FALSE(eos::parseFsId("fsview:999:uNlinked", fsid, unlinked));
  ASSERT_FALSE(eos::parseFsId("fsVIew:1337:unlinked", fsid, unlinked));
}

//------------------------------------------------------------------------------
// Concrete implementation tests
//------------------------------------------------------------------------------
class FileSystemViewF : public eos::ns::testing::NsTestsFixture {};

TEST_F(FileSystemViewF, BasicSanity)
{
  srandom(time(nullptr));
  view()->createContainer("/test/embed/embed1", true);
  std::shared_ptr<eos::IContainerMD> c =
    view()->createContainer("/test/embed/embed2", true);
  view()->createContainer("/test/embed/embed3", true);

  // Create some files
  for (int i = 0; i < 1000; ++i) {
    std::ostringstream o;
    o << "file" << i;
    std::shared_ptr<eos::IFileMD> files[4];
    files[0] = view()->createFile(std::string("/test/embed/") + o.str());
    files[1] = view()->createFile(std::string("/test/embed/embed1/") + o.str());
    files[2] = view()->createFile(std::string("/test/embed/embed2/") + o.str());
    files[3] = view()->createFile(std::string("/test/embed/embed3/") + o.str());

    for (int j = 0; j < 4; ++j) {
      while (files[j]->getNumLocation() != 5) {
        files[j]->addLocation(getRandomLocation());
      }

      view()->updateFileStore(files[j].get());
    }
  }

  // Create some file without replicas assigned
  for (int i = 0; i < 500; ++i) {
    std::ostringstream o;
    o << "noreplicasfile" << i;
    view()->createFile(std::string("/test/embed/embed1/") + o.str());
  }

  // Sum up all the locations
  mdFlusher()->synchronize();
  size_t numReplicas = countReplicas(fsview());
  ASSERT_EQ(numReplicas, 20000);
  size_t numUnlinked = countUnlinked(fsview());
  ASSERT_EQ(numUnlinked, 0);
  ASSERT_EQ(fsview()->getNumNoReplicasFiles(), 500);

  // Unlink replicas
  for (int i = 100; i < 500; ++i) {
    std::ostringstream o;
    o << "file" << i;
    // Unlink some replicas
    std::shared_ptr<eos::IFileMD> f = c->findFile(o.str());
    f->unlinkLocation(f->getLocation(0));
    f->unlinkLocation(f->getLocation(0));
    view()->updateFileStore(f.get());
  }

  mdFlusher()->synchronize();
  numReplicas = countReplicas(fsview());
  ASSERT_EQ(numReplicas, 19200);
  numUnlinked = countUnlinked(fsview());
  ASSERT_EQ(numUnlinked, 800);
  std::list<eos::IFileMD::id_t> file_ids;

  for (int i = 500; i < 900; ++i) {
    std::ostringstream o;
    o << "file" << i;
    // Unlink some replicas
    std::shared_ptr<eos::IFileMD> f{c->findFile(o.str())};
    f->unlinkAllLocations();
    c->removeFile(o.str());
    f->setContainerId(0);
    file_ids.push_back(f->getId());
    view()->updateFileStore(f.get());
  }

  mdFlusher()->synchronize();
  numReplicas = countReplicas(fsview());
  ASSERT_EQ(numReplicas, 17200);
  numUnlinked = countUnlinked(fsview());
  ASSERT_EQ(numUnlinked, 2800);
  // Restart
  shut_down_everything();
  numReplicas = countReplicas(fsview());
  ASSERT_EQ(numReplicas, 17200);
  numUnlinked = countUnlinked(fsview());
  ASSERT_EQ(numUnlinked, 2800);
  ASSERT_EQ(fsview()->getNumNoReplicasFiles(), 500);
  std::shared_ptr<eos::IFileMD> f{
    view()->getFile(std::string("/test/embed/embed1/file1"))};
  ASSERT_EQ(view()->getUri(f.get()), "/test/embed/embed1/file1");
  f->unlinkAllLocations();
  numReplicas = countReplicas(fsview());
  ASSERT_EQ(numReplicas, 17195);
  numUnlinked = countUnlinked(fsview());
  ASSERT_EQ(numUnlinked, 2805);
  f->removeAllLocations();
  numUnlinked = countUnlinked(fsview());
  ASSERT_EQ(numUnlinked, 2800);
  view()->updateFileStore(f.get());
  ASSERT_EQ(fsview()->getNumNoReplicasFiles(), 501);
  view()->removeFile(f.get());
  ASSERT_EQ(fsview()->getNumNoReplicasFiles(), 500);
  shut_down_everything();

  // Cleanup - remove all files
  for (int i = 0; i < 1000; ++i) {
    std::ostringstream o;
    o << "file" << i;
    std::list<std::string> paths;
    paths.push_back("/test/embed/" + o.str());
    paths.push_back("/test/embed/embed1/" + o.str());
    paths.push_back("/test/embed/embed2/" + o.str());
    paths.push_back("/test/embed/embed3/" + o.str());

    for (auto && elem : paths) {
      // Skip the files that have already been removed
      if ((elem == "/test/embed/embed1/file1") ||
          (i >= 500 && i < 900 && elem.find("/test/embed/embed2/") == 0)) {
        continue;
      }

      std::shared_ptr<eos::IFileMD> file{view()->getFile(elem)};
      ASSERT_EQ(view()->getUri(file.get()), elem);
      view()->unlinkFile(file.get());
      file->removeAllLocations();
      view()->removeFile(file.get());
    }
  }

  // Remove the files that were unlinked only
  for (auto && id : file_ids) {
    std::shared_ptr<eos::IFileMD> file = fileSvc()->getFileMD(id);
    file->removeAllLocations();
    view()->removeFile(file.get());
  }

  for (int i = 0; i < 500; ++i) {
    std::ostringstream o;
    o << "noreplicasfile" << i;
    std::string path = "/test/embed/embed1/" + o.str();
    std::shared_ptr<eos::IFileMD> file{view()->getFile(path)};
    ASSERT_EQ(view()->getUri(file.get()), path);
    view()->unlinkFile(file.get());
    view()->removeFile(file.get());
  }

  // Remove all containers
  eos::RmrfHelper::nukeDirectory(view(), "/test/");
  // Remove the root container
  std::shared_ptr<eos::IContainerMD> root{view()->getContainer("/")};
  containerSvc()->removeContainer(root.get());
}

//------------------------------------------------------------------------------
// Test retrieval of random file ids in a filesystem view
//------------------------------------------------------------------------------
TEST_F(FileSystemViewF, RandomFilePicking)
{
  view()->createContainer("/test/", true);

  for (size_t i = 1; i < 200; i++) {
    std::shared_ptr<eos::IFileMD> file = view()->createFile(SSTR("/test/" << i));
    ASSERT_EQ(view()->getUri(file.get()), SSTR("/test/" << i));
    ASSERT_EQ(view()->getUriFut(file->getIdentifier()).get(), SSTR("/test/" << i));

    // Even files go to fs #1, odd go to #2
    if (i % 2 == 0) {
      file->addLocation(1);
    } else {
      file->addLocation(2);
    }

    view()->updateFileStore(file.get());
  }

  mdFlusher()->synchronize();

  for (size_t i = 0; i < 1000; i++) {
    eos::IFileMD::id_t randomPick;
    ASSERT_TRUE(fsview()->getApproximatelyRandomFileInFs(1, randomPick));
    ASSERT_TRUE(randomPick % 2 == 0);

    if (i < 10) {
      std::cout << "Random file in fs #1: " << randomPick << std::endl;
    }

    ASSERT_TRUE(fsview()->getApproximatelyRandomFileInFs(2, randomPick));
    ASSERT_TRUE(randomPick % 2 == 1);

    if (i < 10) {
      std::cout << "Random file in fs #2: " << randomPick << std::endl;
    }
  }

  eos::IFileMD::id_t randomPick;
  ASSERT_FALSE(fsview()->getApproximatelyRandomFileInFs(3, randomPick));
  ASSERT_FALSE(fsview()->getApproximatelyRandomFileInFs(5, randomPick));
  ASSERT_FALSE(fsview()->getApproximatelyRandomFileInFs(4, randomPick));
}

//------------------------------------------------------------------------------
// Test file iterator on top of QHash object
//------------------------------------------------------------------------------
TEST_F(FileSystemViewF, FileIterator)
{
  std::srand(std::time(0));
  std::unordered_set<eos::IFileMD::id_t> input_set;

  for (std::uint64_t i = 0ull; i < 50000; ++i) {
    double frac = std::rand() / (double)RAND_MAX;
    (void)input_set.insert((uint64_t)(UINT64_MAX * frac));
  }

  // Push the set to QuarkDB
  qclient::AsyncHandler ah;
  const std::string key = "set_iter_test";
  qclient::QSet set(qcl(), key);

  for (auto elem : input_set) {
    set.sadd_async(std::to_string(elem), &ah);
  }

  ASSERT_TRUE(ah.Wait());
  std::unordered_set<eos::IFileMD::id_t> result_set;
  auto iter = std::shared_ptr<eos::ICollectionIterator<eos::IFileMD::id_t>>
              (new eos::StreamingFileListIterator(qcl(), key));

  for (; (iter && iter->valid()); iter->next()) {
    result_set.insert(iter->getElement());
  }

  ASSERT_EQ(input_set.size(), result_set.size());

  for (auto elem : input_set) {
    ASSERT_TRUE(result_set.find(elem) != result_set.end());
  }

  qcl().del(key);
}

//------------------------------------------------------------------------------
// Test file list iterator
//------------------------------------------------------------------------------
TEST_F(FileSystemViewF, FileListIterator)
{
  view()->createContainer("/test/", true);
  eos::IFileMDPtr f1 = view()->createFile("/test/f1");
  ASSERT_EQ(f1->getIdentifier(), eos::FileIdentifier(1));
  f1->addLocation(1);
  f1->addLocation(2);
  f1->addLocation(3);
  eos::IFileMDPtr f2 = view()->createFile("/test/f2");
  ASSERT_EQ(f2->getIdentifier(), eos::FileIdentifier(2));
  f2->addLocation(2);
  eos::IFileMDPtr f3 = view()->createFile("/test/f3");
  ASSERT_EQ(f3->getIdentifier(), eos::FileIdentifier(3));
  f3->addLocation(2);
  f3->addLocation(3);
  eos::IFileMDPtr f4 = view()->createFile("/test/f4");
  ASSERT_EQ(f4->getIdentifier(), eos::FileIdentifier(4));
  f4->addLocation(4);
  {
    auto it = fsview()->getFileList(1);
    ASSERT_TRUE(it->valid());
    ASSERT_EQ(it->getElement(), 1u);
    it->next();
    ASSERT_FALSE(it->valid());
  }
  ASSERT_TRUE(eos::ns::testing::verifyContents(fsview()->getFileList(2),
              std::set<eos::IFileMD::id_t> { 1, 2, 3 }));
  ASSERT_TRUE(eos::ns::testing::verifyContents(fsview()->getFileList(3),
              std::set<eos::IFileMD::id_t> { 1, 3 }));
  ASSERT_FALSE(eos::ns::testing::verifyContents(fsview()->getFileList(3),
               std::set<eos::IFileMD::id_t> { 1, 2, 3 }));
  ASSERT_TRUE(eos::ns::testing::verifyContents(fsview()->getFileList(4),
              std::set<eos::IFileMD::id_t> { 4 }));
  shut_down_everything();
  ASSERT_TRUE(eos::ns::testing::verifyContents(fsview()->getFileList(2),
              std::set<eos::IFileMD::id_t> { 1, 2, 3 }));
  ASSERT_TRUE(eos::ns::testing::verifyContents(fsview()->getFileList(3),
              std::set<eos::IFileMD::id_t> { 1, 3 }));
  ASSERT_FALSE(eos::ns::testing::verifyContents(fsview()->getFileList(3),
               std::set<eos::IFileMD::id_t> { 1, 2, 3 }));
  ASSERT_TRUE(eos::ns::testing::verifyContents(fsview()->getFileList(4),
              std::set<eos::IFileMD::id_t> { 4 }));
  ASSERT_TRUE(eos::ns::testing::verifyContents(fsview()->getStreamingFileList(2),
              std::set<eos::IFileMD::id_t> { 1, 2, 3 }));
  ASSERT_TRUE(eos::ns::testing::verifyContents(fsview()->getStreamingFileList(3),
              std::set<eos::IFileMD::id_t> { 1, 3 }));
  ASSERT_FALSE(eos::ns::testing::verifyContents(fsview()->getStreamingFileList(3),
               std::set<eos::IFileMD::id_t> { 1, 2, 3 }));
  ASSERT_TRUE(eos::ns::testing::verifyContents(fsview()->getStreamingFileList(4),
              std::set<eos::IFileMD::id_t> { 4 }));
}

//------------------------------------------------------------------------------
// Tests targetting FileSystemHandler
//------------------------------------------------------------------------------
TEST_F(FileSystemViewF, FileSystemHandler)
{
  // We're only using FileSystemHandler on its own in this test, don't spin
  // up the rest of the namespace..
  std::unique_ptr<folly::Executor> executor;
  executor.reset(new folly::IOThreadPoolExecutor(16));
  {
    eos::FileSystemHandler fs1(1, executor.get(), &qcl(), mdFlusher(), false);
    ASSERT_EQ(fs1.getRedisKey(), "fsview:1:files");
    fs1.insert(eos::FileIdentifier(1));
    fs1.insert(eos::FileIdentifier(8));
    fs1.insert(eos::FileIdentifier(10));
    fs1.insert(eos::FileIdentifier(20));
    ASSERT_TRUE(eos::ns::testing::verifyContents(fs1.getFileList(),
                std::set<eos::IFileMD::id_t> {1, 8, 10, 20}));
    ASSERT_FALSE(eos::ns::testing::verifyContents(fs1.getFileList(),
                 std::set<eos::IFileMD::id_t> {1, 8, 20}));
    ASSERT_FALSE(eos::ns::testing::verifyContents(fs1.getFileList(),
                 std::set<eos::IFileMD::id_t> {1, 8, 10, 20, 30}));
    fs1.erase(eos::FileIdentifier(30));
    ASSERT_TRUE(eos::ns::testing::verifyContents(fs1.getFileList(),
                std::set<eos::IFileMD::id_t> {1, 8, 10, 20}));
    fs1.erase(eos::FileIdentifier(20));
    ASSERT_TRUE(eos::ns::testing::verifyContents(fs1.getFileList(),
                std::set<eos::IFileMD::id_t> {1, 8, 10}));
    fs1.insert(eos::FileIdentifier(20));
    ASSERT_TRUE(eos::ns::testing::verifyContents(fs1.getFileList(),
                std::set<eos::IFileMD::id_t> {1, 8, 10, 20}));
  }
  shut_down_everything();

  // Make sure we pick up on any changes.
  for (int i = 0; i < 3; i++) {
    eos::FileSystemHandler fs1(1, executor.get(), &qcl(), mdFlusher(), false);
    ASSERT_TRUE(eos::ns::testing::verifyContents(fs1.getFileList(),
                std::set<eos::IFileMD::id_t> {1, 8, 10, 20}));
    ASSERT_FALSE(eos::ns::testing::verifyContents(fs1.getFileList(),
                 std::set<eos::IFileMD::id_t> {1, 8, 20}));
    ASSERT_FALSE(eos::ns::testing::verifyContents(fs1.getFileList(),
                 std::set<eos::IFileMD::id_t> {1, 8, 10, 20, 30}));
  }

  // Re-iterate just in case, this time verify contents directly from QDB.
  qclient::QSet qset(qcl(), eos::RequestBuilder::keyFilesystemFiles(1));
  {
    auto it = qset.getIterator();
    ASSERT_TRUE(eos::ns::testing::verifyContents(&it, std::set<std::string> { "1", "8", "10", "20" }));
  }
  {
    auto it = qset.getIterator();
    ASSERT_FALSE(eos::ns::testing::verifyContents(&it, std::set<std::string> { "1", "8", "10" }));
  }
  {
    auto it = qset.getIterator();
    ASSERT_FALSE(eos::ns::testing::verifyContents(&it, std::set<std::string> { "1", "8", "10", "20", "30" }));
  }
  shut_down_everything();
  // Add item, make sure change is reflected in QDB.
  {
    eos::FileSystemHandler fs1(1, executor.get(), &qcl(), mdFlusher(), false);
    ASSERT_TRUE(eos::ns::testing::verifyContents(fs1.getFileList(),
                std::set<eos::IFileMD::id_t> {1, 8, 10, 20}));
    fs1.insert(eos::FileIdentifier(99));
    ASSERT_TRUE(eos::ns::testing::verifyContents(fs1.getFileList(),
                std::set<eos::IFileMD::id_t> {1, 8, 10, 20, 99}));
    mdFlusher()->synchronize();
    // Try streaming iterator
    ASSERT_TRUE(eos::ns::testing::verifyContents(fs1.getStreamingFileList(),
                std::set<eos::IFileMD::id_t> {1, 8, 10, 20, 99}));
  }
  shut_down_everything();
  {
    qclient::QSet qset2(qcl(), eos::RequestBuilder::keyFilesystemFiles(1));
    auto it = qset2.getIterator();
    ASSERT_TRUE(eos::ns::testing::verifyContents(&it, std::set<std::string> { "1", "8", "10", "20", "99" }));
  }
  // Nuke filelist
  {
    eos::FileSystemHandler fs1(1, executor.get(), &qcl(), mdFlusher(), false);
    ASSERT_TRUE(eos::ns::testing::verifyContents(fs1.getFileList(),
                std::set<eos::IFileMD::id_t> {1, 8, 10, 20, 99}));
    fs1.nuke();
    ASSERT_TRUE(eos::ns::testing::verifyContents(fs1.getFileList(),
                std::set<eos::IFileMD::id_t> { }));
  }
  mdFlusher()->synchronize();
  // Ensure it's empty in the backend as well
  {
    qclient::QSet qset2(qcl(), eos::RequestBuilder::keyFilesystemFiles(1));
    auto it = qset2.getIterator();
    ASSERT_TRUE(eos::ns::testing::verifyContents(&it, std::set<std::string> { }));
  }
}

TEST_F(FileSystemViewF, BulkNsObjectLocker)
{
  {
    auto container = view()->createContainer("/test/", true);
    auto container2 = view()->createContainer("/test/d1", true);
    eos::BulkNsObjectLocker<eos::IContainerMDPtr, eos::IContainerMD::IContainerMDReadLocker>
    locker;
    locker.add(container2);
    locker.add(container);
    auto locks = locker.lockAll();
    // The order of the locks should be by ascending order of the container identifier
    ASSERT_EQ(2, locks.size());
    ASSERT_EQ("test", locks[0]->getUnderlyingPtr()->getName());
    ASSERT_EQ("d1", locks[1]->getUnderlyingPtr()->getName());
  }
  {
    auto file1 = view()->createFile("/test/f1");
    auto file2 = view()->createFile("/test/d1/f2");
    eos::BulkNsObjectLocker<eos::IFileMDPtr, eos::IFileMD::IFileMDWriteLocker>
    locker;
    locker.add(file2);
    locker.add(file1);
    auto locks = locker.lockAll();
    ASSERT_EQ(2, locks.size());
    ASSERT_EQ("f1", locks[0]->getUnderlyingPtr()->getName());
    ASSERT_EQ("f2", locks[1]->getUnderlyingPtr()->getName());
  }
}

TEST_F(FileSystemViewF, fileMDLockedSetSize)
{
  auto container = view()->createContainer("/test/", true);
  eos::IFileMDPtr f1 = view()->createFile("/test/f1");
  std::vector<std::thread> workers;
  f1->setSize(100);
  {
    //10 threads, each of them running a loop
    for (int i = 0; i < 10; ++i) {
      workers.push_back(std::thread([this, i, &f1]() {
        eos::IFileMD::IFileMDWriteLocker lock(f1);

        for (int j = 0; j < 10; ++j) {
          if (i % 2 == 0) {
            f1->setSize(f1->getSize() + 1);
          } else {
            f1->setSize(f1->getSize() - 1);
          }
        }

        view()->updateFileStore(f1.get());
      }));
    }
  }
  std::for_each(workers.begin(), workers.end(), [](std::thread & t) {
    t.join();
  });
  //We sleep 6 seconds the time for the ContainerAccountingThread to take into account the size change of the file
  ::sleep(6);
  ASSERT_EQ(100, f1->getSize());
  ASSERT_EQ(100, container->getTreeSize());
}

TEST_F(FileSystemViewF, fileMDLockedClone)
{
  view()->createContainer("/test/", true);
  eos::IFileMDPtr f1 = view()->createFile("/test/f1");
  ASSERT_EQ(f1->getIdentifier(), eos::FileIdentifier(1));
  eos::IFileMDPtr f2;
  {
    eos::IFileMD::IFileMDWriteLocker fileMDLocked(f1);
    f2.reset(fileMDLocked->clone());
    eos::IFileMD::IFileMDReadLocker file2MDLocked(f2);
    ASSERT_EQ(f1->getIdentifier(), f2->getIdentifier());
  }
  ASSERT_EQ(eos::FileIdentifier(1), f1->getIdentifier());
  ASSERT_EQ(f1->getIdentifier(), f2->getIdentifier());
}

TEST_F(FileSystemViewF, fileMDLockedLocation)
{
  view()->createContainer("/test/", true);
  eos::IFileMDPtr f1 = view()->createFile("/test/f1");
  {
    eos::IFileMD::IFileMDWriteLocker lock(f1);
    f1->addLocation(1);
    ASSERT_EQ(1, f1->getLocation(0));
  }
  std::vector<std::thread> addLocationWorkers;
  {
    //10 threads, each one adds a location
    for (int i = 0; i < 10; ++i) {
      addLocationWorkers.push_back(std::thread([this, i, &f1]() {
        eos::IFileMD::IFileMDWriteLocker lock(f1);

        for (int j = 0; j < 10; ++j) {
          f1->addLocation((i * 10) + j);
        }

        view()->updateFileStore(f1.get());
      }));
    }
  }
  std::for_each(addLocationWorkers.begin(),
  addLocationWorkers.end(), [](std::thread & t) {
    t.join();
  });
  std::vector<std::thread> hasLocationWorkers;
  {
    for (int i = 0; i < 10; ++i) {
      hasLocationWorkers.push_back(std::thread([i, &f1]() {
        eos::IFileMD::IFileMDWriteLocker lock(f1);

        for (int j = 0; j < 10; ++j) {
          ASSERT_TRUE(f1->hasLocation((i * 10) + j));
        }
      }));
    }
  }
  std::for_each(hasLocationWorkers.begin(),
  hasLocationWorkers.end(), [](std::thread & t) {
    t.join();
  });
  {
    eos::IFileMD::IFileMDWriteLocker lock(f1);
    ASSERT_EQ(100, f1->getNumLocation());
    auto locations = f1->getLocations();
    ASSERT_EQ(100, locations.size());
  }
  std::vector<std::thread> removeLocationWorkers;
  {
    //10 threads, each one adds a location
    for (int i = 0; i < 10; ++i) {
      removeLocationWorkers.push_back(std::thread([this, i, &f1]() {
        eos::IFileMD::IFileMDWriteLocker lock(f1);

        for (int j = 0; j < 10; ++j) {
          f1->unlinkLocation((i * 10) + j);
          f1->removeLocation((i * 10) + j);
        }

        view()->updateFileStore(f1.get());
      }));
    }
  }
  std::for_each(removeLocationWorkers.begin(),
  removeLocationWorkers.end(), [](std::thread & t) {
    t.join();
  });
  ASSERT_EQ(0, f1->getNumLocation());
  //Add again 100 locations
  addLocationWorkers.clear();

  for (int i = 0; i < 10; ++i) {
    addLocationWorkers.push_back(std::thread([this, i, &f1]() {
      for (int j = 0; j < 10; ++j) {
        f1->addLocation((i * 10) + j);
        view()->updateFileStore(f1.get());
      }
    }));
  }

  std::for_each(addLocationWorkers.begin(),
  addLocationWorkers.end(), [](std::thread & t) {
    t.join();
  });
  //Try the removeAllLocation
  {
    eos::IFileMD::IFileMDWriteLocker lock(f1);
    f1->unlinkAllLocations();
    f1->removeAllLocations();
    view()->updateFileStore(f1.get());
  }
  ASSERT_EQ(0, f1->getNumLocation());
  addLocationWorkers.clear();

  for (int i = 0; i < 10; ++i) {
    addLocationWorkers.push_back(std::thread([this, i, &f1]() {
      for (int j = 0; j < 10; ++j) {
        f1->addLocation((i * 10) + j);
      }

      view()->updateFileStore(f1.get());
    }));
  }

  std::for_each(addLocationWorkers.begin(),
  addLocationWorkers.end(), [](std::thread & t) {
    t.join();
  });
  f1->unlinkLocation(0);
  ASSERT_EQ(1, f1->getUnlinkedLocations().size());
  ASSERT_EQ(1, f1->getNumUnlinkedLocation());
  f1->clearUnlinkedLocations();
  view()->updateFileStore(f1.get());
  ASSERT_EQ(0, f1->getNumUnlinkedLocation());
}

TEST_F(FileSystemViewF, fileMDLockedRemoveLocation)
{
  view()->createContainer("/test/", true);
  eos::IFileMDPtr f1 = view()->createFile("/test/f1");
  std::vector<std::thread> addLocationWorkers;
  std::vector<std::thread> removeLocationWorkers;

  for (int i = 0; i < 10; ++i) {
    addLocationWorkers.push_back(std::thread([this, i, &f1]() {
      eos::IFileMD::IFileMDWriteLocker lock(f1);

      for (int j = 0; j < 10; ++j) {
        f1->addLocation((i * 10) + j);
        view()->updateFileStore(f1.get());
      }
    }));
  }

  std::for_each(addLocationWorkers.begin(),
  addLocationWorkers.end(), [](std::thread & t) {
    t.join();
  });

  for (int i = 0; i < 10; ++i) {
    removeLocationWorkers.push_back(std::thread([this, i, &f1]() {
      eos::IFileMD::IFileMDWriteLocker lock(f1);

      for (int j = 0; j < 10; ++j) {
        f1->unlinkLocation((i * 10) + j);
        f1->removeLocation((i * 10) + j);
        view()->updateFileStore(f1.get());
      }
    }));
  }

  std::for_each(removeLocationWorkers.begin(),
  removeLocationWorkers.end(), [](std::thread & t) {
    t.join();
  });
  ASSERT_EQ(0, f1->getNumLocation());
}

TEST_F(FileSystemViewF, containerMDFindItem)
{
  const int nbLoops = 10;
  eos::IContainerMDPtr testCont = view()->createContainer("/test/", true);

  for (int i = 0; i < nbLoops; ++i) {
    std::stringstream fileName;
    std::stringstream contName;
    fileName << "/test/f" << i;
    contName << "/test/c" << i;
    view()->createFile(fileName.str());
    view()->createContainer(contName.str());
  }

  std::vector<std::thread> workers;

  for (int i = 1; i < nbLoops; ++i) {
    workers.push_back(std::thread([i, testCont]() {
      {
        std::stringstream fileName;
        std::stringstream contName;
        fileName << "f" << i;
        contName << "c" << i;
        eos::IContainerMD::IContainerMDReadLocker containerMDLocker(testCont);
        {
          auto contOrFile = testCont->findItem(fileName.str()).get();
          ASSERT_TRUE(contOrFile.file != nullptr);
        }
        {
          auto contOrFile = testCont->findItem(contName.str()).get();
          ASSERT_TRUE(contOrFile.container != nullptr);
        }
      }
    }));
  }

  {
    eos::IContainerMD::IContainerMDReadLocker containerMDLocker(testCont);
    {
      auto contOrFile = testCont->findItem("f0").get();
      ASSERT_TRUE(contOrFile.file != nullptr);
    }
    {
      auto contOrFile = testCont->findItem("c0").get();
      ASSERT_TRUE(contOrFile.container != nullptr);
    }
  }

  for (auto& t : workers) {
    t.join();
  }
}

TEST_F(FileSystemViewF, containerMDAddContainerThenRemove)
{
  auto rootContainer = view()->createContainer("/root/", true);
  auto rootContainerID = rootContainer->getId();
  eos::IContainerMD::IContainerMDWriteLocker rootLocker(rootContainer);
  auto testContainer = view()->createContainer("/test/", true);
  auto testContainerID = testContainer->getId();
  rootContainer->addContainer(testContainer.get());
  view()->updateContainerStore(rootContainer.get());
  ASSERT_EQ(1, rootContainer->getNumContainers());
  ASSERT_EQ(rootContainerID, testContainer->getParentId());
  ASSERT_EQ(testContainerID,
            testContainer->getIdentifier().getUnderlyingUInt64());
  rootContainer->removeContainer("test");
  view()->updateContainerStore(rootContainer.get());
  ASSERT_EQ(0, rootContainer->getNumContainers());
}

TEST_F(FileSystemViewF, containerMDaddFileThenRemove)
{
  auto rootContainer = view()->createContainer("/root/", true);
  eos::IContainerMD::IContainerMDWriteLocker rootLocker(rootContainer);
  auto testFile = view()->createFile("/root/test");
  rootContainer->addFile(testFile.get());
  ASSERT_EQ(1, rootContainer->getNumFiles());
  rootContainer->removeFile("test");
  ASSERT_EQ(0, rootContainer->getNumFiles());
}

TEST_F(FileSystemViewF, containerMDGetSetName)
{
  auto rootContainer = view()->createContainer("/root/", true);
  ASSERT_EQ("root", rootContainer->getName());
  eos::IContainerMD::IContainerMDWriteLocker rootLocker(rootContainer);
  rootContainer->setName("newname");
  ASSERT_EQ("newname", rootContainer->getName());
}

TEST_F(FileSystemViewF, containerMDBasicGettersSetters)
{
  auto rootContainer = view()->createContainer("/root/", true);
  eos::IContainerMD::IContainerMDWriteLocker rootLocker(rootContainer);
  rootContainer->setCUid(2);
  ASSERT_EQ(2, rootContainer->getCUid());
  rootContainer->setCGid(23);
  ASSERT_EQ(23, rootContainer->getCGid());
  rootContainer->setCloneId(42);
  ASSERT_EQ(42, rootContainer->getCloneId());
  rootContainer->setCloneFST("clone_fst");
  ASSERT_EQ("clone_fst", rootContainer->getCloneFST());
  rootContainer->setMode(S_IRWXU);
  ASSERT_EQ(S_IRWXU, rootContainer->getMode());
  rootContainer->setTreeSize(64);
  ASSERT_EQ(64, rootContainer->getTreeSize());
  eos::IContainerMD::ctime_t tnow;
  clock_gettime(CLOCK_REALTIME, &tnow);
  rootContainer->setCTime(tnow);
  eos::IContainerMD::ctime_t containerTime;
  rootContainer->getCTime(containerTime);
  ASSERT_EQ(tnow.tv_sec, containerTime.tv_sec);
  rootContainer->setMTime(tnow);
  rootContainer->getMTime(containerTime);
  ASSERT_EQ(tnow.tv_sec, containerTime.tv_sec);
  eos::IContainerMD::tmtime_t containerTMTime;
  rootContainer->setTMTimeNow();
  rootContainer->getTMTime(containerTMTime);
  rootContainer->setTMTimeNow();
  eos::IContainerMD::tmtime_t newContainerTMTime;
  rootContainer->getTMTime(newContainerTMTime);
  ASSERT_NE(containerTMTime.tv_nsec, newContainerTMTime.tv_nsec);
}

TEST_F(FileSystemViewF, containerMDSyncTimeAccounting)
{
  auto containerSyncTimeAccounting =
    view()->createContainer("/root/test/containersynctimeaccounting/", true);
  eos::IContainerMDPtr rootContainer;
  eos::IContainerMD::ctime_t rootContainerTimeBeforeNotify,
      rootContainerMTimeAfterNotify;
  {
    eos::IContainerMD::IContainerMDWriteLocker
    containerSyncTimeAccountingLocker(containerSyncTimeAccounting);
    containerSyncTimeAccounting->setAttribute("sys.mtime.propagation", "true");
    auto testContainer = view()->getContainer("/root/test/");
    testContainer->setAttribute("sys.mtime.propagation", "true");
    rootContainer = view()->getContainer("/root/");
    rootContainer->setAttribute("sys.mtime.propagation", "true");
    rootContainer->setMTimeNow();
    rootContainer->getTMTime(rootContainerTimeBeforeNotify);
    ::sleep(1);
    containerSyncTimeAccounting->setMTimeNow();
    containerSyncTimeAccounting->notifyMTimeChange(containerSvc());
    view()->updateContainerStore(containerSyncTimeAccounting.get());
  }
  //Sleep 6 seconds the time the Container Accounting Thread does its job...
  ::sleep(6);
  rootContainer->getTMTime(rootContainerMTimeAfterNotify);
  ASSERT_EQ(rootContainerTimeBeforeNotify.tv_sec + 1,
            rootContainerMTimeAfterNotify.tv_sec);
}

TEST_F(FileSystemViewF, containerMDAttributesOps)
{
  auto rootContainer = view()->createContainer("/root/", true);
  eos::IContainerMD::IContainerMDWriteLocker rootLocker(rootContainer);
  rootContainer->setAttribute("attribute1", "value1");
  rootContainer->setAttribute("attribute2", "value2");
  ASSERT_TRUE(rootContainer->hasAttribute("attribute1"));
  ASSERT_EQ("value1", rootContainer->getAttribute("attribute1"));
  ASSERT_THROW(rootContainer->getAttribute("DOES_NOT_EXIST"), eos::MDException);
  ASSERT_EQ(2, rootContainer->numAttributes());
  ASSERT_EQ(2, rootContainer->getAttributes().size());
  rootContainer->removeAttribute("attribute1");
  ASSERT_EQ(1, rootContainer->numAttributes());
}

TEST_F(FileSystemViewF, getFileOrContainerMDLocked)
{
  view()->createContainer("/root/", true);
  auto file = view()->createFile("/root/file1");
  {
    auto containerWriteLocked = view()->getContainerWriteLocked("/root/");
    containerWriteLocked->getUnderlyingPtr()->setAttribute("testKey", "testValue");
    auto file1WriteLocked = view()->getFileWriteLocked("/root/file1");
    containerWriteLocked->getUnderlyingPtr()->addFile(
      file1WriteLocked->getUnderlyingPtr().get());
  }
  {
    auto containerReadLock = view()->getContainerReadLocked("/root/");
    ASSERT_EQ("testValue",
              containerReadLock->getUnderlyingPtr()->getAttribute("testKey"));
    ASSERT_EQ(file->getContainerId(),
              containerReadLock->getUnderlyingPtr()->getId());
  }
}

TEST_F(FileSystemViewF, getFileWhileBeingWriteLocked)
{
  using namespace std::chrono_literals;
  view()->createContainer("/root/", true);
  auto file = view()->createFile("/root/file1");
  //Create two threads, one will write lock the file and wait X seconds, one will
  //try to retrieve the file from the view
  std::atomic<bool> fileLocked = false;
  uint8_t sleepSeconds = 3;
  auto threadWriteLockingFile = std::thread([&file, &fileLocked, sleepSeconds]() {
    eos::IFileMD::IFileMDWriteLocker fileLocker(file);
    fileLocked = true;
    std::this_thread::sleep_for(std::chrono::duration<double>(sleepSeconds + 0.1));
  });
  std::chrono::time_point<std::chrono::steady_clock> start;
  std::chrono::time_point<std::chrono::steady_clock> stop;
  auto threadGetFile = std::thread([this, &start, &stop, &fileLocked]() {
    while (!fileLocked) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100ms));
    }

    start = std::chrono::steady_clock::now();
    view()->getFile("/root/file1");
    stop = std::chrono::steady_clock::now();
  });
  threadWriteLockingFile.join();
  threadGetFile.join();
  auto diff = std::chrono::duration_cast<std::chrono::seconds>
              (stop - start).count();
  ASSERT_EQ(sleepSeconds, diff);
}

TEST_F(FileSystemViewF, getFileAfterBeingRenamed)
{
  auto root = view()->createContainer("/root/", true);
  auto file = view()->createFile("/root/file1");
  //Create two threads, one will write lock the file and rename it, one will
  //wait X seconds, asks the view to retrieve it and see if it locks.
  std::atomic<bool> fileRenamed = false;
  std::atomic<bool> threadRenameStarted = false;
  uint8_t sleepSeconds = 3;
  auto threadWriteLockingFile = std::thread([this, &file, &root, &fileRenamed,
  &threadRenameStarted, sleepSeconds]() {
    threadRenameStarted = true;
    eos::IFileMD::IFileMDWriteLocker fileLocker(file);
    view()->renameFile(file.get(), "file2");
    view()->updateContainerStore(root.get());
    fileRenamed = true;
    ::sleep(sleepSeconds);
  });
  std::chrono::time_point<std::chrono::steady_clock> start;
  std::chrono::time_point<std::chrono::steady_clock> stop;
  auto threadGetFile = std::thread([this, &start, &stop, &fileRenamed,
  &threadRenameStarted]() {
    while (!threadRenameStarted) {}

    while (!fileRenamed) {}

    ASSERT_THROW(view()->getFileReadLocked("/root/file1"), eos::MDException);
    start = std::chrono::steady_clock::now();
    auto file2 = view()->getFileReadLocked("/root/file2");
    stop = std::chrono::steady_clock::now();
    ASSERT_EQ("file2", file2->getUnderlyingPtr()->getName());
  });
  threadWriteLockingFile.join();
  threadGetFile.join();
  auto diff = std::chrono::duration_cast<std::chrono::seconds>
              (stop - start).count();
  ASSERT_EQ(sleepSeconds, diff);
}

TEST_F(FileSystemViewF, getFileOrContainerWriteLockedTwiceInSameThread)
{
  {
    view()->createContainer("/root/", true);
    view()->createFile("/root/file1");
  }
  auto cont = view()->getContainerWriteLocked("/root/");
  auto cont2 = view()->getContainerWriteLocked("/root/");
  auto cont3 = view()->getContainerReadLocked("/root/");
  auto file = view()->getFileWriteLocked("/root/file1");
  auto file2 = view()->getFileWriteLocked("/root/file1");
  auto file3 = view()->getFileReadLocked("/root/file1");
  ASSERT_EQ(cont->getUnderlyingPtr().get(), cont2->getUnderlyingPtr().get());
  ASSERT_EQ(cont->getUnderlyingPtr().get(), cont3->getUnderlyingPtr().get());
  ASSERT_EQ(file->getUnderlyingPtr().get(), file2->getUnderlyingPtr().get());
  ASSERT_EQ(file->getUnderlyingPtr().get(), file3->getUnderlyingPtr().get());
}


TEST(SetChangeList, BasicSanity)
{
  eos::IFsView::FileList contents;
  contents.set_deleted_key(0);
  contents.set_empty_key(0xffffffffffffffffll);
  eos::SetChangeList<eos::IFileMD::id_t> changeList;
  contents.insert(5);
  contents.insert(9);
  ASSERT_TRUE(eos::ns::testing::verifyContents(contents.begin(), contents.end(),
              std::set<eos::IFileMD::id_t> { 5, 9 }));
  changeList.push_back(10);
  changeList.erase(5);
  changeList.apply(contents);
  ASSERT_TRUE(eos::ns::testing::verifyContents(contents.begin(), contents.end(),
              std::set<eos::IFileMD::id_t> { 9, 10 }));
  changeList.clear();
  changeList.push_back(20);
  changeList.clear();
  changeList.apply(contents);
  ASSERT_TRUE(eos::ns::testing::verifyContents(contents.begin(), contents.end(),
              std::set<eos::IFileMD::id_t> { 9, 10 }));
  changeList.push_back(99);
  changeList.push_back(99);
  changeList.push_back(12);
  changeList.push_back(13);
  changeList.erase(12);
  changeList.apply(contents);
  ASSERT_TRUE(eos::ns::testing::verifyContents(contents.begin(), contents.end(),
              std::set<eos::IFileMD::id_t> { 9, 10, 13, 99 }));
  changeList.clear();
  changeList.push_back(15);
  changeList.push_back(16);
  changeList.erase(10);
  changeList.apply(contents);
  ASSERT_TRUE(eos::ns::testing::verifyContents(contents.begin(), contents.end(),
              std::set<eos::IFileMD::id_t> { 9, 13, 15, 16, 99 }));
  changeList.clear();
  changeList.push_back(17);
  changeList.push_back(17);
  changeList.erase(17);
  changeList.erase(17);
  changeList.apply(contents);
  ASSERT_EQ(changeList.size(), 4u);
  ASSERT_TRUE(eos::ns::testing::verifyContents(contents.begin(), contents.end(),
              std::set<eos::IFileMD::id_t> { 9, 13, 15, 16, 99 }));
}
