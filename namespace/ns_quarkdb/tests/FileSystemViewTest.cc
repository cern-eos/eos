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
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/ns_quarkdb/tests/TestUtils.hh"
#include "namespace/utils/TestHelpers.hh"
#include <gtest/gtest.h>
#include <cstdlib>
#include <cstdint>
#include <ctime>
#include <sstream>
#include <unistd.h>

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
  ASSERT_EQ(eos::keyFilesystemFiles(50), "fsview:50:files");
  ASSERT_EQ(eos::keyFilesystemFiles(123), "fsview:123:files");
  ASSERT_EQ(eos::keyFilesystemUnlinked(10), "fsview:10:unlinked");
  ASSERT_EQ(eos::keyFilesystemUnlinked(999), "fsview:999:unlinked");
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
    view()->unlinkFile(file.get());
    view()->removeFile(file.get());
  }

  // Remove all containers
  view()->removeContainer("/test/", true);
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
  for(size_t i = 1; i < 200; i++) {
    std::shared_ptr<eos::IFileMD> file = view()->createFile(SSTR("/test/" << i));

    // Even files go to fs #1, odd go to #2
    if(i % 2 == 0) {
      file->addLocation(1);
    }
    else {
      file->addLocation(2);
    }

    view()->updateFileStore(file.get());
  }

  mdFlusher()->synchronize();
  for(size_t i = 0; i < 1000; i++) {
    eos::IFileMD::id_t randomPick;
    ASSERT_TRUE(fsview()->getApproximatelyRandomFileInFs(1, randomPick));
    ASSERT_TRUE(randomPick % 2 == 0);

    if(i < 10) {
      std::cout << "Random file in fs #1: " << randomPick << std::endl;
    }

    randomPick;
    ASSERT_TRUE(fsview()->getApproximatelyRandomFileInFs(2, randomPick));
    ASSERT_TRUE(randomPick % 2 == 1);

    if(i < 10) {
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
    set.sadd_async(elem, &ah);
  }

  ASSERT_TRUE(ah.Wait());
  std::unordered_set<eos::IFileMD::id_t> result_set;
  auto iter = std::shared_ptr<eos::ICollectionIterator<eos::IFileMD::id_t>>
              (new eos::QdbFileIterator(qcl(), key));

  for (; (iter && iter->valid()); iter->next()) {
    result_set.insert(iter->getElement());
  }

  ASSERT_EQ(input_set.size(), result_set.size());

  for (auto elem : input_set) {
    ASSERT_TRUE(result_set.find(elem) != result_set.end());
  }

  qcl().del(key);
}
