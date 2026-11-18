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
//! @brief File metadata service tests
//------------------------------------------------------------------------------

#include "namespace/MDLocking.hh"
#include "namespace/locking/BulkNsObjectLocker.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/ns_quarkdb/tests/TestUtils.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include <atomic>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include <vector>

class ContainerMDSvcF : public eos::ns::testing::NsTestsFixture {};

namespace {
//------------------------------------------------------------------------------
// Run the jobs in parallel, once per round, and count the rounds after which
// isStale is true
//------------------------------------------------------------------------------
uint64_t
countStaleRounds(const std::vector<std::function<void(uint64_t)>>& jobs,
                 const std::function<bool()>& isStale, uint64_t rounds)
{
  std::atomic<uint64_t> round{0};
  std::atomic<uint64_t> finished{0};
  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;

  for (const auto& job : jobs) {
    threads.emplace_back([&]() {
      uint64_t seen = 0;

      while (true) {
        uint64_t current;

        while ((current = round.load()) == seen) {
          if (stop.load()) {
            return;
          }

          std::this_thread::yield();
        }

        seen = current;
        job(current);
        finished.fetch_add(1);
      }
    });
  }

  uint64_t stale = 0;

  for (uint64_t r = 1; r <= rounds; ++r) {
    finished.store(0);
    round.store(r);

    while (finished.load() != jobs.size()) {
      std::this_thread::yield();
    }

    if (isStale()) {
      ++stale;
    }
  }

  stop.store(true);

  for (auto& thread : threads) {
    thread.join();
  }

  return stale;
}
} // namespace

//------------------------------------------------------------------------------
// A store without the MD lock of the container must not replace the record
// written by a parallel rename with the older state of the container
//------------------------------------------------------------------------------
TEST_F(ContainerMDSvcF, UnlockedStoreDoesNotUndoRename)
{
  std::shared_ptr<eos::IContainerMD> parent = view()->createContainer("/race/", true);
  std::shared_ptr<eos::IContainerMD> dir = view()->createContainer("/race/n0", true);
  const eos::IContainerMD::id_t id = dir->getId();
  std::vector<std::function<void(uint64_t)>> jobs;
  jobs.push_back([&](uint64_t) { containerSvc()->updateStore(dir.get()); });
  jobs.push_back([&](uint64_t round) {
    eos::MDLocking::BulkMDWriteLock locker;
    locker.add(parent.get());
    locker.add(dir.get());
    auto locks = locker.lockAll();
    view()->renameContainer(dir.get(), "n" + std::to_string(round));
    view()->updateContainerStore(parent.get());
  });

  // Stores of other directories delay the ones waiting for the same mutex
  for (int i = 0, found = 0; found < 2; ++i) {
    auto other = view()->createContainer("/other/d" + std::to_string(i), true);

    if (other->getId() % 64 == id % 64) {
      ++found;
      jobs.push_back([this, other](uint64_t) {
        for (int n = 0; n < 3; ++n) {
          eos::MDLocking::ContainerWriteLock lock(other.get());
          other->setMTimeNow();
          view()->updateContainerStore(other.get());
        }
      });
    }
  }

  const uint64_t stale = countStaleRounds(
      jobs,
      [&]() {
        mdFlusher()->synchronize();
        return eos::MetadataFetcher::getContainerFromId(qcl(),
                                                        eos::ContainerIdentifier(id))
                   .get()
                   .name() != dir->getName();
      },
      2000);
  EXPECT_EQ(stale, 0u);
}

//------------------------------------------------------------------------------
// The same for a file
//------------------------------------------------------------------------------
TEST_F(ContainerMDSvcF, UnlockedFileStoreDoesNotUndoRename)
{
  std::shared_ptr<eos::IContainerMD> parent = view()->createContainer("/race/", true);
  std::shared_ptr<eos::IFileMD> file = view()->createFile("/race/n0", 0, 0);
  const eos::IFileMD::id_t id = file->getId();
  std::vector<std::function<void(uint64_t)>> jobs;
  jobs.push_back([&](uint64_t) { fileSvc()->updateStore(file.get()); });
  jobs.push_back([&](uint64_t round) {
    eos::MDLocking::BulkMDWriteLock locker;
    locker.add(parent.get());
    locker.add(file.get());
    auto locks = locker.lockAll();
    view()->renameFile(file.get(), "n" + std::to_string(round));
    view()->updateFileStore(file.get());
  });
  view()->createContainer("/other/", true);

  for (int i = 0, found = 0; found < 2; ++i) {
    auto other = view()->createFile("/other/f" + std::to_string(i), 0, 0);

    if (other->getId() % 64 == id % 64) {
      ++found;
      jobs.push_back([this, other](uint64_t) {
        for (int n = 0; n < 3; ++n) {
          eos::MDLocking::FileWriteLock lock(other.get());
          other->setMTimeNow();
          view()->updateFileStore(other.get());
        }
      });
    }
  }

  const uint64_t stale = countStaleRounds(
      jobs,
      [&]() {
        mdFlusher()->synchronize();
        return eos::MetadataFetcher::getFileFromId(qcl(), eos::FileIdentifier(id))
                   .get()
                   .name() != file->getName();
      },
      2000);
  EXPECT_EQ(stale, 0u);
}

TEST_F(ContainerMDSvcF, BasicSanity)
{
  std::shared_ptr<eos::IContainerMD> container1 = containerSvc()->getContainerMD(1);
  std::shared_ptr<eos::IContainerMD> container2 = containerSvc()->createContainer(0);
  std::shared_ptr<eos::IContainerMD> container3 = containerSvc()->createContainer(0);
  std::shared_ptr<eos::IContainerMD> container4 = containerSvc()->createContainer(0);
  std::shared_ptr<eos::IContainerMD> container5 = containerSvc()->createContainer(0);

  eos::IContainerMD::id_t id = container1->getId();
  container1->setName("root");
  container1->setParentId(container1->getId());
  container2->setName("subContLevel1-1");
  container3->setName("subContLevel1-2");
  container4->setName("subContLevel2-1");
  container5->setName("subContLevel2-2");
  container5->setCUid(17);
  container5->setCGid(17);
  container5->setMode(0750);
  ASSERT_TRUE(container5->access(17, 12, X_OK | R_OK | W_OK));
  ASSERT_TRUE(container5->access(17, 12, X_OK | R_OK));
  ASSERT_TRUE(!container5->access(12, 17, X_OK | R_OK | W_OK));
  ASSERT_TRUE(!container5->access(12, 17, X_OK | W_OK));
  ASSERT_TRUE(container5->access(12, 17, X_OK | R_OK));
  ASSERT_TRUE(!container5->access(12, 12, X_OK | R_OK));
  container1->addContainer(container2.get());
  container1->addContainer(container3.get());
  container3->addContainer(container4.get());
  container3->addContainer(container5.get());
  ASSERT_EQ((size_t)2, container1->getNumContainers());
  ASSERT_EQ((size_t)0, container1->getNumFiles());
  containerSvc()->updateStore(container1.get());
  containerSvc()->updateStore(container2.get());
  containerSvc()->updateStore(container3.get());
  containerSvc()->updateStore(container4.get());
  containerSvc()->updateStore(container5.get());
  mdFlusher()->synchronize();

  ASSERT_EQ((size_t)5, containerSvc()->getNumContainers());
  container3->removeContainer("subContLevel2-2");
  containerSvc()->removeContainer(container5.get());

  mdFlusher()->synchronize();
  ASSERT_EQ((size_t)1, container3->getNumContainers());
  ASSERT_EQ((size_t)4, containerSvc()->getNumContainers());
  std::shared_ptr<eos::IContainerMD> container6 = containerSvc()->createContainer(0);
  container6->setName("subContLevel2-3");
  container3->addContainer(container6.get());
  containerSvc()->updateStore(container6.get());
  eos::IContainerMD::id_t idAttr = container4->getId();
  container4->setAttribute("test1", "test1");
  container4->setAttribute("test1", "test11");
  container4->setAttribute("test2", "test2");
  container4->setAttribute("test3", "test3");
  containerSvc()->updateStore(container4.get());
  ASSERT_EQ((size_t)3, container4->numAttributes());
  ASSERT_TRUE(container4->getAttribute("test1") == "test11");
  ASSERT_TRUE(container4->getAttribute("test3") == "test3");
  ASSERT_THROW(container4->getAttribute("test15"), eos::MDException);

  shut_down_everything();

  std::shared_ptr<eos::IContainerMD> cont1 = containerSvc()->getContainerMD(id);
  ASSERT_EQ(cont1->getName(), "root");
  std::shared_ptr<eos::IContainerMD> cont2 = cont1->findContainer("subContLevel1-1");
  ASSERT_NE(cont2, nullptr);
  ASSERT_EQ(cont2->getName(), "subContLevel1-1");
  cont2 = cont1->findContainer("subContLevel1-2");
  ASSERT_NE(cont2, nullptr);
  ASSERT_EQ(cont2->getName(), "subContLevel1-2");
  cont1 = cont2->findContainer("subContLevel2-1");
  ASSERT_NE(cont1, nullptr);
  ASSERT_EQ(cont1->getName(), "subContLevel2-1");
  cont1 = cont2->findContainer("subContLevel2-2");
  ASSERT_EQ(cont1, nullptr);
  cont1 = cont2->findContainer("subContLevel2-3");
  ASSERT_NE(cont1, nullptr);
  ASSERT_EQ(cont1->getName(), "subContLevel2-3");
  std::shared_ptr<eos::IContainerMD> contAttrs = containerSvc()->getContainerMD(idAttr);
  ASSERT_EQ(contAttrs->numAttributes(), 3);
  ASSERT_EQ(contAttrs->getAttribute("test1"), "test11");
  ASSERT_EQ(contAttrs->getAttribute("test3"), "test3");
  ASSERT_THROW(contAttrs->getAttribute("test15"), eos::MDException);
  // Clean up all the containers
  container1 = containerSvc()->getContainerMD(1);
  container2 = containerSvc()->getContainerMD(2);
  container3 = containerSvc()->getContainerMD(3);
  container4 = containerSvc()->getContainerMD(4);

  container3->removeContainer(container6->getName());
  container3->removeContainer(container4->getName());
  container1->removeContainer(container3->getName());
  container1->removeContainer(container2->getName());
  containerSvc()->removeContainer(container6.get());
  containerSvc()->removeContainer(container4.get());
  containerSvc()->removeContainer(container3.get());
  containerSvc()->removeContainer(container2.get());
  containerSvc()->removeContainer(container1.get());
  mdFlusher()->synchronize();
  ASSERT_EQ((uint64_t)0, containerSvc()->getNumContainers());
}

TEST_F(ContainerMDSvcF, getContainerMDWhenContIsLockedShouldNotLock) {
  auto cont = view()->createContainer("/root/");
  auto id = cont->getId();
  eos::MDLocking::ContainerWriteLock contLock(cont.get());

  std::thread t([this,id](){
    // Here, we just check that the getContainerMD(id) does not lock
    // despite having the container being locked
    auto cont = containerSvc()->getContainerMD(id);
  });
  t.join();
}
