//------------------------------------------------------------------------------
// File: TreeSizeAccountingManagerTests.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "mgm/treesizeaccounting/TreeSizeAccountingManager.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeAccountingService.hh"
#include <cerrno>
#include <folly/futures/Future.h>
#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <string>

namespace {

class FakeContainerMDSvc : public eos::IContainerMDSvc {
public:
  void
  initialize() override
  {
  }
  void
  configure(const std::map<std::string, std::string>&) override
  {
  }
  void
  finalize() override
  {
  }

  folly::Future<eos::IContainerMDPtr>
  getContainerMDFut(eos::IContainerMD::id_t) override
  {
    return folly::makeFuture<eos::IContainerMDPtr>(eos::IContainerMDPtr{});
  }

  std::shared_ptr<eos::IContainerMD>
  getContainerMD(eos::IContainerMD::id_t) override
  {
    return nullptr;
  }

  std::shared_ptr<eos::IContainerMD>
  getContainerMD(eos::IContainerMD::id_t, uint64_t*) override
  {
    return nullptr;
  }

  bool
  dropCachedContainerMD(eos::ContainerIdentifier) override
  {
    return false;
  }

  std::shared_ptr<eos::IContainerMD>
  createContainer(eos::IContainerMD::id_t) override
  {
    return nullptr;
  }

  void
  updateStore(eos::IContainerMD*) override
  {
  }
  void
  removeContainer(eos::IContainerMD*) override
  {
  }

  uint64_t
  getNumContainers() override
  {
    return 0;
  }

  void
  addChangeListener(eos::IContainerMDChangeListener*) override
  {
  }
  void
  setQuotaStats(eos::IQuotaStats*) override
  {
  }
  void
  notifyListeners(eos::IContainerMD*, eos::IContainerMDChangeListener::Action) override
  {
  }

  std::shared_ptr<eos::IContainerMD>
  getLostFoundContainer(const std::string&) override
  {
    return nullptr;
  }

  std::shared_ptr<eos::IContainerMD>
  createInParent(const std::string&, eos::IContainerMD*) override
  {
    return nullptr;
  }

  void
  setFileMDService(eos::IFileMDSvc*) override
  {
  }
  void
  setContainerAccounting(eos::IFileMDChangeListener*) override
  {
  }

  eos::IContainerMD::id_t
  getFirstFreeId() override
  {
    return 0;
  }

  eos::CacheStatistics
  getCacheStatistics() override
  {
    return {};
  }

  void
  blacklistBelow(eos::ContainerIdentifier) override
  {
  }
};

class FakeFileMDSvc : public eos::IFileMDSvc {
public:
  void
  initialize() override
  {
  }
  void
  configure(const std::map<std::string, std::string>&) override
  {
  }
  void
  finalize() override
  {
  }

  folly::Future<eos::IFileMDPtr>
  getFileMDFut(eos::IFileMD::id_t) override
  {
    return folly::makeFuture<eos::IFileMDPtr>(eos::IFileMDPtr{});
  }

  std::shared_ptr<eos::IFileMD>
  getFileMD(eos::IFileMD::id_t) override
  {
    return nullptr;
  }

  std::shared_ptr<eos::IFileMD>
  getFileMD(eos::IFileMD::id_t, uint64_t*) override
  {
    return nullptr;
  }

  folly::Future<bool>
  hasFileMD(eos::FileIdentifier) override
  {
    return folly::makeFuture(false);
  }

  bool
  dropCachedFileMD(eos::FileIdentifier) override
  {
    return false;
  }

  std::shared_ptr<eos::IFileMD>
  createFile(eos::IFileMD::id_t) override
  {
    return nullptr;
  }

  void
  updateStore(eos::IFileMD*) override
  {
  }
  void
  removeFile(eos::IFileMD*) override
  {
  }

  uint64_t
  getNumFiles() override
  {
    return 0;
  }

  void
  addChangeListener(eos::IFileMDChangeListener*) override
  {
  }
  void
  notifyListeners(eos::IFileMDChangeListener::Event*) override
  {
  }
  void
  setQuotaStats(eos::IQuotaStats*) override
  {
  }
  void
  setContMDService(eos::IContainerMDSvc*) override
  {
  }
  void
  visit(eos::IFileVisitor*) override
  {
  }

  eos::IFileMD::id_t
  getFirstFreeId() override
  {
    return 0;
  }

  eos::CacheStatistics
  getCacheStatistics() override
  {
    return {};
  }

  void
  blacklistBelow(eos::FileIdentifier) override
  {
  }
};

class FakeTreeSizeAccountingService : public eos::ITreeSizeAccountingService {
public:
  std::unique_ptr<eos::TreeSizeJournalCaptureScope>
  StartTreeSizeJournalCapture() override
  {
    return mCaptureController.StartCapture();
  }

  eos::TreeSizeAccountingFenceStats
  AcquireTreeSizeAccountingFence(const eos::TreeSizeAccountingFenceRequest&) override
  {
    eos::TreeSizeAccountingFenceStats stats;
    stats.acquired = true;
    return stats;
  }

  eos::TreeSizeAccountingFenceStats
  ReleaseTreeSizeAccountingFence(eos::TreeSizeAccountingFenceReleaseMode) override
  {
    return {};
  }

private:
  eos::TreeSizeJournalCaptureController mCaptureController;
};

eos::mgm::TreeSizeRecomputeRequest
MakeRequest(eos::IContainerMD::id_t root_id, const std::string& root_specification)
{
  eos::mgm::TreeSizeRecomputeRequest request;
  request.rootId = root_id;
  request.rootSpecification = root_specification;
  return request;
}

} // namespace

TEST(TreeSizeAccountingManager, RejectsSecondSubmitWhileQueued)
{
  FakeContainerMDSvc container_svc;
  FakeFileMDSvc file_svc;
  FakeTreeSizeAccountingService accounting_service;
  eos::mgm::TreeSizeAccountingManager manager;

  const auto first = manager.SubmitRecompute(MakeRequest(42, "/eos/test"), container_svc,
                                             file_svc, &accounting_service);

  ASSERT_EQ(0, first.retc);

  const auto second = manager.SubmitRecompute(
      MakeRequest(43, "/eos/other"), container_svc, file_svc, &accounting_service);

  EXPECT_EQ(EAGAIN, second.retc);
  EXPECT_NE(std::string::npos, second.message.find("already active"));

  const auto status = manager.GetStatus();
  EXPECT_EQ(eos::mgm::TreeSizeRecomputeJobState::Queued, status.state);
  EXPECT_EQ(42ull, status.request.rootId);
  EXPECT_EQ("/eos/test", status.request.rootSpecification);

  const auto status_output = manager.FormatStatus();
  EXPECT_NE(std::string::npos, status_output.find("state=queued"));
  EXPECT_NE(std::string::npos, status_output.find("retc=0"));
  EXPECT_NE(std::string::npos, status_output.find("available=0"));
}
