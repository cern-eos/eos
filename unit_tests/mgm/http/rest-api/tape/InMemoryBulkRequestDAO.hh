//------------------------------------------------------------------------------
//! @file InMemoryBulkRequestDAO.hh
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#ifndef EOS_INMEMORYBULKREQUESTDAO_HH
#define EOS_INMEMORYBULKREQUESTDAO_HH

#include "mgm/bulk-request/dao/IBulkRequestDAO.hh"
#include "mgm/bulk-request/dao/factories/AbstractDAOFactory.hh"
#include "mgm/bulk-request/File.hh"
#include <map>
#include <memory>

EOSBULKNAMESPACE_BEGIN

class InMemoryBulkRequestDAO : public IBulkRequestDAO
{
public:
  using StageStore = std::map<std::string, std::unique_ptr<StageBulkRequest>>;

  explicit InMemoryBulkRequestDAO(std::shared_ptr<StageStore> store = nullptr)
    : mStore(store ? std::move(store) : std::make_shared<StageStore>()) {}

  void addStageRequest(std::unique_ptr<StageBulkRequest> request)
  {
    const std::string id = request->getId();
    (*mStore)[id] = std::move(request);
  }

  void saveBulkRequest(const CancellationBulkRequest* bulkRequest) override
  {
    (void)bulkRequest;
  }

  void saveBulkRequest(const StageBulkRequest* bulkRequest) override
  {
    (*mStore)[bulkRequest->getId()] = cloneStageRequest(*bulkRequest);
  }

  std::unique_ptr<BulkRequest> getBulkRequest(const std::string& id,
      const BulkRequest::Type& type) override
  {
    if (type != BulkRequest::PREPARE_STAGE) {
      return nullptr;
    }

    const auto it = mStore->find(id);

    if (it == mStore->end()) {
      return nullptr;
    }

    return cloneStageRequest(*it->second);
  }

  uint64_t deleteBulkRequestNotQueriedFor(const BulkRequest::Type& type,
                                          const std::chrono::seconds& seconds) override
  {
    (void)type;
    (void)seconds;
    return 0;
  }

  bool exists(const std::string& bulkRequestId,
              const BulkRequest::Type& type) override
  {
    if (type != BulkRequest::PREPARE_STAGE) {
      return false;
    }

    return mStore->count(bulkRequestId) > 0;
  }

  void deleteBulkRequest(const BulkRequest* bulkRequest) override
  {
    mStore->erase(bulkRequest->getId());
  }

private:
  static std::unique_ptr<StageBulkRequest> cloneStageRequest(
    const StageBulkRequest& src)
  {
    auto clone = std::make_unique<StageBulkRequest>(src.getId(), src.getIssuerVid(),
                src.getCreationTime());

    for (const auto& file : *src.getFiles()) {
      auto clonedFile = std::make_unique<File>(file->getPath());

      if (file->getError()) {
        clonedFile->setError(*file->getError());
      }

      clone->addFile(std::move(clonedFile));
    }

    return clone;
  }

  std::shared_ptr<StageStore> mStore;
};

class InMemoryDAOFactory : public AbstractDAOFactory
{
public:
  explicit InMemoryDAOFactory(std::shared_ptr<InMemoryBulkRequestDAO::StageStore> store)
    : mStore(std::move(store)) {}

  std::unique_ptr<IBulkRequestDAO> getBulkRequestDAO() const override
  {
    return std::make_unique<InMemoryBulkRequestDAO>(mStore);
  }

private:
  std::shared_ptr<InMemoryBulkRequestDAO::StageStore> mStore;
};

EOSBULKNAMESPACE_END

#endif // EOS_INMEMORYBULKREQUESTDAO_HH
