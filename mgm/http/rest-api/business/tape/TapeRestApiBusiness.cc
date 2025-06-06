// ----------------------------------------------------------------------
// File: TapeRestApiBusiness.cc
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#include "mgm/http/rest-api/exception/ObjectNotFoundException.hh"
#include "TapeRestApiBusiness.hh"
#include "mgm/http/rest-api/model/tape/common/FilesContainer.hh"
#include "mgm/bulk-request/utils/PrepareArgumentsWrapper.hh"
#include "mgm/bulk-request/interface/RealMgmFileSystemInterface.hh"
#include "mgm/bulk-request/prepare/manager/BulkRequestPrepareManager.hh"
#include "mgm/bulk-request/business/BulkRequestBusiness.hh"
#include "mgm/bulk-request/dao/factories/ProcDirectoryDAOFactory.hh"
#include "mgm/http/rest-api/exception/tape/TapeRestApiBusinessException.hh"
#include "mgm/http/rest-api/exception/tape/FileDoesNotBelongToBulkRequestException.hh"
#include "mgm/http/rest-api/exception/ForbiddenException.hh"
#include "mgm/bulk-request/exception/PersistencyException.hh"
#include "mgm/Stat.hh"

EOSMGMRESTNAMESPACE_BEGIN

std::shared_ptr<bulk::BulkRequest> TapeRestApiBusiness::createStageBulkRequest(
  const CreateStageBulkRequestModel* model, const common::VirtualIdentity* vid)
{
  const FilesContainer& files = model->getFiles();
  EXEC_TIMING_BEGIN("TapeRestApiBusiness::createStageBulkRequest");
  gOFS->MgmStats.Add("TapeRestApiBusiness::createStageBulkRequest", vid->uid,
                     vid->gid, 1);
  bulk::PrepareArgumentsWrapper pargsWrapper(
    "fake_id", Prep_STAGE, files.getPaths(), files.getOpaqueInfos());
  auto prepareManager = createBulkRequestPrepareManager();
  XrdOucErrInfo error;
  int prepareRetCode = prepareManager->prepare(
                         *pargsWrapper.getPrepareArguments(), error, vid);

  if (prepareRetCode != SFS_DATA) {
    throw TapeRestApiBusinessException(error.getErrText());
  }

  EXEC_TIMING_END("TapeRestApiBusiness::createStageBulkRequest");
  return prepareManager->getBulkRequest();
}

void TapeRestApiBusiness::cancelStageBulkRequest(const std::string& requestId,
    const PathsModel* model, const common::VirtualIdentity* vid)
{
  EXEC_TIMING_BEGIN("TapeRestApiBusiness::cancelStageBulkRequest");
  gOFS->MgmStats.Add("TapeRestApiBusiness::cancelStageBulkRequest", vid->uid,
                     vid->gid, 1);
  std::shared_ptr<bulk::BulkRequestBusiness> bulkRequestBusiness =
    createBulkRequestBusiness();
  auto bulkRequest = bulkRequestBusiness->getStageBulkRequest(requestId);

  if (bulkRequest == nullptr) {
    std::stringstream ss;
    ss << "Unable to find the STAGE bulk-request ID = " << requestId;
    throw ObjectNotFoundException(ss.str());
  }

  //First, check if the issuer of the cancellation is root, or is the person who submitted the stage request
  //checkIssuerAuthorizedToAccessStageBulkRequest(bulkRequest.get(), vid,"cancel");
  //Create the prepare arguments, we will only cancel the files that were given by the user
  const FilesContainer& filesFromClient = model->getFiles();
  auto filesFromBulkRequestContainer = bulkRequest->getFilesMap();
  bulk::PrepareArgumentsWrapper pargsWrapper(requestId, Prep_CANCEL);

  for (const auto& fileFromClient : filesFromClient.getPaths()) {
    const auto& fileFromBulkRequestKeyVal = filesFromBulkRequestContainer->find(
        fileFromClient);

    if (fileFromBulkRequestKeyVal != filesFromBulkRequestContainer->end()) {
      auto& fileFromBulkRequest = fileFromBulkRequestKeyVal->second;
      auto& error = fileFromBulkRequest->getError();

      if (!error) {
        //We only cancel the files that do not have any error
        pargsWrapper.addFile(fileFromClient, "");
      }
    } else {
      std::stringstream ss;
      ss << "The file " << fileFromClient << " does not belong to the STAGE request "
         << bulkRequest->getId() << ". No modification has been made to this request.";
      throw FileDoesNotBelongToBulkRequestException(ss.str());
    }
  }

  //Do the cancellation if there are files to cancel
  if (pargsWrapper.getNbFiles() != 0) {
    auto pm = createBulkRequestPrepareManager();
    XrdOucErrInfo error;
    int retCancellation = pm->prepare(*pargsWrapper.getPrepareArguments(), error,
                                      vid);

    if (retCancellation != SFS_OK) {
      std::stringstream ss;
      ss << "Unable to cancel the files provided. errMsg=\""
         << error.getErrText() << "\"";
      throw TapeRestApiBusinessException(ss.str());
    }
  }

  EXEC_TIMING_END("TapeRestApiBusiness::cancelStageBulkRequest");
}

std::shared_ptr<GetStageBulkRequestResponseModel>
TapeRestApiBusiness::getStageBulkRequest(const std::string& requestId,
    const common::VirtualIdentity* vid)
{
  EXEC_TIMING_BEGIN("TapeRestApiBusiness::getStageBulkRequest");
  gOFS->MgmStats.Add("TapeRestApiBusiness::getStageBulkRequest", vid->uid,
                     vid->gid, 1);
  std::shared_ptr<GetStageBulkRequestResponseModel> ret =
    std::make_shared<GetStageBulkRequestResponseModel>();
  auto bulkRequestBusiness = createBulkRequestBusiness();
  std::unique_ptr<bulk::StageBulkRequest> bulkRequest;

  try {
    bulkRequest = bulkRequestBusiness->getStageBulkRequest(requestId);

    if (!bulkRequest) {
      std::stringstream ss;
      ss << "Unable to find the STAGE bulk-request ID =" << requestId;
      throw ObjectNotFoundException(ss.str());
    }
  } catch (bulk::PersistencyException& ex) {
    throw TapeRestApiBusinessException(ex.what());
  }

  //checkIssuerAuthorizedToAccessStageBulkRequest(bulkRequest.get(), vid,"get");
  //Set bulk-request related attributes
  ret->setCreationTime(bulkRequest->getCreationTime());
  ret->setId(bulkRequest->getId());
  //Instanciate prepare manager to get the tape, disk residency and an eventual error (set by CTA)
  bulk::PrepareArgumentsWrapper pargsWrapper(requestId, Prep_QUERY);
  auto files = bulkRequest->getFiles();

  for (auto& file : *files) {
    pargsWrapper.addFile(file->getPath(), "");
  }

  auto pm = createPrepareManager();
  XrdOucErrInfo error;
  auto queryPrepareResult = pm->queryPrepare(*pargsWrapper.getPrepareArguments(),
                            error, vid);

  if (!queryPrepareResult->hasQueryPrepareFinished()) {
    std::stringstream ss;
    ss << "Unable to get information about the files belonging to the request " <<
       requestId << ". errMsg=\"" << error.getErrText() << "\"";
    throw TapeRestApiBusinessException(ss.str());
  }

  for (const auto& queryPrepareResponse :
       queryPrepareResult->getResponse()->responses) {
    const auto& filesFromBulkRequest = bulkRequest->getFilesMap();
    auto fileFromBulkRequestItor = filesFromBulkRequest->find(
                                     queryPrepareResponse.path);

    if (fileFromBulkRequestItor != filesFromBulkRequest->end()) {
      auto& fileFromBulkRequest = fileFromBulkRequestItor->second;
      std::unique_ptr<GetStageBulkRequestResponseModel::File> item =
        std::make_unique<GetStageBulkRequestResponseModel::File>();
      item->mPath = queryPrepareResponse.path;

      if (fileFromBulkRequest->getError()) {
        item->mError = *fileFromBulkRequest->getError();
      } else if (!queryPrepareResponse.error_text.empty()) {
        //Error comes from CTA, so we need to update the state of the file to ERROR
        item->mError = queryPrepareResponse.error_text;
      } else if (!queryPrepareResponse.is_online && !queryPrepareResponse.is_reqid_present) {
        //If there is no request for the file, an error should be returned
        item->mError = "File not requested with request ID " + requestId;
      } else {
        item->mError = "";
      }

      item->mOnDisk = queryPrepareResponse.is_online;
      ret->addFile(std::move(item));
    }
  }

  EXEC_TIMING_END("TapeRestApiBusiness::getStageBulkRequest");
  return ret;
}

void TapeRestApiBusiness::deleteStageBulkRequest(const std::string& requestId,
    const common::VirtualIdentity* vid)
{
  EXEC_TIMING_BEGIN("TapeRestApiBusiness::deleteStageBulkRequest");
  gOFS->MgmStats.Add("TapeRestApiBusiness::deleteStageBulkRequest", vid->uid,
                     vid->gid, 1);
  //Get the prepare request from the persistency
  std::shared_ptr<bulk::BulkRequestBusiness> bulkRequestBusiness =
    createBulkRequestBusiness();
  auto bulkRequest = bulkRequestBusiness->getStageBulkRequest(requestId);

  if (bulkRequest == nullptr) {
    std::stringstream ss;
    ss << "Unable to find the STAGE bulk-request ID = " << requestId;
    throw ObjectNotFoundException(ss.str());
  }

  //checkIssuerAuthorizedToAccessStageBulkRequest(bulkRequest.get(), vid,"delete");
  //Create the prepare arguments, we will cancel all the files from this bulk-request
  auto filesFromBulkRequest = bulkRequest->getFiles();
  bulk::PrepareArgumentsWrapper pargsWrapper(requestId, Prep_CANCEL);

  for (auto& fileFromBulkRequest : *filesFromBulkRequest) {
    pargsWrapper.addFile(fileFromBulkRequest->getPath(), "");
  }

  auto pm = createPrepareManager();
  XrdOucErrInfo error;
  int retCancellation = pm->prepare(*pargsWrapper.getPrepareArguments(), error,
                                    vid);

  if (retCancellation != SFS_OK) {
    std::stringstream ss;
    ss << "Unable to cancel the files provided. errMsg=\"" << error.getErrText() <<
       "\"";
    throw TapeRestApiBusinessException(ss.str());
  }

  //Now that the request got cancelled, let's delete it from the persistency
  try {
    bulkRequestBusiness->deleteBulkRequest(bulkRequest.get());
  } catch (bulk::PersistencyException& ex) {
    throw TapeRestApiBusinessException(ex.what());
  }

  EXEC_TIMING_END("TapeRestApiBusiness::deleteStageBulkRequest");
}

std::shared_ptr<bulk::QueryPrepareResponse> TapeRestApiBusiness::getFileInfo(
  const PathsModel* model, const common::VirtualIdentity* vid)
{
  EXEC_TIMING_BEGIN("TapeRestApiBusiness::getFileInfo");
  gOFS->MgmStats.Add("TapeRestApiBusiness::getFileInfo", vid->uid, vid->gid, 1);
  auto& filesContainer = model->getFiles();
  bulk::PrepareArgumentsWrapper pargsWrapper("fake_id", Prep_QUERY);

  for (const auto& pathFromUser : filesContainer.getPaths()) {
    pargsWrapper.addFile(pathFromUser, "");
  }

  auto pm = createPrepareManager();
  XrdOucErrInfo error;
  auto queryPrepareResult = pm->queryPrepare(*pargsWrapper.getPrepareArguments(),
                            error, vid);

  if (!queryPrepareResult->hasQueryPrepareFinished()) {
    std::stringstream ss;
    ss << "Unable to get information about the files provided. errMsg=\"" <<
       error.getErrText() << "\"";
    throw TapeRestApiBusinessException(ss.str());
  }

  EXEC_TIMING_END("TapeRestApiBusiness::getFileInfo");
  return queryPrepareResult->getResponse();
}

void TapeRestApiBusiness::releasePaths(const PathsModel* model,
                                       const common::VirtualIdentity* vid)
{
  EXEC_TIMING_BEGIN("TapeRestApiBusiness::releasePaths");
  gOFS->MgmStats.Add("TapeRestApiBusiness::releasePaths", vid->uid, vid->gid, 1);
  auto& filesContainer = model->getFiles();
  bulk::PrepareArgumentsWrapper pargsWrapper("fake_id", Prep_EVICT,
      filesContainer.getPaths(),
      filesContainer.getOpaqueInfos());
  auto pm = createBulkRequestPrepareManager();
  XrdOucErrInfo error;
  int retEvict = pm->prepare(*pargsWrapper.getPrepareArguments(), error, vid);

  if (retEvict != SFS_OK) {
    std::stringstream ss;
    ss << "Unable to release the files provided. errMsg=\"" << error.getErrText() <<
       "\"";
    throw TapeRestApiBusinessException(ss.str());
  }

  EXEC_TIMING_END("TapeRestApiBusiness::releasePaths");
}

std::unique_ptr<bulk::BulkRequestPrepareManager>
TapeRestApiBusiness::createBulkRequestPrepareManager()
{
  std::unique_ptr<bulk::RealMgmFileSystemInterface> mgmOfs =
    std::make_unique<bulk::RealMgmFileSystemInterface>(gOFS);
  std::unique_ptr<bulk::BulkRequestPrepareManager> prepareManager =
    std::make_unique<bulk::BulkRequestPrepareManager>(std::move(mgmOfs));
  std::shared_ptr<bulk::BulkRequestBusiness> bulkRequestBusiness =
    createBulkRequestBusiness();
  prepareManager->setBulkRequestBusiness(bulkRequestBusiness);
  return prepareManager;
}

std::unique_ptr<bulk::PrepareManager>
TapeRestApiBusiness::createPrepareManager()
{
  std::unique_ptr<bulk::RealMgmFileSystemInterface> mgmOfs =
    std::make_unique<bulk::RealMgmFileSystemInterface>(gOFS);
  std::unique_ptr<bulk::PrepareManager> prepareManager =
    std::make_unique<bulk::PrepareManager>(std::move(mgmOfs));
  return prepareManager;
}

std::shared_ptr<bulk::BulkRequestBusiness>
TapeRestApiBusiness::createBulkRequestBusiness()
{
  std::unique_ptr<bulk::AbstractDAOFactory> daoFactory(new
      bulk::ProcDirectoryDAOFactory(gOFS,
                                    *gOFS->mProcDirectoryBulkRequestTapeRestApiLocations));
  return std::make_shared<bulk::BulkRequestBusiness>(std::move(daoFactory));
}

void TapeRestApiBusiness::checkIssuerAuthorizedToAccessStageBulkRequest(
  const bulk::StageBulkRequest* bulkRequest, const common::VirtualIdentity* vid,
  const std::string& action)
{
  //First, check if the issuer of the cancellation is root, or is the person who submitted the stage request
  if (vid->uid != 0 && vid->uid != bulkRequest->getIssuerVid().uid) {
    throw ForbiddenException("You are not allowed to " + action +
                             " this bulk-request");
  }
}
EOSMGMRESTNAMESPACE_END
