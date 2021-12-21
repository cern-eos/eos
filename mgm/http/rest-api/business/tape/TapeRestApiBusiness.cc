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

#include "TapeRestApiBusiness.hh"
#include "mgm/http/rest-api/model/tape/common/FilesContainer.hh"
#include "mgm/bulk-request/utils/PrepareArgumentsWrapper.hh"
#include "mgm/bulk-request/interface/RealMgmFileSystemInterface.hh"
#include "mgm/bulk-request/prepare/manager/BulkRequestPrepareManager.hh"
#include "mgm/bulk-request/business/BulkRequestBusiness.hh"
#include "mgm/bulk-request/dao/factories/ProcDirectoryDAOFactory.hh"
#include "mgm/http/rest-api/exception/tape/TapeRestApiBusinessException.hh"

EOSMGMRESTNAMESPACE_BEGIN

std::shared_ptr<bulk::BulkRequest> TapeRestApiBusiness::createStageBulkRequest(const CreateStageBulkRequestModel* model, const common::VirtualIdentity * vid) {
  const FilesContainer & files = model->getFiles();
  bulk::PrepareArgumentsWrapper pargsWrapper("fake_id",Prep_STAGE,files.getOpaqueInfos(),files.getPaths());
  auto prepareManager = createBulkRequestPrepareManager();
  XrdOucErrInfo error;
  int prepareRetCode = prepareManager->prepare(*pargsWrapper.getPrepareArguments(),error,vid);
  if(prepareRetCode != SFS_DATA){
    throw TapeRestApiBusinessException(error.getErrText());
  }
  return prepareManager->getBulkRequest();
}

std::unique_ptr<bulk::BulkRequestPrepareManager> TapeRestApiBusiness::createBulkRequestPrepareManager() {
  std::unique_ptr<bulk::RealMgmFileSystemInterface> mgmOfs = std::make_unique<bulk::RealMgmFileSystemInterface>(gOFS);
  std::unique_ptr<bulk::BulkRequestPrepareManager> prepareManager = std::make_unique<bulk::BulkRequestPrepareManager>(std::move(mgmOfs));
  std::unique_ptr<bulk::AbstractDAOFactory> daoFactory(new bulk::ProcDirectoryDAOFactory(gOFS,*gOFS->mProcDirectoryBulkRequestTapeRestApiLocations));
  std::shared_ptr<bulk::BulkRequestBusiness> bulkRequestBusiness = std::make_shared<bulk::BulkRequestBusiness>(std::move(daoFactory));
  prepareManager->setBulkRequestBusiness(bulkRequestBusiness);
  return prepareManager;
}

EOSMGMRESTNAMESPACE_END