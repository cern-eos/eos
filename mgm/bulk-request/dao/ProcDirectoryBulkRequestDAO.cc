//------------------------------------------------------------------------------
//! @file ProcDirectoryBulkRequestDAO.hh
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

#include "ProcDirectoryBulkRequestDAO.hh"
#include <fcntl.h>

EOSMGMNAMESPACE_BEGIN

ProcDirectoryBulkRequestDAO::ProcDirectoryBulkRequestDAO(const XrdOucString & bulkRequestProcDirectoryPath,eos::IView * namespaceView):mBulkRequestDirectoryPath(bulkRequestProcDirectoryPath),mNamespaceView(namespaceView){

}

void ProcDirectoryBulkRequestDAO::saveBulkRequest(const std::shared_ptr<StageBulkRequest> bulkRequest) {
  createBulkRequestDirectory(bulkRequest);
}

void ProcDirectoryBulkRequestDAO::createBulkRequestDirectory(const std::shared_ptr<BulkRequest> bulkRequest) {
  XrdOucString directoryBulkRequest = generateBulkRequestProcPath(bulkRequest);
  eos_info("msg=\"Persistence of the bulk request %s : creating the directory %s\"", directoryBulkRequest.c_str());
  std::shared_ptr<IContainerMD> bulkReqDirectory = mNamespaceView->createContainer(directoryBulkRequest.c_str());
  bulkReqDirectory->setAttribute("bulk_request_type",BulkRequest::bulkRequestTypeToString(bulkRequest->getType()));
  bulkReqDirectory->setMode(S_IFDIR | S_IRWXU);
  bulkReqDirectory->setCUid(2); // bulk-request directory is owned by daemon
  bulkReqDirectory->setCGid(2);
  mNamespaceView->updateContainerStore(bulkReqDirectory.get());
}

XrdOucString ProcDirectoryBulkRequestDAO::generateBulkRequestProcPath(const std::shared_ptr<BulkRequest> bulkRequest) {
  XrdOucString directoryBulkRequest = mBulkRequestDirectoryPath + "/" + bulkRequest->getId().c_str();
  return directoryBulkRequest;
}

EOSMGMNAMESPACE_END