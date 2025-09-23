// ----------------------------------------------------------------------
// File: TapeRestApiBusiness.hh
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

#ifndef EOS_TAPERESTAPIBUSINESS_HH
#define EOS_TAPERESTAPIBUSINESS_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/business/tape/ITapeRestApiBusiness.hh"
#include "mgm/bulk-request/prepare/manager/BulkRequestPrepareManager.hh"
#include "mgm/bulk-request/prepare/StageBulkRequest.hh"

EOSMGMRESTNAMESPACE_BEGIN

class TapeRestApiBusiness : public ITapeRestApiBusiness
{
public:
  std::shared_ptr<bulk::BulkRequest> createStageBulkRequest(
    const CreateStageBulkRequestModel* model,
    const common::VirtualIdentity* vid,
    const std::string& authz) override;
  void cancelStageBulkRequest(const std::string& requestId,
                              const PathsModel* model, const common::VirtualIdentity* vid, const std::string& authz) override;
  std::shared_ptr<GetStageBulkRequestResponseModel> getStageBulkRequest(
    const std::string& requestId, const common::VirtualIdentity* vid, const std::string& authz) override;
  void deleteStageBulkRequest(const std::string& requestId,
                              const common::VirtualIdentity* vid,
                              const std::string& authz) override;
  std::shared_ptr<bulk::QueryPrepareResponse> getFileInfo(
    const PathsModel* model, const common::VirtualIdentity* vid, const std::string& authz) override;
  void releasePaths(const PathsModel* model,
                    const common::VirtualIdentity* vid,
                    const std::string& authz) override;
protected:
  std::unique_ptr<bulk::BulkRequestPrepareManager>
  createBulkRequestPrepareManager();
  std::unique_ptr<bulk::PrepareManager> createPrepareManager();
  std::shared_ptr<bulk::BulkRequestBusiness> createBulkRequestBusiness();
  /**
   * Checks whether the issuer of a request is allowed to access the stage bulk-request
   * for modification, consultation, deletion...
   * @param bulkRequest the stage bulk-request to check the access
   * @param vid the vid of the user who issued the request against the bulkRequest
   * @param action what action the user issued (cancel, delete, get)
   */
  void checkIssuerAuthorizedToAccessStageBulkRequest(const bulk::StageBulkRequest*
      bulkRequest, const common::VirtualIdentity* vid, const std::string& action);
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESTAPIBUSINESS_HH
