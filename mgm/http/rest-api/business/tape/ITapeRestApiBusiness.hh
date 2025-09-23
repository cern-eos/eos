// ----------------------------------------------------------------------
// File: ITapeRestApiBusiness.hh
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

#ifndef EOS_ITAPERESTAPIBUSINESS_HH
#define EOS_ITAPERESTAPIBUSINESS_HH

#include "mgm/Namespace.hh"
#include <memory>
#include "mgm/bulk-request/BulkRequest.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include "mgm/http/rest-api/model/tape/stage/CreateStageBulkRequestModel.hh"
#include "mgm/http/rest-api/model/tape/stage/PathsModel.hh"
#include "mgm/http/rest-api/model/tape/stage/GetStageBulkRequestResponseModel.hh"
#include "common/VirtualIdentity.hh"

EOSMGMRESTNAMESPACE_BEGIN

class ITapeRestApiBusiness
{
public:
  /**
   * Creates and persists a stage bulk-request from the model passed in parameter
   * @param model the object from which the bulk-request
   * @param vid the issuer vid
   * @param authz token authorizationn (if vid is invalid)
   * @return the created bulk-request
   */
  virtual std::shared_ptr<bulk::BulkRequest> createStageBulkRequest(
    const CreateStageBulkRequestModel* model,
    const common::VirtualIdentity* vid,
    const std::string& authz) = 0;
  /**
   * Cancels a subset of files belonging to a previously staged STAGE bulk-request identified by requestId
   * @param requestId the requestId of the request from which the subset of files will be cancelled
   * @param model the subset of files belonging to a request
   * @param vid the vid of the issuer of the cancellation
   * @param authz token authorizationn (if vid is invalid)
   */
  virtual void cancelStageBulkRequest(const std::string& requestId,
                                      const PathsModel* model,
                                      const common::VirtualIdentity* vid,
                                      const std::string& authz) = 0;
  /**
   * Returns a previously submitted STAGE request identified by requestId
   * @param requestId the id of the previously submitted stage bulk-request
   * @param vid the vid of the issuer of the get
   * @param authz token authorizationn (if vid is invalid)
   * @return the model containing the bulk-request informations
   * @throws ObjectNotFoundException if the request has not been found
   */
  virtual std::shared_ptr<GetStageBulkRequestResponseModel> getStageBulkRequest(
    const std::string& requestId,
    const common::VirtualIdentity* vid,
    const std::string& authz = "") = 0;
  /**
   * Deletes a previously submitted STAGE bulk-request from the persistency
   *
   * It is expected that this method cancels the ongoing STAGE requests
   * @param requestId the id of the previously STAGE bulk-request to delete
   * @param vid the issuer of the deletion
   * @param authz token authorizationn (if vid is invalid)
   * @throws ObjectNotFoundException if the bulk-request has not been found
   */
  virtual void deleteStageBulkRequest(const std::string& requestId,
                                      const common::VirtualIdentity* vid,
                                      const std::string& authz) = 0;
  /**
   * Returns informations about the files contained in the model object
   * @param model the object containing the files to get the information for
   * @param vid the vid of the issuer
   * @param authz token authorizationn (if vid is invalid)
   * @return a query prepare response object containing informations about the files
   */
  virtual std::shared_ptr<bulk::QueryPrepareResponse> getFileInfo(
    const PathsModel* model,
    const common::VirtualIdentity* vid,
    const std::string& authz) = 0;
  /**
   * Releases a set of files, in our case (EOS+CTA), this is equivalent to trigger
   * an eviction on the files provided by the user
   * @param model the object containing the files to release
   * @param vid the vid of the issuer
   * @param authz token authorizationn (if vid is invalid)
   */
  virtual void releasePaths(const PathsModel* model,
                            const common::VirtualIdentity* vid,
                            const std::string& authz) = 0;
  virtual ~ITapeRestApiBusiness() {};
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_ITAPERESTAPIBUSINESS_HH
