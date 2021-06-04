//------------------------------------------------------------------------------
//! @file BulkRequestPrepareManager.hh
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

#ifndef EOS_BULKREQUESTPREPAREMANAGER_HH
#define EOS_BULKREQUESTPREPAREMANAGER_HH

#include "mgm/Namespace.hh"
#include "mgm/bulk-request/prepare/PrepareManager.hh"
#include "mgm/bulk-request/business/BulkRequestBusiness.hh"

EOSBULKNAMESPACE_BEGIN

/**
 * This class allows to implement the template method pattern.
 * This pattern allows to add the bulk-requets management without changing the algorithm of the PrepareManager::prepare() method
 */
class BulkRequestPrepareManager : public PrepareManager {
public:
  BulkRequestPrepareManager(IMgmFileSystemInterface & mgmFsInterface);
  /**
   * Allows to enable the bulk-request management linked to the prepare logic
   * @param bulkRequestBusiness the class that allows to manage operations linked to bulk-requests
   */
  void setBulkRequestBusiness(std::shared_ptr<BulkRequestBusiness> bulkRequestBusiness);
  std::shared_ptr<BulkRequest> getBulkRequest() const;
protected:
  /**
   * This overriding method will instanciate a stage bulk request and will
   * affect its reqid to the variable passed in parameter
   * @param reqid the request id that will be returned to the xrootd client
   */
  void initializeStagePrepareRequest(XrdOucString &reqid) override;
  /**
   * Adds the path passed in parameter to this instance's bulk-request
   * @param path the path to add to the bulk-request
   */
  void addPathToBulkRequest(const std::string & path) override;
  /**
   * Persists the managed bulk-request
   */
  void saveBulkRequest() override;
private:
  //The bulk-request business allowing the persistence of the bulk-request
  std::shared_ptr<BulkRequestBusiness> mBulkRequestBusiness;
  //The bulk request that possibly got created depending on the prepare command triggered
  std::shared_ptr<BulkRequest> mBulkRequest;
};

EOSBULKNAMESPACE_END

#endif // EOS_BULKREQUESTPREPAREMANAGER_HH
