//------------------------------------------------------------------------------
//! @file BulkRequestBusiness.hh
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

#ifndef EOS_BULKREQUESTBUSINESS_HH
#define EOS_BULKREQUESTBUSINESS_HH

#include "mgm/Namespace.hh"
#include "mgm/bulk-request/BulkRequest.hh"
#include "mgm/bulk-request/dao/factories/AbstractDAOFactory.hh"
#include <common/Logging.hh>
#include <memory>

EOSBULKNAMESPACE_BEGIN

/**
 * This class contains the business logic linked to the bulk requests
 * It basically allows to get a bulk request, persist it, ... from the DAOs returned
 * by the AbstractDAOFactory given when constructing this object.
 */
class BulkRequestBusiness : public eos::common::LogId {
public:
  /**
   * Constructor of the BulkRequestBusiness object
   * The daoFactory that is passed in parameter will be used by the different methods
   * of this class so that it instanciate the correct Data Access Object. Depending on the
   * implementation of the AbstractDAOFactory, the underlying persistency layer of the DAO will change
   * @param daoFactory the factory of DAO
   */
  BulkRequestBusiness(std::unique_ptr<AbstractDAOFactory> && daoFactory);
  /**
   * Allows to persist the bulk-request
   * @param req the bulk-request to persist
   */
  void saveBulkRequest(const std::shared_ptr<BulkRequest> req);

  /**
   * Returns the bulk-request persisted
   * @param bulkRequestId the id of the bulk-request
   * @param type the type of the bulk-request
   * @return the bulk-request associated to the id and the type passed in parameter
   */
  std::unique_ptr<BulkRequest> getBulkRequest(const std::string & bulkRequestId, const BulkRequest::Type & type);

  /**
   * Creates or update the persisted bulk-request attributes
   * @param req the bulk-request to update its attributes
   * @param attributes the attributes to add/update
   */
  void addOrUpdateAttributes(const std::shared_ptr<BulkRequest> req, const std::map<std::string,std::string> & attributes);

  /**
   * Returns true if the bulk-request corresponding to the id and the type
   * passed in parameters exists, false otherwise
   * @param bulkRequestId the id of the bulk-request to check
   * @param type the type of the bulk-request to test
   * @throws PersistencyException if an error happens during the check
   */
  bool exists(const std::string & bulkRequestId, const BulkRequest::Type & type);

private:
  std::unique_ptr<AbstractDAOFactory> mDaoFactory;
};

EOSBULKNAMESPACE_END

#endif // EOS_BULKREQUESTBUSINESS_HH
