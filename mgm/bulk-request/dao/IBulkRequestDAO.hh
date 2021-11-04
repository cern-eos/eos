//------------------------------------------------------------------------------
//! @file IBulkRequestPersist.hh
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

#ifndef EOS_IBULKREQUESTDAO_HH
#define EOS_IBULKREQUESTDAO_HH

#include "mgm/Namespace.hh"
#include "mgm/bulk-request/prepare/StageBulkRequest.hh"
#include <memory>
#include <chrono>
#include <map>

EOSBULKNAMESPACE_BEGIN

/**
 * Interface to the bulk request Data Access Object
 * It allows to access the persistency layer of the bulk requests
 */
class IBulkRequestDAO {
public:
  /**
   * This method allows to persist a StageBulkRequest
   * @param bulkRequest the bulk request to save
   */
  virtual void saveBulkRequest(const std::shared_ptr<BulkRequest> bulkRequest) = 0;

  /**
   * Get the bulk-request from the persistence
   * @param id the id of the bulk-request
   * @param type the type of the bulk-request
   * @return the bulk-request if it exists, nullptr otherwise
   */
  virtual std::unique_ptr<BulkRequest> getBulkRequest(const std::string & id, const BulkRequest::Type & type) = 0;

  /**
   * Delete all the bulk-request of a certain type that were not accessed for  hours
   * @param type the bulk-request type to look for
   * @param seconds the number of seconds after which the bulk-requests can be deleted if they were not queried
   * @returns the number of deleted bulk-request
   */
  virtual uint64_t deleteBulkRequestNotQueriedFor(const BulkRequest::Type & type, const std::chrono::seconds & seconds) = 0;

  /**
   * Adds or updates the attributes of a persisted bulk-request
   * @param bulkRequest the bulk-request to update
   * @param attributes the attributes to add/update to the bulk-request
   * @throws PersistencyException if the bulk-request does not exist
   */
  virtual void addOrUpdateAttributes(const std::shared_ptr<BulkRequest> bulkRequest, const std::map<std::string,std::string> & attributes) = 0;
};

EOSBULKNAMESPACE_END

#endif // EOS_IBULKREQUESTDAO_HH
