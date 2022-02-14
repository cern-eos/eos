//------------------------------------------------------------------------------
//! @file BulkRequestFactory.hh
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

#ifndef EOS_BULKREQUESTFACTORY_HH
#define EOS_BULKREQUESTFACTORY_HH

#include "mgm/Namespace.hh"
#include "mgm/bulk-request/prepare/StageBulkRequest.hh"
#include "mgm/bulk-request/prepare/EvictBulkRequest.hh"
#include "mgm/bulk-request/prepare/CancellationBulkRequest.hh"

EOSBULKNAMESPACE_BEGIN

/**
 * Factory of bulk request
 */
class BulkRequestFactory {
public:
  /**
   * Returns a new StageBulkRequest with a unique identifier
   * @param issuerVid the vid of the person who creates this bulk-request
   * @return a new StageBulkRequest
   */
  static std::unique_ptr<StageBulkRequest> createStageBulkRequest(const common::VirtualIdentity & issuerVid);

  /**
   * Returns a new cancel bulk-request
   * @param id the id of the cancel bulk-request (normally is equal to a previously submitted stage bulk-request id)
   * @return a new CancelBulkRequest
   */
  static std::unique_ptr<CancellationBulkRequest> createCancelBulkRequest(const std::string & id);

  /**
   * Creates a new StageBulkRequest with the requestId and the issuerVid of this bulk-request
   * @return
   */
  static std::unique_ptr<StageBulkRequest> createStageBulkRequest(const std::string & requestId, const common::VirtualIdentity & issuerVid);

  /**
   * Creates a new StageBulkRequest with the requestId, issuerVid and the creation time of this bulk-request
   * @return a new StageBulkRequest
   */
  static std::unique_ptr<StageBulkRequest> createStageBulkRequest(const std::string & requestId, const common::VirtualIdentity & issuerVid, const time_t & creationTime);

};

EOSBULKNAMESPACE_END
#endif // EOS_BULKREQUESTFACTORY_HH
