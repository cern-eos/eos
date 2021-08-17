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
#include <mgm/bulk-request/prepare/EvictBulkRequest.hh>

EOSBULKNAMESPACE_BEGIN

/**
 * Factory of bulk request
 */
class BulkRequestFactory {
public:
  /**
   * Returns a new StageBulkRequest with a unique identifier
   * @return a new StageBulkRequest
   */
  static StageBulkRequest * createStageBulkRequest();

  /**
   * Returns a new EvictBulkRequest with a unique identifier
   * @return a new EvictBulkRequest
   */
  static EvictBulkRequest * createEvictBulkRequest();

  /**
   * Instanciate a bulk-request according to the type passed in parameter
   * @param id the id of the bulk-request to create
   * @param type the type of the bulk-request to create
   * @return a pointer to the bulk-request created
   */
  static BulkRequest * createBulkRequest(const std::string & id, const BulkRequest::Type & type);

};

EOSBULKNAMESPACE_END
#endif // EOS_BULKREQUESTFACTORY_HH
