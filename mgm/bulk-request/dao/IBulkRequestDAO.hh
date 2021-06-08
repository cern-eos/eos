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
};

EOSBULKNAMESPACE_END

#endif // EOS_IBULKREQUESTDAO_HH
