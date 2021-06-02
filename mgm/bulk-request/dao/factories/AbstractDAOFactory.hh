//------------------------------------------------------------------------------
//! @file AbstractDAOFactory.hh
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

#ifndef EOS_ABSTRACTDAOFACTORY_HH
#define EOS_ABSTRACTDAOFACTORY_HH

#include "mgm/Namespace.hh"
#include <memory>
#include "mgm/bulk-request/dao/IBulkRequestDAO.hh"

EOSBULKNAMESPACE_BEGIN

/**
 * AbstractFactory of DAO (Data Access Object) linked to bulk-requests
 */
class AbstractDAOFactory {
public:
  /**
   * Returns a BulkRequestDAO object that will allow to access the persistency layer that will be used to store/access bulk requests metadata
   * @return a BulkRequestDAO object that will allow to access the persistency layer that will be used to store/access bulk requests metadata
   */
  virtual std::unique_ptr<IBulkRequestDAO> getBulkRequestDAO() const = 0;
};

EOSBULKNAMESPACE_END

#endif // EOS_ABSTRACTDAOFACTORY_HH
