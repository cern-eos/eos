//------------------------------------------------------------------------------
//! @file ProcDirectoryDAOFactory.hh
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

#ifndef EOS_PROCDIRECTORYDAOFACTORY_HH
#define EOS_PROCDIRECTORYDAOFACTORY_HH
#include "mgm/Namespace.hh"
#include "mgm/bulk-request/dao/factories/AbstractDAOFactory.hh"
#include "mgm/XrdMgmOfs.hh"

EOSMGMNAMESPACE_BEGIN

/**
 * Factory of Data Access Object that will rely on
 * the /eos/.../proc directory
 */
class ProcDirectoryDAOFactory : public AbstractDAOFactory{
public:
  /**
   * Factory of ProcDirectoryDAO objects
   * @param mgmBulkRequestDirectoryPath
   * @param fileSystem that will allow to interact with the /proc/ directory
   */
  ProcDirectoryDAOFactory(XrdMgmOfs * fileSystem);
  /**
   * Returns the ProcDirectory bulk request DAO object to allow the persistence/access of the
   * bulk-requests metada via the /eos/.../proc directory
   * @return the BulkRequestDAO obje
   */
  std::unique_ptr<IBulkRequestDAO> getBulkRequestDAO() const;
private:
  XrdMgmOfs * mFileSystem;
};

EOSMGMNAMESPACE_END

#endif // EOS_PROCDIRECTORYDAOFACTORY_HH
