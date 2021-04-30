//------------------------------------------------------------------------------
//! @file ProcDirectoryBulkRequestDAO.hh
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

#ifndef EOS_PROCDIRECTORYBULKREQUESTDAO_HH
#define EOS_PROCDIRECTORYBULKREQUESTDAO_HH

#include "mgm/Namespace.hh"
#include "mgm/bulk-request/dao/IBulkRequestDAO.hh"
#include <common/Logging.hh>
#include "namespace/interface/IView.hh"

EOSMGMNAMESPACE_BEGIN

/**
 * This class is the bulk request persistency layer using the eos proc directory
 *
 * The bulk request persistence will be ensured by creating and listing a directory in a the /eos/.../proc/bulkrequest.
 */
class ProcDirectoryBulkRequestDAO : public IBulkRequestDAO, eos::common::LogId {
public:
  ProcDirectoryBulkRequestDAO(const XrdOucString & bulkRequestProcDirectoryPath,eos::IView * namespaceView);
  /**
   * Save the Stage bulk request by creating a directory in the /eos/.../proc/ directory and creating one file
   * per path. The paths of the files will be modified in the format like the one in the EOS recycle-bin
   * @param bulkRequest the StageBulkRequest to save
   */
  void saveBulkRequest(const std::shared_ptr<StageBulkRequest> bulkRequest) override;
private:
  //bulk-request directory path on the /eos/.../proc directory
  XrdOucString mBulkRequestDirectoryPath;
  //Interface to the namespace to allow the creation of files and directories
  IView * mNamespaceView;
  /**
   * Creates a directory to store the bulk-request files within it
   * @param bulkRequest the bulkRequest to get the id from
   */
  void createBulkRequestDirectory(const std::shared_ptr<BulkRequest> bulkRequest);
  /**
   * Generate the bulk-request directory path within the /eos/.../proc/ directory
   * It is generated according to the id of the bulk-request
   * @param bulkRequest the bulk-request to generate the path from
   * @return the string containing the path of the directory used to store the bulk-request
   */
  XrdOucString generateBulkRequestProcPath(const std::shared_ptr<BulkRequest> bulkRequest);

};

EOSMGMNAMESPACE_END
#endif // EOS_PROCDIRECTORYBULKREQUESTDAO_HH
