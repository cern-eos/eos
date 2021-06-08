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
#include <mgm/XrdMgmOfs.hh>
#include "mgm/bulk-request/dao/proc/ProcDirectoryBulkRequestLocations.hh"

EOSBULKNAMESPACE_BEGIN

/**
 * This class is the bulk request persistency layer using the eos proc directory
 *
 * The bulk request persistence will be ensured by creating and listing a directory in a the /eos/.../proc/bulkrequest.
 */
class ProcDirectoryBulkRequestDAO : public IBulkRequestDAO, public eos::common::LogId {
public:
  ProcDirectoryBulkRequestDAO(XrdMgmOfs * fileSystem, ProcDirectoryBulkRequestLocations& mBulkRequestDirSchema);
  /**
   * Save the bulk request by creating a directory in the /eos/.../proc/ directory and creating one file
   * per file in the bulk-request:
   * - If a file in the bulk-request exists, the file will be named according to the fileId
   * - If a file in the bulk-request does not exist, the file will be named according to the path provided in the format like the one in the EOS
   * recycle-bin (each '/' will be replaced by "#:#")
   * @param bulkRequest the BulkRequest to save
   */
  void saveBulkRequest(const std::shared_ptr<BulkRequest> bulkRequest) override;
private:
  //Interface to the EOS filesystem to allow the creation of files and directories
  XrdMgmOfs * mFileSystem;
  ProcDirectoryBulkRequestLocations & mProcDirectoryBulkRequestLocations;
  eos::common::VirtualIdentity mVid;
  /**
   * Creates a directory to store the bulk-request files within it
   * @param bulkRequest the bulkRequest to get the id from
   * @param bulkReqProcPath the directory in /proc/ where the files contained in the bulk-request will be saved
   */
  void createBulkRequestDirectory(const std::shared_ptr<BulkRequest> bulkRequest, const std::string & bulkReqProcPath);
  /**
   * Generate the bulk-request directory path within the /eos/.../proc/ directory
   * It is generated according to the id of the bulk-request
   * @param bulkRequest the bulk-request to generate the path from
   * @return the string containing the path of the directory used to store the bulk-request
   */
  std::string generateBulkRequestProcPath(const std::shared_ptr<BulkRequest> bulkRequest);

  /**
   * Insert the files contained in the bulk request into the directory created by createBulkRequestDirectory()
   * @param bulkRequest the bulk-request containing the files to insert in the directory
   * @param bulkReqProcPath the he directory in /proc/ where the files contained in the bulk-request will be saved
   */
  void insertBulkRequestFilesToBulkRequestDirectory(const std::shared_ptr<BulkRequest> bulkRequest, const std::string & bulkReqProcPath);

  /**
   * Performs the cleaning of the bulk-request directory if an exception happens during the persistency of the bulk-request
   * @param bulkRequest the bulk-request that failed to be persisted
   * @param bulkReqProcPath the directory where this bulk-request should have been stored
   */
  void cleanAfterExceptionHappenedDuringBulkRequestSave(const std::shared_ptr<BulkRequest> bulkRequest, const std::string & bulkReqProcPath);
};

EOSBULKNAMESPACE_END

#endif // EOS_PROCDIRECTORYBULKREQUESTDAO_HH
