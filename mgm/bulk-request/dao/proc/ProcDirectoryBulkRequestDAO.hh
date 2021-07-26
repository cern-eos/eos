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
#include "mgm/bulk-request/dao/proc/ProcDirBulkRequestFile.hh"

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

  /**
   * Get the bulk-request from the /eos/.../proc directory
   * @param id the id of the bulk-request
   * @param type the type of bulk-request
   * @return the bulk-request associated to the id and the type, nullptr if it does not exist
   */
  std::unique_ptr<BulkRequest> getBulkRequest(const std::string & id, const BulkRequest::Type & type) override;

private:
  //Interface to the EOS filesystem to allow the creation of files and directories
  XrdMgmOfs * mFileSystem;
  ProcDirectoryBulkRequestLocations & mProcDirectoryBulkRequestLocations;
  eos::common::VirtualIdentity mVid;

  const char * ERROR_MSG_ATTR_NAME = "error_msg";

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
   * Generate the bulk-request directory path within the /eos/.../proc/ directory
   * It is generated according to the id of the bulk-request
   * @param bulkRequestId the id of the bulk-request to generate the proc directory path
   * @param type the type of the bulk-request to generate the proc directory path
   * @return the string containing the path of the directory used to store the bulk-request
   */
  std::string generateBulkRequestProcPath(const std::string & bulkRequestId, const BulkRequest::Type & type);

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

  /**
   * Persists the error of the file by adding an extended attribute "bulk_req_error"
   * @param persistedFilePath the path of the file in the /proc/ directory
   * @param file the file that may have an error
   */
  void persistErrorIfAny(const std::string & persistedFilePath,const bulk::File & file);

  /**
   * Returns true if the directory path passed in parameter exists, false otherwise
   * @param dirPath the path of the directory to check its existence
   * @return true if the directory path passed in parameter exists, false otherwise
   */
  bool existsAndIsDirectory(const std::string & dirPath);

  /**
   * Fills the bulk-request with the information provided in the /proc/ directory
   * @param bulkRequestProcPath the path of the bulk-request in the proc directory
   * @param bulkRequest the bulk-request to fill
   */
  void fillBulkRequest(const std::string & bulkRequestProcPath, BulkRequest & bulkRequest);

  /**
   * Fills the directoryContent map passed in parameter. The key is the full path of the bulk-request directory
   * the value is the file names that are located in the bulk-request directory.
   * Reminder: the files that are in the directory of the bulk-request are the fileIds of the files that were submitted with the bulk-request (or
   * transformed paths for the files that were submitted did not exist)
   * @param bulkRequestProcPath the path of the bulk-request in the proc directory
   * @param directoryContent the map that will be filled with the content of the bulk-request proc directory
   */
  void fillBulkRequestDirectoryContentMap(const std::string & bulkRequestProcPath, std::map<std::string, std::set<std::string>> & directoryContent);

  /**
   * Fetch the error from the extended attributes of the file passed in parameter
   * and assign this potential error to it.
   * This method will basically call a `attr ls` on the full path of this file and look for the error attribute
   * defined in the variable ERROR_MSG_ATTR_NAME
   * @param file the file to fetch and assign the potential error to.
   * @param xattrs the extended attributes of the file where the potential error might be
   */
  void fillFileErrorIfAny(ProcDirBulkRequestFile & file, eos::IContainerMD::XAttrMap & xattrs);

  /**
   * Fetch the extended attributes of the file passed in parameter
   * @param file the file to fetch the extended attribute
   * @param xattrs the extended attributes map that will be filled by this method
   */
  void fetchFileExtendedAttributes(const ProcDirBulkRequestFile & file, eos::IContainerMD::XAttrMap & xattrs);

  /**
   * Asynchronously fetch the file metadata by using the eosFileService->getFileMDFut() method. The filesWithFuture map will
   * be filled by this method
   * @param file the file to asynchronously fetch the metadata
   * @param filesWithFuture the map to include the file and its future
   */
  void initiateFileMDFetch(const ProcDirBulkRequestFile & file,std::map<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>> & filesWithFuture);

  /**
   * Wait for the futures associated to the files inserted in the filesWithFuture map (got filled by the initiateFileMDFetch() method).
   * Once all the futures have returned
   * @param filesWithFuture the map containing the files and the associated future to get the metadata from it
   * @param bulkRequest
   */
  void getFilesPathAndAddToBulkRequest(std::map<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>> & filesWithFuture, BulkRequest & bulkRequest);
};

EOSBULKNAMESPACE_END

#endif // EOS_PROCDIRECTORYBULKREQUESTDAO_HH
