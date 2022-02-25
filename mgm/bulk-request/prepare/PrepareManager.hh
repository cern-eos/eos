//------------------------------------------------------------------------------
//! @file PrepareManager.hh
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
#ifndef EOS_PREPAREMANAGER_HH
#define EOS_PREPAREMANAGER_HH

#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include <XrdSfs/XrdSfsInterface.hh>
#include <list>
#include <mgm/bulk-request/business/BulkRequestBusiness.hh>
#include <mgm/bulk-request/prepare/StageBulkRequest.hh>
#include <string>
#include "mgm/XrdMgmOfs.hh"
#include "mgm/bulk-request/interface/IMgmFileSystemInterface.hh"

EOSMGMNAMESPACE_BEGIN

/**
 * This class manages all the operations linked to the preparation of a file:
 * - queue it for retrieval on the tape system
 * - query the preparation
 */
class PrepareManager : public eos::common::LogId {
public:
  /**
   * Constructor
   * @param pargs Xrootd prepare arguments
   * @param error Xrootd error information to fill if there are any errors
   * @param client the client who issued the prepare
   */
  PrepareManager(IMgmFileSystemInterface & mgmFsInterface);

  /**
   * Allows to enable the bulk-request management linked to the prepare logic
   * @param bulkRequestBusiness the class that allows to manage operations linked to bulk-requests
   */
  void setBulkRequestBusiness(std::shared_ptr<BulkRequestBusiness> bulkRequestBusiness);

  /**
   * Allows to launch a prepare logic on the files passed in parameter
   * @param pargs Xrootd prepare arguments (containing the path of the files)
   * @param error Xrootd error information to fill if there are any errors
   * @param client the client who issued the prepare
   * @returns the status code of the issued prepare request
   */
  int prepare(XrdSfsPrep &pargs, XrdOucErrInfo & error, const XrdSecEntity* client);

  /**
   * Returns the pointer to the bulk-request associated to the operation that occured or nullptr if the
   * prepare operation did not create a BulkRequest
   * @return the bulk-request pointer or nullptr
   */
  std::shared_ptr<BulkRequest> getBulkRequest() const;
private:

  /**
   * Utility method to convert the prepare options to string options
   * @param opts the prepare options to convert to string
   * @return the prepare options in the string format
   */
  std::string prepareOptsToString(const int opts) const;

  /**
   * Returns the Prepare actions to perform from the options given by Xrootd (XrdSfsPrep.opts)
   * @param pargsOpts the prepare options given by Xrootd (XrdSfsPrep.opts)
   * @return the Prepare actions to perform from the options given by Xrootd (XrdSfsPrep.opts)
   */
  const int getPrepareActionsFromOpts(const int pargsOpts) const;

  /**
   * @return true if this prepare request is a stage one, false otherwise
   */
  const bool isStagePrepare() const;

  /**
   * Triggers the prepare workflow to all the pathsToPrepare passed in parameter
   * @param pathsToPrepare the paths of the files on which we want to trigger a prepare workflow
   * @param cmd the command to run in the Workflow engine
   * @param event the event to trigger (sync::prepare, sync::evict_prepare...)
   * @param reqid the requestId of this prepare request
   * @param error The error that will be returned to the client if an error happens
   * @param vid the identity of the person who issued the prepare request
   */
  void triggerPrepareWorkflow(const std::list<std::pair<char**, char**>> & pathsToPrepare, const std::string & cmd, const std::string &event, const XrdOucString & reqid, XrdOucErrInfo & error, const eos::common::VirtualIdentity& vid);

  /**
   * Will call the business layer to persist the bulk request
   */
  void saveBulkRequest();

  /**
   * Perform the prepare logic
   * @param pargs Xrootd prepare arguments
   * @param error Xrootd error information to fill if there are any errors
   * @param client the client who issued the prepare
   * @returns the status code of the issued prepare request
   */
  int doPrepare(XrdSfsPrep &pargs, XrdOucErrInfo & error, const XrdSecEntity* client);
  const std::string mEpname="prepare";
  //The bulk request that possibly got created depending on the prepare command triggered
  std::shared_ptr<BulkRequest> mBulkRequest;
  //Business logic to manage bulkRequest actions (persistency for example)
  std::shared_ptr<BulkRequestBusiness> mBulkRequestBusiness;

  IMgmFileSystemInterface & mMgmFsInterface;
};

EOSMGMNAMESPACE_END

#endif // EOS_PREPAREMANAGER_HH
