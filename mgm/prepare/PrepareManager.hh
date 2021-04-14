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

#include "mgm/Namespace.hh"
#include <XrdSfs/XrdSfsInterface.hh>
#include <string>
#include "common/Logging.hh"
#include <list>

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
  PrepareManager();
  /**
   * Allows to prepare the file
   * @param pargs Xrootd prepare arguments
   * @param error Xrootd error information to fill if there are any errors
   * @param client the client who issued the prepare
   * @returns the status code of the issued prepare request
   */
  int prepare(XrdSfsPrep &pargs, XrdOucErrInfo & error, const XrdSecEntity* client);
private:

  /**
   * Utility method to convert the prepare options to string options
   * @param opts the prepare options to convert to string
   * @return the prepare options in the string format
   */
  std::string prepareOptsToString(const int opts) const;

  /**
   * Generate a stage prepare request id
   * @param requestId the variable where the requestId will be put after generation
   */
  void generatePrepareStageRequestId(XrdOucString & requestId);

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

  bool mIsStagePrepare = false;
  bool mGeneratedStageRequestId = false;
  const std::string mEpname="prepare";
};

EOSMGMNAMESPACE_END

#endif // EOS_PREPAREMANAGER_HH
